"""
maq_tools.py

module provides functions needed for the MAQ flow.
"""


##############################
#      IMPORT PACKAGES       #
##############################


import json, shutil, os, pymsteams
from datetime import datetime

# sql connections #
import pandas as pd
from sqlalchemy import create_engine, text

# custom modules
import maqc

# prefect modules #
from prefect import task, get_run_logger



##############################
#      UPDATE FUNCTIONS      #
##############################


@task(name="Resetting json")
def rejson():
    """
    Resets the 'archive' key to default and empties the files list for each 
    prefix.
    """
    logger = get_run_logger()   # instantiate logger
    logger.info("Resetting files field in json")  # log process

    try:
        with open(maqc.JSON_FILE, "r+") as f:
            accepted = json.load(f)  # load json

            for prefix in accepted['process']:
                # reset 'files' key to an empty list
                accepted['process'][prefix]['files'] = []

            # update json
            f.seek(0)
            json.dump(accepted, f, indent=4)
            f.truncate()

    except Exception as e:
        logger.error(f"Problem with resetting json: {e}")


@task(name="Updating last modified date")
def last_updated(time, prefix=None):
    """
    Updates the most recent modification date (key: 'lastMod') in the json file 
    'acceptable_files' and the most recent modification date for each file 
    prefix (the values in key: 'process')

    Args:
        time   (datetime):  Date/time of when the main flow started for this run

    """
    logger = get_run_logger()   # instatiate logger

    try:
        # convert datetime object to string
        time = time.strftime("%Y-%m-%d %H:%M:%S")

        with open(maqc.JSON_FILE, 'r+') as f:
            accepted = json.load(f)

            # updates json's last_updated field
            if prefix == None:
                prefix = "json file"
                accepted['last_update'] = time
            else:   # updates prefix last_up field
                accepted['process'][prefix]['last_up'] = time

            # update json
            f.seek(0)
            json.dump(accepted, f, indent=4)
            f.truncate()

        # log success
        logger.info(f"Updated last modified date for {prefix}: {time}")

    except Exception as e:
        logger.error(f"Problem with updating json: {e}")
        raise



##############################
#         ARCHIVING          #
##############################


@task(name="Archiving files")
def archive_files(prefix_dict):
    """
    Archives files in fileList, moving them into their corresponding date 
    folders named in the format  of YYYY-MM.

    Args:
        prefix_dict (dict):     A dictionary of the prefix in acceptable_files
    """
    logger = get_run_logger()   # instantiate logger

    try:
        for file in prefix_dict['files']:    # iterate over files
            logger.info(f"Archiving {file}")

            mod = (file.split('_')[-1]).replace('.csv', '')

            # account for intraday
            if prefix_dict['name'] == "Green Plains Intraday" \
                    or prefix_dict['name'] == "GPG Intraday":
                mod = file.split('_')[1]

            # directory name created from the file's date
            mod_time = datetime.strptime(mod, '%Y%m%d')
            dir_name = datetime.strftime(mod_time, '%Y-%m')
            
            # move the file from staging to archive
            shutil.move(f"{maqc.STAGE}/{file}", 
                            f"{maqc.ARCHIVE}/{dir_name}/{file}")

            # check if file exists
            assert os.path.exists(f"{maqc.ARCHIVE}/{dir_name}/{file}"
                                ), f"Failed to archive {file}"

        # log success
        logger.info(f"{prefix_dict['name']} archiving complete.")

    except AssertionError as e:
        logger.error(f"Failed to archive {file}: {e}")

    except Exception as e:
        logger.error(f"Problem with archiving {prefix_dict['name']}: {e}")
        raise



##############################
#      FILE PROCESSING       #
##############################


@task(name="Preparing SQL Upsert Query")
def prep_sql(file, prefix, update_time, extra_col=False):
    """
    Prepares an SQL upsert query for upsertting files. The function adds
    columns that are needed but do not exist in the csv file. prep_sql()   
    also replaces placeholders for the file_path in the sql file and creates
    a temporary csv file for the sql server.

    Args:
        file        (str):      String of file name
        prefix      (str):      String of the file prefix, which is 
                                also the name of the sql file
        update_time (datetime): Datetime object of this session's 
                                timestamp of when the main flow was ran
        extra_col       (bool): True if an extra column needs to be     
                                inserted, False otherwise. 

    Returns:
        sql                  (str):     SQL merge/insert/delete query
    """
    logger = get_run_logger()  # instantiate logger

    try:
        # read csv file
        df = pd.read_csv(f"{maqc.STAGE}/{file}", float_precision='round_trip')

        # add col if it does not exist for money_line
        if extra_col and 'INTEREST' not in df.columns:
            df['INTEREST'] = 0.0

        # add fileDt column to store the file date
        df['fileDt'] = file.replace('.csv', '')[-8:]
        df['last_update'] = datetime.strftime(
            update_time, "%Y-%m-%d %H:%M:%S.%f")[:-3]  # millisec

        # make a temp file with the new columns to be processed
        file_name = f"{file.replace('.csv','')}_temp"

        df.to_csv(f"{maqc.BULK}/{file_name}", index=False,
                  header=False, float_format="%.5f")

        with open(f"{maqc.MAQ_SQL}/{prefix}.sql", "r") as f:
       
            sql = f.read()
            sql = sql.replace('file_path', f"{maqc.BULK}/{file_name}")

        logger.info(f"SQL statement for {file} prepped successfully")

        # return the query to be executed
        return sql

    except Exception as e:
        logger.error(f"Problem with preparing the SQL query: {e}")
        raise


@task(name="Upserting File")
def upsert_file(file, conn_url, sql):
    """
    Connects to the SQL Server database and performs an upsert for the given 
    file. Furthermore, creates a csv file containing the action performed with 
    the data (update/insert/delete/none)

    Args:
        file            (str):      String of file name
        conn_url        (str):      String of connection to sql server
        sql             (str):      String of the sql query to be executed
    """
    logger = get_run_logger()   # Instantiate logger
    logger.info(f"Upserting {file}")

    try:
        engine = create_engine(conn_url)  # create connection
        conn = engine.connect()           # connect to sql server

        # execute query
        conn.execute(text(sql))
        conn.commit()

        # create a merge output csv
        res = pd.read_sql("select * from #merge_output", conn)
        res.to_csv(f"{maqc.OUTPUT}/{file}_result.csv", index=False)

        # record updated/inserted/deleted rows
        updated = pd.read_sql(
            "select count(*) from #merge_output where action = 'UPDATE'", conn)
        inserted = pd.read_sql(
            "select count(*) from #merge_output where action = 'INSERT'", conn)
        deleted = pd.read_sql(
            "select count(*) from #merge_output where action = 'DELETE'", conn)

        conn.execute(text("drop table #merge_output"))  # drop temp table
        conn.close()        # close connection
        engine.dispose()    # dispose unneeded engine

        # remove temp file
        os.remove(f"{maqc.BULK}/{file.replace('.csv','')}_temp")

        # log success
        logger.info(f"Upsert Successful: {file}")

        # log merge results
        logger.info(f"{updated.iloc[0,0]} Rows Updated")
        logger.info(f"{inserted.iloc[0,0]} Rows Inserted")
        logger.info(f"{deleted.iloc[0,0]} Rows Deleted")

    except Exception as e:
        logger.error(f"Problem with upserting {file}: {e}")
        raise


@task(name="Updating File")
def update_file(file, conn_url, sql, temp_table):
    """
    Connects to the SQL Server database and performs an update for the given 
    file. This task is performed for GPLAIN_PS1, CONSENSYS, and GREENPLAINSINTRA

    Args:
        file            (str):      File name
        conn_url        (str):      Connection to sql server
        sql             (str):      Sql query to be executed
    """
    logger = get_run_logger()   # Instantiate logger
    logger.info(f"Upserting {file}")

    try:
        engine = create_engine(conn_url)  # create connection
        conn = engine.connect()           # connect to sql server

        # execute query
        conn.execute(text(sql))
        conn.commit()

        # log success
        logger.info(f"Upsert Successful: {file}")

        # intraday files only truncate and insert new data
        inserted = pd.read_sql("select rows from #rowcount", conn)

        # drop temp tables
        conn.execute(text(f"drop table {temp_table}, #rowcount"))

        # remove temp file
        os.remove(f"{maqc.BULK}/{file.replace('.csv','')}_temp")

        conn.close()        # close connection
        engine.dispose()    # dispose unneeded engine

        # log merge results
        logger.info(f"All Previous Rows Deleted")
        logger.info(f"{inserted.iloc[0,0]} New Rows Inserted")

    except Exception as e:
        logger.error(f"Problem with upserting {file}: {e}")
        raise


@task(name="Sending Teams notification message")
def notify_team(msg):
    """
    Sends a Teams message. Usually notifies teams of an error or problem with 
    the MAQ settles.

    Args:
        msg     (str):  String of the notification message to send
    """
    teams_msg = pymsteams.connectorcard(maqc.TEAMS_HOOK)
    teams_msg.text(f"Encountered error while running MAQ sftp: {msg}")
    teams_msg.send()
