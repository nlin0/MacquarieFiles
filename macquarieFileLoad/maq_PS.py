"""
maq_PS.py

Upsert module for files with the prefix "GPLAIN_PS1"
"""


##############################
#      IMPORT PACKAGES       #
##############################

from datetime import datetime

# custom modules #
import maq_tools, maqc, db_utils3

# sql connections #
from sqlalchemy.engine import URL

# prefect modules #
from prefect import flow, get_run_logger



##############################
#      PS TRADES FLOW        #
##############################


@flow(name="FLOW: Processing GPLAIN PS Trades")
def ps_proc(ps_dict, update_time):
    """
    This flow processes and upserts files with the prefix "GPLAIN_PS1"

    Args:
        ps_dict         (dict):     Dictionary of the file/prefix to be
                                    processed
        update_time (datetime):     Datetime object of this session's 
                                    timestamp of when the main flow was ran
    """
    logger = get_run_logger()   # instantiate logger

    try:
        # create connection to sql server
        conn_url = URL.create(
            "mssql+pyodbc",
            query={"odbc_connect": "Driver={SQL Server};"
                    f"Server={maqc.SERVER};"\
                    f"Database={maqc.DB};Trusted_Connection=yes"})

        logger.info(f"Processing {ps_dict['name']}")

        for file in ps_dict['files']:

            # create query
            sql = maq_tools.prep_sql(file, "GPLAIN_PS1", update_time)

            # upsert file
            maq_tools.update_file(file, conn_url, sql, "#pstrade")

        # archive files and update last_updated time
        maq_tools.archive_files(ps_dict)
        maq_tools.last_updated(update_time, 'GPLAIN_PS1')

        # log process
        logger.info(f"Finished Processing {ps_dict['name']}."
                    f" Last Update changed to: {update_time}")

        # write to zProcessLog db
        db_utils3.proc_log(ps_dict['process_name'], f"Job Complete")

    except Exception as e:
        # log errors
        logger.error(f"Problem with processing {ps_dict['name']}: {e}")
        db_utils3.proc_log(ps_dict['process_name'],
                        f"UNSUCCESSFUL: Problem proccessing GPLAIN_PS1", {e})


##############################
#           GUARD            #
##############################

if __name__ == "__main__":
    ps_proc(
        ps_dict={
            "name": "PS Trades",
            "archive": False,
            "last_up": "2023-07-07 08:41:03",
            "files": [],
            "table": "dbo.broker_PSMAQ",
            "module": "maq_PS",
            "flow": "ps_proc",
            "process_name": "Maquarie PS Trades"
        },
        update_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
