"""
maq_INTRA.py

Upsert module for files with the prefix "GREENPLAINSINTRA"
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
#   GREEN PLAINS INTRADAY    #
##############################


@flow(name="FLOW: Processing Green Plains Intraday")
def intra_proc(intra_dict, update_time):
    """
    This flow processes and upserts files with the prefix "GREENPLAINSINTRA"

    Args:
        intra_dict      (dict):     Dictionary of the file/prefix to be
                                    processed
        update_time (datetime):     Datetime object of this session's 
                                    timestamp of when the main flow was ran
    """
    logger = get_run_logger()

    try:
        # create connection to sql server
        conn_url = URL.create(
            "mssql+pyodbc",
            query={"odbc_connect": "Driver={SQL Server};"
                    f"Server={maqc.SERVER};"\
                    f"Database={maqc.DB};Trusted_Connection=yes"})

        logger.info(f"Processing {intra_dict['name']}")

        for file in intra_dict['files']:

            # create query
            sql = maq_tools.prep_sql(file, "GREENPLAINSINTRA", update_time)

            # upsert file
            maq_tools.update_file(file, conn_url, sql, "#intraday")

        # archive files and update last_updated time
        maq_tools.archive_files(intra_dict)
        maq_tools.last_updated(update_time, 'GREENPLAINSINTRA')

        # log process
        logger.info(f"Finished Processing {intra_dict['name']}."
                    f" Last Update changed to: {update_time}")

        # write to zProcessLog db
        db_utils3.proc_log(intra_dict['process_name'], f"Job Complete")

    except Exception as e:
        # log errors
        logger.error(f"Problem with processing {intra_dict['name']}: {e}")
        db_utils3.proc_log(intra_dict['process_name'],
                    f"UNSUCCESSFUL: Problem proccessing GREENPLAINSINTRA", {e})


        
##############################
#           GUARD            #
##############################

if __name__ == "__main__":
    intra_proc(
        intra_dict={
            "name": "Green Plains Intraday",
            "archive": False,
            "last_up": "2023-01-01 00:00:00",
            "files": [],
            "table": "dbo.broker_intradayMaq",
            "module": "maq_INTRA",
            "flow": "intra_proc",
            "process_name": "MAQ Intraday File Load"
        },
        update_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
