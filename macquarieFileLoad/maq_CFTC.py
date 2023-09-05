"""
maq_CFTC.py

Upsert module for files with the prefix "CONSENSYS_GP"
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
#    CFTC POSITIONS FLOW     #
##############################


@flow(name="FLOW: Processing CFTC Positions")
def cftc_proc(cftc_dict, update_time):
    """
    This flow proccesses and upserts files with the prefix "CONSENSYS_GP"

    Args:
        cftc_dict       (dict):     Dictionary of the file/prefix to be
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

        logger.info(f"Processing {cftc_dict['name']}")

        for file in cftc_dict['files']:
            # create query
            sql = maq_tools.prep_sql(file, "CONSENSYS_GP", update_time)

            # upsert file
            maq_tools.update_file(file, conn_url, sql, "#cftcpos")

        # archive files and update last_updated time
        maq_tools.archive_files(cftc_dict)
        maq_tools.last_updated(update_time, 'CONSENSYS_GP')

        # log process
        logger.info(f"Finished Processing {cftc_dict['name']}."
                    f" Last Update changed to: {update_time}")

        # write to zProcessLog db
        db_utils3.proc_log(cftc_dict['process_name'], f"Job Complete")

    except Exception as e:
        # log errors
        logger.error(f"Problem with processing {cftc_dict['name']}: {e}")
        db_utils3.proc_log(cftc_dict['process_name'],
                        f"UNSUCCESSFUL: Problem proccessing CONSENSYS_GP", {e})



##############################
#           GUARD            #
##############################

if __name__ == "__main__":
    cftc_proc(
        cftc_dict={
            "name": "CFTC Positions",
            "archive": False,
            "last_up": "2023-07-07 08:41:03",
            "files": [],
            "table": "dbo.broker_maq_CFTCPositions",
            "module": "maq_CFTC",
            "flow": "cftc_proc",
            "process_name": "Maquarie CFTC Positions"
        },
        update_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
