"""
maq_OP.py

Upsert module for files with the prefix "GPLAIN_OP1"
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
#    OPEN POSITIONS FLOW     #
##############################


@flow(name="FLOW: Processing Open Positions")
def op_proc(op_dict, update_time):
    """
    This flow proccesses and upserts files with the prefix "GPLAIN_OP1"

    Args:
        op_dict         (dict):     Dictionary of the file/prefix to be
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

        logger.info(f"Processing {op_dict['name']}")

        for file in op_dict['files']:

            # create query
            sql = maq_tools.prep_sql(file, "GPLAIN_OP1", update_time)

            # upsert file
            maq_tools.upsert_file(file, conn_url, sql)

        # archive files and update last_updated time
        maq_tools.archive_files(op_dict)
        maq_tools.last_updated(update_time, 'GPLAIN_OP1')

        # log process
        logger.info(f"Finished Processing {op_dict['name']}."
                    f" Last Update changed to: {update_time}")

        # write to zProcessLog db
        db_utils3.proc_log(op_dict['process_name'], f"Job Complete")

    except Exception as e:
        # log errors
        logger.error(f"Problem with processing {op_dict['name']}: {e}")
        db_utils3.proc_log(op_dict['process_name'],
                        f"UNSUCCESSFUL: Problem proccessing GPLAIN_OP1", {e})


        
##############################
#           GUARD            #
##############################

if __name__ == "__main__":
    op_proc(
        op_dict={
            "name": "Open Positions",
            "archive": False,
            "last_up": "2023-07-07 08:41:03",
            "files": [],
            "table": "dbo.broker_openPositionsMAQ",
            "module": "maq_OP",
            "flow": "op_proc",
            "process_name": "Maquarie Open Positions"
        },
        update_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
