"""
maq_MONEYLINE.py

Upsert module for files with the prefix "GPLAIN_LOAN1" or "GPLAIN_FIN1"
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
#      MONEY LINE FLOW       #
##############################


counter = 0  # initiate global variable


@flow(name="FLOW: Processing GPLAIN Money Line")
def moneyline_proc(moneyline_dict, update_time, children):
    """
    This flow proccesses and upserts files with the prefix "GPLAIN_LOAN1" 
    and "GPLAIN_FIN1." The Money Line process is complete IF AND ONLY IF both 
    prefixes have finished processing.

    Args:
        moneyline__dict  (dict):        Dictionary of the file/prefix to be
                                        processed
        update_time   (datetime):       Datetime object of this session's 
                                        timestamp of when the main flow was ran

    """
    logger = get_run_logger()
    logger.info(f"Processing {moneyline_dict['name']}")
    
    proc = "GPLAIN_FIN1"
    global counter  # access global variable

    try:
        # create connection to sql server
        conn_url = URL.create(
            "mssql+pyodbc",
            query={"odbc_connect": "Driver={SQL Server};"
                   f"Server={maqc.SERVER};"
                   f"Database={maqc.DB};Trusted_Connection=yes"})

        # change process name if needed
        if moneyline_dict['name'] == 'Loans':
            proc = "GPLAIN_LOAN1"

        for file in moneyline_dict['files']:
            # create query
            sql = maq_tools.prep_sql(file, proc, update_time, True)

            # upsert file
            maq_tools.upsert_file(file, conn_url, sql)

        # archive files and update last_updated time
        maq_tools.archive_files(moneyline_dict)
        maq_tools.last_updated(update_time, proc)

        # log process
        logger.info(f"Finished Processing {moneyline_dict['name']}."
                    f" Last Update changed to: {update_time}")

        # use a counter to keep track of processing FIN1 and LOAN1
        if moneyline_dict['name'] == 'Loans':
            counter += 3
        elif moneyline_dict['name'] == 'Financials':
            counter += 2

        # if 2 files (one FIN and one LOAN) have been processed
        if counter == 5:
            counter = 0  # reset counter

            # write to zProcessLog db and json file
            db_utils3.proc_log(moneyline_dict['process_name'], f"Job Complete")
            db_utils3.json_log("Maquerie Money Line")

            # call dependent flows if children is set to true
            if children:
                db_utils3.call_depen("Maquerie Money Line", (False,))

         # in case counter goes over 5
        elif counter > 5 or counter == 4:
            logger.error("One too many FIN1 or LOAN1 has been processed. " \
                         "Please reset counter in maq_MONEYLINE")

    except Exception as e:
        # log errors
        logger.error(f"Problem with processing/archiving {file}: {e}")
        db_utils3.proc_log(moneyline_dict['process_name'],
                           f"UNSUCCESSFUL: Problem proccessing {proc}", {e})



##############################
#           GUARD            #
##############################

if __name__ == "__main__":
    moneyline_proc(
        moneyline_dict={
            "name": "Loans",
            "archive": False,
            "last_up": "2023-07-07 08:41:03",
            "files": [],
            "table": "dbo.broker_moneyLineMAQ",
            "module": "maq_MONEYLINE",
            "flow": "moneyline_proc",
            "process_name": "Maquarie Loan Money Line"
        },
        update_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        children = False
    )
