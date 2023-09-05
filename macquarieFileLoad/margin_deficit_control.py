"""
margin_deficit_control.py

This job generates a margins report, and emails it to those who need the report
"""


##############################
#      IMPORT PACKAGES       #
##############################


import json, HTML
import pandas as pd
from exchangelib import DELEGATE, Account, Credentials, Message, Mailbox, HTMLBody

# sql connections #
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# custom modules #
import maqc
import db_utils3 as db

# prefect modules #
from prefect import task, flow, get_run_logger
from prefect.blocks.system import Secret


##############################
#           TASKS            #
##############################


# ? can be generalized and moved to db_utils3 for other functions to use
@task(name="Verifying Previous Task is Complete...")
def check_task(bypass=False):
    """
    Checks to see if the previous task, Maquarie Money Line is completed. If it
    is completed, then this task will run. If not, the task will not run.

    Args:
        bypass  (boolean):  True if you want to bypass the previous task check.
                            False if you want to check the previous task.
                            Defaults to False.

    """
    logger = get_run_logger()   # instantiate logger

    if bypass:                  # skip the check if bypass is set to True
        logger.info("Skipping Maquerie Money Line check")
        return True

    try:                        # check if Maquerie Money Line is complete
        with open(maqc.STATUS_FILE, "r+") as f:
            status = json.load(f)   # load json file

            # return false if Money Line has not completed
            if not status["Maquerie Money Line"]:
                logger.error("Maquerie Money Line has not yet been completed")
                return False

            # log completion success
            logger.info(f"Maquerie Money Line has completed")

            # reset Money Line and return
            status["Maquerie Money Line"] = False
            return True

    except Exception as e:
        logger.error(f"Problem checking completion of previous task: {e}")


@task(name="Grabbing Margin Deficit Control SQL Results")
def get_results():
    """
    Executes the Margin Deficit Control SQL query and returns the results
    """
    logger = get_run_logger()   # instantiate logger

    try:
        # get sql query
        with open(maqc.MARGIN_SQL, 'r') as f:
            sql = f.read()

        # create connection
        engine = create_engine(
            URL.create("mssql+pyodbc",
                       query={"odbc_connect": "Driver={SQL Server};"
                              f"Server={maqc.SERVER};Database="
                              f"{maqc.DB};Trusted_Connection=yes"}))

        # connect to sql server
        with engine.connect() as conn:
            # grab query results as a dataframe
            df = pd.read_sql(sql, conn)

        if df.count()[0] == 0:
            res = "NO RESULTS!"
        else:
            values = df.values.tolist()
            res = HTML.table(values, header_row=list(df.columns.values))

        return res

    except Exception as e:
        logger.error(f"Problem executing Margin Deficit Control SQL: {e}")
        raise


@task(name="Emailing Results")
def send_results(res):
    """
    Emails the Margin Deficit Control query results 

    Args:
        res    (str):      The results of the Margin Deficit Control query as 
                            a string
    """
    logger = get_run_logger()   # instantiate logger

    # load creditials from block
    user = Secret.load("username")
    passw = Secret.load("passw")

    try:
        # create creditials object
        creds = Credentials(
            username=f"<!! DOMAIN NAME !!>\\{user.get()}",
            password=passw.get())

        # create account object
        acc = Account(
            primary_smtp_address="<!! SENDER EMAIL !!>",
            credentials=creds,
            autodiscover=True,
            access_type=DELEGATE)

        # create message object
        msg = Message(
            account=acc,
            subject='Margin Deficit Control',
            body=HTMLBody(res),
            to_recipients=[
                Mailbox(email_address="<!! RECIPIENT EMAIL !!>")
            ]
        )

        # send email
        msg.send()

    except Exception as e:
        logger.error(f"Problem with emailing query results: {e}")

    
    
##############################
#           FLOW             #
##############################

@flow(name="FLOW: Emailing Margin Deficit Control")
def email_margin(bypass=False):
    """
    This flow executes the Margin Deficit Control SQL and emails the results to 
    the respective recipients.

    Args:
        bypass  (bool):     True to bypass checking the previous job, Maquerie
                            MoneyLine. False otherwise. Defaults to False.
    """
    logger = get_run_logger()   # instantiate logger

    try:

        # don't run if Money Line has not yet been completed
        if not check_task(bypass):
            db.proc_log('Margin Deficit Control',
                        f"UNSUCCESSFUL: Previous Job not complete", e)

            raise Exception('Maquerie MoneyLine has not yet completed')

        # grab html results
        res = get_results()

        # email the html results
        send_results(res)

    except Exception as e:
        logger.error(f"Problem with executing Margin Deficit Control job: {e}")


##############################
#           GUARD            #
##############################

if __name__ == "__main__":
    email_margin(bypass=False)
