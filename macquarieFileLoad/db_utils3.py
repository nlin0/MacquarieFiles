"""
db_utils3.py    

This module contains functions that log the process of saving, processing, and archiving files. It marks a process as successful or unsuccessful (1 or 0, respectfilly) in the database.
"""



##############################
#      IMPORT PACKAGES       #
##############################


import re, getpass, json, importlib
from sqlalchemy.engine import URL
from sqlalchemy import create_engine, text

# custom modules #
import maqc

# prefect modules #
from prefect import get_run_logger, task, flow



##############################
#        PROCESS LOG         #
##############################


def execute_sql(query):
    """
    Executes the SQL query

    Args:
        query   (str):  String of the SQL query to execute
    """
    logger = get_run_logger()
    
    # establish connection
    conn_url = URL.create(
    "mssql+pyodbc",
    query={"odbc_connect": "Driver={SQL Server};"\
    f"Server={maqc.SERVER};Database={maqc.DB};Trusted_Connection=yes"})
    
    # create engine
    engine = create_engine(conn_url)
    
    try:
        with engine.connect() as conn:  # connect to database
            # execute query
            conn.execute(text(query))
            conn.commit()
    
    except Exception as e:
        logger.error(f"Problem writing to process log: {e}")

        
@task(name="Logging Status in Database")
def proc_log(proc, sub_proc, error_log=None):
    """
    Records if a given process is either successful (1) or unsuccessful (0)

    Args:
        proc                  (str):    The process name
        sub_proc              (str):    The subprocess name or prefix
        error_log  (exception, opt):    Error message. Defaults to None
    """
    logger = get_run_logger()   # instantiate logger
    
    try: 
        if error_log == None:
            success_indicator = '1'
            error_log = ''
        else:
            success_indicator = '0'
            error_log = re.sub(r'[\"\'\,]', '', str(error_log))

        # grab username from enviornment/password db
        username = getpass.getuser()

        # write sql query
        query = f"insert into dbo.zProcessLog select "\
            f"'{proc}', '{sub_proc}', '{username}', current_timestamp, "\
            f"{success_indicator}, '{error_log}'"
        
        # execute sql query
        execute_sql(query)
        
        # prefect logger message
        msg = f"{proc}, {sub_proc}, {username}, {success_indicator}." 
        
        # log success
        logger.info(f"{proc} has been logged: {msg}")
    
    except Exception as e:
        logger.error(f"Problem creating processing query: {e}")



##############################
#          JSON LOG          #
##############################


@task(name = "Logging Status in Json")
def json_log(process):
    """
    Marks the process as completed (true) in the json file. 

    Args:
        process (str):     The process name
    """
    logger = get_run_logger()   # instantiate logger
    
    try:
        # open status json file
        with open(maqc.STATUS_FILE, 'r+') as f:
            status = json.load(f)
            # log the status of the process as complete
            status[process]['status'] = True
    
        # log success
        logger.info(f"Status logged as True in json")
    
    except Exception as e:
        logger.error(f"Problem with logging in json: {e}")


        
##############################
#        HELPER FLOW         #
##############################


@flow(name = "Calling Dependent Flows")
def call_depen(process, params=None):
    """
    Calls the dependent flows and their respective modules. Relies on the 
    proc_status.json file, which includes a field "dependencies" under each
    process. Dependencies are a dictionaries in the form of:        
    {module_name: flow_name}

    Args:
        process  (str): The current process name that has dependent flows
        params (tuple): The parameters to pass into the dependent flows
    """
    logger = get_run_logger()   # instantiate logger
    
    try:
        # open status json file
        with open (maqc.STATUS_FILE, 'r') as f:
            status = json.load(f)
            
            # grab dependencies list
            dependents = status[process]["dependencies"]
            
            # for each module in dependents dict
            for key in dependents:
                mod_name = importlib.import_module(key)
                flow_name = dependents[key]
                
                # create function object
                proc = getattr(mod_name, flow_name)
                
                # call the flow with given parameters (param)
                proc(*params)
    
    except Exception as e:
        logger.error(f"Error calling dependent flows: {e}")
    
        