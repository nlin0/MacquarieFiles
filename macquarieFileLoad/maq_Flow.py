"""
maq_flow.py

Contains the main prefect flow. This job grabs new files in the remote directory, processes the files, and archives them after the processing has concluded. This flow will also update the acceptable_files.json.
"""


##############################
#      IMPORT PACKAGES       #
##############################


import importlib, json, os, shutil

# date imports
from datetime import datetime
from dateutil.relativedelta import relativedelta

# custom modules #
import maq_tools, maq_SFTP, maqc

# prefect modules #
from prefect import task, flow, get_run_logger



##############################
#        CHECK TASKS         #
##############################


@task(name="Check Archive Folder")
def check_archive(archive_path, date):
    """
    Checks if the archive folder under the name (YYYY-MM) exists. If not, 
    creates the folder. 

    Args:
        archive_path    (str):  The string path to the local archive directory
        date            (str):  The date input that will be used to check if a 
                                directory under the format YYYY-MM exists.

    Returns:
        list:                   The current montha and previous month directory 
                                names (YYYY-MM) that new files should be 
                                compared/placed into.
    """
    logger = get_run_logger()   # instantiate logger

    try:
        # get dir_name from date input
        dir_name = datetime.strftime(date, "%Y-%m")

        # get the previous month
        prev_month = datetime.strftime(
            date - relativedelta(months=1), "%Y-%m")

        # make directory if it does not exist
        if not os.path.exists(f"{archive_path}/{dir_name}"):
            os.mkdir(f"{archive_path}/{dir_name}")

        # return the list containing current and previous month
        return [dir_name, prev_month]

    except Exception as e:
        logger.error(f"There's a problem with checking archive folders: {e}")


@task(name="Verifying Files in Staging")
def check_stage(curr_date):
    """
    Double checks the file in the staging directory. If the file is old (> 2 
    months), then the file is logged moved to the unknown directory.

    Args:
        curr_date   (datetime): The date/time of when the flow was started
    """
    logger = get_run_logger()   # instantiate logger

    previous_date = curr_date - relativedelta(months=2)

    try:
        for file in os.listdir(f"{maqc.STAGE}"):
            # get file date
            file_date = datetime.fromtimestamp(
                os.path.getmtime(f"{maqc.STAGE}/{file}"))

            # if the file is as old as, or older than 2 months
            if file_date <= previous_date or not file.endswith('.csv'):

                # log old file
                logger.warning(f"Problem with {file}. Check unknown folder.")

                # move the file to unkown folder
                shutil.move(f"{maqc.STAGE}/{file}", f"{maqc.UNKNOWN}")

                # send teams notification
                maq_tools.notify_team.fn(
                    f"{file} has been in the staging directory for a while.\n"
                    f"The last modified date was {file_date}). It has been"
                    f"moved to the unknown folder.")

    except Exception as e:
        logger.error(f"Problem with verifying files in staging: {e}")


@task(name="Deleting Old Merge Output Files")
def del_output(curr_date):
    """
    Deletes the csv files in the merge output directory that are older than 2 
    weeks. 

    Args:
        curr_date   (datetime): The current date of when the flow was started
        out_path    (str):      String path of the directory 'output' 
                                containing the csv files of the merge outputs
    """
    logger = get_run_logger()  # instantiate logger

    try:
        # get previous week's time
        prev_week = curr_date - relativedelta(days=7)

        # for each file in output directory
        for file in os.listdir(maqc.OUTPUT):

            # csv file path
            path = f"{maqc.OUTPUT}/{file}"

            # get file's last modified dated
            file_dt = datetime.fromtimestamp(
                os.path.getmtime(path)).strftime('%Y-%m-%d')
            file_dt = datetime.strptime(file_dt, '%Y-%m-%d')

            # delete if file is older than 1 week
            if prev_week >= file_dt:
                os.remove(path)

    except Exception as e:
        logger.error(f"Problem with deleting old merge output files: {e}")



##############################
#        HELPER FLOW         #
##############################


@flow(name="Start Processing of Files in Staging...")
def process_file(prefix, curr_date, children):
    """
    Runs a flow specific to the prefix provided.

    Args:
        prefix      (str):      The prefix of the file to be processed
        curr_date   (datetime): The current date of when the flow was started

    """
    logger = get_run_logger()   # instantiate logger

    try:
        with open(maqc.JSON_FILE, "r") as f:
            accepted = json.load(f)  # load json

            # grab the module name and flow names of the prefix
            mod_str = accepted['process'][prefix]['module']
            mod_name = importlib.import_module(mod_str)
            flow_name = accepted['process'][prefix]['flow']
            
            # grab if the process has children
            has_children = accepted['process'][prefix]['children']

        # call the flow via mod_name.flow_name
        proc = getattr(mod_name, flow_name)
        
        # if process has children, pass into parameter
        if has_children:
            proc(accepted['process'][prefix], curr_date, children)
        else:
            proc(accepted['process'][prefix], curr_date)

    except Exception as e:
        logger.error(f"Problem with starting upsert for {prefix}: {e}")



##############################
#         MAIN FLOW          #
##############################


@flow(name="FLOW: Flow of Flows")
def maq(children=True):
    """
    This flow grabs the new files from the remote server, storing them in the
    json file acceptable_files.json. It the processes the files and archives
    them depending on the file date.

    Args:
        children    (boolean):  True if Macquarie should launch children flows 
                                (dependencies). False otherwise

    """

    logger = get_run_logger()       # initiate logger

    curr_time = datetime.now()      # get the current time
    logger.info(f"Time is currently {curr_time}")

    maq_tools.rejson()   # reset the json

    # check archive and get directory names
    dir_names = check_archive(maqc.ARCHIVE, curr_time)

    # get a list of files to be upserted
    files = maq_SFTP.new_files(curr_time, dir_names)

    # check for issues in staging
    check_stage(curr_time)

    # delete old output files
    del_output(curr_time)

    # loop through list of prefixes that need to be processed/upserted
    for prefix in files:
        process_file(prefix, curr_time, children)

    # update json's last_update filed after all files processed
    maq_tools.last_updated(curr_time)
    logger.info(f"json last_update time changed to {curr_time}")



##############################
#           GUARD            #
##############################

if __name__ == "__main__":
    maq(True)
