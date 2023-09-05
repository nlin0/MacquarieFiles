"""
maq_stfp.py    

This module contains prefect tasks that grab the new files from the remote path, and archives the file if it does not need to be upserted. Returns the list of file prefixes that need to be upserted.   
"""


##############################
#      IMPORT PACKAGES       #
##############################


import json, pysftp, paramiko, os
from datetime import datetime
from io import StringIO

# custom modules #
import maq_tools, db_utils3, maqc

# prefect modules #
from prefect import task, flow, get_run_logger


##############################
#      GRABBING FILES        #
##############################

def check(file, file_date, last_time, dir_Name):
    """
    Returns True if the file should be grabbed from the remote directory. 
    Otherwise, it returns false.

    Args:
        file        (str):  Path to the file
        file_date   (str):  File's modified time
        last_time   (str):  Lst updated time
        dir_Name   (list):  List containing the current month directory name
                            and the previous month's directory name in the form
                            YYYY-MM

    Returns:
        Boolean          :  True if file_date is more recent than the last
                            updated time, or if the file is not found in the
                            current and last month's directories. False 
                            otherwise
    """
    return file_date > last_time or \
        (file not in os.listdir(f"{maqc.ARCHIVE}/{dir_Name[0]}") and
            (file not in os.listdir(f"{maqc.ARCHIVE}/{dir_Name[1]}")))


@task(name="Fetching New Files")
def get_new_files(dir_Name):
    """
    Saves files from the remote path that if:
        1) The modified file time is more recent than file prefix's last_update
        2) Does not exist in the current and previous month's local archive

    Furthermore, get_new_files() will add the grabbed files to each of the 
    prefixs' (ex: GPLAIN_FEE1) file list in the accpetableFiles json.

    Args:
        dir_Name   (list):  List of directory names from check_folder() in the
                            format of: [current Month, previous Month]
    """
    logger = get_run_logger()       # instantiate logger

    try:
        # create key object
        f = open('<!! PATH TO SSH KEYFILE !!>', 'r')
        key = paramiko.RSAKey.from_private_key(StringIO(f.read()))

        # create connections
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        # connect to remote server
        with pysftp.Connection(
                host='<!! SFTP HOST NAME !!>',
                username='<!! SFTP USERNAME !!>',
                private_key=key,
                cnopts=cnopts) as sftp:

            sftp.cwd('outbox')    # change directory to 'outbox'

            with open(maqc.JSON_FILE, 'r+') as f:
                accepted = json.load(f)  # load json file

                # store process name
                process_name = accepted['process_name']

                # log information
                logger.info(f"Checking for new files...")

                # loop through files in sftp
                for file in sftp.listdir():

                    # get identifying file information
                    file_name = os.path.basename(file).split('/')[-1]

                    date_str = datetime.fromtimestamp(
                        (sftp.stat(file).st_mtime)).strftime(
                        '%Y-%m-%d %H:%M:%S')

                    file_date = datetime.strptime(date_str,
                                                  "%Y-%m-%d %H:%M:%S")

                    # get the file prefixes
                    prefix = "_".join(file.split('_', 2)[:2])
                    if len(prefix) > 12:
                        prefix = (file.split('_'))[0]

                    # get last updated time for the prefix
                    update = datetime.strptime(
                        accepted['process'][prefix]['last_up'],
                        "%Y-%m-%d %H:%M:%S")

                    # check if file should be grabbed
                    if check(file, file_date, update, dir_Name):
                        try:
                            # include file in json file
                            s = accepted['process'][prefix]
                            s['files'].append(file_name)

                            # move file to staging
                            sftp.get(remotepath=file,
                                     localpath=f"{maqc.STAGE}/{file}",
                                     preserve_mtime=True)

                            # log success
                            logger.info(f"Saved {file_name}")
                            
                        except Exception as e:
                            # if file not recognize, move to unknown folder
                            sftp.get(remotepath=file,
                                     localpath=f"{maqc.UNKNOWN}/{file}",
                                     preserve_mtime=True)

                            # log unknown file and notify team
                            logger.warning(f"Unknown File: {file_name}")
                            maq_tools.notify_team.fn(f"{file_name} was not "
                             f"recognized. Please check the unknown folder.")

                    # update the json
                    f.seek(0)
                    json.dump(accepted, f, indent=4)
                    f.truncate()

            sftp.cd('..')  # go back a directory
        logger.info("Finished grabbing new files.")  # log success

    except Exception as e:
        # log errors
        logger.error(f"Problem with getting new files: {e}")
        db_utils3.proc_log.fn(process_name,
                              f"UNSUCCESSFUL: Problem grabbing new files", e)
        raise


        
##############################
#           FLOW             #
##############################


@flow(name="FLOW: Grabbing New Files")
def new_files(curr_time, dir_Name):
    """
    This flow grabs the new files from the remote path. 

    Args:
        curr_time   (datetime): The current datetime
        dir_Name    (list):     List containing the directory name of the 
                                current date and the previous date
    """
    logger = get_run_logger()       # instantiate logger

    # try-catch since can be raised from the functions from maq_tools
    try:
        # grab new files
        get_new_files(dir_Name)

        # create a list to hold files that need to be processed
        to_upsert = []

        # auto archive files
        with open(maqc.JSON_FILE, 'r') as f:
            accepted = json.load(f)  # load json
            process_name = accepted['process_name']  # store process name

            # log last updated time
            last_time = datetime.strptime(accepted['last_update'],
                                          "%Y-%m-%d %H:%M:%S")
            logger.info(f"Last update was at {last_time}")

            # loop through processes in json file
            for prefix in accepted['process']:
                if (accepted['process'][prefix]['archive']) and (
                        len(accepted['process'][prefix]['files']) != 0):

                    # archive and update process names whose archive === true
                    maq_tools.archive_files(accepted['process'][prefix])
                    maq_tools.last_updated(curr_time, prefix)

                elif len(accepted['process'][prefix]['files']) > 0:
                    # if archive == false, then prepare to upsert
                    to_upsert.append(prefix)

        # proc_log success
        db_utils3.proc_log(process_name, f"Job Complete")

        # return list of files to be upserted
        return to_upsert

    except Exception as e:
        # log errors
        logger.error(f"Problem with grabbing/archiving files marked 'archive"
                     f":  true' in json file: {e}")
        db_utils3.proc_log(process_name,
                              f"UNSUCCESSFUL: Problem grabbing new files", e)



##############################
#           GUARD            #
##############################

if __name__ == "__main__":
    new_files(
        curr_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
