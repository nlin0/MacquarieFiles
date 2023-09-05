"""
maqc.py

Constants used for the flow maq_Flow.py
OPTIONAL: This can be a general constants file to store constants for processes
"""


##############################
#      SHARED CONSTANTS      #
##############################

# process file status path
STATUS_FILE = "<!!  NETWORK FOLDER PATH !!>/macquarieFileLoad/proc_status.json"

# SQL server to connect to
SERVER = "<!! DATABASE SERVER NAME !!>"

# SQL database
DB = "<!! DATABASE NAME !!>"

# webhook link
TEAMS_HOOK = "<!! TEAMS WEBHOOK !!>"


##############################
#     MAQUERIE CONSTANTS     #
#        maq_Flow.py         #
##############################

# path of the staging directory
STAGE = "<!! NETWORK FOLDER PATH !!>/brokerFiles/macquarieFiles/Prod/staging"

# path of the archive directory
ARCHIVE = "<!! NETWORK FOLDER PATH !!>/brokerFiles/macquarieFiles/Prod/archive"

# path of the unknown directory
UNKNOWN = "<!! NETWORK FOLDER PATH !!>/brokerFiles/macquarieFiles/Prod/unknown"

# path of the output directory
OUTPUT = "<!! NETWORK FOLDER PATH !!>/brokerFiles/macquarieFiles/Prod/outputs"

# the path where temp files are stored for a bulk insert
BULK = "//<!! DATABASE SERVER SHARE !!>/staging"

# path of the maquerie SQL files
MAQ_SQL = "<!! NETWORK FOLDER PATH !!>/SQL procedures/maq_queries"

# json file path
JSON_FILE = "<!! NETWORK FOLDER PATH !!>/acceptable_files.json"


##############################
#      MARGIN CONSTANTS      #
#  margin_deficit_control.py #
##############################

MARGIN_SQL = "<!!  NETWORK FOLDER PATH !!>/SQL procedures/margin_deficit_control.sql"
