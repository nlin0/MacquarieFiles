# Maquerie Broker File Processing
Executes a SFTP and processes each of the files by upserting them into the SQL database. The result of the upsert is stored as an output file that will be stored for 2 weeks. The files are then archived by date. Includes error catching and logging, as well as sending teams messages when there is something wrong with the flow or the code.

**Parameters**  
|children| True if children (dependent) flows should be ran. False otherwise. Defaults to True.   |
|-------------|-------------|

<br> 

________________________________________________________________


**Table of Contents**
- [important files](#important-files) 
- [additional documentation](#additional-documentationn)
- [before using](#before-using)   
- [exception catching note](#exception-catching-note)
- [quick fixable problems](#quick-fixable-problems)


________________________________________________________________

<br> 

## IMPORTANT FILES
Files thare are required for this process are listed below, as well as a short description of what they do

### Modules  

*main flows*  
|Module Name| Description   |
|-------------|-------------|
**mac_Flow.py**  | Main flow module for Maquerie. Contains the flow of flows   
**margin_deficit_control.py**  |Main flow module for margin deficit control emailing  

<br>

*sub flows*  
|Module Name| Description   |
|-------------|-------------|
**maq_SFTP.py**     | Flow that executes a sftp to grabs new files  
**maq_OP.py**      |Flow that processes files with prefix ***'GPLAIN_OP1'***  
**maq_TD.py**      |Flow that processes files with prefix ***'GPLAIN_TD1'***  
**maq_PS.py**      |Flow that processes files with prefix ***'GPLAIN_PS1'***  
**maq_INTRA.py**   |Flow that processes files with prefix ***'GREENPLAINSINTRA'***  
**maq_MONEYLINE.py**  |Flow that processes files with prefix ***'GPLAIN_LOAN1'*** and ***'GPLAIN_FIN1'***  
**maq_CONSENSYS.py**  |Flow that processes files with prefixes ***'CONSENSYS_GP'***  
 
<br>

*other*  
|Module Name| Description   |
|-------------|-------------|
**maqc.py**  |Contains constants for Maquerie processing.  *(NOTE maqc.py also contains constants for margin deficit control. this module can changed to be a general constants module)*  
**maq_tools.py**  |Contains helper functions/tasks utilized in Maquerie processing  
**db_utils3.py**  |Contains helper functions/tasks utilized in Maquerie processing *and* other processes  
**HTML.py**  |Copy of module to create html tables (for margin_deficit_control)

<br> 

### JSON files
|JSON File Name| Description   |
|-------------|-------------|
**acceptable_files.json**  | JSON file with Maquerie file name processes and execution flow details for each prefix."
**proc_status.json**  | JSON file containing the execution status (complete=True/not complete=False) of a process flow *(such as Maquerie or margin_deficit_control)*, as well as details of the flow

<br> 
<br> 

________________________________________________________________

## ADDITIONAL DOCUMENTATION
Documentation for modules should already be included in the code's specs. However, since JSON files don't allow comments, documentation for them is included here

<br>

### acceptable_files.json
The JSON file contains information about processing the Maquerie Broker files. Listed below is a description for each field.  

*Main JSON*
|Key| Value  |
|-------------|-------------|
process | A dictionary containing all of 10 of the file's prefixes
last_update | The last time the flow was called
process_name | The process name to be logged into zProcessLog after new files have been fetched

<br>

*process Dictionary*  
Referring to each prefix's key-values in the nested 'process' dictionary.
|Key| Value  |
|-------------|-------------|
name | The general name of the file, for readibility and identification purposes
archive | true if the file should be automatically archived. false otherwise
last_up | The last time a file with that prefix was successfully processed
files | The list of new file names that were grabbed. Used in file processing
table | The SQL Sever table that files will upsert into
module | The module containing the code that handles the file processing of the specific prefix
flow | The flow name that handles the file processing of the specific prefix
process_name | The process name to be logged into zProcessLog after the file has been processed into SQL Server
children | true if the process has dependencies (children), false otherwise.

<br>

### proc_status
*process name dictionary*  
Referring to the key-values under each process name
|Key| Value  |
|-------------|-------------|
status| If the process is complete. true if the process has completed, false otherwise. child flows will reset the status to false
dependencies| Nested dictionary of the child processes. In the form of ```{module_name: flow_name}```

<br> 
<br> 

________________________________________________________________

## BEFORE USING
### Adjust Constants in maqc.py
**This module can be generalized.**  
Adjust the constants module so it works on your local server. Descriptions for each constants are provided in the comments of the file, but additional information can be found below.

|Constant Name| Description   |
|-------------|-------------|
STATUS_FILE | Path to the file proc_status.json
SERVER | Server of SQL database to upsert to 
DB | Database of SQL database to upsert to
TEAMS_HOOK | Teams Webhook URL (to send Teams notifications when something is wrong with the code)
STAGE | Folder/directory path where files will be processed into the SQL database. The folder can be named anything.
ARCHIVE | Folder/directory path where files are stored/archived. The folder can be named anything.
UNKNOWN | Folder/directory path where unknown files will be stored. The folder can be named anything.
OUTPUT | Folder/directory path where upsert results will be stored for 2 weeks, before it will be automatically cleared
BULK | Directory/Folder where temporary process files will be stored, and eventually bulk inserted into a temporary table. This is where the server has to match the location of where the file is stored. Can be the same as the staging directory, in which case set ```BULK = STAGE```
MAQ_SQL | Folder/directory where the SQL query files are located for Maquerie Broker files
JSON_FILE | Path to the file acceptable_files.json

<br>

### Add Folders For The Current and Previous Month In the Archive
Create two folders within the "archive" directory. Name the two folders based on the current and previous month, using the format (YYYY-MM). For example, if it was July 14th 2023 (2023-07), you should create the following two folders in the "archive" directory:  

![Example](https://imgur.com/V5Cds3b.png)
NOTE: The code will check if files exist in the previous two months. So if the remote directory contains files from the previous month (2023-06 using the above image example), then it will grab the file if it is located outside of the 2023-06 folder.     

***Therefore, you should move processed files from the previous and current month to their respective folders. This does not have to be done for files that are not in the remote directory.***    

For example, if a file with the date 2023-06-31 is not in the remote directory, there is no need for it to be stored in the folder '2023-06' since the check is never conducted. But if there is a file with the date '2023-07-01', then you should store the existing file in the '2023-07' folder to avoid duplicate files.


<br>

### Set Up With Prefect

*Create required Prefect Blocks*  
There are 3 blocks that need to be created:
|Prefect Block| Set up Instructions   |
|-------------|-------------|
Microsoft Teams Webhook | Set up a Microsoft Teams Webhook using [THIS URL](<!! TEAMS WEBHOOK !!>) that has already been connected to the Microsoft Teams. The current configuration will notify Teams if a flow has been running for over 30 minutes, or if a work pool health has been poor for over 30 minutes. However, these configurations can be changed.
Secret Block (username) | A Secret block is used to store the credidentials of risk desk. This is used for emailing in margin_deficit_control.py. This block stores the username for risk desk.
Secret Block (password) | A Secret block is used to store the credidentials of risk desk. This is used for emailing in margin_deficit_control.py. This block stores the password for risk desk.

<br>

*Create Automations*  
This is not mandatory, and the automation/notification can be changed.  
Use the Webhook to create an automation where it sends a Teams notification when a flow has been running for over 30 minutes. Here is the body of the flow automated nofication:   

**Subject: Something is wrong with the flow**

```
---
Flow has been running for over 30 minutes. It has been automatically cancelled.  
All tasks may have all finished running, and this is just a Prefect being buggy.
Please check izProcessLog to see if tasks ran ok. Check Flow log if you suspect 
additional issues.
--


Flow run {{ flow.name }}/{{ flow_run.name }} observed in state `{{ flow_run.state.name }}` at {{ flow_run.state.timestamp }}.

Flow ID: {{ flow_run.flow_id }}

Flow run ID: {{ flow_run.id }}

Flow run URL: {{ flow_run|ui_url }}
```

<br>

*Deploy Flow*  
After the Yaml file has been modified to preferred settings, use [Prefect's Docs](https://docs.prefect.io/2.10.21/concepts/deployments/#create-a-deployment) to create the deployment.

<br> 
<br> 

________________________________________________________________
<br>

## EXCEPTION CATCHING NOTE
Currently, some of the exceptions will fail/crash the Prefect flow when caught. This is helpful for zProcessLog logging, since the caught exceptions will be logged in the database with whichever module/process that caused the error.  

To allow the flow to continue running despite errors in specific parts of the processes, remove the ```raise``` statement in the catch blocks of the functions that should not crash the flow. By removing ```raise```, the zProcessLog will not log the exception or the module/process that caused the error.

Currently only maq_SFTP.get_new_files along with some tasks in maq_tools.py will stop the flow.

For important errors, a teams message can be sent by adding ```maq_tools.notify_team("message")``` to the except block.

<br> 
<br> 

________________________________________________________________

##  QUICK FIXABLE PROBLEMS
These issues should have been fixed during testing through small adjustments, so they most likely will not pop up again. But just in case these issues were to arise, then they can be fixed by making the same small adjustments.

### ! the rounding is off after the file is upserted
|Possible underlying Error| Possible Solution   |
|-------------|-------------|
File currently is only rounding 5 decimal places when processing in *maq_tools.prep_sql* | Change the 5 to however many decimals it should round off to in ```df.to_csv(float_format%.5f)```

### ! sql error
|Possible underlying Error| Possible Solution   |
|-------------|-------------|
Truncation Error during file processing. Currently, the SQL query is using a lot of varchar(30) for variables to optimize storage. | If problem for a file persists, or if storage is not an issue, varchar(60)+ can be used
