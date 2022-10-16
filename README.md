# Demisto Archive Attathments And Artifacts

### Archive attachments and artifacts on a multi-tenant deployment
To archive artifacts and attachments in a multi-tenant deployment for a specific period, just run the script with your chosen arguments.

The script will work as following:
1. stop the service
2. move files to backup directory
3. start the service again
4. archive the files
5. delete the archived files

### Requirements
- python 3.8 or newer (it is recommended to install python 3.10 or newer for future improvements).
- Install all modules in the requirements.txt file.

### Example
`python3 archive_attachments_artifacts.py --accounts all --from $(date -d "-5 months" +"%Y-%m-%d") --to $(date -d "-100 days" +"%Y-%m-%d")`
