#!/usr/bin/python3

import argparse
import glob
import logging
import os
import subprocess
import traceback

import dateparser

from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from logging.handlers import RotatingFileHandler


try_start_the_service = 0  # Will try to start the service for 5 times, otherwise it will fail and exit.


class ArgException(Exception):
    def __init__(self, msg):
        logger.error(msg)


class OSCommandException(Exception):
    def __init__(self, msg):
        global try_start_the_service
        logger.error(msg)
        if try_start_the_service < 5:
            try_start_the_service += 1
            out, err = call_os(demisto_status)
            if out == "inactive":
               call_os(demisto_start)
    
    
# Define and get parser.
def get_parser():
    parser = argparse.ArgumentParser("Archive attachments and artifacts.")
    parser.add_argument("--accounts", nargs="+", required=True, help="SOAR accounts e.g. acc_T3, type 'all' for all available accounts.")
    parser.add_argument("--from", dest="time_from", help="Archive files modified after or at this date. Leave empty to archive files since date 0000/00/00")
    parser.add_argument("--to", dest="time_to", required=True, help="Archive files modified before or at this date.")
    return parser.parse_args()


def call_os(command, timeout=7200, cwd=None):
    p = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd=cwd)
    err = p.stderr.decode("UTF-8").strip()
    out = p.stdout.decode("UTF-8").strip()
    if err:
        logger.error(f"{command}: {err}")
    elif out:
        logger.debug(f"{command}: {out}")
    else:
        logger.debug(command)
    return out, err


def get_available_accounts(accounts_dir, files_types):
    available_accounts = []
    accounts = os.listdir(accounts_dir)
    
    for account in accounts:
        ls_account = os.listdir(Path(accounts_dir, account))
        for file_type in files_types:
            if file_type not in ls_account:
                ArgException("Please make sure to enter accounts that are present inthis server.")
        else:
            available_accounts.append(account)
    return available_accounts


def create_directory(directory):
    Path(directory).mkdir(parents=True, exist_ok=True)


def validate_accounts(accounts, available_accounts):
    for account in accounts:
        if account not in available_accounts:
            err = f"Account '{account}' is not found in the accounts directory or it does not have an artifacts or an attachments directory."
            ArgException(err)


def validate_time(time_from, time_to, now):
    if time_from.date() >= now.date():
        raise ArgException(f"'--from' argument should present a date in the past: --to {time_from}")
    elif time_to.date() >= now.date():
        pass
        # raise ArgException(f"'--to' argument should present a date in the past: --to {time_to}")
    elif time_to <= time_from:
        raise ArgException("'--from' argument should present a date older than '--to' argument.")
    elif (now - time_to).days < 90 or (now - time_from).days < 90:
        pass
        # raise ArgException("'--from' and '--to' arguments should present dates older than 90 days from now.")


def get_time(time):
    try:
        time = dateparser.parse(time, settings={'TIMEZONE': "+0300"})
    except Exception as err:
        raise ArgException(err)
    return time


cur_dir = Path(__file__).resolve().parent
log_dir = Path(cur_dir, "logs")
log_file = Path(log_dir, "archiving.log") 

# create logging file
create_directory(log_dir)
log_file.touch(exist_ok=True)

# Logging configurations
LOG_FILE_SIZE = 104857600 # 100 MB in bytes
logger = logging.getLogger("Demisto Archiving")
handler = RotatingFileHandler(log_file, maxBytes=LOG_FILE_SIZE, backupCount=10)
formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %I:%M:%S")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# Variables initialization
out = None
err = None
files_types = ['attachments', 'artifacts']
moved_files = {}
now = datetime.now()
str_now = now.strftime("%Y-%m-%d")
args = get_parser()
time_from = get_time(args.time_from) if args.time_from else get_time("2010/1/1")
time_to = get_time(args.time_to)
mtime_minus = f" -mtime -{(now-time_from).days + 1}" if (now-time_from).days != 0 else ""
mtime_plus = f" -mtime +{(now-time_to).days - 1}" if (now-time_to).days != 0 else ""
accounts_dir = "/var/lib/demisto/tenants"
available_accounts = get_available_accounts(accounts_dir, files_types) 
accounts = available_accounts if args.accounts[0].lower() == "all" else args.accounts
backup_dir = "/var/lib/demisto-archive/"

# OS commands initialization
demisto_status = "systemctl is-active demisto"
demisto_start = "systemctl start demisto"
demisto_stop = "systemctl stop demisto"
mv_files_to_backup_directory = partial("find {from_directory} -maxdepth 1 -type f{mtime_minus}{mtime_plus} -print0 | xargs --no-run-if-empty -0 mv -t {to_directory}".format, mtime_minus=mtime_minus, mtime_plus=mtime_plus)
archive_files = "tar -cf {file_name} {files_dir}"
delete_files = "rm -rf {files_dir}"
moved_files = {account: {file_type: False for file_type in files_types}  for account in accounts}

# validate input
validate_time(time_from, time_to, now)
validate_accounts(accounts, available_accounts)

# create backup directories
logger.debug("Started making backup directories.")
for account in accounts:
    for file_type in files_types:
        files_backup_dir = Path(backup_dir, account, file_type)
        create_directory(files_backup_dir)
logger.debug("Successfully created backup directories.")

try:
    # stop then check demisto service
    out, err = call_os(demisto_status)
    if err:
        raise OSCommandException(err)
    if out == "active":
        _, err = call_os(demisto_stop)
    if err:
        raise OSCommandException(err)
    out, err = call_os(demisto_status)
    if err:
        raise OSCommandException(err)
    if out != "inactive":
        raise OSCommandException(out)

    # move files to backup directory
    for account in accounts:
        for file_type in files_types:
            # initiate variables
            files_dir = Path(accounts_dir, account, file_type)
            files_backup_dir = Path(backup_dir, account, file_type)
            mv_files_command = mv_files_to_backup_directory(from_directory=files_dir, to_directory=files_backup_dir)

            # move files
            call_os(mv_files_command)
    
    # start the service
    call_os(demisto_start)
    
    # archive and delete the moved files
    for account in accounts:
        for file_type in files_types:
            # initiate variables
            account_backup_dir = Path(backup_dir, account)
            files_backup_dir = Path(account_backup_dir, file_type)
            # backup_files = " ".join(os.listdir(files_backup_dir))
            archive_file_name = f"{file_type}_{str_now}.tar.gz"
            archive_files_command = archive_files.format(files_dir=file_type, file_name=archive_file_name)
            delete_files_dir_command = delete_files.format(files_dir=file_type)

            # archive and delete files
            is_empty = len(os.listdir(files_backup_dir)) == 0
            
            if not is_empty:
                _, err = call_os(archive_files_command, cwd=account_backup_dir)
                if err:
                    continue
                call_os(delete_files_dir_command, cwd=account_backup_dir)
            else:
                is_empty = len(os.listdir(files_backup_dir)) == 0
                if is_empty:
                    call_os(delete_files_dir_command, cwd=Path(backup_dir, account))
    logger.debug("Done")
except Exception as err:
    logger.error(traceback.format_exc())
    raise OSCommandException(err)

