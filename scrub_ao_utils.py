import os
import argparse
import logging
import subprocess
from datetime import datetime, timedelta
from io import StringIO


def parse_args():
    now = datetime.now().strftime('%Y%m%d')

    parser = argparse.ArgumentParser(description='Move KOA Data')
    parser.add_argument('--tel', type=int, help='Telescope Number (1 or 2)',
                        default=1)
    parser.add_argument('--nscrub', type=int, help='Number of days to scrub',
                        default=90)
    parser.add_argument('--ncopy', type=int, help='Number of days to copy',
                        default=90)
    parser.add_argument("--utd", type=str, default=now,
                        help="Start date to process YYYY-MM-DD.")
    parser.add_argument("--dev", action="store_true",
                        help="Only log the commands,  do not execute")

    return parser.parse_args()


def run_cmd(cmd, log):
    """
    Run a system command.

    :param cmd: <list> the command list ready for subprocess.
    :param log: <log> the log file pointer.
    :return: <int> 0 on success,  -1 on error.
    """
    try:
        log.info(f"cmd: {cmd}")
        subprocess.run(cmd, stdout=subprocess.DEVNULL, check=True)
    except subprocess.CalledProcessError:
        log.warning(f"cmd failed: {cmd}")
        return -1

    return 0


def next_date(start_date):
    """
    Iterator to provide the next date as a datetime.

    :param start_date: <datetime> the initial date as a datetime object.
    :return: <datetime> yield the next date as a datetime object.
    """
    return_date = start_date

    while True:
        yield return_date
        return_date += timedelta(days=1)


def count_local(files_path, log):
    """
    Count the files to be moved (descend into sub-directories).
    These are the local KOA files.

    :param files_path: <dict> the path to the 'summit and 'hq' files.
    :param log: <class 'logging.Logger'> the log

    :return: <int> the number of files found at files_path
    """
    cnt = {}
    for keys in files_path:
        n_files = 0
        for _, _, files in os.walk(files_path[keys]):
            n_files += len(files)
        cnt[keys] = n_files

    log.info(f"File count: {cnt}")

    return cnt


def create_logger(name, logdir):
    """
    Set the logger for writing to a log file,  and capturing the
    warnings/errors to be sent in an email.

    :param logdir: <str> the director for the log file to be written
    :return: <str> log_name (including date+time)
             <_io.StringIO> the log stream handler
    """
    now = datetime.now().strftime('%Y%m%d_%H:%M:%S')
    log_name = f'{name}_{now}'
    log_fullpath = f'{logdir}/{log_name}.log'

    try:
        #Create logger object
        log = logging.getLogger(log_name)

        log.setLevel(logging.DEBUG)

        #file handler (full debug logging)
        handler = logging.FileHandler(log_fullpath)
        handler.setLevel(logging.DEBUG)
        handler.suffix = "%Y%m%d"
        log.addHandler(handler)

        fmt = '%(asctime)s - %(levelname)s: %(message)s'
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        log.addHandler(handler)

        # stream handler
        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        handler.setLevel(logging.WARNING)
        formatter = logging.Formatter(' %(levelname)8s: %(message)s')
        handler.setFormatter(formatter)
        log.addHandler(handler)
    except:
        return None, None

    return log_name, log_stream


def write_emails(log_stream, mailto, prefix=''):
    """
    Finish up the scrubbers,  create and send the emails.

    :param config: the pointer to the config file.
    :param log_stream: the logging stream.
    :param report: the report to send.
    :param prefix: prefix for the subject of the email.
    """
    now = datetime.now().strftime('%Y-%m-%d')

    if log_stream:
        log_contents = log_stream.getvalue()
        log_stream.close()

        if log_contents:
            send_email(log_contents, mailto,
                       f'{prefix} Nightly Directory Scrubber Warnings: {now}')


def send_email(email_msg, mailto, subject):
    """
    send an email if there are any warnings or errors logged.

    :param email_msg: <str> message to mail.
    :param config: <class 'configparser.ConfigParser'> the config file parser.
    """
    import smtplib
    mailfrom = 'data_scrubber@keck.hawaii.edu'

    msg = f"From: {mailfrom}\r\nTo: {mailto}\r\n"
    msg += f"Subject: {subject}\r\n\r\n{email_msg}"

    server = smtplib.SMTP('mail.keck.hawaii.edu')
    server.sendmail(mailfrom, mailto, msg)
    server.quit()