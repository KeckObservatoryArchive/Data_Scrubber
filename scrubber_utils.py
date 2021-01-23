import requests
import argparse
import logging
import os
import sys

from io import StringIO
from datetime import datetime, timedelta


def chk_file(file_location, filename=None):
    """
    Check the existence of the file, ie stage_file:
        /s/sdata125/hires6/2020nov23/hires0003.fits

    :param file_location: <str> path or path+filename for the file
    :param filename: <str> filename to add to path

    :return: <bool> does the file exist?
    """
    if not file_location:
        return False

    if filename:
        file_location = "/".join((file_location, filename))

    return os.path.exists(file_location)


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


def query_rti_api(url, qtype, type_val, val=None, columns=None, key=None,
                  add=None, utd=None, utd2=None):
    """
    Query the API to get or update information in the KOA RTI DB.

    :param url: <str> the API url.
    :param qtype: <str> type of query [search, update].
    :param type_val: <str> the query name,  [GENERAL, HEADER, etc].
    :param columns: <str> comma separated string of columns to return
    :param key: <str> the search key to match with val.
    :param val: <str> the value to match with search.
    :param add: <str> additional query parameters to add at end of query.
    :param utd: <str> the initial date, YYYY-MM-DD.
    :param utd2: <str> the final date, YYYY-MM-DD.
    :return:
    """

    if qtype not in ['search', 'update']:
        return None

    loc = locals()
    url = f"{url}?{qtype}={type_val}"

    for dict_key, dict_val in loc.items():
        if dict_val and dict_key not in ['url', 'qtype', 'type_val']:
            url += f"&{dict_key}={dict_val}"

    response = requests.get(url)
    results = response.content

    return results


def create_report(metrics):
    """
    Form the report to be emailed at the end of a scrub run.

    :param metrics: <dict> the values of files,  moved, removed, total.
    :return: <str> the report.
    """

    report = f"\nNumber of files archived: {metrics['n_results']}"
    if 'n_deleted' in metrics:
        report += f"\nNumber of files deleted: {metrics['n_deleted']}"
    if 'n_moved' in metrics:
        report += f"\nNumber of KOAIDs moved: {metrics['n_moved']}"
    report = f"Total number of files not previously deleted (any status): "
    report += f"{metrics['total_files']}"

    return report


def create_logger(name, logdir):
    """
    Set the logger for writing to a log file,  and capturing the
    warnings/errors to be sent in an email.

    :param name: <str> root logname,  date+time will be added as suffix.
    :param logdir: <str> the director for the log file to be written
    :return: <str> log_name (including date+time)
             <_io.StringIO> the log stream handler
    """
    now = datetime.now().strftime('%Y%m%d_%H:%M:%S')
    log_name = f'{name}_{now}'
    log_fullpath = f'{logdir}/{log_name}.log'
    try:
        #Create logger object
        logger = logging.getLogger(log_name)

        logger.setLevel(logging.DEBUG)

        #file handler (full debug logging)
        handler = logging.FileHandler(log_fullpath)
        handler.setLevel(logging.DEBUG)
        handler.suffix = "%Y%m%d"
        logger.addHandler(handler)

        fmt = '%(asctime)s - %(levelname)s: %(message)s'
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # stream handler
        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        handler.setLevel(logging.WARNING)
        formatter = logging.Formatter(' %(levelname)8s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    except:
        return None, None

    return log_name, log_stream


#TODO update default log directory
def parse_args():
    """
    Parse the command line arguments.

    :return: <obj> commandline arguments
    """
    now = datetime.now()
    parser = argparse.ArgumentParser(description="Run the Data Scrubber")

    parser.add_argument("--dev", action="store_true",
                        help="Only log the commands,  do not execute")
    parser.add_argument("--move", action="store_true",
                        help="move the processed DEP files from the lev0 to "
                             "the storage servers.")
    parser.add_argument("--remove", action="store_true",
                        help="delete the files from the instrument servers")
    parser.add_argument("--storagedir", type=str,
                        help="Change the path of the storage server from the"
                             " one in the configuration file.")
    parser.add_argument("--logdir", type=str, default='log',
                        help="Define the directory for the log.")
    parser.add_argument("--utd", type=str,
                        default=(now - timedelta(days=14)).strftime('%Y-%m-%d'),
                        help="Start date to process YYYY-MM-DD.")
    parser.add_argument("--utd2", type=str,
                        default=(now - timedelta(days=21)).strftime('%Y-%m-%d'),
                        help="End date to process YYYY-MM-DD.")
    parser.add_argument("--include_inst", type=str,
                        default=None,
                        help="comma separated list of instruments to include, "
                             "the default is all instruments.")
    parser.add_argument("--exclude_inst", type=str,
                        default=None,
                        help="comma separated list of instruments to exclude, "
                             "the default is to exclude no instruments.")
    return parser.parse_args()


def define_args(args):
    """
    Set the lists of instruments to include / exclude
    :param args: <class 'argparse.Namespace'> parsed command line arguments.
    :return: <list, list> the lists of included and excluded instruments.
    """
    exclude_insts = args.exclude_inst
    include_insts = args.include_inst
    if args.exclude_inst:
        exclude_insts = exclude_insts.replace(" ", "").split(",")
    if args.include_inst:
        include_insts = include_insts.replace(" ", "").split(",")

    return exclude_insts, include_insts


def get_config_param(config, section, param_name):
    """
    Function used to read the config file,  and exit if key or value does not
    exist.

    :param config: <class 'configparser.ConfigParser'> the config file parser.
    :param section: <str> the section name in the config file.
    :param param_name: <str> the 'key' of the parameter within the section.
    :return: <str> the config file value for the parameter.
    """
    try:
        param_val = config[section][param_name]
    except KeyError:
        err_msg = f"Check Config file, there is no parameter name - "
        err_msg += f"section: {section} parameter name: {param_name}"
        sys.exit(err_msg)

    if not param_val:
        err_msg = f"Check Config file, there is no value for "
        err_msg += f"section: {section} parameter name: {param_name}"
        sys.exit(err_msg)

    return param_val


def get_key_val(result_dict, key_name):
    """
    Use to avoid an error while accessing a key that does not exist.

    :param result_dict: (dict) dictionary to check
    :param key_name: (str) key name

    :return: dictionary value
    """
    if result_dict and key_name in result_dict:
        return result_dict[key_name]

    return None