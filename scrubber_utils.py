import requests
import argparse
import logging
import os
import sys
from glob import glob
import pexpect
import tempfile

import subprocess
from collections import namedtuple

from io import StringIO
from datetime import datetime, timedelta


def chk_file_exists(file_location, filename=None):
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


def glob_file_exists(file_location, filename=None):
    """
    Check the existence of the file, ie stage_file:
        /s/sdata125/hires6/2020nov23/hires0003.fits

    :param file_location: <str> path or path+filename for the file
    :param filename: <str> filename to add to path

    :return: <bool> does the file exist?
    """
    if not file_location:
        return []

    if filename:
        file_location = "/".join((file_location, filename))

    return glob(file_location)


def send_email(email_msg, mailto, mailfrom, mailserver, subject, log):
    """
    send an email if there are any warnings or errors logged.

    :param email_msg: <str> message to mail.
    :param config: <class 'configparser.ConfigParser'> the config file parser.
    """
    if not email_msg:
        return

    import smtplib

    msg = f"From: {mailfrom}\r\nTo: {mailto}\r\n"
    msg += f"Subject: {subject}\r\n\r\n{email_msg}"

    try:
        server = smtplib.SMTP(mailserver)
        server.sendmail(mailfrom, mailto, msg)
        server.quit()
    except Exception as err:
        log.warning(f"Error sending Email. Error: {err}.")


def query_rti_api(url, qtype, type_val, val=None, columns=None, key=None, inst=None,
                  utd=None, utd2=None, update_val=None, add=None, log=None, level=None):
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

    url = f"{url}?{qtype}={type_val}"

    # iterate through all the args passed
    loc = locals()
    for dict_key, dict_val in loc.items():
        if dict_val and dict_key not in ['url', 'qtype', 'type_val', 'log']:
            url += f"&{dict_key}={dict_val}"

    response = requests.get(url)
    results = response.content

    if log:
        log.info(f'API URL: {url}')

    return results


def exists_remote(host, path):
    cmd = ['ssh', host, 'ls', path]
    status = subprocess.call(cmd)
    if status == 0:
        return True
    if status == 1:
        return False
    raise Exception('SSH failed')


def create_rti_report(args, metrics, move, inst):
    """
    Form the report to be emailed at the end of a scrub run.

    :param metrics: <dict> the values of files,  moved, removed, total.
    :return: <str> the report.
    """

    report = f"\nRTI Data Scrubber Results for {inst.upper()} " \
             f"{args.utd} to {args.utd2}."

    header = "Totals"
    report += f"\n\n{header}" + "\n" + "-" * len(header)
    report += f"\n{metrics['total_koa_mv']} : Total KOA files moved."
    report += f"\n{metrics['total_storage_mv']} : Total Storage difference."

    header = "Number of results"
    report += f"\n\n{header}" + "\n" + "-" * len(header)
    report += f"\n{metrics['nresults']['mv0'][0]} : KOAID in results to move (lev0)."
    report += f"\n{metrics['nresults']['mv0'][1]} : verified results to move (lev0)."
    report += f"\n{metrics['nresults']['mv1'][0]} : KOAID in results to move (lev1)."
    report += f"\n{metrics['nresults']['mv1'][1]} : verified results to move (lev1)."
    report += f"\n{metrics['nresults']['mv2'][0]} : KOAID in results to move (lev2)."
    report += f"\n{metrics['nresults']['mv2'][1]} : verified results to move (lev2)."

    for val in {'mv0', 'del0', 'mv1'}:
        diff = metrics['nresults'][val][0] - metrics['nresults'][val][1]
        if diff > 0:
            report += f"\n\nErrors: "
            for err in metrics['warnings']:
                report += f"\n    {err}"

    if move:
        header = "Fits Files created by DEP on vm-koarti"
        report += f"\n\n{header}" + "\n" + "-" * len(header)
        report += f"\n{metrics['staged'][0]} : Stage files found."
        report += f"\n{metrics['staged'][1]} : Stage files moved."

    report += f"\n\nTotal number of KOAIDs not previously deleted (any status): "
    report += f"{metrics['total_files']}"

    return report


def create_sdata_report(args, metrics, inst):
    """
    Form the report to be emailed at the end of a scrub run.

    :param metrics: <dict> the values of files,  moved, removed, total.
    :return: <str> the report.
    """

    report = f"\nRTI Data Scrubber Results for {inst.upper()} " \
             f"{args.utd} to {args.utd2}."

    header = "Totals"
    report += f"\n\n{header}" + "\n" + "-" * len(header)
    report += f"\n{metrics['total_sdata_mv']} : Total SDATA files moved."

    header = "Number of results"
    report += f"\n\n{header}" + "\n" + "-" * len(header)

    report += f"\n{metrics['nresults']['sdata'][0]} : KOAID in sdata results to delete (lev0)."
    report += f"\n{metrics['nresults']['sdata'][1]} : verified sdata results to delete (lev0)."

    for val in {'sdata'}:
        diff = metrics['nresults'][val][0] - metrics['nresults'][val][1]
        if diff > 0:
            report += f"\n\nErrors: "
            for err in metrics['warnings']:
                report += f"\n    {err}"

    header = "Files on Instrument servers"
    report += f"\n\n{header}" + "\n" + "-" * len(header)
    report += f"\n{metrics['sdata'][0]} : OFNAME Files found."
    report += f"\n{metrics['sdata'][1]} : OFNAME Files deleted."

    report += f"\n\nTotal number of SDATA not previously deleted (any status): "
    report += f"{metrics['total_files']}"

    return report


def create_nightly_report(metrics, utd, utd2):
    """
    Form the report to be emailed at the end of a scrub run.

    :param metrics: <dict> the values of files,  moved, removed, total.
    :return: <str> the report.
    """

    report = f"KOA DEP Files moved to storage for dates: {utd} to {utd2}"
    report += f"\n\n{metrics['koa_before']} : Total KOA files BEFORE."
    report += f"\n{metrics['store_before']} : Total Storage files BEFORE."
    report += f"\n{metrics['koa_after']} : Total KOA files AFTER."
    report += f"\n{metrics['store_after']} : Total Storage files AFTER."

    diff_koa = metrics['koa_before'] - metrics['koa_after']
    diff_mv = metrics['store_after'] - metrics['store_before']

    report += f"\n\n{diff_koa} : Number of files removed from KOA."
    report += f"\n{diff_mv} : Number of files moved to storage.\n"

    return report


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


def clean_empty_dirs(root_dir, log):
    """
    Remove any empty directories below the root_dir

    :param root_dir: <str>
    :param log:
    :return:
    """
    cln_cmd = ['find', root_dir, '-depth', '-type', 'd', '-empty',
               '-exec', 'rmdir', '{}', ';']

    log.info(f"Cleaning directories at {root_dir}")

    try:
        subprocess.run(cln_cmd, stdout=subprocess.DEVNULL, check=True)
    except subprocess.CalledProcessError:
        log.warning(f"Error removing empty directories in: {root_dir}, "
                    f"line: {sys.exc_info()[-1].tb_lineno}")
        log.info(f"Failed clean command {cln_cmd}")
        return 0


def write_emails(config, report, log, log_stream=None, errors=None, prefix=''):
    """
    Finish up the scrubbers,  create and send the emails.

    :param config: the pointer to the config file.
    :param report: the report to send.
    :param log_stream: the logging stream.
    :param errors: <dict> error key,  koaid list val
    :param prefix: prefix for the subject of the email.
    """
    now = datetime.now().strftime('%Y-%m-%d')
    mailto = get_config_param(config, 'email', 'admin')
    mailfrom = get_config_param(config, 'email', 'from')
    mailserver = get_config_param(config, 'email', 'server')

    send_email(report, mailto, mailfrom, mailserver,
               f'{prefix} Scrubber Report: {now}', log)

    if log_stream:
        log_contents = log_stream.getvalue()
        log_stream.close()

        if log_contents:
            mailto = get_config_param(config, 'email', 'warnings')
            send_email(log_contents, mailto, mailfrom, mailserver,
                       f'{prefix} Scrubber Warnings: {now}', log)

    if errors:
        error_report = "ERRORS FOUND / KOAID\n\n"
        for err in errors.keys():
            error_report += f'ERROR: {err} \n {errors[err]}\n\n'

        mailto = get_config_param(config, 'email', 'warnings')
        send_email(error_report, mailto, mailfrom, mailserver,
                   f'{prefix} Scrubber Warnings: {now}', log)


def create_logger(name, logdir):
    """
    Set the logger for writing to a log file,  and capturing the
    warnings/errors to be sent in an email.

    :param logdir: <str> the director for the log file to be written
    :return: <str> log_name (including date+time)
             <_io.StringIO> the log stream handler
    """
    # now = datetime.now().strftime('%Y%m%d_%H:%M:%S')
    now = datetime.now().strftime('%Y%m')
    log_name = f'{name}_{now}'
    log_fullpath = f'{logdir}/{log_name}.log'
    print('log', log_fullpath)
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

    except Exception as err:
        print('Failed to create logger: {err}')
        return None, None

    return log_name, log_stream


def parse_args(config):
    """
    Parse the command line arguments.

    :return: <obj> commandline arguments
    """
    now = datetime.now()
    start = int(get_config_param(config, 'TIMEFRAME', 'start'))
    end = int(get_config_param(config, 'TIMEFRAME', 'end'))

    insts = get_config_param(config, 'inst_list', 'insts')
    insts = f'{insts}, {insts.lower()}'
    inst_set = set(insts.split(', '))

    parser = argparse.ArgumentParser(description="Run the Data Scrubber")

    parser.add_argument("--dev", action="store_true",
                        help="Only log the commands,  do not execute")
    parser.add_argument("--logdir", type=str,
                        help="Define the directory for the log.")
    parser.add_argument("--utd", type=str,
                        default=(now - timedelta(days=start)).strftime('%Y-%m-%d'),
                        help="Start date to process YYYY-MM-DD.")
    parser.add_argument("--utd2", type=str,
                        default=(now - timedelta(days=end)).strftime('%Y-%m-%d'),
                        help="End date to process YYYY-MM-DD.")
    parser.add_argument("--inst", type=str, choices=inst_set, required=True,
                        help="Name of instrument to run the scrubber for.")

    return parser.parse_args()


def define_insts(include, exclude):
    """
    Set the lists of instruments to include / exclude

    :param include: <str> a comma separated string of instruments to include
    :param exclude: <str> a comma separated string of instruments to exclude

    :return: <list, list> the lists of included and excluded instruments.
    """
    exclude_insts = []
    include_insts = []
    if exclude:
        exclude_insts = exclude.replace(" ", "").split(",")
    if include:
        include_insts = include.replace(" ", "").split(",")

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


def remote_df(user, ip, path):
    """
    Executes df on remote host and return
    (total, free, used) as int in bytes
    """
    Result = namedtuple('diskfree', 'total used free')
    output = subprocess.check_output(['ssh', f'{user}@{ip}', '-C', 'df', path],
                                     shell=False)
    output = output.splitlines()
    for line in output[1:]:
        result = line.decode().split()

        if result[-1] == path and len(result) > 3:
            used = result[2]
            free = result[3]
            total = used + free
            return Result(total, used, free)
        else:
            raise Exception('Path "%s" not found' % path)


def inst_disk_usage_ok(inst, config, config_type, log):
    """
    Determine if the used disk space is less than the free disk space.

    :param inst: <str> the instrument name
    :param config: <class 'configparser.ConfigParser'>
        the pointer to the config file
    :param config_type: <str> either dev or default
    :param log: <class 'logging.Logger'> the log
    :return: <bool> True if disk used < disk free
    """
    koa_disk, storage_disk = get_locations(inst, config, config_type)
    user = get_config_param(config, config_type, 'user')
    server = get_config_param(config, 'servers', 'user')

    stats = remote_df(user, server, koa_disk)
    log.info(f'Disk Space Statistics for {inst} in {server}: {koa_disk}.')
    log.info(f'Total: {stats.total}, Used: {stats.used}, Free: {stats.free}')

    return stats.used < stats.free


def get_locations(inst, config, config_type):
    """
    Determine the directories on each of the disks.

    :param inst: <str> the instrument
    :param config: the config file pointer
    :return: <(str,str)> the paths to the files
    """
    koa_disk_root = get_config_param(config, 'koa_disk', 'path_root')
    koa_disk_num = get_config_param(config, 'koa_disk', inst)
    storage_disk_root = get_config_param(config, config_type, 'storage_root')
    storage_disk_num = get_config_param(config, 'storage_disk', inst)

    koa_disk = f'{koa_disk_root}{koa_disk_num}'
    storage_disk = f'{storage_disk_root}{storage_disk_num}'

    return koa_disk, storage_disk


def make_remote_dir(host, dir_name, log):
    """
    Create a directory on a remote server.

    :param host: <str> user@server_name
    :param dir_name: <str> the directory to create
    :param log: <class 'logging.Logger'> the log
    :return:
    """
    cmd = ["ssh", host, "mkdir", "-p", dir_name]
    log.info(f"created directory: {host}:{dir_name}")
    subprocess.run(cmd, stdout=subprocess.DEVNULL, check=True)


def make_storage_dir(storage_dir, storage_root, log):
    """
    Create the storage directory.  If it does not exists,  go up
    creating directories in the path.

    :param storage_dir: <str>
        ie: /koadata/test_storage/koastorage02/KCWI/koadata28/20210116/lev0/
    :return: <int> status,  1 on success 0 on failure
    """
    try:
        os.mkdir(storage_dir)
        log.info(f"created directory: {storage_dir}")
    except FileExistsError:
        return 1
    except FileNotFoundError:
        log.info(f"Directory: {storage_dir}, does not exist yet.")
        one_down = '/'.join(storage_dir.split('/')[:-1])
        if len(one_down) > len(storage_root):
            make_storage_dir(one_down, storage_root, log)
        else:
            return 0
    except:
        return 0

    # rewind
    return_val = make_storage_dir(storage_dir, storage_root, log)

    return return_val


def count_koa(files_path, log):
    """
    Count the files to be moved (descend into sub-directories).
    These are the local KOA files.

    :param files_path: <str> the path to the KOA (DEP) files.
    :param log: <class 'logging.Logger'> the log

    :return: <int> the number of files found at files_path
    """
    n_koa = 0
    for _, _, files in os.walk(files_path):
        n_koa += len(files)

    log.info(f"{n_koa} : files at {files_path}.")

    return n_koa


def count_store(user, store_server, store_path, utd, log):
    """
    Count the files on the remote storage server.

    :param user:
    :param store_server:
    :param store_path: <str> the path to store the files.
    :param utd: <str> date YYYYMMDD
    :param log: <class 'logging.Logger'> the log

    :return: the file count for the directory
    """
    n_store = 0
    cmd = ['ssh', f'{user}@{store_server}', 'find',
           f'{store_path}/{utd}/', '-type', 'f', '|', 'wc', '-l']

    try:
        n_store = int(subprocess.check_output(cmd).decode('utf-8'))
    except Exception as err:
        log.warning(f'Error: {err} line: {sys.exc_info()[-1].tb_lineno}')
        log.warning(f'Could not count files for: {store_path}')

    log.info(f"{n_store} : files at {store_server}:{store_path}/{utd}")

    return n_store


def diff_list(list1, list2):
    """
    Determine the different elements between two lists

    :param list1: <list> list one
    :param list2: <list> list two

    :return: <list> a list of the different elements between the two lists.
    """
    return [i for i in list1 + list2 if i not in list1 or i not in list2]


def dir_exists(user, store_server, store_path, utd):
    """
    Check if directory exists on remote server.

    :param user:
    :param store_server:
    :param store_path: <str> the path to store the files.
    :param utd: <str> date YYMMDD
    :return: <bool> 1 if file exists
    """
    cmd = ['ssh', f'{user}@{store_server}', 'test', '-d',
           f'{store_path}/{utd}/', '&&', 'echo', '1', '||', 'echo', '0']

    return int(subprocess.check_output(cmd).decode('utf-8'))


def count_files(path_str):
    """
    Count the files in directory with a wildcard.

    :param path_str: <str> the path + pattern to match
    :return: <int> the number of files matching search criteria
    """
    return len(glob(path_str))


def count_koa_files(args):
    '/koadata/NIRES/20210223/lev0/'
    '/koadata/NIRES/stage/20210223/s/sdata1500/nires9/2021feb23/s210223_0002.fits'
    utd2 = int(args.utd2.replace('-', ''))
    diff = utd2 - int(args.utd.replace('-', ''))

    sfiles = 0
    ofiles = 0
    for i in range(0, diff + 1):
        utd = utd2 - i

        sfile_list = [rslt for rslt in
                      glob(f'/koadata/*/stage/{utd}/**', recursive=True)
                      if not os.path.isdir(rslt)]

        ofile_list = [rslt for rslt in
                       glob(f'/koadata/*/{utd}/**', recursive=True)
                       if not os.path.isdir(rslt)]

        sfiles += len(sfile_list)
        ofiles += len(ofile_list)

    return sfiles + ofiles


def determine_storage(koaid, config, config_type, level=0, ofname=None):
    """
    Find the storage directory from the KOAID.

    :param koaid: <str> <inst>.utd.#####.## (ie: KB.20210116.57436.94)
    :return: <str> full path to storage directory (including lev0)
    """
    id_parts = koaid.split('.')
    if len(id_parts) != 4:
        return None

    inst = get_config_param(config, 'inst_prefix', id_parts[0])
    utd = id_parts[1]

    storage_root = get_config_param(config, config_type, 'storage_root_rti')
    store_num = get_config_param(config, 'storage_disk', inst)
    koa_num = get_config_param(config, 'koa_disk', inst)
    koa_root = get_config_param(config, 'koa_disk', 'path_root')

    storage_path = f"{storage_root}{store_num}/{inst}/"

    # storing stage files
    if ofname:
        dirs = ofname.split('/')
        if 'fits' not in dirs[-1]:
            return None
        s_root = '/'.join(dirs[:-1])
        storage_path += f"stage/{inst}/{utd}/{s_root}"

    # storing lev0 and lev1 files
    else:
        storage_path += f"{koa_root}{koa_num}/{utd}/lev{level}/"

    return storage_path


def execute_remote_cmd(host, cmd, user, password, timeout=30, bg_run=False):
    """SSH'es to a host using the supplied credentials and executes a command.
    Throws an exception if the command doesn't return 0.
    bgrun: run command in the background"""

    fname = tempfile.mktemp()
    fout = open(fname, 'w')

    options = '-q -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null -oPubkeyAuthentication=no'
    if bg_run:
        options += ' -f'
    ssh_cmd = 'ssh %s@%s %s "%s"' % (user, host, options, cmd)
    child = pexpect.spawnu(ssh_cmd,timeout=timeout)  # spawnu for Python 3
    child.expect(['[pP]assword: '])
    child.sendline(password)
    child.logfile = fout
    child.expect(pexpect.EOF)
    child.close()
    fout.close()

    fin = open(fname, 'r')
    stdout = fin.read()
    fin.close()

    if 0 != child.exitstatus:
        raise Exception(stdout)

    return stdout
