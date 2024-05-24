import os
import logging
import argparse
import configparser
from datetime import datetime, timedelta

import scrubber_utils as utils

APP_PATH = os.path.abspath(os.path.dirname(__file__))
CONFIG_FILE = f'{APP_PATH}/scrub_sdata_config.live.ini'


def remove_guider_imgs(direct, utd2):
    utd_obj = datetime.strptime(utd2, '%Y-%m-%d')

    server = utils.get_config_param(config, 'servers', 'KPF')
    cnt = 0
    expected = 0

    for root, dirs, files in os.walk(direct):
        for file in files:
            try:
                file_path = os.path.join(root, file)
                creation_time = os.path.getctime(file_path)
                creation_date = datetime.fromtimestamp(creation_time)
            except Exception as err:
                log.warning(f'Error getting time: {err}')
                continue

            # uncomment to see what it is doing
            if creation_date < utd_obj:
                # remove_path = '/' + file_path.lstrip('/').lstrip('s')
                # cmd = f'/bin/rm {remove_path}'
                cmd = f'/bin/rm {file_path}'
                if '.fits' not in cmd:
                    continue

                cnt += remove_file(log, server, cmd, file_path)
                expected += 1

    return cnt, expected

def remove_file(log, server, cmd, local_path):
    # remove_path = '/' + local_path.lstrip('/').lstrip('s')
    try:
        std_out = utils.execute_remote_cmd(server, cmd, account, pw)
    except Exception as err:
        log.warning(f'exception in executing remote command: {err}')
        log.warning(f'error with command: {server} {cmd} {account} {pw}')
        return 0

    # check that it was removed locally (with /s)
    if utils.chk_file_exists(local_path):
        # log.error(f"File not removed,  check path: {remove_path}")
        log.error(f"File not removed,  check path: {local_path}, "
                  f"ssh/rm output: {std_out}")
        return 0

    # log.info(f"File removed from: {remove_path}")
    log.info(f"File removed from: {local_path}")

    return 1


def setup_log(config):
    if not args.logdir:
        log_dir = utils.get_config_param(config, config_type, 'log_dir')
    else:
        log_dir = args.logdir

    log_name, log_stream = utils.create_logger('kpf_guide_scrubber', log_dir)
    log = logging.getLogger(log_name)
    print(f'writing log to: {log_dir}/{log_name}')

    return log


def parse_args(config, inst):
    """
    Parse the command line arguments.

    :return: <obj> commandline arguments
    """
    now = datetime.now()

    parser = argparse.ArgumentParser(description="Run the Data Scrubber")

    parser.add_argument("--dev", action="store_true",
                        help="Only log the commands,  do not execute")
    parser.add_argument("--logdir", type=str,
                        help="Define the directory for the log.")

    # add inst specific start/end ndays from the config if exist
    args, unknown_args = parser.parse_known_args()

    try:
        start = int(utils.get_config_param(config, 'TIMEFRAME', f'{inst.lower()}_start'))
        end = int(utils.get_config_param(config, 'TIMEFRAME', f'{args.inst.lower()}_end'))
    except:
        start = int(utils.get_config_param(config, 'TIMEFRAME', 'start'))
        end = int(utils.get_config_param(config, 'TIMEFRAME', 'end'))

    parser.add_argument("--utd", type=str,
                        default=(now - timedelta(days=start)).strftime('%Y-%m-%d'),
                        help="Start date to process YYYY-MM-DD.")
    parser.add_argument("--utd2", type=str,
                        default=(now - timedelta(days=end)).strftime('%Y-%m-%d'),
                        help="End date to process YYYY-MM-DD.")

    return parser.parse_args()


if __name__ == '__main__':
    """
    to run:
        python scrub_kpf_guide.py --utd 2021-02-11 --utd2 2021-02-12
    """

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    args = parse_args(config, 'KPF')
    print(args)

    if args.dev:
        config_type = 'DEV'
    else:
        config_type = 'DEFAULT'

    direct = utils.get_config_param(config, config_type, 'kpf_guide_dir')
    account = utils.get_config_param(config, config_type, 'kpf_account')
    pw = utils.get_config_param(config, 'passwords', 'eng_account')

    log = setup_log(config)
    log.info(f"Scrubbing kpfguider images created before: {args.utd2}\n")
    log.info(f"directory: {direct}.\n")

    cnt, expected_cnt = remove_guider_imgs(direct, args.utd2)

    if cnt != expected_cnt:
        report = f"The count from the KPF Guide Scrubber does not match,  " \
                 f"expected to be deleted: {expected_cnt},  actually " \
                 f"deleted; {cnt}"
        utils.write_emails(config, report, log, prefix=f'KPF GUIDE SDATA')
