import argparse
import configparser
import logging
import subprocess
from os import path
from datetime import datetime, timedelta
import scrubber_utils as utils


APP_PATH = path.abspath(path.dirname(__file__))
CONFIG_FILE = f'{APP_PATH}/scrubber_config.live.ini'


class StoreData:
    def __init__(self, inst):
        self.log = logging.getLogger(log_name)
        self.inst = inst
        self.koa_disk = None
        self.storage_disk = None
        self.n_koa_before = 0
        self.n_koa_after = 0
        self.n_store_before = 0
        self.n_store_after = 0

    def get_metrics(self):
        """
        Access to the file counts.

        :return: <dict> the count,  before and after moving files.
        """
        metrics = {'store_before': self.n_store_before,
                   'store_after': self.n_store_after,
                   'koa_before': self.n_koa_before,
                   'koa_after': self.n_koa_after}

        return metrics

    def move_data(self, utd1, utd2):
        """
        Move the data.

        :param utd1: <str> date YYYYMMDD
        :param utd2: <str> date YYYYMMDD
        """
        log.info(f"moving dates: {utd1} - {utd2} for instruments: {self.inst}")
        if type(self.inst) == list:
            for inst in self.inst:
                utils.inst_disk_usage_ok(inst, config, config_type, log)
                self.move_inst_data(utd1, utd2, inst)
        else:
            utils.inst_disk_usage_ok(self.inst, config, config_type, log)
            self.move_inst_data(utd1, utd2, self.inst)

    def move_inst_data(self, utd1, utd2, inst):
        """
        The main move function to move KOA (DEP) files from local directories
        to the remote storage server.

        :param utd1: <str> date YYYYMMDD
        :param utd2: <str> date YYYYMMDD
        :param inst: <str> the instrument name
        """
        funcs = [self.stage_data_loc, self.processed_data_loc,
                 self.log_files_loc]
        len_funcs = len(funcs) - 1
        start = datetime.strptime(utd1, '%Y-%m-%d')
        end = datetime.strptime(utd2, '%Y-%m-%d')
        diff = end - start

        dirs_made = []
        for i in range(diff.days + 1):
            utd = (start + timedelta(i)).strftime('%Y%m%d')
            log.info(f"working on {inst} date: {utd}\n")

            for itr, func in enumerate(funcs):
                self.koa_disk, self.storage_disk = utils.get_locations(
                    inst, config, config_type)
                files_path, store_path = func(utd, inst)

                if store_path not in dirs_made:
                    utils.make_remote_dir(f'{user}@{store_server}',
                                          store_path, log)
                    dirs_made.append(store_path)

                if itr != len_funcs:
                    self.n_koa_before += utils.count_koa(files_path, log)
                    self.n_store_before += utils.count_store(
                        user, store_server, store_path, utd, log)

                self._rsync_files(files_path, store_path)

                if itr != len_funcs:
                    self.n_koa_after += utils.count_koa(files_path, log)
                    self.n_store_after += utils.count_store(
                        user, store_server, store_path, utd, log)

    @staticmethod
    def _rsync_files(files_path, store_path):
        """
        rsync all the files to bring them to storage.

        :param files_path: <str> the archive path or the DEP files.
        :param store_path: <str> the path to store the files.

        :return: <int> 0 on success, 1 == not moved
        """
        if not utils.chk_file_exists(files_path):
            log_str = f'skipping file {files_path} -- already moved'
            log_str += ' or does not exist.'
            log.info(log_str)
            return 1

        cln_cmd = None
        if config_type == "DEV":
            rsync_cmd = ["rsync", "-avz", "-e", "ssh", files_path,
                         f'{user}@{store_server}:{store_path}']
        else:
            rsync_cmd = ["rsync", "--remove-source-files", "-avz", "-e", "ssh",
                         files_path, f'{user}@{store_server}:{store_path}']
            if '.log' not in files_path:
                cln_cmd = ['find', files_path, '-depth', '-type', 'd',
                           '-empty', '-exec', 'rmdir', '{}', ';']

        try:
            log.info(f"rsync cmd: {rsync_cmd}")
            subprocess.run(rsync_cmd, stdout=subprocess.DEVNULL, check=True)
            if cln_cmd:
                log.info(f"cleaning directories: {cln_cmd}")
                subprocess.run(cln_cmd, stdout=subprocess.DEVNULL, check=True)
        except subprocess.CalledProcessError:
            log.warning(f"Move failed: {rsync_cmd}")
            return 1

        return 0

    def log_files_loc(self, utd, inst):
        """
        Compose the path to the KOA log files.

        :param utd: <str> date YYYYMMDD
        :param inst: <str> the instrument name
        :return: <(<str>, <str>)> KOA log path,  path to storage
        """
        files_path = f'{self.koa_disk}/{inst}/dep_{inst}_{utd}.log'
        store_path = f'{self.storage_disk}/{inst}/logs/'

        return files_path, store_path

    def processed_data_loc(self, utd, inst):
        """
        Compose the path to the Processed (DEP) KOA files.

        :param utd: <str> date YYYYMMDD
        :param inst: <str> the instrument name
        :return: <(<str>, <str>)> KOA DEP path,  path to storage
        """
        files_path = f'{self.koa_disk}/{inst}/{utd}'
        store_path = f'{self.storage_disk}/{inst}/{self.koa_disk}/'

        return files_path, store_path

    def stage_data_loc(self, utd, inst):
        """
        Compose the path to the KOA stage files.

        :param utd: <str> date YYYYMMDD
        :param inst: <str> the instrument name
        :return: <(<str>, <str>)> KOA stage path,  path to storage
        """
        files_path = f'{self.koa_disk}/stage/{inst}/{utd}'
        store_path = f'{self.storage_disk}/{inst}/stage/{inst}/'

        return files_path, store_path


def parse_args():
    inst_list = list(utils.get_config_param(config, 'inst_list', 'insts').split(', '))
    now = datetime.now()

    parser = argparse.ArgumentParser(description='Move KOA Data')
    parser.add_argument('--inst', type=str, help='Instrument Name',
                        default=inst_list)
    parser.add_argument("--utd", type=str,
                        default=(now - timedelta(days=21)).strftime('%Y-%m-%d'),
                        help="Start date to process YYYY-MM-DD.")
    parser.add_argument("--utd2", type=str,
                        default=(now - timedelta(days=14)).strftime('%Y-%m-%d'),
                        help="End date to process YYYY-MM-DD.")
    parser.add_argument("--dev", action="store_true",
                        help="Only log the commands,  do not execute")

    return parser.parse_args()


if __name__ == '__main__':
    """
    To run:
        python3 scrub_koa_nightly.py --dev
    """
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    args = parse_args()

    if args.dev:
        config_type = "DEV"
    else:
        config_type = "DEFAULT"

    site = utils.get_config_param(config, config_type, f'site_{args.tel}')
    user = utils.get_config_param(config, config_type, 'user')
    store_server = utils.get_config_param(config, config_type, 'store_server')
    storage_root = utils.get_config_param(config, config_type, 'storage_root')

    log_dir = utils.get_config_param(config, config_type, 'log_dir')
    log_name, log_stream = utils.create_logger('koa_scrubber', log_dir)
    log = logging.getLogger(log_name)

    delete_obj = StoreData(args.inst)
    delete_obj.move_data(args.utd, args.utd2)

    # send a report of the scrub
    metrics = delete_obj.get_metrics()
    report = utils.create_nightly_report(metrics, args.utd, args.utd2)
    log.info(report)
    utils.write_emails(config, report, log_stream=log_stream, prefix='KOA')
