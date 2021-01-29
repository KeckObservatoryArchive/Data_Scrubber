import argparse
import configparser
import logging
import subprocess
from os import path
from datetime import datetime, timedelta
import scrubber_utils as utils


APP_PATH = path.abspath(path.dirname(__file__))
CONFIG_FILE = f'{APP_PATH}/scrubber_config.ini'


class StoreData:
    def __init__(self, inst):
        self.log = logging.getLogger(log_name)
        self.inst = inst
        self.koa_disk = None
        self.storage_disk = None

    def move_data(self, utd1, utd2):
        log.info(f"moving dates: {utd1} - {utd2} for instruments: {self.inst}")
        if type(self.inst) == list:
            for inst in self.inst:
                self.disk_usage_ok(inst)
                self.move_inst_data(utd1, utd2, inst)
        else:
            self.disk_usage_ok(self.inst)
            self.move_inst_data(utd1, utd2, self.inst)

    def move_inst_data(self, utd1, utd2, inst):
        funcs = [self.stage_data_loc, self.processed_data_loc,
                 self.log_files_loc]
        start = datetime.strptime(utd1, '%Y-%m-%d')
        end = datetime.strptime(utd2, '%Y-%m-%d')
        diff = end - start

        dirs_made = []
        for i in range(diff.days + 1):
            utd = (start + timedelta(i)).strftime('%Y%m%d')
            log.info(f"working on {inst} date: {utd}")
            for func in funcs:
                self.koa_disk, self.storage_disk = utils.get_locations(inst, config, config_type)
                files_path, store_path = func(utd, inst)

                if store_path not in dirs_made:
                    if utils.make_storage_dir(store_path, storage_root, log) == 0:
                        log.warning(f"Error creating storage dir: {store_path}")
                        continue
                    dirs_made.append(store_path)

                self._rsync_files(files_path, store_path)

    @staticmethod
    def _rsync_files(files_path, store_path):
        """
        rsync all the files to bring them to storage.

        :param files_path: <str> the archive path or the DEP files.
        :param store_path: <str> the path to store the files.
        """
        if not args.dev:
            log.warning("Not ready to start moving files: use --dev")
            return

        # TODO
        # command = 'rsync --remove-source-files -av -e ssh koaadmin@vm-koaserver5:FROMLOC TOLOC'

        rsync_cmd = ["rsync", "-ave", "ssh",
                     "koaadmin@vm-koaserver5:" + files_path, store_path]

        try:
            log.info(f"rsync cmd: {rsync_cmd}")
            # subprocess.run(rsync_cmd, stdout=subprocess.DEVNULL, check=True)
        except subprocess.CalledProcessError:
            log.warning(f"Move failed: {rsync_cmd}")
            return 0

        return 1

    def disk_usage_ok(self, inst):
        return utils.inst_disk_usage_ok(inst, config, config_type, log)

    def log_files_loc(self, utd, inst):
        files_path = f'{self.koa_disk}/{inst}/dep_{inst}_{utd}.log'
        store_path = f'{self.storage_disk}/{inst}/logs/'

        return files_path, store_path

    def processed_data_loc(self, utd, inst):
        files_path = f'{self.koa_disk}/{inst}/{utd}'
        store_path = f'{self.storage_disk}/{inst}/{self.koa_disk}/'

        return files_path, store_path

    def stage_data_loc(self, utd, inst):
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
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    args = parse_args()

    if args.dev:
        config_type = "DEV"
    else:
        config_type = "DEFAULT"

    site = utils.get_config_param(config, config_type, 'site')
    storage_root = utils.get_config_param(config, config_type, 'storage_root')

    log_dir = utils.get_config_param(config, config_type, 'log_dir')
    log_name, log_stream = utils.create_logger('koa_scrubber', log_dir)
    log = logging.getLogger(log_name)

    delete_obj = StoreData(args.inst)
    delete_obj.move_data(args.utd, args.utd2)
