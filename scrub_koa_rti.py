import os
import sys
import configparser
import logging
import json
import subprocess
import scrubber_utils as utils

#temporary for KPF
# from datetime import datetime

APP_PATH = os.path.abspath(os.path.dirname(__file__))
CONFIG_FILE = f'{APP_PATH}/scrubber_config.live.ini'


class ToDelete:
    def __init__(self, inst):
        self.utd = args.utd
        self.utd2 = args.utd2
        self.log = logging.getLogger(log_name)
        self.db_obj = ChkArchive(inst)
        self.to_move = self.db_obj.get_files_to_move()
        self.dirs_made = []
        self.lev1_moved = []
        self.lev2_moved = []
        self.metrics = {'staged': [0, 0], 'koaid': [0, 0],
                        'inst': [0, 0], 'nresults': self.db_obj.get_nresults(),
                        'warnings': self.db_obj.get_warnings()}

        if config_type == "DEV":
            self.rm = ""
        else:
            self.rm = "--remove-source-files"

    def get_metrics(self):
        return self.metrics

    def del_mv(self, file_type, func):
        """
        Skeleton function to delete or move a file list,  file by file.

        :return: <list<int>,<int>> number moved/deleted, number in list.
        """
        file_list = self.db_obj.get_files_to_move(file_type=file_type)
        if not file_list:
            return [0, 0]

        nfiles_found = len(file_list)

        n_files_touched = 0
        for result in file_list:
            # rsync will return 1 per file,  when it succeeds, 0 fails,
            # -1 if file not found
            ret_val = func(result)
            if ret_val < 0:
                nfiles_found += ret_val
            else:
                n_files_touched += ret_val

        return [nfiles_found, n_files_touched]

    def store_lev0_func(self, result):
        """
        move the files matching koaid to storage.

        :param result: <dict> single db row,  the query result for the file.
        :return: <int> 1 if file removed successfully,  or 1
        """
        koaid = result['koaid']
        mv_path = result['process_dir']

        storage_dir = self.get_storage_dir(koaid, mv_path)
        if not storage_dir:
            return 0

        log.info(f'running store lev0,  storage dir {storage_dir}')

        return_val = self._rsync_files(mv_path, storage_dir, koaid)

        if not self.add_archived_dir(koaid, storage_dir, level=0):
            self.log.warning(f"archive_dir not set for {koaid}")

        log.info('running store stage')

        # TODO temporarily store component files for KPF
        ofname = result['ofname']
        if args.inst == 'KPF' and 'L0' in ofname:
            storage_root = '/s/sdata1701'
            storage_dir = ofname.replace(storage_root, '/instr1/KPF')
            storage_dir = storage_dir.split('L0')[0]

            # make directory if it doesn't exist
            utils.make_remote_dir('koaadmin@storageserver', storage_dir, log)

            kpf_components = utils.kpf_component_files(ofname, storage_dir, log)
            if kpf_components:
                # copy, don't remove the files
                orig_rm = self.rm
                self.rm = ''
                for mv_path in kpf_components:
                    component_name = mv_path.split('/')[-2]
                    storage_now = f'{storage_dir}/{component_name}/'
                    if not utils.exists_remote('koaadmin@storageserver', storage_now):
                        utils.make_remote_dir('koaadmin@storageserver', storage_now, log)
                    log.info(f'component directories {mv_path} {storage_now}')
                    self._rsync_files(mv_path, storage_now)

                # copy the L0 file as well
                storage_now = f'{storage_dir}/L0/'
                if not utils.exists_remote('koaadmin@storageserver', storage_now):
                    utils.make_remote_dir('koaadmin@storageserver', storage_now, log)
                self._rsync_files(ofname, storage_now)

                self.rm = orig_rm

        return return_val


    def store_lev1_func(self, result):
        """
        move the files matching koaid to storage.

        :param result: <dict> single db row,  the query result for the file.
        :return: <int> 1 if file removed successfully,  or 1
        """
        return_val = 0
        koaid = result['koaid']
        mv_path = result['process_dir']

        if 'lev1' not in mv_path:
            self.log.warning(f"lev1 path format is incorrect: {mv_path}")
            return -1

        storage_dir = self.get_storage_dir(koaid, mv_path, level=1)
        if not storage_dir:
            return -1

        log.info(f'running store lev1,  storage dir {storage_dir}')

        if mv_path not in self.lev1_moved:
            return_val = self._rsync_files(mv_path, storage_dir)
            self.lev1_moved.append(mv_path)

        if not self.add_archived_dir(koaid, storage_dir, level=1):
            self.log.warning(f"archive_dir not set for {koaid}")

        return return_val

    def store_lev2_func(self, result):
        """
        move the files matching koaid to storage.

        :param result: <dict> single db row,  the query result for the file.
        :return: <int> 1 if file removed successfully,  or 1
        """
        return_val = 0
        koaid = result['koaid']
        mv_path = result['process_dir']

        if 'lev2' not in mv_path:
            self.log.warning(f"lev2 path format is incorrect: {mv_path}")
            return -1

        storage_dir = self.get_storage_dir(koaid, mv_path, level=2)
        if not storage_dir:
            return -1

        log.info(f'running store lev2,  storage dir {storage_dir}')

        if mv_path not in self.lev2_moved:
            return_val = self._rsync_files(mv_path, storage_dir)
            self.lev2_moved.append(mv_path)

        if not self.add_archived_dir(koaid, storage_dir, level=2):
            self.log.warning(f"archive_dir not set for {koaid}")

        return return_val

    def store_stage_func(self, result):
        """
        move the stage file to storage.  The 'stage_file' path/filename to
        move is stored in the db record['stage_file']

        :param result: <dict> single db row,  the query result for the file.
        :return: <int> 1 if file removed successfully,  or 1
        """
        koaid = result['koaid']
        mv_path = result['stage_file']
        ofname = result['ofname']

        storage_dir = self.get_storage_dir(koaid, mv_path, ofname=ofname)
        if not storage_dir:
            return 0

        return self._rsync_files(mv_path, storage_dir)

    def get_storage_dir(self, koaid, mv_path, ofname=None, level=0):
        """
        get storage location and make the directory if needed.

        :param koaid: <str> the koaid of files.
        :param mv_path: <str> the path to the files(s) to move
        :param ofname: <str> ofname (koa_status table)
        :return: <str/list> storage directory (or None) and list of storage dirs
        """
        storage_dir = utils.determine_storage(koaid, config, config_type,
                                              ofname=ofname, level=level)

        if not storage_dir:
            self.log.warning("Could not determine storage path!")
            self.log.warning(f"Files at: {mv_path} where not moved!")
            return None

        if storage_dir not in self.dirs_made:
            utils.make_remote_dir(f'{user}@{store_server}', storage_dir, log)
            self.dirs_made.append(storage_dir)

        return storage_dir

    def mark_deleted(self, koaid):
        """
        Add deleted to the koa_status (source_deleted) table for the
        given koaid.

        :param koaid: <str> koaid of file to mark as deleted
        """
        results = utils.query_rti_api(site, 'update', 'MARKDELETED',
                                      log=log, val=koaid)
        self._log_update(koaid, results, 'SOURCE_DELETED')

    def add_archived_dir(self, koaid, archive_path, level=0):
        """
        Add the path to the storage / archived files.

        :param koaid: <str> the koaid
        :param archive_path: <str> storage path where files were moved/archived.
        """
        self.log.info(f"setting archive_dir {archive_path} for: {koaid}")
        archive_loc = utils.get_config_param(config, 'db_columns', 'archive_directory')

        results = utils.query_rti_api(site, 'update', 'GENERAL', log=log,
                                      columns=archive_loc, key='koaid',
                                      update_val=archive_path, val=koaid,
                                      add=f' LEVEL={level}')
        return self._log_update(koaid, results, 'ARCHIVE_DIR')

    def _log_update(self, koaid, results, column):
        """
        Log the update

        :param koaid: <str> koaid of files to update
        :param results: <str> the database results in json format
        :param column: <str> the column name for logging
        """
        try:
            results = json.loads(results)
        except Exception as err:
            self.log.warning(f"Error: {err}, line: {sys.exc_info()[-1].tb_lineno}")

        if results and type(results) == dict and results['success'] == 1:
            self.log.info(f"{column} set for koaid: {koaid}")
        else:
            self.log.warning(f"{column} not set for: {koaid}, "
                             f"line: {sys.exc_info()[-1].tb_lineno}")
            return False

        return True

    def _rsync_files(self, mv_path, storage_dir, koaid=None):
        """
        rsync all the DEP files to bring them to storage.

        :param koaid: <str> the koaid used to find the files.
        :param mv_path: <str> the archive path or the DEP files.
        :param storage_dir: <str> the path to store the files.
        """
        if not utils.chk_file_exists(mv_path):
            if '.fits' in mv_path and '.gz' not in mv_path:
                return self._rsync_files(mv_path + '.gz', storage_dir,
                                         koaid=koaid)

            log_str = f'skipping file {mv_path} -- already moved'
            log_str += ' or does not exist.'
            log.info(log_str)
            return -1

        server_str = f"{mv_path}"
        store_loc = f'{user}@{store_server}:{storage_dir}'

        log.info(f'rsync files from: {server_str} to: {store_loc}')
        log.info(f'koaid: {koaid}')

        if koaid:
            rsync_cmd = ["rsync", self.rm, "-avz", "-e", "ssh",
                         "--include", f"{koaid}*",
                         "--exclude", "*", f"{server_str}/", store_loc]
        elif '.fits' in server_str:
            rsync_cmd = ["rsync", self.rm, "-vz", server_str, store_loc]
        else:
            rsync_cmd = ["rsync", self.rm, "-avz", server_str, store_loc]

        log.info(f'rsync cmd: {rsync_cmd}')

        try:
            subprocess.run(rsync_cmd, stdout=subprocess.DEVNULL, check=True)
        except subprocess.CalledProcessError:
            log.warning(f"File(s) {mv_path} not moved to storage - {rsync_cmd}")
            return 0

        return 1

class ChkArchive:
    def __init__(self, inst):
        self.log = logging.getLogger(log_name)
        self.archived_key = utils.get_config_param(config, 'archive', 'archived')
        self.deleted_column = utils.get_config_param(config, 'db_columns', 'deleted')
        self.status_col = utils.get_config_param(config, 'db_columns', 'status')
        self.nresults = {'del0': [0, 0], 'mv0': [0, 0], 'mv1': [0, 0], 'mv2': [0, 0]}

        self.uniq_warn = []
        self.errors_dict = {}

        self.to_move = []
        self.move_lev1 = []
        self.move_lev2 = []

        if move:
            self.to_move = self.file_list(args.utd, args.utd2, inst,
                                          "ARCHIVE_DIR IS NULL", 'mv')
                                          # "ARCHIVE_DIR IS NULL", 'mv')
        if lev1:
            self.move_lev1 = self.file_list(
                args.utd, args.utd2, inst, "ARCHIVE_DIR IS NULL", 'mv', level=1)

        if lev2:
            self.move_lev2 = self.file_list(
                args.utd, args.utd2, inst, "ARCHIVE_DIR IS NULL", 'mv', level=2)

    def get_errors(self):
        return self.errors_dict

    def get_nresults(self):
        return self.nresults

    def get_warnings(self):
        return self.uniq_warn

    def get_files_to_move(self, file_type=None):
        """
        Access to the list of files to delete

        :return: <list/dict> the file list to of files to move
        """
        if file_type:
            return getattr(self, f'move_{file_type}')

        return self.to_move

    def num_all_files(self, utd, utd2):
        """
        Make a query to find the number of results without restricting by
        status.

        :param utd: <str> UT date at start of range.
        :param utd2: <str> UT date at end of range.
        :return: <int> the number of files in the archive between the two dates.
        """
        try:
            results = utils.query_rti_api(site, 'search', 'GENERAL', log=log,
                                          key=self.deleted_column, val='0',
                                          columns='koaid', utd=utd, utd2=utd2)
            archived_results = json.loads(results)
            return len(archived_results['data'])
        except:
            return 0

    def file_list(self, utd, utd2, inst, add, cmd_type, level=0):
        """
        Query the database for the files to delete or move.  Verify
        the results are valid

        :param utd: <str> YYYY-MM-DD initial date
        :param utd2: <str> YYYY-MM-DD the final date,  if None,  only one day
                           is searched.
        :param add: <str> the tail of the query string.
        :return: <dict> the verified data results from the query
        """
        cmd_type = f'{cmd_type}{level}'

        # the columns to return
        columns = utils.get_config_param(config, 'db_columns', f'lev{level}')

        # the database search column / value
        if not level:
            key = self.status_col
            val = self.archived_key
        else:
            key = None
            val = None

        search_type = 'GENERAL'

        try:
            results = utils.query_rti_api(site, 'search', search_type, log=log,
                                          columns=columns, key=key, val=val,
                                          add=add, utd=utd, utd2=utd2, inst=inst, level=level)
            archived_results = json.loads(results)
        except Exception as err:
            self.log.info(f"NO RESULTS from query,  error: {err}")
            return None

        if archived_results.get('success') == 1:
            self.log.info(f'{level} API Results = Success')

            data = archived_results.get('data', [])
            d_before = [dat['koaid'] for dat in data]
            self.nresults[cmd_type][0] = len(data)

            data = self.verify_db_results(data, columns)
            if data:
                self.nresults[cmd_type][1] = len(data)
                d_after = [dat['koaid'] for dat in data]
            else:
                d_after = []

            self.log.info(f"LEVEL {level} KOAIDs filtered from list: "
                          f"{utils.diff_list(d_before, d_after)}")

            return data

        return None

    def verify_db_results(self, data, column_str):
        """
        Verify the results.

        :param data: <list / dict> the data portion of the json db results.
        :return: data: <list / dict> cleaned db results.
        """
        if not data:
            return None

        columns = column_str.replace(' ', '').split(',')

        filter_data = []
        for result in data:

            err_msg = self._verify_result(result, columns)
            if err_msg:

                koaid = result.get('koaid', '')
                if err_msg in self.errors_dict:
                    if koaid not in self.errors_dict[err_msg]:
                        self.errors_dict[err_msg].append(koaid)
                else:
                    self.errors_dict[err_msg] = [koaid]

                self.log.warning(f"ERROR: {err_msg}")
                self.log.warning(f"ERROR with results for KOAID: {result['koaid']}")
                self.log.warning(f"{result}")
                continue

            filter_data.append(result)

        return filter_data

    def _verify_result(self, result, columns):
        """
        Verify that the results make sense before sending them to be deleted.

        :param result: <dict> a single row from the db query
        :return: <bool, str> True if valid,  err_msg when invalid.
        """
        if not result:
            return "No results found from query."

        err = None
        for col in columns:
            if err:
                break
            val = result.get(col)

            if not val and col not in ['status_code', 'archive_dir', 'level']:
                err = "INCOMPLETE RESULTS"
            elif col == 'status' and val != self.archived_key:
                err = f"INVALID STATUS, STATUS must be = {self.archived_key}"
            elif col == 'process_dir' and 'lev' not in val.split('/')[-1]:
                err = "INVALID ARCHIVE DIR"

        if err and err not in self.uniq_warn:
            self.uniq_warn.append(err)

        return err


if __name__ == '__main__':
    """
    to run:
        python scrub_koa_rti.py --utd 2021-02-11 --utd2 2021-02-12
    """
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    args = utils.parse_args(config)

    if args.dev:
        config_type = 'DEV'
    else:
        config_type = 'DEFAULT'

    move = int(utils.get_config_param(config, 'MODE', 'move'))
    lev1 = int(utils.get_config_param(config, 'MODE', 'lev1'))
    lev2 = int(utils.get_config_param(config, 'MODE', 'lev2'))

    site = utils.get_config_param(config, config_type, 'site')
    user = utils.get_config_param(config, config_type, 'user')
    store_server = utils.get_config_param(config, config_type, 'store_server')
    storage_root = utils.get_config_param(config, config_type, 'storage_root_rti')

    storage_num = utils.get_config_param(config, 'storage_disk', args.inst)

    if not args.logdir:
        log_dir = utils.get_config_param(config, config_type, 'log_dir')
    else:
        log_dir = args.logdir

    log_name, log_stream = utils.create_logger('rti_scrubber', log_dir)
    log = logging.getLogger(log_name)
    files_root = utils.get_config_param(config, 'koa_disk', 'path_root')

    nfiles_before = utils.count_koa_files(args)

    storage_direct = storage_root + storage_num
    store_before = utils.count_store(user, store_server, f'{storage_direct}',
                                     f'{args.inst}/*', log)

    log.info(f"Scrubbing data in UT range: {args.utd} to {args.utd2}\n")
    log.info(f"MOVE KOA PROCESSED FILES to storage: {move}")

    delete_obj = ToDelete(args.inst)
    metrics = delete_obj.get_metrics()
    if move:
        metrics['koaid'] = delete_obj.del_mv(None, delete_obj.store_lev0_func)
        metrics['staged'] = delete_obj.del_mv(None, delete_obj.store_stage_func)

    if lev1:
        metrics['lev1'] = delete_obj.del_mv('lev1', delete_obj.store_lev1_func)

    if lev2:
        metrics['lev2'] = delete_obj.del_mv('lev2', delete_obj.store_lev2_func)

    utils.clean_empty_dirs(files_root, log)
    nfiles_after = utils.count_koa_files(args)
    store_after = utils.count_store(user, store_server, f'{storage_direct}',
                                    f'{args.inst}/*', log)

    log.info(f'Number of KOA FILES before: {nfiles_before}')
    log.info(f'Number of KOA FILES after: {nfiles_after}')

    metrics['total_koa_mv'] = nfiles_before - nfiles_after
    metrics['total_storage_mv'] = store_after - store_before
    metrics['total_files'] = delete_obj.db_obj.num_all_files(args.utd, args.utd2)

    report = utils.create_rti_report(args, metrics, move, args.inst)
    log.info(report)

    # only send report if difference in totals.
    if metrics['total_koa_mv'] == metrics['total_storage_mv']:
        report = None

    utils.write_emails(config, report, log, errors=delete_obj.db_obj.get_errors(),
                       prefix='RTI')




