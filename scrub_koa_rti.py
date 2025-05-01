import os
import configparser
import logging
import json
import glob
import subprocess

import scrubber_utils as utils

APP_PATH = os.path.abspath(os.path.dirname(__file__))
CONFIG_FILE = f'{APP_PATH}/scrubber_config.live.ini'


class ToDelete:
    def __init__(self, inst):
        self.inst = inst
        self.utd = args.utd
        self.utd2 = args.utd2
        self.log = logging.getLogger(log_name)
        self.db_obj = ChkArchive(inst)
        self.to_move = self.db_obj.get_files_to_move()
        self.dirs_made = []
        self.lev1_moved = []
        self.lev2_moved = []
        self.dir2store = set()
        self.metrics = {'staged': [0, 0], 'koaid': [0, 0],
                        'inst': [0, 0], 'nresults': self.db_obj.get_nresults(),
                        'warnings': self.db_obj.get_warnings()}

        self.koaadmin_uid = 175
        self.koaadmin_gid = 20

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

        files_funked = []
        n_files_touched = 0
        for result in file_list:
            if 'process_dir' in result and result['process_dir'] in files_funked:
                continue

            # rsync will return 1 per file,  when it succeeds, 0 fails,
            # -1 if file not found
            ret_val = func(result)
            if ret_val < 0:
                nfiles_found += ret_val
            else:
                n_files_touched += ret_val

            if 'process_dir' in result:
                files_funked.append(result['process_dir'])

        return [nfiles_found, n_files_touched]

    def store_lev0_func(self, result):
        """
        move the files matching koaid to storage.

        :param result: <dict> single db row,  the query result for the file.
        :return: <int> 1 if file removed successfully,  or 1
        """
        koaid = result['koaid']
        # mv_path = result['process_dir']
        mv_path = f"/{args.tel}{result['process_dir'].strip('/')}"

        storage_dir = self.get_storage_dir(koaid, mv_path)
        if not storage_dir:
            return 0

        log.info(f'running store lev0,  storage dir {storage_dir}')

        return_val = self._rsync_files(mv_path, storage_dir, koaid)

        if return_val != 1:
            return return_val

        return return_val

    def store_kpf_components(self):
        # copy all Files,  in case some are not in the headers
        all_dirs = ['CaHK', 'CRED2', 'ExpMeter', 'FVC1', 'FVC2', 'FVC3',
                    'Green', 'L0', 'Red', 'script_logs']
        log.info(f'dir2store {self.dir2store}')
        for dir_set in self.dir2store:
            kpf_comp_root = dir_set[0]
            storage_dir = dir_set[1]

            # make directory if it doesn't exist
            cmd = ['mkdir', '-p', f'/net/storageserver/{storage_dir}']
            if not utils.run_cmd_as_user(self.koaadmin_uid, self.koaadmin_gid, cmd, log):
                continue

            # copy, don't remove the files
            orig_rm = self.rm
            self.rm = ''
            for comp_dir in all_dirs:
                storage_now = f'{storage_dir}/{comp_dir}/'
                nfs_store_now = f'/net/storageserver/{storage_now}'
                mv_path = f'{kpf_comp_root}{comp_dir}'
                log.info(f'component directory: {comp_dir}, {mv_path}, {storage_now}')
                if not os.path.isdir(nfs_store_now):
                    cmd = ['mkdir', '-p', nfs_store_now]
                    if not utils.run_cmd_as_user(self.koaadmin_uid, self.koaadmin_gid, cmd, log):
                        continue
                log.info(f'component directories, from: {mv_path} to: {storage_now}')
                self._rsync_files(mv_path, storage_now, sync_all=True)

            self.rm = orig_rm

        return


    def store_lev1_func(self, result):
        """
        move the files matching koaid to storage.

        :param result: <dict> single db row,  the query result for the file.
        :return: <int> 1 if file removed successfully,  or 1
        """
        return_val = 0
        koaid = result['koaid']
        mv_path = f"/{args.tel}{result['process_dir'].strip('/')}"

        if 'lev1' not in mv_path:
            self.log.warning(f"lev1 path format is incorrect: {mv_path}")
            return -1

        storage_dir = self.get_storage_dir(koaid, mv_path, level=1)
        if not storage_dir:
            return -1

        if mv_path.endswith('lev1'):
            mv_path = f'{mv_path}/'

        log.info(f'running store lev1,  storage dir {storage_dir}')

        if mv_path not in self.lev1_moved:
            return_val = self._rsync_files(mv_path, storage_dir)
            self.lev1_moved.append(mv_path)

        if return_val != 1:
            return return_val

        # if successfully moved,  add archive dir to DB entry
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
        mv_path = f"/{args.tel}{result['process_dir'].strip('/')}"

        if 'lev2' not in mv_path:
            self.log.warning(f"lev2 path format is incorrect: {mv_path}")
            return -1

        storage_dir = self.get_storage_dir(koaid, mv_path, level=2)
        if not storage_dir:
            return -1

        if mv_path.endswith('lev2'):
            mv_path = f'{mv_path}/'

        log.info(f'running store lev2, mv path {mv_path}, storage dir {storage_dir}')

        if mv_path not in self.lev2_moved:
            log.info(f'rsyncing {mv_path} to {storage_dir}')
            return_val = self._rsync_files(mv_path, storage_dir)
            self.lev2_moved.append(mv_path)

        if return_val != 1:
            return return_val

        # if successfully moved,  add archive dir to DB entry
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
        mv_path = f"/{args.tel}{result['stage_file'].strip('/')}"
        ofname = result['ofname']

        log.info(f'Storing Stage for: {koaid}')

        storage_dir = self.get_storage_dir(koaid, mv_path, ofname=ofname)
        if not storage_dir:
            log.error(f'Could not get storage dir for: {koaid}')
            return 0

        return_val = self._rsync_files(mv_path, storage_dir)
        if return_val != 1:
            return return_val

        # if successfully moved,  add archive dir to DB entry
        if not self.add_archived_dir(koaid, storage_dir, level=0):
            self.log.warning(f"archive_dir not set for {koaid}")

        return return_val

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
            cmd = ['mkdir', '-p',  f'/net/storageserver/{storage_dir}']
            if utils.run_cmd_as_user(self.koaadmin_uid, self.koaadmin_gid, cmd, log):
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
            self.log.warning(f"Error: {err}")

        if results and type(results) == dict and results['success'] == 1:
            self.log.info(f"{column} set for koaid: {koaid}")
        else:
            self.log.warning(f"{column} not set for: {koaid}")
            return False

        return True

    def _rsync_files(self, mv_path, storage_dir, koaid=None, sync_all=False):
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
        store_loc = f'/net/storageserver/{storage_dir}'

        log.info(f'rsync files from: {server_str} to: {store_loc}')
        log.info(f'koaid: {koaid}')

        if koaid:
            if 'lev0' in server_str and 'KPF' not in server_str and 'HIRES' not in server_str:
                koaid += "."

            rsync_cmd = ["rsync", "-avz",
                         "--include", f"{koaid}*",
                         "--exclude", "*", f"{server_str}/", store_loc]
            files_wild = f'{server_str}/{koaid}*'
        elif sync_all:
            rsync_cmd = ["/usr/bin/rsync", "-avz",
                         "--include", f"*fits*",
                         "--exclude", "*", f"{server_str}/", store_loc]
            files_wild = f'{server_str}/*fits'
        elif '.fits' in server_str:
            rsync_cmd = ["rsync", "-avz", server_str, store_loc]
            files_wild = f'{server_str}'
        else:
            # sync a full directory of files
            rsync_cmd = ["rsync", "-avz", server_str, store_loc]
            files_wild = f'{server_str}'

        log.info(f"rsync command: {rsync_cmd}")
        if not utils.run_cmd_as_user(self.koaadmin_uid, self.koaadmin_gid, rsync_cmd, log):
            return 0

        if self.rm:
            try:
                cmd = f"rm -r {files_wild}"
                subprocess.run(cmd, check=True, shell=True)
                log.info(f"Removed: {files_wild}, cmd: {cmd}")
            except subprocess.CalledProcessError as e:
                log.error(f"Failed to remove {files_wild}: {e}")
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

        if not args.force:
            add_str = "ARCHIVE_DIR IS NULL"
        else:
            add_str = None

        if move:
            self.to_move = self.file_list(args.utd, args.utd2, inst,
                                          add_str, 'mv')
        if lev1:
            self.move_lev1 = self.file_list(
                args.utd, args.utd2, inst, add_str, 'mv', level=1)

        if lev2:
            self.move_lev2 = self.file_list(
                args.utd, args.utd2, inst, add_str, 'mv', level=2)

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
        python scrub_koa_rti.py --inst NIRC2 --tel k2 --utd 2021-02-11 --utd2 2021-02-12
    """
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    args = utils.parse_args(config)
    print(f"UT Dates: {args.utd} to {args.utd2}")
    if args.dev:
        config_type = 'DEV'
    else:
        config_type = 'DEFAULT'

    move = int(utils.get_config_param(config, 'MODE', 'move'))
    lev1 = int(utils.get_config_param(config, 'MODE', 'lev1'))
    lev2 = int(utils.get_config_param(config, 'MODE', 'lev2'))

    site = utils.get_config_param(config, config_type, f'site_{args.tel}')
    user = utils.get_config_param(config, config_type, 'user')
    store_server = utils.get_config_param(config, config_type, 'store_server')
    storage_root = utils.get_config_param(config, config_type, 'storage_root_rti')

    storage_num = utils.get_config_param(config, 'storage_disk', args.inst)

    if not args.logdir:
        log_dir = utils.get_config_param(config, config_type, 'log_dir')
    else:
        log_dir = args.logdir

    log_name, log_stream = utils.create_logger('rti_scrubber', log_dir, args.inst)
    log = logging.getLogger(log_name)

    log.info(f"Starting Scrub data in UT range: {args.utd} to {args.utd2}\n")

    # this should be /koadata,  files_root becomes /k1koadata
    basic_root = utils.get_config_param(config, 'koa_disk', 'path_root')
    files_root = f"/{args.tel}{basic_root.strip('/')}"

    nfiles_before = utils.count_koa_files(args, files_root)
    # nfiles_before =9999
    storage_direct = storage_root + storage_num
    store_before = utils.count_store(user, store_server, f'{storage_direct}',
                                     f'{args.inst}/*', log)
    # store_before = 9999

    log.info(f"MOVE KOA PROCESSED FILES to storage: {move}")

    delete_obj = ToDelete(args.inst)
    metrics = delete_obj.get_metrics()
    if move:
        metrics['koaid'] = delete_obj.del_mv(None, delete_obj.store_lev0_func)
        # if args.inst == 'KPF':
        #     delete_obj.store_kpf_components()
        metrics['staged'] = delete_obj.del_mv(None, delete_obj.store_stage_func)

    if lev1:
        metrics['lev1'] = delete_obj.del_mv('lev1', delete_obj.store_lev1_func)

    if lev2:
        metrics['lev2'] = delete_obj.del_mv('lev2', delete_obj.store_lev2_func)

    utils.clean_empty_dirs(files_root, log)
    nfiles_after = utils.count_koa_files(args, files_root)
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




