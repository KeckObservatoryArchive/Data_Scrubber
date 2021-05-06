import os
import sys
import configparser
import logging
import json
import subprocess
import scrubber_utils as utils

APP_PATH = os.path.abspath(os.path.dirname(__file__))
CONFIG_FILE = f'{APP_PATH}/scrubber_config.ini'


class ToDelete:
    def __init__(self):
        self.utd = args.utd
        self.utd2 = args.utd2
        self.log = logging.getLogger(log_name)
        self.db_obj = ChkArchive()
        self.to_delete = self.db_obj.get_files_to_delete()
        self.to_move = self.db_obj.get_files_to_move()
        self.dirs_made = []
        self.metrics = {'staged': [0, 0], 'sdata': [0, 0], 'koaid': [0, 0],
                        'nresults': self.db_obj.get_nresults(),
                        'warnings': self.db_obj.get_warnings()}

        if config_type == "DEV":
            self.rm = ""
        else:
            self.rm = "--remove-source-files"

    def num_all_files(self):
        """
        Provide access to the total number of files without the restriction
        on status.

        :return: <int> number of files in the data range
        """
        return self.db_obj.num_all_files(self.utd, self.utd2)

    def get_metrics(self):
        return self.metrics

    def delete_files(self):
        """
        This will find and delete the files for the specified date range.

        :return: <(int, int)> number deleted, number found to delete.
        """
        return self.del_mv(self.to_delete, self.delete_func)

    def store_stage_files(self):
        """
        move the original file copy (stage_file) to storage.

        :return: <(int, int)> number moved, number found matching move criteria.
        """
        return self.del_mv(self.to_move, self.store_stage_func)

    def store_lev0_files(self):
        """
        Move the KOA DEP files to storage.

        :return: <(int, int)> number moved, number found matching move criteria.
        """
        return self.del_mv(self.to_move, self.store_lev0_func)

    def del_mv(self, file_list, func):
        """
        Skeleton function to delete or move a file list,  file by file.

        :return: <list<int>,<int>> number moved/delted, number in list.
        """
        if not file_list:
            return [0, 0]

        n_files_touched = 0
        for result in file_list:
            # rsync will return 1 per file,  when it succeeds
            n_files_touched += func(result)

        return [len(file_list), n_files_touched]

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

        return_val = self._rsync_files(mv_path, storage_dir, koaid)
        # if return_val == 1:
        if not self.add_archived_dir(koaid, storage_dir):
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

    def delete_func(self, result):
        """
        delete by file.

        :param result: <dict> single db row,  the query result for the file.
        :return: <int> 1 if file removed successfully,  or 1
        """
        full_filename = result['ofname']
        try:
            #TODO this needs to remove on the remote instrument servers
            self.log.info(f"os.remove {full_filename}")
        except OSError as error:
            self.log.warning(f"Error while removing: {full_filename}, {error}, "
                             f"line: {sys.exc_info()[-1].tb_lineno}")
            return 0

        # TODO this needs to be added once the files are being deleted
        # self.mark_deleted(result['koaid'])

        # TODO update to 1 once it is removing data
        return 0

    def get_storage_dir(self, koaid, mv_path, ofname=None):
        """
        get storage location and make the directory if needed.

        :param koaid: <str> the koaid of files.
        :param mv_path: <str> the path to the files(s) to move
        :param ofname: <str> ofname (koa_status table)
        :return: <str/list> storage directory (or None) and list of storage dirs
        """
        storage_dir = self.determine_storage(koaid, ofname)

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
        Add deleted to the koa_status (ofname_deleted) table for the
        given koaid.

        :param koaid: <str> koaid of file to mark as deleted
        """
        results = utils.query_rti_api(site, 'update', 'MARKDELETED',
                                      log=log, val=koaid)
        self._log_update(koaid, results, 'OFNAME_DELETED')

    def add_archived_dir(self, koaid, archive_path):
        """
        Add the path to the storage / archived files.

        :param koaid: <str> the koaid
        :param archive_path: <str> storage path where files were moved/archived.
        """
        self.log.info(f"setting archive_dir for: {koaid}")
        results = utils.query_rti_api(site, 'update', 'GENERAL', log=log,
                                      columns='archive_dir', key='koaid',
                                      update_val=archive_path, val=koaid)
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
            self.log.info(f"{results['data']}")
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
            log_str = f'skipping file {mv_path} -- already moved'
            log_str += ' or does not exist.'
            log.info(log_str)
            return 0

        server_str = f"{mv_path}"
        store_loc = f'{user}@{store_server}:{storage_dir}'
        if koaid:
            rsync_cmd = ["rsync", self.rm, "-avz", "-e", "ssh",
                         "--include", f"{koaid}*",
                         "--exclude", "*", f"{server_str}/", store_loc]
        else:
            rsync_cmd = ["rsync", self.rm, "-avz",
                         server_str, store_loc]

        self.log.info(f"rsync cmd: {rsync_cmd}")

        try:
            subprocess.run(rsync_cmd, stdout=subprocess.DEVNULL, check=True)
        except subprocess.CalledProcessError:
            log.warning(f"File(s) {mv_path} not moved to storage - {rsync_cmd}")
            return 0

        return 1

    @staticmethod
    def determine_storage(koaid, ofname=None):
        """
        Find the storage directory from the KOAID.
        # koadmin@storageserver:/koastorage04/DEIMOS/koadata39/

        :param koaid: <str> <inst>.utd.#####.## (ie: KB.20210116.57436.94)
        :return: <str> full path to storage directory (including lev0)
        """
        id_parts = koaid.split('.')
        if len(id_parts) != 4:
            return None

        inst = utils.get_config_param(config, 'inst_prefix', id_parts[0])
        utd = id_parts[1]

        store_num = utils.get_config_param(config, 'storage_disk', inst)
        koa_num = utils.get_config_param(config, 'koa_disk', inst)
        koa_root = utils.get_config_param(config, 'koa_disk', 'path_root')

        storage_path = f"{storage_root}{store_num}/{inst}/"

        # storing stage files
        if ofname:
            dirs = ofname.split('/')
            if 'fits' not in dirs[-1]:
                return None
            s_root = '/'.join(dirs[:-1])
            storage_path += f"stage/{inst}/{utd}/{s_root}"

        # storing lev0 files
        else:
            storage_path += f"{koa_root}{koa_num}/{utd}/lev0/"

        return storage_path

    @staticmethod
    def _chk_inst(inst):
        if exclude_insts and inst in exclude_insts:
            return False

        if include_insts and inst not in include_insts:
            return False

        return True


class ChkArchive:
    def __init__(self):
        self.log = logging.getLogger(log_name)
        self.archived_key = utils.get_config_param(config, 'archive', 'archived')
        self.nresults = [0, 0]
        self.uniq_warn = []

        self.to_move = []
        self.to_delete = []

        if args.remove:
            self.to_delete = self.file_list(args.utd, args.utd2, "")
        if args.move:
            self.to_move = self.file_list(args.utd, args.utd2,
                                          "AND ARCHIVE_DIR IS NULL")

    def get_nresults(self):
        return self.nresults

    def get_warnings(self):
        return self.uniq_warn

    def get_files_to_delete(self):
        """
        Access to the list of files to delete

        :return: <list/dict> the file list to delete
        """
        return self.to_delete

    def get_files_to_move(self):
        """
        Access to the list of files to delete

        :return: <list/dict> the file list to of files to move
        """
        return self.to_move

    def num_all_files(self, utd, utd2):
        """
        Make a query to find the number of results without restricting by
        status.

        :param utd: <str> UT date at start of range.
        :param utd2: <str> UT date at end of range.
        :return: <int> the number of files in the archive between the two dates.
        """
        columns = 'koaid'
        key = 'OFNAME_DELETED'
        val = '0'
        try:
            results = utils.query_rti_api(site, 'search', 'GENERAL', log=log,
                                          columns=columns, key=key, val=val,
                                          utd=utd, utd2=utd2)
            archived_results = json.loads(results)
            return len(archived_results['data'])
        except:
            return 0

    def file_list(self, utd, utd2, add):
        """
        Query the database for the files to delete or move.  Verify
        the results are valid

        :param utd: <str> YYYY-MM-DD initial date
        :param utd2: <str> YYYY-MM-DD the final date,  if None,  only one day
                           is searched.
        :param add: <str> the tail of the query string.
        :return: <dict> the verified data results from the query
        """
        columns = 'koaid,status,status_code,ofname,stage_file,'
        columns += 'process_dir,archive_dir'
        key = 'status'
        val = self.archived_key
        try:
            results = utils.query_rti_api(site, 'search', 'GENERAL', log=log,
                                          columns=columns, key=key, val=val,
                                          add=add, utd=utd, utd2=utd2)
            archived_results = json.loads(results)
        except Exception as err:
            self.log.info(f"NO RESULTS from query,  error: {err}")
            return None

        if utils.get_key_val(archived_results, 'success') == 1:
            self.log.info('API Results = Success')
            data = utils.get_key_val(archived_results, 'data')

            d_before = [dat['koaid'] for dat in data]
            self.nresults[0] = len(data)
            data = self.verify_db_results(data)
            self.nresults[1] = len(data)
            d_after = [dat['koaid'] for dat in data]

            self.log.info(f"KOAIDs filtered from list: "
                          f"{utils.diff_list(d_before, d_after)}")

            return data

        return None

    def verify_db_results(self, data):
        """
        Verify the results.

        :param data: <list / dict> the data portion of the json db results.
        :return: data: <list / dict> cleaned db results.
        """
        if not data:
            return None

        filter_data = []
        for result in data:
            self.log.info(f"Checking KOAID {result['koaid']}")
            err_msg = self._verify_result(result)
            if err_msg:
                msg = f"koaid: {utils.get_key_val(result, 'koaid')} {err_msg}"
                self.log.warning(f"{msg}, REMOVED FROM DELETE LIST.")
                self.log.info(f"{result}")
                continue

            filter_data.append(result)

        return filter_data

    def _verify_result(self, result):
        """
        Verify that the results make sense before sending them to be deleted.

        :param result: <dict> a single row from the db query
        :return: <bool, str> True if valid,  err_msg when invalid.
        """
        koaid = utils.get_key_val(result, 'koaid')
        status = utils.get_key_val(result, 'status')
        status_code = utils.get_key_val(result, 'status_code')
        ofname = utils.get_key_val(result, 'ofname')
        stage_file = utils.get_key_val(result, 'stage_file')
        process_dir = utils.get_key_val(result, 'process_dir')

        err = None
        if (not koaid or not status or not ofname or not process_dir
                or not stage_file):
            err = "INCOMPLETE RESULTS"
        elif status != self.archived_key:
            err = "INVALID STATUS"
        elif status_code:
            err = f"STATUS CODE: {status_code}"
        elif process_dir.split('/')[-1] != 'lev0':
            err = "INVALID ARCHIVE DIR"
        # elif not utils.chk_file_exists(stage_file):
        #     err = f"STAGE FILE NOT FOUND"
            # + {stage_file}

        if err and err not in self.uniq_warn:
            self.uniq_warn.append(err)

        return err


if __name__ == '__main__':
    """
    to run:
        python scrub_koa_rti.py --dev --utd 2021-02-11 --utd2 2021-02-12 --move --remove
    """
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    args = utils.parse_args()
    exclude_insts, include_insts = utils.define_args(args)

    if args.dev:
        config_type = "DEV"
    else:
        config_type = "DEFAULT"

    site = utils.get_config_param(config, config_type, 'site')
    user = utils.get_config_param(config, config_type, 'user')
    store_server = utils.get_config_param(config, config_type, 'store_server')
    storage_root = utils.get_config_param(config, config_type, 'storage_root_rti')

    log_dir = utils.get_config_param(config, config_type, 'log_dir')
    log_name, log_stream = utils.create_logger('rti_scrubber', log_dir)
    log = logging.getLogger(log_name)
    files_root = utils.get_config_param(config, 'koa_disk', 'path_root')

    nfiles_before = utils.count_koa_files(args)
    store_before = utils.count_store(user, store_server,
                                     f'{storage_root}*', '*', log)

    log.info(f"Scrubbing data in UT range: {args.utd} to {args.utd2}\n")
    log.info(f"REMOVE ORIGINAL (OFNAME) FILES: {args.remove}")
    log.info(f"MOVE KOA PROCESSED FILES to storage: {args.move}")

    delete_obj = ToDelete()
    metrics = delete_obj.get_metrics()
    if args.move:
        metrics['koaid'] = delete_obj.store_lev0_files()
        metrics['staged'] = delete_obj.store_stage_files()

    if args.remove:
        metrics['sdata'] = delete_obj.delete_files()

    utils.clean_empty_dirs(files_root, log)
    nfiles_after = utils.count_koa_files(args)
    store_after = utils.count_store(user, store_server,
                                    f'{storage_root}*', '*', log)

    log.info(f'Number of KOA FILES before: {nfiles_before}')
    log.info(f'Number of KOA FILES after: {nfiles_after}')

    metrics['total_koa_mv'] = nfiles_before - nfiles_after
    metrics['total_storage_mv'] = store_after - store_before
    metrics['total_files'] = delete_obj.num_all_files()

    report = utils.create_rti_report(args, metrics)
    log.info(report)
    utils.write_emails(config, log_stream, report, 'RTI')




