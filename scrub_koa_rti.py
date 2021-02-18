import os
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
        self.database_obj = ChkArchive()
        self.dirs_made = []
        self.to_delete = self.database_obj.get_files_to_delete()
        self.to_move = self.database_obj.get_files_to_move()
        self.n_koa_before = 0
        self.n_store_before = 0
        self.n_koa_after = 0
        self.n_store_after = 0

    def num_all_files(self):
        """
        Provide access to the total number of files without the restriction
        on status.

        :return: <int> number of files in the data range
        """
        return self.database_obj.num_all_files(self.utd, self.utd2)

    def get_num_moved(self):
        num_files = {'store_before': self.n_store_before,
                     'store_after': self.n_store_after,
                     'koa_before': self.n_koa_before,
                     'koa_after': self.n_koa_after}

        return num_files

    # TODO this only logs the info,  it does not remove any files
    def delete_files(self):
        """
        This will find and delete the files for the specified date range.

        :return: <(int, int)> number deleted, number found to delete.
        """
        return self.delete_mv_list(self.to_delete, self.delete_func)

    def store_stage_files(self):
        """
        move the original file copy (stage_file) to storage.

        :return: <(int, int)> number moved, number found matching move criteria.
        """
        return self.delete_mv_list(self.to_move, self.store_stage_func)

    def store_lev0_files(self):
        """
        Move the DEP files to storage.

        :return: <(int, int)> number moved, number found matching move criteria.
        """
        return self.delete_mv_list(self.to_move, self.store_lev0_func)

    def delete_mv_list(self, file_list, func):
        """
        Skeleton function to delete or move a file list,  file by file.

        :return: <int> number moved/delted, number in list.
        """
        if not file_list:
            return 0, 0

        n_files_touched = 0
        for result in file_list:
            n_files_touched += func(result)

        return n_files_touched, len(file_list)

    def delete_func(self, result):
        """
        delete by file.

        :param result: <dict> single db row,  the query result for the file.
        :return: <int> 1 if file removed successfully,  or 1
        """
        full_filename = result['ofname']
        try:
            self.log.info(f"os.remove {full_filename}")
        except OSError as error:
            self.log.warning(f"Error while removing: {full_filename}, {error}")
            return 0

        self.mark_deleted(result['koaid'])

        return 1

    def store_stage_func(self, result):
        """
        move the stage file to storage.

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

        return self._rsync_files(mv_path, storage_dir, koaid)

    def get_storage_dir(self, koaid, mv_path, ofname=None):
        """
        get storage location and make the directory if needed.

        :param koaid: <str> the koaid of files.
        :param mv_path: <str> the path to the files(s) to move
        :param ofname: <str> ofname (dep_status table)
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

    # TODO only logs the command update not being performed (change on API)
    def mark_deleted(self, koaid):
        """
        Add deleted to the dep_status (ofname_deleted) table for the
        given koaid.

        :param koaid: <str> koaid of file to mark as deleted
        """
        results = utils.query_rti_api(site, 'update', 'MARKDELETED',
                                      log=log, val=koaid)
        self._log_update(koaid, results, 'OFNAME_DELETED')

    # TODO only logs the command - update not being performed (change on API)
    def add_archived_dir(self, koaid, archive_path):
        """
        Add the path to the storage / archived files.

        :param koaid: <str> the koaid
        :param archive_path: <str> storage path where files were moved/archived.
        """
        results = utils.query_rti_api(site, 'update', 'GENERAL', log=log,
                                      columns='archive_dir',
                                      update_val=archive_path, val=koaid)
        self._log_update(koaid, results, 'ARCHIVE_DIR')

    def _log_update(self, koaid, results, column):
        """
        Log the update

        :param koaid: <str> koaid of files to update
        :param results: <str> the database results in json format
        :param column: <str> the column name for logging
        """
        try:
            results = json.loads(results)
        except:
            self.log.warning(f"Could not set {column} for: {koaid}")

        if results and type(results) == dict and results['success'] == 1:
            self.log.info(f"{results['data']}")
            self.log.info(f"{column} set for koaid: {koaid}")
        else:
            self.log.warning(f"{column} not set for: {koaid}")

    def _rsync_files(self, mv_path, storage_dir, koaid=None):
        """
        rsync all the DEP files to bring them to storage.

        :param koaid: <str> the koaid used to find the files.
        :param mv_path: <str> the archive path or the DEP files.
        :param storage_dir: <str> the path to store the files.
        """
        # TODO
        # "rsync --remove-source-files -av -e ssh koaadmin@"$server":"$dir"
        #               "$storageDir[$i]"

        self.n_koa_before += utils.count_koa(mv_path, log)
        self.n_store_before += utils.count_store(user, store_server, '',
                                                 storage_dir, log)

        server_str = f"{mv_path}/"
        store_loc = f'{user}@{store_server}:{storage_dir}'
        if koaid:
            rsync_cmd = ["rsync", "-avz", "-e", "ssh",
                         "--include", f"{koaid}*",
                         "--exclude", "*", server_str, store_loc]
        else:
            rsync_cmd = ["rsync", "-avz", server_str, store_loc]

        self.log.info(f"rsync cmd: {rsync_cmd}")

        try:
            subprocess.run(rsync_cmd, stdout=subprocess.DEVNULL, check=True)
        except subprocess.CalledProcessError:
            log.warning(f"File(s) {mv_path} not moved to storage")
            return 0

        self.n_koa_after += utils.count_koa(mv_path, log)
        self.n_store_after += utils.count_store(user, store_server, '',
                                                storage_dir, log)

        return 1

    def determine_storage(self, koaid, ofname=None):
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

    def _chk_inst(self, inst):
        if exclude_insts and inst in exclude_insts:
            return False

        if include_insts and inst not in include_insts:
            return False

        return True


class ChkArchive:
    def __init__(self):
        self.log = logging.getLogger(log_name)
        self.archived_key = utils.get_config_param(config, 'archive', 'archived')

        add_query = 'AND OFNAME_DELETED=0'
        self.to_delete = self.file_list(args.utd, args.utd2, add_query)

        add_query = "AND ARCHIVE_DIR IS NULL"
        self.to_move = self.file_list(args.utd, args.utd2, add_query)

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
        Query the database for the files to delete.  Verify the results are
        valid

        :param utd: <str> YYYY-MM-DD initial date
        :param utd2: <str> YYYY-MM-DD the final date,  if None,  only one day
                           is searched.
        :param add: <str> the tail of the query string.
        :return: <dict> the verified data results from the query
        """
        columns = 'koaid,status,status_code,ofname,stage_file,'
        columns += 'process_dir,archive_dir,ofname_deleted'
        key = 'status'
        val = self.archived_key
        try:
            results = utils.query_rti_api(site, 'search', 'GENERAL', log=log,
                                          columns=columns, key=key, val=val,
                                          add=add, utd=utd, utd2=utd2)
            archived_results = json.loads(results)
        except:
            self.log.info("NO RESULTS")
            return None

        if utils.get_key_val(archived_results, 'success') == 1:
            data = utils.get_key_val(archived_results, 'data')
            data = self.verify_db_results(data)

            return self.chk_stage_exists(data)

        return None

    def verify_db_results(self, data):
        """
        Verify the results.

        :param data: <list / dict> the data portion of the json db results.
        :return: data: <list / dict> cleaned db results.
        """
        if not data:
            return None

        for idx, result in enumerate(data):
            valid, err_msg = self._verify_result(result)
            if not valid:
                self.log.warning(f"REMOVING FROM DELETE LIST,  {err_msg}")
                self.log.warning(f"{result}")
                data.pop(idx)

        return data

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

        if (not koaid or not status or not ofname or not process_dir
                or not stage_file):
            return False, "INCOMPLETE RESULTS"
        elif status != self.archived_key:
            return False, "INVALID STATUS"
        elif status_code:
            return False, f"STATUS CODE: {status_code}"
        elif process_dir.split('/')[-1] != 'lev0':
            return False, "INVALID ARCHIVE DIR"

        return True, ""

    def chk_stage_exists(self, data):
        """
        Verify that the file exits in the stage directory.

        :param data: <list / dict> the data portion of the json db results.
        :return: data: <list / dict> cleaned db results.
        """
        if not data:
            return None

        for idx, result in enumerate(data):
            file_location = utils.get_key_val(result, 'stage_file')
            if not utils.chk_file_exists(file_location):
                self.log.warning(f"REMOVING FROM DELETE LIST, ")
                self.log.warning(f"STAGE FILE (copy of original) NOT FOUND: {result}")
                data.pop(idx)

        return data


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
        exit("ONLY READY FOR DEV MODE! use --dev")

    site = utils.get_config_param(config, config_type, 'site')
    user = utils.get_config_param(config, config_type, 'user')
    store_server = utils.get_config_param(config, config_type, 'store_server')
    if config_type == "DEV":
        storage_root = utils.get_config_param(config, config_type, 'storage_root_rti')
    else:
        storage_root = utils.get_config_param(config, config_type, 'storage_root')

    log_dir = utils.get_config_param(config, config_type, 'log_dir')
    log_name, log_stream = utils.create_logger('data_scrubber', log_dir)
    log = logging.getLogger(log_name)

    log.info(f"Scrubbing data in UT range: {args.utd} to {args.utd2}")
    log.info(f"REMOVE ORIGINAL (OFNAME) FILES: {args.remove}")
    log.info(f"MOVE KOA PROCESSED FILES to storage: {args.move}")

    metrics = {}
    delete_obj = ToDelete()
    if args.move:
        metrics['n_moved'], metrics['n_movable'] = delete_obj.store_lev0_files()
        metrics['n_staged'], metrics['n_stagable'] = delete_obj.store_stage_files()
        moved = delete_obj.get_num_moved()
        metrics.update(moved)
    if args.remove:
        metrics['n_deleted'], metrics['n_deletable'] = delete_obj.delete_files()

    metrics['total_files'] = delete_obj.num_all_files()

    report = utils.create_rti_report(metrics)
    log.info(report)
    utils.write_emails(config, log_stream, report, 'RTI')








