import configparser
import logging
import json
import subprocess
import scrubber_utils as utils


class ToDelete:
    def __init__(self):
        self.utd = args.utd
        self.utd2 = args.utd2
        self.log = logging.getLogger(log_name)
        database_obj = ChkArchive()
        self.to_delete = database_obj.get_files_to_delete()

    # TODO this only logs the info,  it does not remove any files
    def delete_files(self):
        """
        This will find and delete the files for the specified date range.
        """
        if not self.to_delete:
            return

        for result in self.to_delete:
            full_filename = result['ofname']
            log_str = f"os.remove {full_filename}"
            self.log.info(log_str)
            self.mark_deleted(result['koaid'])

    # TODO only logs the command not update being performed
    # TODO API will need to be updated with permission koa -> koaadmin and
    # TODO update call to be updateGENERAL not updateMARKDELETED
    def mark_deleted(self, koaid):
        """
        Add deleted to the dep_status table for the given koaid.

        update dep_status set ofname_deleted = True where koaid='HI.20201104.9999.91';

        :param koaid: <str> koaid of file to mark as deleted
        :return:
        """

        results = utils.query_rti_api(site, 'update', 'MARKDELETED', val=koaid)
        try:
            results = json.loads(results)
        except:
            self.log.warning(f"Could not mark deleted for: {koaid}")

        if results and type(results) == dict and results['success'] == 1:
            self.log.info(f"{results['data']}")
            self.log.info(f"OFNAME_DELETE flag set for koaid: {koaid}")
        else:
            self.log.warning(f"OFNAME_DELETED not set for: {koaid}")

    # TODO this only logs,  does not move files.
    def move_files(self):
        """
        Move the DEP files to storage.
        """
        if not self.to_delete:
            return

        synced_paths = []
        for result in self.to_delete:
            files_path = result['archive_dir']

            storage_dir = args.storagedir
            if not storage_dir:
                storage_dir = self.determine_storage(files_path)

            if not storage_dir:
                self.log.warning("Could not determine storage path!")
                self.log.warning(f"Files in {files_path} where not moved!")
                continue

            files_path = files_path
            files_path = files_path.split('/lev0')[0]

            if files_path + storage_dir not in synced_paths:
                self._rsync_files(files_path, storage_dir)
                synced_paths.append(files_path + storage_dir)

    def _rsync_files(self, files_path, storage_dir):
        """
        rsync all the DEP files to bring them to storage.

        :param koaid: <str> the koaid used to find the files.
        :param files_path: <str> the archive path or the DEP files.
        :param storage_dir: <str> the path to store the files.
        """
        # "rsync --remove-source-files -av -e ssh koaadmin@"$server":"$dir" "$storageDir[$i]"
        server_str = f"{files_path}"

        if args.dev:
            rsync_cmd = ['rsync', '-av', '-e', 'ssh', server_str, storage_dir]
        else:
            log.warning("Not ready to start moving files: use --dev")
            return
            # TODO
            # rsync_cmd = ['rsync', '--remove-source-files', '-av', '-e',
            #              'ssh', server_str, storage_dir]

        print(rsync_cmd)
        print("rsync-ing into TMP/,  this takes awhile.")
        exit_val = subprocess.run(rsync_cmd, stdout=subprocess.DEVNULL)
        log_str = f"rsync({files_path}, {storage_dir})"
        self.log.info(log_str)

    # TODO needs the storage path in archive_dir/koaid.txt
    @staticmethod
    def determine_storage_dir(koaid, archive_dir):
        """

        :param koaid:
        :param archive_dir:
        :return:
        """
        storage_path = None
        full_path = "/".join(archive_dir, koaid)
        f_handle = open(full_path, "r")
        for line in f_handle:
            if 'storage' in line:
                storage_path = line

        return storage_path

    def determine_storage(self, files_path):
        """
        # koadmin@storageserver:/koastorage04/DEIMOS/koadata39/

        :param files_path:
        :return:
        """
        path_parts = files_path.split('/')
        inst_idx = path_parts.index('koadata') + 1
        if len(path_parts) <= inst_idx:
            return None
        inst = path_parts[inst_idx]
        if not self._chk_inst(inst):
            return None

        return utils.get_config_param(config, 'storage', inst)

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

        self.archived_files = self.files_to_delete(args.utd, args.utd2)

    def get_files_to_delete(self):
        """
        Access to the list of files to delete

        :return: <list/dict> the file list to delete
        """
        return self.archived_files

    #TODO change config file ARCHIVED / COMPLETED
    def files_to_delete(self, utd, utd2):
        """
        Query the database for the files to delete.  Verify the results are
        valid

        :param utd: <str> YYYY-MM-DD initial date
        :param utd2: <str> YYYY-MM-DD the final date,  if None,  only one day
                           is searched.
        :return: (dict) the verified data results from the query
        """
        columns = 'koaid,status,ofname,stage_file,archive_dir,ofname_deleted'
        key = 'status'
        val = self.archived_key
        add = 'AND OFNAME_DELETED=0'
        try:
            results = utils.query_rti_api(site, 'search', 'GENERAL',
                                          columns=columns, key=key, val=val,
                                          add=add, utd=utd, utd2=utd2)
            archived_results = json.loads(results)
        except:
            self.log.warning("NO RESULTS,  OR ERROR LOADING RESULTS AS JSON")
            archived_results = None

        if self.get_key_val(archived_results, 'success') == 1:
            data = self.get_key_val(archived_results, 'data')
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
        koaid = self.get_key_val(result, 'koaid')
        status = self.get_key_val(result, 'status')
        ofname = self.get_key_val(result, 'ofname')
        stage_file = self.get_key_val(result, 'stage_file')
        archive_dir = self.get_key_val(result, 'archive_dir')
        deleted = self.get_key_val(result, 'ofname_deleted')

        if (not koaid or not status or not ofname or not archive_dir
                or not stage_file):
            return False, "INCOMPLETE RESULTS"
        elif deleted:
            return False, "FILE ALREADY MARKED AS DELETED"
        elif len(koaid) != 20:
            return False, "INVALID KOAID"
        elif status != self.archived_key:
            return False, "INVALID STATUS"
        elif ofname.split('.')[-1] != 'fits':
            return False, "INVALID OFNAME"
        elif ofname.split('/')[-1] != stage_file.split('/')[-1]:
            return False, "INVALID EQUALITY BETWEEN OFNAME AND STAGE_FILE"
        elif archive_dir.split('/')[-1] != 'lev0':
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
            file_location = self.get_key_val(result, 'stage_file')
            if not utils.chk_file(file_location):
                self.log.warning(f"REMOVING FROM DELETE LIST, ")
                self.log.warning(f"STAGE FILE NOT FOUND: {result}")
                data.pop(idx)

        return data

    @staticmethod
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


if __name__ == '__main__':
    config_filename = 'scrubber_config.ini'
    config = configparser.ConfigParser()
    config.read(config_filename)

    site = utils.get_config_param(config, 'DEV', 'site')

    args = utils.parse_args()
    exclude_insts, include_insts = utils.define_args(args)

    log_name, log_stream = utils.create_logger('data_scrubber', args.logdir)
    log = logging.getLogger(log_name)

    log.info(f"Running Checks for utd: {args.utd} to {args.utd2}")

    delete_obj = ToDelete()
    if args.move:
        delete_obj.move_files()
    if args.remove:
        delete_obj.delete_files()

    log_contents = log_stream.getvalue()
    log_stream.close()

    if log_contents:
        utils.send_email(log_contents, config)









