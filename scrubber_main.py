import os
import configparser
import logging
import json
import subprocess
from datetime import datetime
import scrubber_utils as utils


class ToDelete:
    def __init__(self):
        self.utd = args.utd
        self.utd2 = args.utd2
        self.log = logging.getLogger(log_name)
        self.database_obj = ChkArchive()
        self.to_delete = self.database_obj.get_files_to_delete()

    def num_all_files(self):
        """
        Provide access to the total number of files without the restriction
        on status.

        :return: <int> number of files in the data range
        """
        return self.database_obj.num_all_files(self.utd, self.utd2)

    # TODO this only logs the info,  it does not remove any files
    def delete_files(self):
        """
        This will find and delete the files for the specified date range.

        :return: <int> number deleted, number found matching deletion criteria.
        """
        if not self.to_delete:
            return 0, 0

        n_deleted = 0
        for result in self.to_delete:
            full_filename = result['ofname']
            try:
                self.log.info(f"os.remove {full_filename}")
            except OSError as error:
                self.log.warning(f"Error while removing: {full_filename}, "
                                 f"{error}")
                continue

            self.mark_deleted(result['koaid'])
            n_deleted += 1

        return n_deleted, len(self.to_delete)

    # TODO this only copies,  does not move files.
    def move_files(self):
        """
        Move the DEP files to storage.

        :return: <int> number moved, number found matching move criteria.
        """
        if not self.to_delete:
            return 0, 0

        num_moved = 0
        storage_created = []
        for result in self.to_delete:
            koaid = result['koaid']
            file_path = result['archive_dir']

            storage_dir = args.storagedir
            if not storage_dir:
                storage_dir = self.determine_storage(koaid)

            if not storage_dir:
                self.log.warning("Could not determine storage path!")
                self.log.warning(f"File at: {file_path} where not moved!")
                continue

            if storage_dir not in storage_created:
                if self.make_storage_dir(storage_dir) == 0:
                    log.warning(f"Error creating storage dir: {storage_dir}")
                    continue

                storage_created.append(storage_dir)

            num_moved += self._rsync_files(koaid, file_path, storage_dir)

        return num_moved, len(self.to_delete)

    def make_storage_dir(self, storage_dir):
        """
        Create the storage directory.  If it does not exists,  go up
        creating directories in the path.

        :param storage_dir: <str>
            ie: /koadata/test_storage/koastorage02/KCWI/koadata28/20210116/lev0/
        :return: <int> status,  1 on success 0 on failure
        """
        try:
            os.mkdir(storage_dir)
            self.log.info(f"created directory: {storage_dir}")
        except FileExistsError:
            return 1
        except FileNotFoundError:
            self.log.info(f"Directory: {storage_dir}, does not exist yet.")
            one_down = '/'.join(storage_dir.split('/')[:-1])
            if len(one_down) > len(storage_root):
                self.make_storage_dir(one_down)
            else:
                return 0
        except:
            return 0

        # rewind
        return_val = self.make_storage_dir(storage_dir)

        return return_val

    # TODO only logs the command not update being performed
    def mark_deleted(self, koaid):
        """
        Add deleted to the dep_status (ofname_deleted) table for the
        given koaid.

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

    def _rsync_files(self, koaid, file_path, storage_dir):
        """
        rsync all the DEP files to bring them to storage.

        :param koaid: <str> the koaid used to find the files.
        :param file_path: <str> the archive path or the DEP files.
        :param storage_dir: <str> the path to store the files.
        """
        if not args.dev:
            log.warning("Not ready to start moving files: use --dev")
            # TODO
            # "rsync --remove-source-files -av -e ssh koaadmin@"$server":"$dir"
            #               "$storageDir[$i]"

        server_str = f"{file_path}/"
        rsync_cmd = ["rsync", "-av", "--include", koaid + "*",
                     "--exclude", "*", server_str, storage_dir]

        try:
            subprocess.run(rsync_cmd, stdout=subprocess.DEVNULL, check=True)
        except subprocess.CalledProcessError:
            log.warning(f"File with KOAID: {koaid} not moved to storage")
            return 0

        self.log.info(f"rsync cmd: {rsync_cmd}")

        return 1

    def determine_storage(self, koaid):
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

        storage_inst = utils.get_config_param(config, 'storage', inst)
        storage_path = f"{storage_root}{storage_inst}/{utd}/lev0/"

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
        self.archived_files = self.files_to_delete(args.utd, args.utd2)

    def get_files_to_delete(self):
        """
        Access to the list of files to delete

        :return: <list/dict> the file list to delete
        """
        return self.archived_files

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
            results = utils.query_rti_api(site, 'search', 'GENERAL',
                                          columns=columns, key=key, val=val,
                                          utd=utd, utd2=utd2)
            archived_results = json.loads(results)
            return len(archived_results['data'])
        except:
            return 0

    #TODO change config file ARCHIVED / COMPLETED
    def files_to_delete(self, utd, utd2):
        """
        Query the database for the files to delete.  Verify the results are
        valid

        :param utd: <str> YYYY-MM-DD initial date
        :param utd2: <str> YYYY-MM-DD the final date,  if None,  only one day
                           is searched.
        :return: <dict> the verified data results from the query
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
        ofname = utils.get_key_val(result, 'ofname')
        stage_file = utils.get_key_val(result, 'stage_file')
        archive_dir = utils.get_key_val(result, 'archive_dir')
        deleted = utils.get_key_val(result, 'ofname_deleted')

        if (not koaid or not status or not ofname or not archive_dir
                or not stage_file):
            return False, "INCOMPLETE RESULTS"
        elif status != self.archived_key:
            return False, "INVALID STATUS"

        #TODO what about moving without deleting?
        elif deleted:
            return False, "FILE ALREADY MARKED AS DELETED"
        # elif ofname.split('/')[-1] != stage_file.split('/')[-1]:
        #     return False, "INVALID EQUALITY BETWEEN OFNAME AND STAGE_FILE"
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
            file_location = utils.get_key_val(result, 'stage_file')
            if not utils.chk_file(file_location):
                self.log.warning(f"REMOVING FROM DELETE LIST, ")
                self.log.warning(f"STAGE FILE NOT FOUND: {result}")
                data.pop(idx)

        return data


if __name__ == '__main__':
    config_filename = 'scrubber_config.ini'
    config = configparser.ConfigParser()
    config.read(config_filename)

    args = utils.parse_args()
    exclude_insts, include_insts = utils.define_args(args)

    if args.dev:
        config_type = "DEV"
    else:
        config_type = "DEFAULT"

    site = utils.get_config_param(config, config_type, 'site')
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
        metrics['n_moved'], metrics['n_results'] = delete_obj.move_files()
    if args.remove:
        metrics['n_deleted'], metrics['n_results'] = delete_obj.delete_files()

    metrics['total_files'] = delete_obj.num_all_files()

    # send a report of the scrub
    now = datetime.now().strftime('%Y-%m-%d')
    report = utils.create_report(metrics)
    mailto = utils.get_config_param(config, 'email', 'admin')
    utils.send_email(report, mailto, f'RTI Scrubber Report: {now}')

    # if log_stream:
    log_contents = log_stream.getvalue()
    log_stream.close()

    if log_contents:
        mailto = utils.get_config_param(config, 'email', 'warnings')
        utils.send_email(log_contents, mailto, f'RTI Scrubber Warnings: {now}')









