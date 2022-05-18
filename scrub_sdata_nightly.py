import os
import sys
import configparser
import logging
import json
import subprocess
import scrubber_utils as utils

APP_PATH = os.path.abspath(os.path.dirname(__file__))
CONFIG_FILE = f'{APP_PATH}/scrub_sdata_config.live.ini'


class ToDelete:
    def __init__(self, inst):
        self.utd = args.utd
        self.utd2 = args.utd2
        self.log = logging.getLogger(log_name)
        self.db_obj = ChkArchive(inst)
        self.sdata_move = sdata_move
        self.dirs_made = []
        self.lev1_moved = []
        self.metrics = {'staged': [0, 0], 'sdata': [0, 0], 'koaid': [0, 0],
                        'inst': [0, 0], 'nresults': self.db_obj.get_nresults(),
                        'warnings': self.db_obj.get_warnings()}

    def get_metrics(self):
        return self.metrics

    def rm_sdata_files(self, sdata_files):
        """
        function to delete or move a file list,  file by file.

        :return: <list<int>,<int>> number moved/deleted, number in list.
        """
        if not sdata_files:
            return [0, 0]

        n_files_touched = 0
        for result in sdata_files:
            # rsync will return 1 per file,  when it succeeds
            n_files_touched += self.rm_sdata_func(result)

        return [len(sdata_files), n_files_touched]

    def rm_sdata_func(self, result):
        """
        remove the sdata files.  The path to move is:

        ; source = /net/nuu  + (OFNAME - /s)
        /net/
        ofname = result['ofname']

        :param result: <dict> single db row,  the query result for the file.
        :return: <int> 1 if file removed successfully,  or 1
        """
        ofname = result['ofname']
        if not ofname or len(ofname) < 2:
            return 0

        # strip the leading /s for /s/sdata...
        mv_path_local = f"{inst_root}{inst_comp}/{ofname[2:]}"
        mv_path_remote = f"{ofname[2:]}"

        moved = self._rm_files(mv_path_local, mv_path_remote)

        if moved:
            self.mark_deleted(result['koaid'])

        return moved

    def mark_deleted(self, koaid):
        """
        Add deleted to the koa_status (source_deleted) table for the
        given koaid.

        :param koaid: <str> koaid of file to mark as deleted
        """
        results = utils.query_rti_api(site, 'update', 'MARKDELETED',
                                      log=log, val=koaid)
        self._log_update(koaid, results, 'SOURCE_DELETED')

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

    def _rm_files(self, mv_path_local, mv_path_remote):
        """
        remove sdata files

        :param koaid: <str> the koaid used to find the files.
        :param mv_path: <str> thef archive path or the DEP files.
        :param log_only:
        """
        if not utils.chk_file_exists(mv_path_local):
            log_str = f'skipping {mv_path_local} -- moved or does not exist.'
            log.info(log_str)
            return 0

        inst = args.inst.lower()
        account = None
        for direct in mv_path_remote.split('/'):
            if inst in direct and 'fits' not in direct:
                account = direct

        if not account:
            log.error(f'could not determine the account from: {mv_path_remote}')
            return 0

        acnt_numb = account.strip(inst).zfill(2)
        pw = f"{numbered_prefix}{acnt_numb}{numbered_suffix}"

        if not pw:
            return 0

        cmd = f'/bin/rm {mv_path_remote}'
        log.info(f'remote command: {server} {cmd} {account} {pw} {mv_path_remote}')
        utils.execute_remote_cmd(server, cmd, account, pw)

        # check that it was removed
        if utils.chk_file_exists(mv_path):
            self.log.error(f"File not removed,  check path: {mv_path}")
            return 0

        return 1


class ChkArchive:
    def __init__(self, inst):
        self.log = logging.getLogger(log_name)
        self.nresults = {'sdata': [0, 0]}
        self.uniq_warn = []
        self.errors_dict = {}
        self.move_sdata = []

        if sdata_move:
            add = f"{deleted_col} IS NULL"
            self.move_sdata = self.get_file_list(args.utd, args.utd2, inst, add)

    def get_errors(self):
        return self.errors_dict

    def get_nresults(self):
        return self.nresults

    def get_warnings(self):
        return self.uniq_warn

    def get_files_to_move(self):
        """
        Access to the list of files to delete

        :return: <list/dict> the file list to of files to move
        """
        return self.move_sdata

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
                                          key=deleted_col, val='0',
                                          columns='koaid', utd=utd, utd2=utd2)
            archived_results = json.loads(results)
            return len(archived_results['data'])
        except:
            return 0

    def get_file_list(self, utd, utd2, inst, add):
        """
        Query the database for the files to delete or move.  Verify
        the results are valid

        :param utd: <str> YYYY-MM-DD initial date
        :param utd2: <str> YYYY-MM-DD the final date,  if None,  only one day
                           is searched.
        :param inst: <str> the instrument name
        :param add: <str> the tail of the query string.

        :return: <list><dict> the verified data results from the query
        """
        cmd_type = 'sdata'

        # the columns to return
        columns = utils.get_config_param(config, 'db_columns', 'sdata')

        # the database search column / value (status=COMPLETE)
        key = status_col
        val = archived_key

        try:
            results = utils.query_rti_api(site, 'search', 'GENERAL', log=log,
                                          columns=columns, key=key, val=val,
                                          add=add, utd=utd, utd2=utd2, inst=inst)
            archived_results = json.loads(results)
        except Exception as err:
            self.log.info(f"NO RESULTS from query,  error: {err}")
            return []

        success = archived_results.get('success')
        if success != 1:
            return []

        self.log.info(f"API Results = Success {success}")

        data = archived_results.get('data', [])
        d_before = [dat['koaid'] for dat in data]

        # check that all values in the list have been archived
        archived_data = []
        for dat in data:
            if not self.check_file_stored(dat):
                continue

            archived_data.append(dat)

        self.nresults[cmd_type][0] = len(archived_data)

        archived_data = self.verify_db_results(archived_data, columns)
        if archived_data:
            self.nresults[cmd_type][1] = len(archived_data)
            d_after = [dat['koaid'] for dat in archived_data]
        else:
            d_after = []

        self.log.info(f"KOAIDs filtered from list: "
                      f"{utils.diff_list(d_before, d_after)}")

        return archived_data


    @staticmethod
    def check_file_stored(dat):
        ofname = dat.get('ofname', None)
        koaid = dat.get('koaid', None)
        if not koaid or not ofname:
            log.error(f'not removing, cannot determine ofname or koaid: {dat}')
            return False

        store_dir = utils.determine_storage(koaid, config, config_type,
                                            ofname=ofname)

        filename = ofname.split('/')[-1]
        path = f'{store_dir}/{filename}*'
        if not utils.exists_remote(f'{user}@{store_server}', path):
            log.error(f'data not on storage: {path} data: {dat}')
            return False

        log.info(f'File found stored at: {path}')

        return True

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

                self.log.warning(f"ERROR: {err_msg} for {result}")
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

            if not val and col not in ['status_code', 'archive_dir', 'source_deleted']:
                err = "INCOMPLETE RESULTS"
            elif col == 'status_code' and val and not result.get('reviewed'):
                err = f"STATUS CODE: {val}"
            elif col == 'status' and val != archived_key:
                err = f"INVALID STATUS, STATUS must be = {archived_key}"
            elif col == 'process_dir' and 'lev' not in val.split('/')[-1]:
                err = "INVALID ARCHIVE DIR"

        if err and err not in self.uniq_warn:
            self.uniq_warn.append(err)

        return err


if __name__ == '__main__':
    """
    to run:
        python scrub_sdata_nightly.py --utd 2021-02-11 --utd2 2021-02-12
    """
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    args = utils.parse_args(config)

    if args.dev:
        config_type = 'DEV'
    else:
        config_type = 'DEFAULT'

    try:
        sdata_move = int(utils.get_config_param(config, 'SDATA_REMOVE',
                                                args.inst))
    except KeyError:
        sdata_move = 0

    site = utils.get_config_param(config, config_type, 'site')
    user = utils.get_config_param(config, config_type, 'user')
    store_server = utils.get_config_param(config, config_type, 'store_server')

    deleted_col = utils.get_config_param(config, 'db_columns', 'deleted')
    archived_key = utils.get_config_param(config, 'db_columns', 'archived')
    status_col = utils.get_config_param(config, 'db_columns', 'status')

    numbered_prefix = utils.get_config_param(config, 'passwords', 'numbered_prefix')
    numbered_suffix = utils.get_config_param(config, 'passwords', 'numbered_suffix')

    inst_root = utils.get_config_param(config, 'inst_disk', 'path_root')
    inst_comp = utils.get_config_param(config, 'inst_disk', args.inst)

    server_user = utils.get_config_param(config, 'accounts', args.inst)
    server = utils.get_config_param(config, 'servers', args.inst)

    if not args.logdir:
        log_dir = utils.get_config_param(config, config_type, 'log_dir')
    else:
        log_dir = args.logdir

    log_name, log_stream = utils.create_logger('sdata_scrubber', log_dir)
    log = logging.getLogger(log_name)
    print(f'writing log to: {log_dir}/{log_name}')

    log.info(f"Scrubbing sdata in UT range: {args.utd} to {args.utd2}\n")

    delete_obj = ToDelete(args.inst)
    metrics = delete_obj.get_metrics()
    sdata_files = delete_obj.db_obj.get_files_to_move()

    if not sdata_files:
        exit("No files found to remove.")

    try:
        ofname = sdata_files[0]['ofname']
        mv_path = f"{inst_root}{inst_comp}/{ofname[2:]}"
    except (TypeError, KeyError, IndexError):
        mv_path = None

    if mv_path:
        nfiles_before = utils.count_koa(mv_path, log)
    else:
        nfiles_before = 0

    if sdata_move:
        metrics['sdata'] = delete_obj.rm_sdata_files(sdata_files)

    # TODO TBD if this is required.
    # utils.clean_empty_dirs(files_root, log)

    # count files after
    nfiles_after = utils.count_koa(mv_path, log)

    log.info(f'Number of SDATA FILES before: {nfiles_before}')
    log.info(f'Number of SDATA FILES after: {nfiles_after}')

    metrics['total_sdata_mv'] = nfiles_before - nfiles_after
    metrics['total_files'] = delete_obj.db_obj.num_all_files(args.utd, args.utd2)

    report = utils.create_sdata_report(args, metrics, args.inst)
    log.info(report)

    utils.write_emails(config, report, log, errors=delete_obj.db_obj.get_errors(),
                       prefix='SDATA')




