import os
import re
import sys
import configparser
import logging
import json
import subprocess
import scrubber_utils as utils

from datetime import datetime, timedelta
from glob import glob

APP_PATH = os.path.abspath(os.path.dirname(__file__))
CONFIG_FILE = f'{APP_PATH}/scrub_sdata_config.live.ini'


class ToDelete:
    def __init__(self, inst):
        self.inst = inst
        self.utd = args.utd
        self.utd2 = args.utd2
        self.log = logging.getLogger(log_name)
        self.db_obj = ChkArchive(inst)
        self.sdata_move = sdata_move
        self.dirs_made = []
        self.lev1_moved = []
        self.paths2cln = set()
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

        if self.inst == 'KPF':
            self.clean_up_kpf()

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

        # strip the leading /s for /s/sdata...  add the /net/server (non-summit servers)
        # local_path = ofname
        mv_path_remote = f"{ofname[2:]}"
        local_path = f"{inst_comp}/{mv_path_remote}"
        moved = self._rm_files(local_path, mv_path_remote)

        self.log.info(f"rm_sdata_func -- file moved: {moved}")

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

    def _rm_files(self, local_path, remove_path, directory=False,
                  only_dir=False, recursive=True):
        """
        remove sdata files

        removes remove_path - which is original same as local_path without /s
        The remote disks use same location,  but are not mounted with /s

        :param local_path: <str> the /s/sdata... fullpath including filename
        :param remove_path: <str> the remote /sdata... fullpath including filename
        :param log_only:
        """
        # check for executedMasks directory
        if not directory and 'mosfire' in local_path:
            exc_path_remote = remove_path.rsplit('/', 1)[0]
            exc_path_local = local_path.rsplit('/', 1)[0]
            if not exc_path_remote or not exc_path_local:
                return 0

            exc_path_remote += '/executedMasks'
            exc_path_local += '/executedMasks'

            self._rm_files(exc_path_local, exc_path_remote, directory=True)

        # check that the file exists
        if not utils.chk_file_exists(local_path):
            log_str = f'skipping {local_path} -- moved or does not exist.'
            log.info(log_str)
            return 0

        inst_name = args.inst.upper()
        inst = utils.get_config_param(config, 'accounts', inst_name).lower()
        account = None
        for direct in remove_path.split('/'):
            if inst in direct and 'fits' not in direct:
                account = direct

        if not account:
            for direct in remove_path.split('/'):
                if 'eng' in direct and 'fits' not in direct:
                    account = direct

        if inst_name == 'KPF':
            account = 'kpfeng'

        if not account:
            log.error(f'could not determine the account from: {remove_path}')
            return 0

        acnt_numb = account.strip(inst).zfill(2)
        try:
            int(acnt_numb)
            pw = f"{numbered_prefix}{acnt_numb}{numbered_suffix}"
        except ValueError:
            if 'eng' in account.split(inst):
                pw = eng_pw
            elif 'eng' in account:
                pw = eng_pw
            elif inst_name == 'KPF':
                pw = eng_pw
            else:
                log.warning(f'could not determine the password from path: {remove_path}')
                return 0

        if not pw:
            return 0

        if inst_name == 'KPF':
            pw = eng_pw

        # temporary for KPF
        if inst_name == 'KPF' and recursive:
            if not self.kpf_components(local_path, remove_path):
                return False

        if only_dir:
            cmd = f'/bin/rmdir {remove_path}'
        elif directory:
            cmd = f'/bin/rm -r {remove_path}'
        else:
            cmd = f'/bin/rm {remove_path}'


        # TODO for testing
        # cmd = f'/bin/ls {remove_path}'

        log.info(f'remote command: {server} {cmd} {account} {pw} {remove_path}')

        # TODO this was diabled for testing
        try:
            utils.execute_remote_cmd(server, cmd, account, pw)
        except Exception as err:
            self.log.warning(f'exception in executing remote command: {err}')
            self.log.warning(f'error with command: {server} {cmd} {account} {pw}')
            return 0

        # check that it was removed locally
        # TODO this causes issues
        # if utils.chk_file_exists(local_path):
        #     self.log.error(f"File not removed,  check path: {remove_path} local path: {local_path}")
        #     print('returning 0')
        #     return 0

        self.log.info(f"File removed from: {remove_path}")

        return 1

    def kpf_components(self, local_path, remove_path):
        files_to_remove = utils.kpf_component_files(local_path, remove_path, log)
        if files_to_remove:
            for mv_path in files_to_remove:
                storage_dir = re.sub(rf'{koa_disk_num[-1]}.', '/instr1/KPF', mv_path)
                log.info(f'component files: {mv_path} storage: {storage_dir}')

                remove_path_new = '/' + mv_path.split('/s/')[-1]
                self._rm_files(mv_path, remove_path_new, recursive=False)

                try:
                    # remove filename and component
                    component_path = f"{inst_comp}/{mv_path.rsplit('/', 2)[0]}"
                    remote_comp_path = remove_path_new.rsplit('/', 2)[0]
                    storage_dir_path = storage_dir.rsplit('/', 2)[0]
                    self.paths2cln.add((component_path, remote_comp_path, storage_dir_path))
                except:
                    pass

        return True

    def clean_up_kpf(self):

        # all_dirs = ['CaHK', 'ExpMeter', 'FVC1', 'FVC2', 'FVC3',
        #             'Green', 'L0', 'Red', 'script_logs']
        all_dirs = ['CaHK', 'Green', 'Red']

        # clean up remaining files
        for pth in self.paths2cln:
            for cdir in all_dirs:
                local = f'{pth[0]}/{cdir}/'
                store = f'{pth[2]}/{cdir}'

                # TODO
                local = f"{inst_comp}/{pth[1]}/{cdir}/"

                # local = /s/sdata1701/kpfeng/2023feb02/Red/
                # pth[1] = /sdata1701/kpfeng/2023feb02
                # store = /instr1/KPF/kpfeng/2023feb02/Red
                self.log.info(f'clean up paths: {local}, {pth[1]}, {store}')
                file_list = glob(f'{local}/*', recursive=False)
                for file in file_list:
                    self.log.info(f'clean up files: {file}')
                    try:
                        filename = file.rsplit('/', 1)[1]
                    except:
                        pass

                    self.log.info(f'clean up removing filename: {filename}')

                    moved = self._rm_files(f'{local}{filename}', file, recursive=False)
                    if cdir == 'L0' and moved:
                        try:
                            koaid = filename.split('.fit')[0]
                            self.mark_deleted(koaid)
                        except IndexError:
                            log.warning('clean_up_kpf: cannot determine KOAID')
                            continue

                # remove component directory
                self.log.info(f'clean up component directories: {local}')
                self._rm_files(local, local, only_dir=True, recursive=False)

            # remove date directory
            self.log.info(f'clean up date directories: /s{pth[1]}')
            self._rm_files(f'/s{pth[1]}', pth[1], only_dir=True, recursive=False)


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

    def kpf_move_data(self, utd, utd2):
        def daterange(start_date, end_date):
            for n in range(int((end_date - start_date).days)):
                yield start_date + timedelta(n)

        kpf_root = '/s/sdata1701'

        utd_dt = datetime.strptime(utd, '%Y-%m-%d')
        utd_dt2 = datetime.strptime(utd2, '%Y-%m-%d')
        file_paths = []
        for utd_str in daterange(utd_dt, utd_dt2):
            mon_dir = utd_str.strftime("%Y%b%d").lower()
            file_path = f'{kpf_root}/*/{mon_dir}/*/*fits'
            try:
                files = glob(file_path, recursive=False)
                file_paths += files
            except FileNotFoundError:
                pass

        return file_paths

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
        # path = f'{store_dir}/{filename}*'
        # if not utils.exists_remote(f'{user}@{store_server}', path):

        storage_path = f'/net/storageserver/{store_dir}/{filename}*'
        if not utils.chk_file_exists(storage_path):
        # if not utils.exists_remote(f'{user}@{store_server}', path):
            log.error(f'data not on storage: {storage_path} data: {dat}')
            return False

        log.info(f'File found stored at: {storage_path}')

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

            # skip files with paths that include the 'path_exclude' string
            if path_exclude and path_exclude in result['ofname']:
                log_str = f"skipping {result['ofname']} -- contains: {path_exclude}."
                log.info(log_str)
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

            if not val and col not in ['status_code', 'archive_dir',
                                       'source_deleted']:
                err = "INCOMPLETE RESULTS"
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

    site = utils.get_config_param(config, config_type, f'site_{args.tel}')
    user = utils.get_config_param(config, config_type, 'user')
    store_server = utils.get_config_param(config, config_type, 'store_server')

    deleted_col = utils.get_config_param(config, 'db_columns', 'deleted')
    archived_key = utils.get_config_param(config, 'archive', 'archived')
    status_col = utils.get_config_param(config, 'db_columns', 'status')

    numbered_prefix = utils.get_config_param(config, 'passwords', 'numbered_prefix')
    numbered_suffix = utils.get_config_param(config, 'passwords', 'numbered_suffix')

    try:
        path_exclude = utils.get_config_param(config, 'path_exclude', args.inst)
    except:
        path_exclude = None

    eng_pw = utils.get_config_param(config, 'passwords', 'eng_account')

    # update the PW for new AD PW for NIRC2
    if args.inst.lower() == 'nirc2':
        pw_suffix = utils.get_config_param(config, 'passwords', f'{args.inst}_suffix')
        eng_pw += pw_suffix
        numbered_suffix += pw_suffix

    inst_root = utils.get_config_param(config, 'inst_disk', 'path_root')
    try:
        inst_comp = f"{inst_root}/{utils.get_config_param(config, 'inst_disk', args.inst)}"
    except:
        inst_comp = None

    server_user = utils.get_config_param(config, 'accounts', args.inst)
    server = utils.get_config_param(config, 'servers', args.inst)

    if not args.logdir:
        log_dir = utils.get_config_param(config, config_type, 'log_dir')
    else:
        log_dir = args.logdir

    log_name, log_stream = utils.create_logger('sdata_scrubber', log_dir)
    log = logging.getLogger(log_name)
    print(f'writing log to: {log_dir}/{log_name}')

    print(f"Scrubbing sdata in UT range: {args.utd} to {args.utd2}\n")
    log.info(f"Scrubbing sdata in UT range: {args.utd} to {args.utd2}\n")
    log.info(f"Avoiding paths with: {path_exclude}")

    delete_obj = ToDelete(args.inst)
    metrics = delete_obj.get_metrics()
    sdata_files = delete_obj.db_obj.get_files_to_move()

    if not sdata_files:
        exit("No files found to remove.")

    try:
        mv_path = sdata_files[0]['ofname']
    except:
        mv_path = None

    if mv_path:
        nfiles_before = utils.count_koa(mv_path, log)
    else:
        nfiles_before = 0

    koa_disk_num = utils.get_config_param(config, 'koa_disk', args.inst)
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
                       prefix=f'{args.inst} SDATA')




