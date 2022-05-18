import logging
from os import path, mkdir, rmdir, walk
from datetime import datetime, timedelta
import scrub_ao_utils as utils

"""
Currently needs to be run as aobld@k1aoserver-new.  The HQ directories/files
are owned by aobld.
k1aoserver-new{/home/aobld/bin}: 
/usr/local/anaconda/bin/python3 scrub_ao_nightly.py --nscrub 1 --ncopy 1 --utd 20200918
"""


class ScrubAO:
    def __init__(self, args):
        self.tel = args.tel
        self.ao_user = f'k{self.tel}obsao'
        self.ao_server = f'k{self.tel}aoserver-new'
        self.utd = datetime.strptime(args.utd, '%Y%m%d')
        self.copy_start = self.utd - timedelta(days=(args.ncopy-1))
        self.scrub_start = self.copy_start - timedelta(days=args.nscrub)

        log.info(f"Copy/Sync from: {self.copy_start.strftime('%Y%m%d')}"
                 f" to {args.utd}")
        log.info(f"Scrubbing from: {self.scrub_start.strftime('%Y%m%d')}"
                 f" to {self.copy_start.strftime('%Y%m%d')}")

    def cp_ao_nightly(self):
        """
        iterate from the 'copy_start' to today's date syncing the summit
        directory with HQ directories.
        """
        for utd_datetime in utils.next_date(self.copy_start):
            if utd_datetime > self.utd:
                break

            log.info(f"-- Copying (sync) files for: "
                     f"{utd_datetime.strftime('%Y%m%d')} --")

            self.sync_ao_nightly(self.rsync_cp, utd_datetime)

    def scrub_ao_nightly(self):
        """
        iterate between 'scrub_start' and 'copy_start' removing data for the
        summit directories after syncing with HQ.
        """
        for utd_datetime in utils.next_date(self.scrub_start):
            if utd_datetime >= self.copy_start:
                break

            log.info(f"-- Scrubbing (sync/remove) files for: "
                     f"{utd_datetime.strftime('%Y%m%d')} --")

            self.sync_ao_nightly(self.rsync_mv, utd_datetime)

    def sync_ao_nightly(self, func, utd_datetime):
        paths = self.get_paths(utd_datetime)

        cnt1 = utils.count_local(paths, log)
        if func(paths) == 1:
            return

        cnt2 = utils.count_local(paths, log)

        if cnt1['summit'] > cnt2['hq']:
            log.warning('The file count at the summit is greater than at HQ'
                        f" after sync! UTD = {utd_datetime.strftime('%Y%m%d')},"
                        f" paths = {paths}")

    def rsync_cp(self, paths, remove=False):
        """
        The command to copy/sync the directory to HQ.

        :param paths: <dict> the paths: summit - source, HQ - destination
        :param remove: <bool> True if the source files should be removed.

        :return: <int> 0 on success, 1 on path not found, -1 on error
        """
        if not path.exists(paths['summit']):
            log.info(f"Path: {paths['summit']} has already been cleaned.")
            return 1

        hq_month_path = paths['hq_month']
        if not path.exists(hq_month_path):
            try:
                mkdir(hq_month_path)
                log.info(f"Created the directory for the month {hq_month_path}")
            except OSError:
                # check to see if the year needs to be made
                try:
                    mkdir(path.dirname(hq_month_path.strip('/')))
                except:
                    log.info(f"Error creating the directory for the year "
                             f"{path.dirname(hq_month_path.strip('/'))}")
            except:
                log.info(f"Error creating the directory for the month "
                         f"{hq_month_path}")

        if remove:
            rsync_cmd = ["rsync", "--remove-source-files", "-avz",
                         paths['summit'], paths['hq']]
        else:
            rsync_cmd = ["rsync", "-avz", paths['summit'], paths['hq']]

        ret_val = utils.run_cmd(rsync_cmd, log)
        if ret_val != 0:
            log.warning('Error syncing files,  check paths!')

        return ret_val

    def rsync_mv(self, paths):
        """
        command to sync the summit directory to HQ,  then remove summit files

        :param paths: <dict> the paths: summit - source, HQ - destination
        :return: <int> 0 on success, -1 on error
        """
        # TODO need to rsync in the sub directories
        # TODO should work,  untested
        # print('summit', paths['summit'])
        #
        dirs = sorted([x[0] for x in walk(paths['summit'])],
                      key=len, reverse=True)
        ret_val = 0
        # the last will be the shortest path
        if dirs and not path.exists(dirs[-1]):
            mkdir(dirs[-1])

        for direct in dirs:
            paths['summit'] = direct
            ret_val = self.rsync_cp(paths, remove=True)
            if ret_val != 0:
                log.warning(f'Error syncing files,  check paths! {paths}')
                ret_val += ret_val

            # TODO need way to deal with .swp files
            # rsync: send_files failed to open "/net/k2aoserver/k2aodata/nightly/21/08/18/.telnet-sc2.log.swp": Permission denied (13)
            # -rw-------  1 k2obsao aodev 16384 Aug 19 23:37 .telnet-sc2.log.swp

        # ---
        # clean the empty directories left behind.
        if path.isfile(paths['summit']):
            cln_cmd = ['find', paths['summit'], '-depth', '-type', 'd',
                       '-empty', '-not', '-path', paths['summit'], '-exec',
                       'rmdir', '{}', ';']

            utils.run_cmd(cln_cmd, log)

            # clean the date directory,  otherwise above cmd gets permission denied.
            cln_cmd = ['ssh', f'{self.ao_user}@{self.ao_server}', 'rmdir',
                       paths['summit']]
            utils.run_cmd(cln_cmd, log)

        # clean the month directory if empty
        summit_month_path = paths['summit'].rsplit('/', 1)[0]
        try:
            rmdir(summit_month_path)
        except OSError:
            pass

        return ret_val

    def get_paths(self, utd_datetime):
        """
        Determine the paths from the date.
            Summit: /net/k1aoserver/k1aodata/nightly/21/02/26/
            HQ:     /h/nightly1/ao/21/02/26

        :param utd_datetime: <datetime> the date in a datetime object.
        :return: <dict> the summit and HQ paths
        """
        utd_str = datetime.strftime(utd_datetime, '%y/%m/%d')
        utd_month = datetime.strftime(utd_datetime, '%y/%m')

        paths = {'summit': f'/net/k{self.tel}aoserver/k{self.tel}aodata/nightly/{utd_str}/',
                 'hq': f'/h/nightly{self.tel}/ao/{utd_str}/',
                 'hq_month': f'/h/nightly{self.tel}/ao/{utd_month}/'}

        return paths


if __name__ == '__main__':
    """
    To run:
        python3 scrub_ao_nightly.py --tel 2
    """
    mailto = 'lfuhrman@keck.hawaii.edu'
    log_dir = '/home/aobld/log/'

    args = utils.parse_args()

    log_name, log_stream = utils.create_logger('ao_nightly_dir', log_dir,
                                               args.tel)
    if not log_name:
        print("Error while starting logging,  could not create logger.")

    log = logging.getLogger(log_name)
    log.info(f"AO nightly directory sync/scrub.\n"
             f"\t\tUT date to end: {args.utd},\n"
             f"\t\tNumber of days to copy: {args.ncopy},\n"
             f"\t\tNumber of days to scrub:  {args.nscrub}.")

    scrub_obj = ScrubAO(args)
    scrub_obj.cp_ao_nightly()
    scrub_obj.scrub_ao_nightly()

    utils.write_emails(log_stream, mailto, 'AO')

    log.info("DONE")

