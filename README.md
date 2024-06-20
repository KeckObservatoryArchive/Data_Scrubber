# RTI_Data_Scrubber

Functions:

    remove OFNAME (dep_status table) file.
    move ARCHIVE_DIR (dep_status table) files to storage.
    move STAGE_FILE (dep_status table) files to storage.

Parameters:

    --dev
        Only log the commands,  do not execute.
    --storagedir
        Change the path of the storage server from the one in the configuration file.
    --logdir
        Change the directory for the log.
    --inst 
        the instrument to scrubber,  mandatory input
    --remove
        removes the processed data while moving to storage (overrides the configuration parameter)
    --utd
        Start date to process YYYY-MM-DD.
    --utd2
        End date to process YYYY-MM-DD.


Configuration File (scrubber_config.ini):

    [inst_list]
    include
        comma separated list of instruments to include, the default (if empty string) is all instruments.
    exclude_inst
        comma separated list of instruments to exclude, the default (if empty string) is to exclude no instruments.


Delete:

   Query API / Database for files within date range (status = Archived)
        'koaid,status,status_code,ofname,stage_file,
         process_dir,archive_dir,ofname_deleted'
         
         Query restricted to: 
            source_deleted = 0
                 
   Results in delete list are verified:
   
        * Requires: 
            koaid, status, ofname, process_dir, stage_file
        * status == Archived (or the value set as archived)
        * no STATUS_CODE (dep_status table) -- any status code
          is assumed to require re-processing so files not deleted or moved.
        * check filenames:  OFNAME == STAGE_FILE
        * check archive_dir ends with: 'lev0' 

   Checks results that STAGE_FILE (dep_status table) exists
       
       * STAGE_FILE is a copy of OFNAME,  so ensures a copy exists
         before deleting OFNAME
       * If doesn't exist:
           Checks storage STAGE_FILE exists
       * If neither exists,  do not delete OFNAME file 
   
   Files are deleted.
   
        * OFNAME files (dep_status table) are deleted.
        * files are removed (os.remove) individually by the OFNAME.
        * the process is logged.
        * The OFNAME_DELETED flag in koa->dep_status is set from 0 to 1 (koa rti API call).    
   
Move:

   Query API / Database for files within date range (status = Archived)
        'koaid,status,status_code,ofname,stage_file,
         process_dir,archive_dir,ofname_deleted'
       
        Query restricted to: ARCHIVE_DIR = '' or NULL

   Results are verified (same verification as above for delete):
   
        * Requires: 
            koaid, status, ofname, process_dir, stage_file
        * status == Archived (or the value set as archived)
        * no STATUS_CODE (dep_status table) -- any status code
          is assumed to require re-processing so files not deleted or moved.
        * check filenames:  OFNAME == STAGE_FILE
        * check archive_dir ends with: 'lev0' 
        
   Files are moved:
   
        * rsync lev0 files -- by koaid to the storage location 
          with the dir numbers defined in the configuration file.
          ** not in dev mode,  the rsync remove-files flag is set
          ** ie: files with matching koaid in:
               '/koadata/NIRES/20210124/lev0' 
                 are moved to:
               '/koadata/test_storage/koastorage03/NIRES//koadata32/20210124/lev0/'

        * rsync the 'stage_file' (dep_status table) files.  
          The 'stage' file is a copy of the original file OFNAME (dep_status table)
          to the stage storage location. 
          ** not in dev mode,  the rsync remove-files flag is set
          ** ie: fits files:
               '/koadata/NIRES/stage/20210124//s/sdata1500/nires6/2021jan24//v210124_0162.fits'
                 are moved to:
               '/koadata/test_storage/koastorage03/NIRES/stage/NIRES/20210124//s/sdata1500/nires6/2021jan24/'
               
 
   Report / Email generated:
   
        Number of files deleted: 368
        Number of files found: 368
        
        Number of lev0/ files moved: 368
        Number of lev0/ files found: 368
        
        Number of stage files moved: 368
        Number of stage files found: 368
        
        Total number of files not previously deleted (any status): 526
        
        
   Required:
   
   ssh key must be on data and storage servers
   (in authorized_keys):
   
   Cron:
       0  9 * * 5 /usr/local/home/koarti/lfuhrman/Scrubber/scrub.csh > /dev/null 2>&1

        
# KOA Nightly DEP Data Scrubber

Functions:

    move the KOA DEP files from data server to the storage server.  The
    servers are defined in the configuration files.  The function is intended 
    for the KOA processed data that is processed each night,  not for 
    realtime-ingestion (RTI).


Parameters:
    --inst
        The instrument name, default is all instruments (defined in
        scrubber_config.ini)
    --utd
        define the UT start date,  format: YYYY-MM-DD. The default
        is 21 days before the current date.
    --utd2
        define the UT end date,  format: YYYY-MM-DD. The default
        is 14 days before the current date.
    --dev
        test mode only copies data,  it does not remove the data
        from data server.  The mode is configured in the 
        scrubber_config.ini file but can be overwritten here.


Logs:
    written to,  and can be changed in the configuration file.
        /log/scrubber_logs/koa_scrubber_<YYYYMMDD>_<HH:MM:SS>.log
        

Report / Email generated:
   
    KOA DEP Files moved to storage for dates: 2021-02-02 to 2021-02-03
    
    8460 : Total KOA files BEFORE.
    8460 : Total Storage files BEFORE.
    8460 : Total KOA files AFTER.
    8460 : Total Storage files AFTER.
    
    0 :Number of files removed from KOA.
    0 :Number of files moved to storage.
