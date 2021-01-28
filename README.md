# RTI_Data_Scrubber

Functions:

    remove OFNAME (dep_status table) file.
    move ARCHIVE_DIR (dep_status table) files to storage.
    move STAGE_FILE (dep_status table) files to storage.


Parameters:

    --dev"
        Only log the commands,  do not execute.
    --move"
        move the processed DEP files from the lev0 to the storage servers.
    --remove"
        delete the files from the instrument servers.
    --storagedir
        Change the path of the storage server from the one in the configuration file.
    --logdir
        Define the directory for the log.
    --utd
        Start date to process YYYY-MM-DD.
    --utd2
        End date to process YYYY-MM-DD.
    --include_inst
        comma separated list of instruments to include, the default is all instruments.
    --exclude_inst
        comma separated list of instruments to exclude, the default is to exclude no instruments.

Delete:

   Query API / Database for files within date range (status = Archived)
        'koaid,status,status_code,ofname,stage_file,
         process_dir,archive_dir,ofname_deleted'
         
         Query restricted to: OFNAME_DELETED = 0
                 
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
        * The OFNAME_DELETED flag in koa->dep_status is set from 0 to 1 (koarti API call).    
   
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
        
        