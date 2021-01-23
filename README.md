# RTI_Data_Scrubber

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
        'koaid,status,ofname,stage_file,archive_dir,ofname_deleted'
        
   Results are verified:
        * Requires: 
            koaid, status, ofname, archive_dir, stage_file
        * status == Archived (or the value set as archived):
        * check filenames:  OFNAME == STAGE_FILE
        * check archive_dir ends with: 'lev0'
   
   Files are deleted.
        * files are removed (os.remove) individually by the OFNAME.
        * the process is logged.
        * The OFNAME_DELETED flag in koa->dep_status is set from 0 to 1 (koarti API call).    
   
Move:
   Query API / Database for files within date range (status = Archived)
        'koaid,status,ofname,stage_file,archive_dir,ofname_deleted'
        ** same query if both are run together
       
   Results are verified:
        * Requires: 
            koaid, status, ofname, archive_dir, stage_file
        * status == Archived (or the value set as archived):
        * check filenames:  OFNAME == STAGE_FILE
        * check archive_dir ends with: 'lev0' 
        
   Files are moved:
        * rsync each file -- by koaid to the storage location defined in the
        configuration file.