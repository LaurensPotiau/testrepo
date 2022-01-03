import sys
from datetime import datetime

from file_handler import FileHandler

from aws_logging.log import Log


### I was here 

def start_dpg_file_handler(prefix, category, active_flag):
    """ Starts the data transfer of the dpg files. """

    # Start log 
    l = Log('application22222', 'dpg-file-handler')
    ts = datetime.now()
    l.add_log('dpg-file-handler.start', 'DPG Handler - Script started for category: {}'.format(category), l.RUN_STATUS_OK, ts, ts)

    # Instantiate FileHandler object & process 
    step_name = 'dpg-file-handler.instantiate-file-handler-object'
    step_descr = 'dpg-file-handler - Instantiate FileHandler object'
    try:
        sts = datetime.now()
        file_handler = FileHandler(prefix, category, active_flag)
        
    except Exception as e:
        ets = datetime.now()
        l.add_log(
            step_name, 
            step_descr, 
            l.RUN_STATUS_FAIL, 
            sts, 
            ets, 
            e
        )
        return l
    
    ets = datetime.now()
    l.add_log(
        step_name, 
        step_descr, 
        l.RUN_STATUS_OK, 
        sts, 
        ets
    )
    
    # Process 
    l = file_handler.process(l)

    # Wrap up logging
    ts = datetime.now()
    l.add_log(
        'dpg-file-handler.end', 
        'DPG File Handler - Script ended.', 
        l.RUN_STATUS_OK, 
        ts, 
        ts
    )
    
    l.commit()

if __name__ == "__main__":
        
    start_dpg_file_handler(sys.argv[1], sys.argv[2], sys.argv[3])

