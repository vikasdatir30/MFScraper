###############################################################
#   Date        :   21-07-2021
#   Desc        :   This is common module use to get new logger
#   Use         :   To configure new logger
#   Author      :   Vikas Datir
#   Modified    :
###############################################################


import logging
from datetime import datetime as dt


log_format=logging.Formatter("[%(asctime)s] %(levelname)-8s  %(filename)-8s:%(lineno)d  %(message)s")

def config_logger(module_name="",file_location="",log_level=logging.INFO ):
    #configure handler
    log_filename = module_name
    log_filename = log_filename+"_"+str(dt.now().strftime("%Y-%m-%d_%I_%M_%p"))+".txt"
    #log_filename = file_location + "/" + log_filename

    handler = logging.FileHandler(log_filename)
    handler.setFormatter(log_format)
    logger = logging.getLogger(module_name)

    #setting level for log
    logger.setLevel(log_level)
    logger.addHandler(handler)

    return logger

if __name__ =='__main__':
    print('This should not be run as main')