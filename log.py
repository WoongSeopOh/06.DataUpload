# ---------------------------------------------------------------------------------------------------------------------------------------------------
# Description: 로깅
# ---------------------------------------------------------------------------------------------------------------------------------------------------
from datetime import datetime
import logging
import logging.handlers
import constant


def create_logger(logger_name):
    str_today = str(datetime.now().strftime("%Y%m%d%H%M%S"))

    # Create Logger
    log_obj = logging.getLogger(logger_name)
    log_obj.setLevel(logging.INFO)

    # Check handler exists
    if len(log_obj.handlers) > 0:
        return log_obj  # Logger already exists

    log_obj.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s: %(message)s")

    # Create Handlers
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    log_obj.addHandler(stream_handler)

    # Create FileHandler
    log_folder = constant.log_path
    log_nm = str_today + "_batchjob.log"
    file_handler = logging.FileHandler(log_folder + log_nm)
    file_handler.setFormatter(formatter)
    log_obj.addHandler(file_handler)

    return log_obj


logger = create_logger("batchjob")
