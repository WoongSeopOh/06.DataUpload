# ---------------------------------------------------------------------------------------------------------------------------------------------------
# Description: 로깅
# ---------------------------------------------------------------------------------------------------------------------------------------------------
from datetime import datetime
import logging
import logging.handlers
import constant

# logger 셋팅
str_today = str(datetime.now().strftime("%Y%m%d"))
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s: %(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

log_folder = constant.log_path
log_nm = str_today + "_batchjob.log"
file_handler = logging.FileHandler(log_folder + log_nm)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def create_logger(logger_name):
    # Create Logger
    log_obj = logging.getLogger(logger_name)

    # Check handler exists
    if len(log_obj.handlers) > 0:
        return log_obj  # Logger already exists

    log_obj.setLevel(logging.DEBUG)

    formatter = logging.Formatter('\n[%(levelname)s|%(name)s|%(filename)s:%(lineno)s] %(asctime)s > %(message)s')

    # Create Handlers
    streamHandler = logging.StreamHandler()
    streamHandler.setLevel(logging.DEBUG)
    streamHandler.setFormatter(formatter)

    log_obj.addHandler(streamHandler)

    return log_obj


# logger = create_logger("Utils")
