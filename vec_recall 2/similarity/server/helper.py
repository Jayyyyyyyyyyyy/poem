import logging
import logging.handlers


def set_logger(context, verbose=False):
    logger = logging.getLogger(context)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s:' + context + ':[%(filename)s:%(funcName)s:%(lineno)d]:%(message)s', datefmt=
        '%m-%d %H:%M:%S')
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    console_handler.setFormatter(formatter)
    logger.handlers = []
    logger.addHandler(console_handler)
    
    return logger


def set_timeFile_logger(context, verbose=False):
    logger = logging.getLogger(context)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # import os 
    # print( os.getcwd())
    LOGGING_MSG_FORMAT  = '[%(asctime)s] [%(levelname)s]' + context + ':[%(filename)s:%(funcName)s:%(lineno)d]: %(message)s'
    LOGGING_DATE_FORMAT = r'%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(fmt=LOGGING_MSG_FORMAT, datefmt=LOGGING_DATE_FORMAT)
    filehandler = logging.handlers.TimedRotatingFileHandler("logs/qsim.log", when='midnight', interval=1, backupCount=7)
    filehandler.suffix = r'%Y-%m-%d' 
    filehandler.setLevel(logging.DEBUG if verbose else logging.INFO)
    filehandler.setFormatter(formatter)

    logger.addHandler(filehandler)
    return logger
