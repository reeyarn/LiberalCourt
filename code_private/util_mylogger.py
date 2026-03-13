"""
to print log to a single file: 

python3 -u bmc_de_2123_bertopic.py > output.log 2>&1 

Version 8 Jan 2025
"""

import logging

import datetime 
_start_time = datetime.datetime.now()

import __main__

import inspect
import sys
import os

def setup_logger(name, level=logging.INFO, level_file=None, log_dir="/tmp", include_console=True):
    """
    Set up logger with file handler including date and PID
    
    Args:
        name: Logger name
        log_dir: Directory for log files
        include_console: Whether to add console handler
    """
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True) 

    if not level_file:
        level_file = level 

    # Get current date and PID
    today = datetime.datetime.now().strftime('%Y%m%d')
    pid = os.getpid()
    
    # Create log filename
    log_filename = os.path.join(log_dir, f"log_{name}_{today}_pid{pid}.log")
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Clear any existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Create file handler
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(level_file)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - PID:%(process)d - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Add console handler if requested
    if include_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    #return logger, log_filename
    return logger

class MyLogger:
    def __init__(self, name=__name__, level="INFO", add_file_handler=False):
        self.name = name
        self.logger = logging.getLogger(name)
        self.set_level(level)
        # Clear existing handlers
        if self.logger.hasHandlers():
            for handler in self.logger.handlers:
                handler.close()
            self.logger.handlers.clear()
        # Check if a StreamHandler for stderr already exists
        stderr_handler_exists = any(
            isinstance(handler, logging.StreamHandler) and handler.stream == sys.stderr
            for handler in self.logger.handlers
        )
        if not stderr_handler_exists:
            self._add_handler()  # Add stderr handler if it doesn't exist

        self.logger.propagate = True  # Important for file handler to work

        if add_file_handler:
            self._add_file_handler(add_file_handler)

    def info(self, message):
        parent_frame = inspect.currentframe().f_back
        parent_function_name = parent_frame.f_code.co_name
        self.logger.info( "{}: {}".format(parent_function_name, message))
        #print(f"INFO: {parent_function_name}: {message}")

    def debug(self, message):
        parent_frame = inspect.currentframe().f_back
        parent_function_name = parent_frame.f_code.co_name

        self.logger.debug("{}: {}".format(parent_function_name, message))
        #print(f"DEBUG: {parent_function_name}: {message}", end="\r")

    def warning(self, message):
        parent_frame = inspect.currentframe().f_back
        parent_function_name = parent_frame.f_code.co_name

        self.logger.warning("{}: {}".format(parent_function_name, message))
        #print(f"WARNING: {parent_function_name}: {message}", end="\r")
    def error(self, message):
        parent_frame = inspect.currentframe().f_back
        parent_function_name = parent_frame.f_code.co_name
        self.logger.error(  "{}: {}".format(parent_function_name, message))
        #print(f"ERROR: {parent_function_name}: {message}", end="\r")

    def exception(self, message):
        parent_frame = inspect.currentframe().f_back
        parent_function_name = parent_frame.f_code.co_name
        self.logger.exception("{}: {}".format(parent_function_name, message))
        #print(f"EXCEPTION: {parent_function_name}: {message}", end="\r")

    def critical(self, message):
        parent_frame = inspect.currentframe().f_back
        parent_function_name = parent_frame.f_code.co_name
        self.logger.critical("{}: {}".format(parent_function_name, message))
        #print(f"CRITICAL: {parent_function_name}: {message}", end="\r")

    def set_level(self, level):
        levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if level in levels:
            self.logger.setLevel(level)

    def _add_handler(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - PID:%(process)d - %(levelname)s - %(message)s')
        sh = logging.StreamHandler(sys.stderr)  # Explicitly use stderr
        sh.setFormatter(formatter)
        self.logger.addHandler(sh)

    def _add_file_handler(self, add_file_handler=None):
        if isinstance(add_file_handler, str):
            log_filename = add_file_handler
        else:
            log_filename = "/tmp/log_" + "{}".format(self.name) + '.log'

        formatter = logging.Formatter('%(asctime)s %(levelname)-8s pid%(process)d %(name)s %(message)s')
        logfile = logging.FileHandler(log_filename, mode='a')  # Append mode to avoid overwriting
        logfile.setFormatter(formatter)
        logfile.setLevel(logging.DEBUG)  # Ensure file handler logs all levels
        self.logger.addHandler(logfile)
        print(f"Logging file: {log_filename}")  # Indicate where the log file is
