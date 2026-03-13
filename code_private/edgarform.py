"""
MIT License 
Copyright 2023--2025 Reeyarn Li
VERSION 2025.12.14.01

# DOCUMENTATION

In this Python code, I  develop the python classes for processing SEC EDGAR Forms.

 
"""
#EDGAR_ROOT = "/text/edgar/"
#USER_AGENT_TEXT = "University of Mannheim reli@uni-mannheim.de"



# Import Python's native modules
import datetime
_start_time = datetime.datetime.now() 
import functools
from multiprocessing import Pool
from p_tqdm import p_umap
from tqdm import tqdm
from multiprocessing import cpu_count

import multiprocessing.pool
import functools


from functools import partial
from itertools import chain
import os, sys, io, time
import regex as re
import argparse
import gzip 
from typing import Union
import html
import mimetypes
import zipfile
#import socket
#import warnings
import random
import json
import subprocess
# Import PIP packages
import requests
import logging
import pysbd
# if __name__=="__main__":
#     import dropbox

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup, NavigableString
import markdown
#import util_trygmail
import time
import roman
from html.parser import HTMLParser
import lxml
from lxml import etree
import traceback
#from util_gzip import gzip_to_df, df_to_gzip
from unidecode import unidecode
import logging
from .util_mylogger import MyLogger

if __name__=="__main__":
    pid = os.getpid()
    LOGFILE = f"/tmp/ef_{str(datetime.date.today())}_{pid}.log"
    logger = MyLogger("edgarform", "INFO", LOGFILE)
else:
    logger = logging.getLogger(__name__)


#    logger = MyLogger(__name__, "INFO", True)

# If needed, should  install 'django-environ' rather than 'environ' package.


#table_html = lxml.etree.tostring(lxml.etree.HTML(case_html).xpath("//html/body/div[1]/div[4]/table/tbody/tr/td/table/tbody/tr/td/table/tbody/tr/td/table[2]")


# # multicore df.apply # https://github.com/jmcarpenter2/swifter
# import swifter
# from swifter import set_defaults
# set_defaults(
#     npartitions=26,
#     dask_threshold=1,
#     scheduler="processes",
#     progress_bar=False,
#     progress_bar_desc=None,
#     allow_dask_on_strings=True,
#     force_parallel=True,
# )

# with warnings.catch_warnings():
#     warnings.filterwarnings("ignore", category=UserWarning, module="swifter")


def timeout(max_timeout):
    """Timeout decorator, parameter in seconds."""
    def timeout_decorator(item):
        """Wrap the original function."""
        @functools.wraps(item)
        def func_wrapper(*args, **kwargs):
            """Closure for function."""
            pool = multiprocessing.pool.ThreadPool(processes=1)
            async_result = pool.apply_async(item, args, kwargs)
            try:
                # raises a TimeoutError if execution exceeds max_timeout
                return async_result.get(max_timeout)
            except TimeoutError:
                #logger.warning("Timeout occurred for function: %s", item.__name__)
                parameter_string = ", ".join([repr(arg) for arg in args] + [f"{k}={repr(v)}" for k, v in kwargs.items()])
                logger.warning("Timeout occurred for function: %s, Parameters: %s", item.__name__, parameter_string)                
                # Re-raise the TimeoutError
                raise TimeoutError
        return func_wrapper
    return timeout_decorator


def timeout__orig(max_timeout):
    """Timeout decorator, parameter in seconds."""
    def timeout_decorator(item):
        """Wrap the original function."""
        @functools.wraps(item)
        def func_wrapper(*args, **kwargs):
            """Closure for function."""
            pool = multiprocessing.pool.ThreadPool(processes=1)
            async_result = pool.apply_async(item, args, kwargs)
            # raises a TimeoutError if execution exceeds max_timeout
            return async_result.get(max_timeout)
        return func_wrapper
    return timeout_decorator





def create_parser():
    """Argument Parser
    This function automatically creats a HELP message if run this python code in commandline with -h or --help
    And takes the argument
    
    In the default behavior of argparse, the translation from the command-line argument to the attribute name does involve replacing dashes (-) with underscores (_).
    See documentation: https://docs.python.org/3/library/argparse.html
    """
    class CustomFormatter(argparse.HelpFormatter):
        def _split_lines(self, text, width):
            lines = super()._split_lines(text, width)
            #print("DOC "+ str(width))
            #for line in lines:
            #    print("DOC: "+ line + str(len(line)))
            #return [line if line.strip() else '' for line in lines]    
            return [line + "\n" if len(line)<width-5 else line for line in lines]    
    parser = argparse.ArgumentParser(formatter_class=CustomFormatter)
    
    parser = argparse.ArgumentParser(description='This python code can build SEC EDGAR filing  index and process filing forms and extracting items from form 10-k')
    
    # To execute certain functions of the code
    parser.add_argument("-bidx", "--build-edgar-index", action = "store_true", help = """To run the EdgarIndex().download_index(), download the quarterly index zip files from SEC EDGAR website and save it to Local and/or Cloud FS, and create year x filingtype index pd.DataFrame() and save .csv.gz file""" )
    
    parser.add_argument("-dw10k", "--download-10k-years", type=str, nargs='?', default='', help="EG --download-10k-years=year; --download-10k-years=year1..year2; To download form 10K filings and process to individual document files in the tfnm folder")
    parser.add_argument("-form", "--downloadform", type=str, default="8k" )
    
    parser.add_argument("-bf10k", "--build-10k-formbody-years", type=str, nargs='?', default='', help="EG --buildformbody-10k-years=year; --buildformbody-10k-years=year1..year2; To build 10k body_df as CSV and save in the tfnm folder")
    
    parser.add_argument("-xbrltb", "--get_xbrl_textblock_title", action = "store_true", help = """To save xbrl instance file, and extract textblock with title; example: edgarform.py --years 2009..2023 -xbrltb""" )
    
    
    parser.add_argument("-getnote10k", "--get-note-10k-by-keyword", type=str, nargs="?", default = "", help = "EG -getnote10k income_tax"
                        )
    
    parser.add_argument("-getbodydf", "--get-bodydf-by-filename", type=str, help="To re-generate body_df for particular filename. EG: -getbodydf edgar/data/83402/0000950116-03-004989.txt ")
    
    # 3. 8-K Processing (New)
    parser.add_argument("-meta8k", "--process-8k-metadata", action="store_true", 
                        help="Process 8-K filings to extract header items, detect Earnings Announcements (Item 2.02), and find Non-GAAP metrics.")
    
    parser.add_argument("-ciks", "--ciks", type=str, help="When running -dw10k, restrict to a subset of ciks separated by comma, such as `-ciks 123,45,12123` ")
    
    parser.add_argument("-years", "--years", type=str, help="When running, restrict to a subset of years separated by .., such as `1996..2003` ")
    
    
    parser.add_argument("-edgdir", "--edgar-root-dir", type=str, default="/mnt/text/edgar/", help = """Assign the edgar root directory. """ )
    
    parser.add_argument("-rmold", "--remove-legacy-files", action="store_true", help="To delete legacy filing .txt file when running wiht -dw10k")
    
    
    
    
    
    # Additional Info needed
    parser.add_argument('-ua', '--http-user-agent', type=str, required=False, default= "Your Institution name youremail@mail.yourinstitute.edu",
        help="""SEC EDGAR requires that bots declare your user agent in request headers:
        Sample Company Name AdminContact@<sample_company_domain>.com
        See https://www.sec.gov/os/accessing-edgar-data for more info""")
    
    parser.add_argument("-test", "--test", action="store_true")
    return parser




class ManageOvercloud(object):
    """
    This class replaces local file IO functions such as os.path.isfile(), os.path.isdir(), open(...).read(), open(...).write() 
        and operates flexibles on local storage and dropbox cloud.
    When provided with a dropbox access token, this class module can use Dropbox Python SDK API to do the above operations on Dropbox cloud.
    When use_localfs, use_dropbox = True, Flase, it reduces to os. method
    When use_localfs, use_dropbox = True, True,  it will write to both, but read from local
    When sync_if_missing_file = True, when one file exists in one location but not the other, it will attempt to sync to the other location.


    Dropbox python API documentation:
    https://dropbox-sdk-python.readthedocs.io/en/latest/index.html

    
    self= ManageOvercloud(True, True)
    rel_path = "/tmp2/d2"
    """
    #self.dbx=None

    def __init__(self, logger: logging.Logger = logging.getLogger(__name__),
                 dropbox_app_key: str = "", dropbox_app_secret: str = "",
                 use_localfs = True, use_dropbox = False, 
                 local_prefix = "", cloud_prefix = "", 
                 sync_if_missing_file=False):
        self.logger = logger
        self.logger.debug(f"""ManageOvercloud init... \n  Use Local HD: {use_localfs}, local_prefix = {local_prefix} ;
        \n  Dbx Cloud {use_dropbox}, mylc cloud_prefix = {cloud_prefix}.
        \n  Dropbox app key {dropbox_app_key}, app secret {dropbox_app_secret}""")    
        self.use_localfs  = use_localfs
        self.use_dropbox  = use_dropbox
        self.local_prefix = local_prefix
        self.cloud_prefix = cloud_prefix
        self.sync_if_missing_file = sync_if_missing_file
        self.dropbox_app_key = dropbox_app_key
        self.dropbox_app_secret = dropbox_app_secret
        #if not 'dbx' in globals():
        #    self.use_dropbox = False
        #    if use_dropbox:
        #        raise Exception("use_dropbox = True but no dbx instance is assigned")
        if self.use_dropbox:
            access_token = self.get_existing_dropbox_token()
            dbx = self.connect_dropbox(access_token)
            if dbx == None:
                self.use_dropbox = False; 
            else:
                self.dbx = dbx
        elif not self.use_localfs:
            self.logger.info("Dropbox not on. Assume local FS is on")
            self.use_localfs = True
        self.logger.debug(f"Finished init. Dbx Cloud status {self.use_dropbox}")

    @staticmethod
    def _remove_doubleslash_endslash(rel_path):
        while '//' in rel_path:
            rel_path = rel_path.replace('//', '/')
        if rel_path.endswith("/"):
            rel_path = rel_path[:len(rel_path)-1]            
        return rel_path        
    
    def makedirs(self, rel_path):
        """Tested on both: DONE
        rel_path = '/text/edgar/by-index/abc'
        rel_path = '/text/edgar/'
        """
        rel_path = self._remove_doubleslash_endslash(rel_path)
        if self.use_localfs:
            local_full_path=self.local_prefix + rel_path
            if not os.path.exists(local_full_path):
                os.makedirs(local_full_path, exist_ok=True)
                self.logger.debug(f"Make dir in local filesystem: {local_full_path}")
        if self.use_dropbox:
            cloud_full_path = self.cloud_prefix + rel_path    
            cloud_full_path = self._remove_doubleslash_endslash(cloud_full_path)
            #try: 
            #    metadata = dbx.files_get_metadata(cloud_full_path, include_media_info=True)
            try: 
                self.dbx.files_create_folder_v2(cloud_full_path)
                self.logger.info(f"Make dir in dbx-cloud filesystem: {cloud_full_path}")
            except dropbox.exceptions.ApiError as e:
                self.logger.error(f"Dropbox Failed to create directory {cloud_full_path}, {str(e)}")
                    
    def rename(self, source, destination):
        """
        source = "/tmp/d1"
        destination = "/tmp/d3"
        Tested with dropbox
        """
        if self.use_localfs:
            if not os.path.exists(self.local_prefix + destination):
                # Move the file
                if os.path.exists(self.local_prefix + source):
                    os.rename(self.local_prefix + source, self.local_prefix + destination)
                    self.logger.debug("Local Move/Rename successful: {} -> {}".format(self.local_prefix + source, self.local_prefix + destination))
                else: 
                    self.logger.warning("Source {} does not exist. Move aborted.".format(self.local_prefix + source))    
            else:
                self.logger.warning("Destination {} already exists. Move aborted.".format(self.local_prefix + destination))    
        if self.use_dropbox:
            try:
                full_dest = self._remove_doubleslash_endslash(self.cloud_prefix + destination)                    
                full_from = self._remove_doubleslash_endslash(self.cloud_prefix + source)
                self.dbx.files_move(full_from, full_dest)
                self.logger.debug("Dbx Cloud Move/Rename successful: {} -> {}".format(full_from, full_dest))
            except dropbox.exceptions.ApiError as e:
                if isinstance(e.error, dropbox.files.RelocationError):
                    self.logger.warning("A conflict occurred. The destination already exists, or the source does not exist")
                else:
                    self.logger.critical("While trying to rename in Dropbox cloud, Undefined Exception occurred " + str(e))       

    def remove(self, rel_path):
        return_value, local_return_value, dbx_return_value = None, None, None
        local_full_path = self.local_prefix + rel_path
        cloud_full_path = self._remove_doubleslash_endslash(self.cloud_prefix + rel_path)
        if self.use_localfs:
            try:
                os.remove(local_full_path)
                self.logger.info(f"Deleted file from local filesystem {local_full_path}")
                local_return_value = True
            except OSError as e:
                self.logger.info(f"Unable to delete file from local filesystem {local_full_path} " + str(e))
        else:
            local_return_value = True        
        if self.use_dropbox:
            try: 
                dbx.files_delete_v2(cloud_full_path)
                dbx_return_value = True
            except dropbox.exceptions.ApiError as e:
                self.logger.info(f"Unable to delete file from dropbox filesystem {cloud_full_path} " + str(e))
        else:
            dbx_return_value = True        
        return_value = local_return_value and dbx_return_value        
        return return_value
        
    def listdir(self, rel_path):
        """
        Syncing if missing file is not implemented for ManageOvercloud.listdir(); but the returned list is the subset of files that exist on both locations
        """
        return_value, local_return_value, dbx_return_value = [], [], []
        local_full_path = self.local_prefix + rel_path
        cloud_full_path = self._remove_doubleslash_endslash(self.cloud_prefix + rel_path)
        if self.use_localfs:
            local_return_value = os.listdir(local_full_path)
        if self.sync_if_missing_file or not local_return_value:  
            if self.use_dropbox:  
                try:
                    result = dbx.files_list_folder(cloud_full_path)
                    dbx_return_value = [entry.name for entry in result.entries]                
                except Exception as e:
                    self.logger.critical("Error listing dropbox folder {cloud_full_path}")    
        if self.sync_if_missing_file:
            self.logger.debug("Syncing if missing file is not implemented for ManageOvercloud.listdir(); but the returned list is the subset of files that exist on both locations")
            return_value = [folder for folder in local_return_value if folder in dbx_return_value]
        else:    
            return_value = local_return_value or dbx_return_value
        return return_value
                    
    def path_exists(self, rel_path ):
        """
        DEPRECIATED. 
        
        USE path_isfile or path_isdir instead
        rel_path = os.path.expanduser("~/Dropbox/Codes/")
        rel_path = '/text/edgar/full-index/1998-QTR4.csv.gz'
        rel_path='/text/edgar/by-index/'
        """
        self.logger.critical("Should not have called ManageOvercloud.path_exists(). Call ManageOvercloud.path_isfile() or ManageOvercloud.path_isdir() instead")
        return_value = False
        if self.use_localfs:
            local_full_path=self.local_prefix + rel_path
            local_return_value = os.path.exists(local_full_path)
            return_value =  return_value or local_return_value
            self.logger.debug(f"Local path_exists {local_full_path} local result: {local_return_value}")
        if not return_value:        
            if self.use_dropbox:
                cloud_full_path = self._remove_doubleslash_endslash(self.cloud_prefix + rel_path)
                try:
                    dbx_return_value = self.dbx.files_get_metadata(cloud_full_path)
                    self.logger.debug(f"Dbx Cloud path {cloud_full_path} exists.")
                    return_value = return_value or bool(dbx_return_value)
                except dropbox.exceptions.ApiError as e:
                    return_value =  return_value or  False
        return return_value
      
    def path_isfile(self, rel_path, check_onlyone_overrule = False):
        """
        rel_path = '/text/edgar/by-index/year2021_10k.csv'
        rel_path = "/text/edgar"
        self = mylc
        In general, when self.sync_if_missing_file = False, this function returns True as long as file exists in one place.
        When self.sync_if_missing_file =True, globally relative to the whole ManageOvercloud() instance, this function returns True only when files exist in both places; otherwise it will upload/download to make it happen when the file exists in only one place, and return False
        When check_onlyone_overrule = True, as long as file exists in one place before invoking this function, this function returns true, regardless of self.sync_if_missing_file and the upload/download action it may trigger within this function.
        Use check_onlyone_overrule = True for checking raw files directly from SEC to avoid redundant download when the file already exists in one place.
        """
        return_value, local_return_value, dbx_return_value = False, False, False
        local_full_path=self.local_prefix + rel_path
        cloud_full_path = self._remove_doubleslash_endslash(self.cloud_prefix + rel_path)
        if self.use_localfs:
            local_return_value =  os.path.isfile(self.local_prefix + rel_path)
            self.logger.debug(f"Local path_isfile {local_full_path} result: {local_return_value}")
        if self.sync_if_missing_file or not local_return_value:
            if self.use_dropbox:
                try:
                    #dbx_return_value = dbx.files_get_metadata(cloud_full_path)
                    metadata = self.dbx.files_get_metadata(cloud_full_path, include_media_info=True)
                    if  isinstance(metadata, dropbox.files.FileMetadata):
                        dbx_return_value = True
                        self.logger.debug(f"Dbx Cloud path_isfile exists: {cloud_full_path}")
                        if self.sync_if_missing_file and not local_return_value:
                            # Need to download from Dropbox to local
                            self.logger.debug(f"""Local file {local_full_path} not existing, but found in dropbox {cloud_full_path}. With --sync-if-missing-file, Start Downloading.""")
                            self.dbx_download(cloud_full_path, local_full_path)
                            self.logger.info(f"""Local file {local_full_path} not existing, but found in dropbox {cloud_full_path}. With --sync-if-missing-file, Finished Downloading.""")
                    elif isinstance(metadata, dropbox.files.FolderMetadata):
                        self.logger.info(f"Dbx Cloud path_isfile checking: {cloud_full_path} may exist but is a folder")    
                    else:
                        self.logger.info(f"Dbx Cloud path_isfile checking: {cloud_full_path} may exist but not a file")    
                except:
                    self.logger.debug(f"Dbx Cloud path_isfile checking FAILED: {cloud_full_path} ")    
                if self.sync_if_missing_file and self.use_localfs and not dbx_return_value:
                    # Need to Upload from local to dropbox.
                    self.logger.debug(f"""Local file {local_full_path}  existing, but not found in dropbox {cloud_full_path}. With --sync-if-missing-file, Start Reading local file.""")
                    cloud_parent_folder = "/"+ os.path.join(*rel_path.split("/")[:-1])
                    if not self.path_isdir(cloud_parent_folder, check_both=True):
                        self.makedirs(cloud_parent_folder)
                    if local_return_value: 
                        # if local file exist but not in cloud, read local and upload to dropbox
                        with open(local_full_path, "rb") as f:
                            bytes_data = f.read()
                            self.logger.debug(f"""Local file {local_full_path}  existing, but not found in dropbox {cloud_full_path}. With --sync-if-missing-file, Start Uploading local file.""")
                            self.dbx_upload(bytes_data, cloud_full_path)
                            self.logger.info(f"""Local file {local_full_path}  existing, but not found in dropbox {cloud_full_path}. With --sync-if-missing-file, Finished Uploading local file.""")
        # if self.sync_if_missing_file:    
        #     return_value = local_return_value and dbx_return_value
        # else:    
        #     return_value = local_return_value or dbx_return_value
        # if check_onlyone_overrule:
        #     return_value = local_return_value or dbx_return_value
        return_value = local_return_value or dbx_return_value
        return return_value
    
    def path_isdir(self, rel_path, check_both=False):
        return_value, local_return_value, dbx_return_value = False, False, False
        if self.use_localfs:
            local_full_path= self.local_prefix + rel_path
            local_return_value = os.path.isdir(local_full_path)
            self.logger.debug(f"Local path_isdir {local_full_path} result: {local_return_value}")
        if check_both or self.sync_if_missing_file or not local_return_value:      
            if self.use_dropbox :
                cloud_full_path=self._remove_doubleslash_endslash(self.cloud_prefix + rel_path)
                try:
                    #dbx_return_value = dbx.files_list_folder(cloud_full_path)
                    dbx_return_value = False
                    metadata = self.dbx.files_get_metadata(cloud_full_path, include_media_info=True)
                    if isinstance(metadata, dropbox.files.FolderMetadata):
                        self.logger.debug(f"Dbx Cloud path_isdir {cloud_full_path} dbx-cloud result: True ")
                        dbx_return_value = True
                except:
                    self.logger.debug(f"Dbx Cloud path_dir checking FAILED: {cloud_full_path} ")    
        # if check_both or self.sync_if_missing_file:
        #     return_value = local_return_value and bool(dbx_return_value)
        # else:    
        return_value = local_return_value or bool(dbx_return_value)
        if self.sync_if_missing_file:
            if return_value:
                if not local_return_value or not dbx_return_value:
                    self.makedirs(rel_path)                        
        return return_value
    
    def dbx_upload(self, f: bytes, dbx_full_path):
        while '//' in dbx_full_path:
            dbx_full_path = dbx_full_path.replace('//', '/')
        dbx_full_path = self._remove_doubleslash_endslash(dbx_full_path)
        use_dropbox = False
        if self.use_dropbox:
            use_dropbox = True
        elif "dbx" in globals():
            use_dropbox = True
        if use_dropbox:
            if len(f)< 150_000_000:
                req_started_at = round(time.time() * 1000)
                self.dbx.files_upload(f, dbx_full_path, dropbox.files.WriteMode.overwrite)
                req_ended_at = round(time.time() * 1000)
                speed =  round(len(f) / (req_ended_at - req_started_at) / 1000,2)
                self.logger.info(f"Upload to dropbox cloud file {dbx_full_path}, speed {speed} Mb/s over {(req_ended_at - req_started_at) / 1000} seconds")
            else: 
                self.logger.critical(f"Cannot upload to dropbox with size bigger than 150M: {dbx_full_path}") 
        else:
            self.logger.critical(f"use_dropbox = False but called dbx_upload() for file {dbx_full_path}")    

    def dbx_download(self, dbx_full_path, local_full_path = None):
        """
        read a file from dropbox cloud, and return it as bytes value, 
        if provided local_full_path, then saved as local file
        """
        dbx_full_path = self._remove_doubleslash_endslash(dbx_full_path)    
        use_dropbox = False
        if self.use_dropbox:
            use_dropbox = True
        elif "dbx" in globals():
            use_dropbox = True
        if use_dropbox:
            try:
                req_started_at = round(time.time() * 1000)
                md, res = self.dbx.files_download(dbx_full_path)
                data = res.content
                req_ended_at = round(time.time() * 1000)
                speed =  round(len(data) / (req_ended_at - req_started_at) / 1000,2)
                self.logger.info(f"Read from dropbox cloud file {dbx_full_path}, speed {speed} Mb/s over {(req_ended_at - req_started_at) / 1000} seconds")
                if local_full_path:
                    with open(local_full_path, "wb") as file:
                        file.write(data)
                        self.logger.info(f"Save to local file {local_full_path}")
            except dropbox.exceptions.HttpError as err:
                self.logger.critical('***  dbx_download HTTP error', err)
        else:
            self.logger.critical("use_dropbox = False but called dbx_download()")    
        return data

    def write(self, data: Union[bytes, str], rel_path, use_gzip=False):
        """
        write(...) allows writing to both local storage and dropbox cloud;
        but writing to dropbox cloud only when file size less than 150M
        If not written to local nor dropbox cloud, raise an exception
        """
        upload_success = False
        if isinstance(data, str):
            data=data.encode(encoding="utf-8")
        if use_gzip:
            data_gzipped = gzip.compress(data) 
            if not rel_path.endswith("gz"):
                rel_path = "%s.gz" % rel_path
        else: 
            data_gzipped = data
        if self.use_localfs:
            local_full_path = self.local_prefix + rel_path
            with open(local_full_path, 'wb') as file:
                file.write(data_gzipped)
                self.logger.debug(f"Written file to local FS: {local_full_path}")
            upload_success = True    
        if self.use_dropbox:
            if len(data_gzipped) < 150_000_000:
                cloud_full_path = self._remove_doubleslash_endslash (self.cloud_prefix + rel_path)
                self.dbx_upload(data_gzipped, cloud_full_path) # will add self.cloud_prefix +  at dbx_upload
                self.logger.debug(f"Written file to cloud FS: {cloud_full_path}")
                upload_success = True
            if len(data_gzipped) >= 150_000_000:    
                self.logger.critical("File size over 150M, cannot write to Dropbox {self.cloud_prefix}{rel_path}")
        if not upload_success:
            self.logger.critical(f"use_localfs = False; Neither can write to dropbox {rel_path}")

    def read(self, rel_path, read_mode = "rb", use_gzip=False):
        """
        write(...)  allows writing to both local storage and dropbox cloud
        but  read() will try reading from local first if allowed; otherwise if allowed read from dropbox
        self = mylc
        rel_path = "/tmp/var1.txt.gz"
        """
        return_value = bytes()
        bytes_data, txt = bytes(), str()
        if self.use_localfs:
            if os.path.isfile(self.local_prefix + rel_path):
                with open(self.local_prefix + rel_path, 'rb') as file:
                    bytes_data = file.read()  
        if not bytes_data  and self.use_dropbox :
            dbx_full_path = self._remove_doubleslash_endslash (self.cloud_prefix + rel_path)
            bytes_data = self.dbx_download(dbx_full_path = dbx_full_path)
        #else:
        #    self.logger.critical("use_localfs and use_dropbox are both False")    
        if use_gzip:
            decompressed_bytes = gzip.decompress(bytes_data)  
        else: 
            decompressed_bytes = bytes_data
        return_value =  decompressed_bytes
        if not read_mode=="rb":
            return_value = decompressed_bytes.decode()
        return return_value

    def sync_file(self, local_rel_path, cloud_rel_path, from_cloud_to_local = False):
        """This method has not been called anywhere?"""
        if self.use_localfs and self.use_dropbox:
            dbx_full_path = self._remove_doubleslash_endslash(self.cloud_prefix + cloud_rel_path)
            if from_cloud_to_local:
                self.dbx_download(dbx_full_path = dbx_full_path, local_full_path = self.local_prefix + local_rel_path )
            else: #from_local_to_cloud
                with open(self.local_prefix + local_rel_path, 'rb') as file:
                    bytes_data = file.read()  
                    self.dbx_upload(bytes_data,  dbx_full_path = dbx_full_path)
        else:
            self.logger.error("Cannot sync unless both local and cloud are turned on.")

    @staticmethod
    def get_existing_dropbox_token():
        """Attempt to obtain dropbox_token from 
            1. args
            2. stored file
        If neither is provided, return None
        """
        access_token = None

        if args.dropbox_access_token:
            access_token = args.dropbox_access_token  

        elif os.path.isfile("./.dropbox_access_token"):
            with open("./.dropbox_access_token", "r") as f:
                access_token = f.read()

        return access_token

    #@staticmethod
    def authorize_dropbox_over_web(self, app_key: str, app_secret: str):
        #token_access_type (str) – the type of token to be requested. From the following enum:
        #None - creates a token with the app default (either legacy or online)
        #legacy - creates one long-lived token with no expiration
        #online - create one short-lived token with an expiration
        #offline - create one short-lived token with an expiration with a refresh token
        auth_flow = dropbox.oauth.DropboxOAuth2FlowNoRedirect(app_key, app_secret, token_access_type="legacy")
        authorize_url = auth_flow.start()
        self.logger.info("Please authorize the app by visiting this URL:", authorize_url)
        print("Please authorize the app by visiting this URL:", authorize_url)
        auth_code = input("Enter the authorization code: ")
        #auth_code='pahWL4CseewAAAAAAAAAOsDGXxgpo0WxMkV384dzD40'
        oauth_result = auth_flow.finish(auth_code)
        access_token = oauth_result.access_token
        with open("./.dropbox_access_token", "wt") as f:
            f.write(access_token)
        print(f"Dropbox Account Authorization of App with app-key {app_key} completed!")    
        return access_token

    def connect_dropbox(self, access_token = None, retrying_already = False):
        """
        Setup Dropbox Connection
        You need to apply for a Dropbox APP, then get an API token from Dropbox app console
        Dropbox app console: https://www.dropbox.com/developers/apps?_tk=pilot_lp&_ad=topbar4&_camp=myapps

        To assign dropbox token: 
        * Either run in console: export DBX_TOKEN = ${your_token}
            Shortened example: export DBX_TOKEN=sl.BeoSWaH2atxxxxtm-4 
        Or include it in the parameter --

        See Dropbox Python API Documentation: https://dropbox-sdk-python.readthedocs.io/en/latest/
        """
        if access_token==None: 
            access_token = self.authorize_dropbox_over_web(self.dropbox_app_key, self.dropbox_app_secret)
        dbx = dropbox.Dropbox(access_token)
        try:
            account = dbx.users_get_current_account()
            self.logger.info("Connected to Dropbox successfully! ")
            #self.logger.info(f"Account information: {account}")
        except dropbox.exceptions.AuthError as e:
            if retrying_already:
                self.logger.critical("CANNOT ACCESS DROPBOX")
                dbx = None
            else:    
                dbx = self.connect_dropbox(access_token = None, retrying_already=True)
        return dbx

class EdgarIndex(object):
    """
    
    This class handles downloading the quarterly index files from the SEC EDGAR website, saving it to the disk to avoid repeated downloads, and when needed, can pull an index subset that belong to a year range and/or specific form type such as 10-K.
    
    Changed from https://github.com/edgarminers/python-edgar/blob/master/edgar/main.py
    integrating with my class ManageOvercloud()

    """
    EDGAR_PREFIX = "https://www.sec.gov/Archives/"
    SEP = "|"
    # Current max request rate: 10 requests/second.
    # https://www.sec.gov/os/accessing-edgar-data
    REQUEST_BUDGET_MS = 100
    def __init__(self, logger: logging.Logger = logging.getLogger(__name__), 
                 user_agent = "Your Institution name youremail@mail.yourinstitute.edu", 
                 edgar_root_dir = "./edgar/"):
        self.user_agent = user_agent
        self.edgar_root_dir = edgar_root_dir
        self.logger = logger
    #@staticmethod
    def _quarterly_idx_list(self, since_year=1993):
        """
        Generate the list of quarterly zip files archived in EDGAR
        since 1993 until this previous quarter
        """
        self.logger.debug("EdgarIndex: downloading files since %s" % since_year)
        years = range(since_year, datetime.date.today().year + 1)
        quarters = ["QTR1", "QTR2", "QTR3", "QTR4"]
        history = list((y, q) for y in years for q in quarters)
        #most_recent_complete_quarter = "QTR%s" % ((datetime.date.today().month - 1) // 3 )
        # Calculate the most recent complete quarter
        most_recent_complete_quarter = "QTR%s" % ((datetime.date.today().month - 1) // 3 + 1)
        # drop quarters after current quarter
        history.reverse()
        while history:
            _, q = history[0]
            if q == most_recent_complete_quarter:
                break
            else:
                history.pop(0)
        history.reverse()        
        __quarterly_idx_list_gen = [
            (
                EdgarIndex.EDGAR_PREFIX + "edgar/full-index/%s/%s/master.zip" % (x[0], x[1]),
                "master%s-%s.idx.csv" % (x[0], x[1]),
            )
            for x in history
        ]
        
        __quarterly_idx_list_gen.append((
            EdgarIndex.EDGAR_PREFIX + "edgar/full-index/master.zip", 
            "master%s-%s.idx.csv" % (years[-1], "QTR"+ str(((datetime.date.today().month - 1) // 3 ) +1))
        ))
        return __quarterly_idx_list_gen
    #@staticmethod
    def _download(self, task, dest):
        """
        Download an idx archive from EDGAR.
        This will read idx files, unzip archives, read the master.idx file inside,
        process lines, and save as a compressed .csv.gz file.
        """
        if not dest.endswith("/"):
            dest = "%s/" % dest
        
        url = task[0]
        dest_name = task[1]
        index_file_path = os.path.join(dest, dest_name + ".gz")

        if url.endswith("zip"):
            try:
                req = requests.get(url, headers={'User-Agent': self.user_agent}, stream=False, verify=True)
                req.raise_for_status()
                
                content = req.content
                zip_bytes = io.BytesIO(content)
                
                raw_bytes = bytes()
                with zipfile.ZipFile(zip_bytes).open("master.idx") as z:
                    raw_bytes = z.read()
                
                # Decode and process text
                try:
                    raw_txt = raw_bytes.decode("latin-1")
                except UnicodeDecodeError:
                    raw_txt = raw_bytes.decode("utf-8", errors="ignore")

                lines = raw_txt.splitlines()
                # Skip header lines (usually top 10-11 lines)
                if len(lines) > 11:
                    lines = lines[11:]
                
                # Expand lines to include full HTML index path
                lines_expanded = [
                    line + EdgarIndex.SEP + line.split(EdgarIndex.SEP)[-1].replace(".txt", "-index.html")
                    for line in lines
                ]
                
                processed_text = "\n".join(lines_expanded) + "\n"
                processed_bytes = processed_text.encode("utf-8")
                
                # Write to local disk using gzip
                with gzip.open(index_file_path, 'wb') as f:
                    f.write(processed_bytes)
                    
            except Exception as e:
                self.logger.error(f"Failed to download or process {url}: {str(e)}")
                return 1
        else:
            self.logger.error("python-edgar only supports zipped index files")
            return 1
        return 0    

    def _is_file_recent(self, file_path, days=14):
        """Helper to check if a file is recent."""
        if not os.path.exists(file_path):
            return False
        mtime = os.path.getmtime(file_path)
        return (time.time() - mtime) < (days * 86400)
            
    def download_index(self, dest="./", since_year=1993, overwrite=False):
        """
        Convenient method to download all files at once
        """
        if not os.path.exists(dest):
            os.makedirs(dest, exist_ok=True)
            
        tasks = self._quarterly_idx_list(since_year)
        self.logger.info(f"{len(tasks)} index files to retrieve")
        
        current_year = str(datetime.date.today().year)
        
        for i, task in enumerate(tasks):
            # Naive rate limiting: 100ms budget
            start = round(time.time() * 1000)
            dest_name = task[1]
            index_file_path = os.path.join(dest, dest_name + ".gz")
            task_year = re.search(r"\d\d\d\d", task[1]).group(0)
            
            should_download = False
            
            if overwrite or not os.path.isfile(index_file_path):
                self.logger.info(f"Downloading file {task[1]}")
                should_download = True
            elif task_year == current_year and not self._is_file_recent(index_file_path, 14):
                self.logger.info(f"Downloading old file of current year {task[1]}")
                should_download = True
            
            elapsed = self.REQUEST_BUDGET_MS
            if should_download:
                self._download(task, dest)
                elapsed = round(time.time() * 1000) - start
            
            if elapsed < self.REQUEST_BUDGET_MS:
                sleep_for = self.REQUEST_BUDGET_MS - elapsed
                if sleep_for > 0 and sleep_for < 10000:
                    self.logger.debug("Sleeping for %dms because we are going too fast", sleep_for)
                    time.sleep(sleep_for / 1000)
        return 0

    def get_index_df(self, years=[y for y in range(1993, datetime.date.today().year + 1)], form_re="^(10-K|10K)", allow_download=True):
        
        formtype = re.sub(r"[^A-Za-z0-9]", "", form_re.split("|")[0]).lower()
        by_index_dir = os.path.join(self.edgar_root_dir, "by-index")
        full_index_dir = os.path.join(self.edgar_root_dir, "full-index")
        
        if not os.path.exists(by_index_dir):
            os.makedirs(by_index_dir, exist_ok=True)
            
        final_csvgz_file_name = os.path.join(by_index_dir, "years{}_{}_{}.csv.gz".format(years[0], years[-1], formtype))

        # Check if aggregate file exists and valid (unless it ends in current year, then always refresh)
        if os.path.isfile(final_csvgz_file_name) and not years[-1] == datetime.date.today().year:
            final_df = pd.read_csv(final_csvgz_file_name, sep=EdgarIndex.SEP, compression='gzip')
        else:
            dfs = list()
            for year in years:
                yearly_csvgz_file_name = os.path.join(by_index_dir, "year{}_{}.csv.gz".format(year, formtype))
                
                # For current year, always attempt download/refresh
                if year >= datetime.date.today().year and allow_download: 
                    self.download_index(dest=full_index_dir, since_year=year)
                
                # If yearly file exists and is not recent year, load it
                if os.path.isfile(yearly_csvgz_file_name) and not year >= datetime.date.today().year - 1:
                    df_year = pd.read_csv(yearly_csvgz_file_name, sep=EdgarIndex.SEP, compression='gzip')
                    dfs.append(df_year)
                else:
                    # Construct yearly from quarterly files
                    raw_qtr_index_filenames = [os.path.join(full_index_dir, f"master{year}-QTR{q}.idx.csv.gz") for q in range(1, 5)]
                    year_dfs = []
                    
                    for filename in raw_qtr_index_filenames:
                        if os.path.isfile(filename):
                            try:
                                qtrdf = pd.read_csv(
                                    filename, 
                                    sep=EdgarIndex.SEP, 
                                    names=['CIK', 'CompanyName', 'FormType', 'DateFiled', 'FileName', "IndexPage"],
                                    compression='gzip'
                                )
                                year_dfs.append(qtrdf)
                            except Exception as e:
                                self.logger.error(f"Error reading {filename}: {e}")
                        else:
                            self.logger.warning(f"Could not find file: {filename}")    
                            if allow_download:
                                self.logger.info("Attempting to download missing index...")
                                self.download_index(dest=full_index_dir, since_year=year)
                                # Recursive call with download disabled to prevent infinite loops
                                return self.get_index_df(years, form_re, allow_download=False)
                            else: 
                                self.logger.error("Cannot access or download raw index file.")
                    
                    if year_dfs:
                        df_year = pd.concat(year_dfs).reset_index(drop=True)
                        # Filter by Form Type Regex
                        df_year = df_year.loc[df_year.FormType.apply(lambda ft: True if re.search(form_re, str(ft)) else False)].reset_index(drop=True)
                        
                        # Save yearly aggregate
                        df_year.to_csv(yearly_csvgz_file_name, index=False, sep=EdgarIndex.SEP, compression='gzip')
                        dfs.append(df_year)
            
            if dfs:
                final_df = pd.concat(dfs)
                # Save final aggregate
                final_df.to_csv(final_csvgz_file_name, index=False, sep=EdgarIndex.SEP, compression='gzip')
            else:
                final_df = pd.DataFrame()

        return final_df

import bs4
from bs4.element import Tag


NON_BREAKING_ELEMENTS = ['a', 'abbr', 'acronym', 'audio', 'b', 'bdi', 'bdo', 'big', 'button', 'canvas', 'cite', 'code', 'data', 'datalist', 'del', 'dfn', 'em', 'embed', 'i', 'iframe', 'img', 'input', 'ins', 'kbd', 'label', 'map', 'mark', 'meter', 'noscript', 'object', 'output', 'picture', 'progress', 'q', 'ruby', 's', 'samp', 'script', 'select', 'slot', 'small', 'span', 'strong', 'sub', 'sup', 'svg', 'template', 'textarea', 'time', 'u', 'tt', 'var', 'video', 'wbr', "td"] + ["ix:nonFraction", "ix:fraction", "ix:numeric", "ix:nonNumeric", "ix:tuple", "ix:continuation", "ix:exclude"]




def depr_find_elements_without_children(soup, excl_tags = NON_BREAKING_ELEMENTS):
    #soup = BeautifulSoup(html, "html.parser")
    last_element = None
    for element in soup.find_all():
        if all( child.name in excl_tags or not isinstance(child, bs4.element.Tag)     for child in element.children):
            if last_element is None or not element in last_element.find_all():
                last_element = element
                #print(0, str(element))
                yield element
            #else:
            #    print(1, str(element))
            
# es = list(find_elements_without_children(html))
# ts = [e.get_text(strip=True) for e in es]
# hs = (str(e) for e in es)
# df = pd.DataFrame( {"html": hs, "text": ts } )
# df.shape
# df.to_excel("/tmp/df0.xlsx") 
# df  = df.drop_duplicates(subset="text",keep="first").reset_index(drop=True)
# df.shape
# df.to_excel("/tmp/df1.xlsx") 


class EdgarForm(object):
    """
    class EdgarForm is for all form types.
    sub-class EdgarForm10K inherit from it and have 10K specific methods
    
    This class handles downloading the filing from the SEC EDGAR website, saving it to the disk, separating individual documents/exhigits from within <DOCUMENT>...</DOCUMENT>, etc, that can apply to all EDGAR filing forms
    
    """
    DEFINED_SEP_TAGS = ["title", "table", "tr", "div", "p", "br", "span", "li", "h1", "h2", "h3", "h4", "h5", "h6", "section", "article", "aside", "header", "footer", "nav", "address", "blockquote", "pre", "fieldset", "legend", "figure", "figcaption", "main", "time"]
    NON_BREAKING_ELEMENTS = ['a', 'abbr', 'acronym', 'audio', 'b', 'bdi', 'bdo', 'big', 'button', 'canvas', 'cite', 'code', 'data', 'datalist', 'del', 'dfn', 'em', 'embed', 'i', 'iframe', 'img', 'input', 'ins', 'kbd', 'label', 'map', 'mark', 'meter', 'noscript', 'object', 'output', 'picture', 'progress', 'q', 'ruby', 's', 'samp', 'script', 'select', 'slot', 'small', 'span', 'strong', 'sub', 'sup', 'svg', 'template', 'textarea', 'time', 'u', 'tt', 'var', 'video', 'wbr', "td"]
    # Current max request rate: 10 requests/second. Do it at 8.5 and play safe (when running multicore)    #See https://www.sec.gov/os/accessing-edgar-data
    REQUEST_BUDGET_MS = 116
    _tmpfilename_last_sec_download_time = "/tmp/edgarform_last_reqtime.txt"
    _last_sec_download_time = 0;  _num_filings_read_from_file = 0; _num_filings_downloaded = 0; _num_filings_get_note = 0;
    
    def __init__(self, fcik: str, tfnm: str, logger: logging.Logger = logging.getLogger(__name__), 
        formname = "10-K", edgar_root_dir = "/mnt/text/edgar/",
        user_agent = "Your University Name your_email@youruniversity.edu", rmold=False):
        """formname is needed to determine the form type and thus folder to save"""
        self.fcik, self.tfnm, self.formname = str(fcik), tfnm, formname
        self.formtype = self._get_form_type(formname)
        self._last_sec_download_time = round(time.time() * 1000)
        self.user_agent = user_agent
        self.edgar_root_dir = edgar_root_dir
        self._legacy_local_edgar_root_old = edgar_root_dir # Only to recover data of earlier download version 
        self._remove_legacy_files = rmold
        self.logger=logger
        #filing_raw_txt = self.get_raw_filing()
        self.logger.debug(f"""Init EdgarForm, fcik {self.fcik}, tfnm {self.tfnm}, formname {self.formname}, formtype {self.formtype}, user-agent {self.user_agent}, edgar_root_dir {self.edgar_root_dir}\n""")
        self._internal_note_number = 0 
    
    def __str__(self):
        return f"cik {self.fcik}, tfnm: {self.tfnm}, formname: {self.formname}"

    def _read_file(self, filepath, use_gzip=True):
        if not os.path.isfile(filepath):
            return bytes()
        try:
            if use_gzip:
                with gzip.open(filepath, 'rb') as f:
                    return f.read()
            else:
                with open(filepath, 'rb') as f:
                    return f.read()
        except Exception as e:
            self.logger.error(f"Failed to read file {filepath}: {e}")
            return bytes()

    def _write_file(self, data, filepath, use_gzip=True):
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            if use_gzip:
                with gzip.open(filepath, 'wb') as f:
                    f.write(data)
            else:
                with open(filepath, 'wb') as f:
                    f.write(data)
        except Exception as e:
            self.logger.error(f"Failed to write file {filepath}: {e}")
                
    def _decode_buffer(self, buffer: bytes):
        raw_txt = ""
        try:
            # Start with UTF-8
            raw_txt = buffer.decode("utf-8")
        except UnicodeDecodeError as _:
            try:
                # Fallback to ISO 8859-1
                self.logger.debug("Falling back to ISO 8859-1 after failing to decode with UTF-8...")
                raw_txt = buffer.decode("iso-8859-1")
            except UnicodeDecodeError as _:
                # Give up if we can't
                self.logger.warning("Unable to decode with either UTF-8 or ISO 8859-1; giving up...")  
                raw_txt = ""
        return raw_txt
    
    @staticmethod
    def _get_secondstamp():
        today = datetime.datetime.utcnow().date()
        start_of_day = datetime.datetime.combine(today, datetime.time.min)
        now_ms = round((time.time() - start_of_day.timestamp())*1000)
        return now_ms
    
    def _check_if_need_sleep(self):
        need_sleep_for_ms = 0
        if os.path.exists(EdgarForm._tmpfilename_last_sec_download_time):
            with open(EdgarForm._tmpfilename_last_sec_download_time, "r") as file:
                last_request_time = file.read()
            try:
                self._last_sec_download_time = int(last_request_time)   
            except Exception as e:
                self.logger.error(f"From log file read time {last_request_time} but cannot do int()")
                self.logger.error(f"Error: {e}")
                self._last_sec_download_time = self._get_secondstamp() + random.randint(11, 30)
                time.sleep(1)
            #self.logger.debug(f"Read from lock file last time stamp : {str(self._last_sec_download_time)}") 
        else:
            if self._last_sec_download_time == 0:
                self._last_sec_download_time = self._get_secondstamp()    

        elapsed =  self._get_secondstamp() - self._last_sec_download_time
        if elapsed < self.REQUEST_BUDGET_MS:
            need_sleep_for_ms = self.REQUEST_BUDGET_MS-elapsed
        return need_sleep_for_ms
    
    def _retrieve_url(self, url, already_retrying=False):
        """
        I am running the same python code over multithreads, each would attempt to request URL from a website. I want to restrict the overall requests to five per second. Each time an URL request is called, a temperary lock file is saved at /tmp/lock and print the request time. And for the other python threads, before sending the URL requests again, it will first check if the /tmp/lock exists, if so, read the time from it. And wait after 0.2 seonds since the time printed, before sending the URL request. Please help me write the python code
        
        HTTP RESPONSE CODE
        200: OK - The request was successful, and the server has returned the requested content.
        201: Created - The request has been fulfilled, and a new resource has been created as a result.
        204: No Content - The server has successfully processed the request, but there is no content to return.
        400: Bad Request - The server cannot understand the request due to malformed syntax or invalid parameters.
        401: Unauthorized - Authentication is required to access the requested resource.
        403: Forbidden - The server understood the request but refuses to authorize it.
        404: Not Found - The requested resource could not be found on the server.
        405: Method Not Allowed - The request method is not supported for the requested resource.
        500: Internal Server Error - A generic server error occurred.
        502: Bad Gateway - The server received an invalid response from an upstream server.
        503: Service Unavailable - The server is temporarily unavailable, usually due to being overloaded or undergoing maintenance.
        
        """
        content = bytes()
        need_sleep_for_ms = 1000
        while need_sleep_for_ms > 0:
            need_sleep_for_ms = self._check_if_need_sleep()
            if need_sleep_for_ms >3 and need_sleep_for_ms<10000:
                time.sleep(min(0.1, need_sleep_for_ms/1000))
                need_sleep_for_ms = self._check_if_need_sleep()       
            else:
                need_sleep_for_ms =0
        #Write requset starting time to lock file    
        self._last_sec_download_time = self._get_secondstamp()
        with open(EdgarForm._tmpfilename_last_sec_download_time, "w") as file:
            file.write(str(self._last_sec_download_time) )
        self.logger.debug(f"Write to lock file last time stamp when started requesting URL: {str(self._last_sec_download_time)    }")     
        
        self.logger.debug(f"Starting to get URL {url}")
        req_started_at = round(time.time() * 1000)
        #req = requests.get(url, headers={'User-Agent': self.user_agent}, stream=False, verify=True)
        #req_status = str(req.status_code)
        try:
            req = requests.get(url, headers={'User-Agent': self.user_agent}, stream=False, verify=True)
            req_status = str(req.status_code)
        except Exception as e:
            self.logger.error(f"Request failed for {url}: {e}")
            return bytes()        
        
        req_ended_at = round(time.time() * 1000)
        
        #Write requset end time to lock file    
        self._last_sec_download_time = self._get_secondstamp()
        with open(EdgarForm._tmpfilename_last_sec_download_time, "w") as file:
            file.write(str(self._last_sec_download_time) )   
        str_last_download_time = str(self._last_sec_download_time)    
        self.logger.debug(f"Write to lock file last time stamp when finished requesting URL: {str_last_download_time}")     
        
        if req_status == "200":    
            content = req.content # in bytes
            speed =  round(len(content) / (req_ended_at - req_started_at) / 1000,2)
            self.logger.info(f"Finished getting URL {url} speed {speed} Mb/s over {(req_ended_at - req_started_at) / 1000} seconds")
        elif req_status.startswith ("2"): 
            self.logger.error(f"When getting url {url} gets status_code other than 200: {req_status}")
        elif req_status.startswith ("5"):
            self.logger.debug(f"sleeping for 500ms due to temporary error {req_status}")
            time.sleep(500)            
            if already_retrying:
                content = bytes()
            else:
                content = self._retrieve_url(url, already_retrying=True)
        elif req_status.startswith ("404"):
            self.logger.error(f"Cannot get url {url} due to permanent error: {req_status}")
        elif req_status.startswith ("429"):
            self.logger.error(f"Cannot get url {url} due to permanent error: {req_status} too many requst 429. sleep for 10 minute 30s")
            # Sleep for 10 minutes (600 seconds)
            time.sleep(630)
        elif req_status.startswith ("4"):
            self.logger.critical(f"Cannot get url {url} due to permanent error: {req_status}")
            content = bytes()
        return content
        
    @staticmethod    
    def _get_form_type(formname):
        """
        For a complete list, see https://github.com/sec-edgar/sec-edgar/blob/master/secedgar/core/filing_types.py
        """
        formtype= None
        if re.search(r"^10-?[kK]", formname):
            formtype = "10k"
        elif re.search(r"^10-?[qQ]", formname):    
            formtype = "10q"
        elif re.search(r"^8-?[kK]", formname):        
            formtype = "8k"
        elif re.search(r"^424", formname):        
            formtype = "424"
        elif re.search(r"^def.?14a", formname):        
            formtype = "def14a"
        elif re.search(r"^13d", formname):        
            formtype = "13d"    
        elif re.search(r"^13g", formname):        
            formtype = "13g"        
        else:
            formtype = "other"
        return formtype
    
    def get_filing_index_page(self):
        save_path = self.edgar_root_dir + "{}-bycik/{}/{}/".format(self.formtype, self.fcik, self.tfnm)
        save_fullpath = save_path + "filing-index.html.gz"
        #edgar/data/320193/0000320193-22-000108-index.html
        buffer = bytes()
        if os.path.isfile(save_fullpath):    
            buffer = self._read_file(save_fullpath, use_gzip=True)
        
        if len(buffer)<100: #either didnt have file, or have an empty file stored due to HTTP error
            url = "https://www.sec.gov/Archives/edgar/data/{}/{}-index.html".format(self.fcik, self.tfnm) 
            buffer = self._retrieve_url(url)
            if len(buffer)>100:
                self._write_file(buffer, save_fullpath, use_gzip=True)   
            else:
                self.logger.info(f"Got empty index file from url {url}")
        raw_htm = self._decode_buffer(buffer)    
        if len(raw_htm)< 100:
            raw_htm = ""
        return raw_htm
        
    def get_filing_size_from_index_page(self):
        complete_filing_size_value = 0
        index_raw_htm = self.get_filing_index_page()
        if len(index_raw_htm) > 100:
            #_form_size_value_xpath = "/html/body/div[4]/div[3]/div/table/tr[2]/td[5]"
            #                         "/html/body/div[4]/div[2]/div/table/tbody/tr[3]/td[5]"
            txt_value = ""
            try:
                #_form_table_xpath = "/html/body/div[4]/div[3]/div/table"
                #_table_htm = lxml.etree.tostring(lxml.etree.HTML(index_raw_htm).xpath(_form_table_xpath)[0]).decode()
                _table_htm = "<html><body>" + index_raw_htm[index_raw_htm.find("<table") -1 : index_raw_htm.find("</table>")+10] + "</body></html>"
                num_rows = len(re.findall(r"</tr>", _table_htm))
                for irow in range(0,num_rows):
                    _description_value_xpath = f"/html/body/table/tr[{str(irow+2)}]/td[2]"
                    _size_value_xpath = f"/html/body/table/tr[{str(irow+2)}]/td[5]"
                    _description = lxml.etree.tostring(lxml.etree.HTML(_table_htm).xpath(_description_value_xpath)[0]).decode()
                    _size_value_html = lxml.etree.tostring(
                        lxml.etree.HTML(_table_htm).xpath(_size_value_xpath)[0]
                    ).decode()
                    txt_value = BeautifulSoup(_size_value_html, 'html.parser').get_text(strip=True)
                    if "Complete submission text file" in _description:
                        break
                complete_filing_size_value = int(txt_value)
            except:
                self.logger.debug(f"Cannot get form size from index html")
        else: 
            self.logger.error(f"Cannot get form size from EMPTY index html")        
        return complete_filing_size_value
    
    def _get_legacy_raw_filing(self):
        """
        legacy file does not exist in the cloud, use local read
        """
        buffer=bytes()
        legacy_save_path = self._legacy_local_edgar_root_old + "/{}-bycik/{}/".format(self.formtype, self.fcik)
        legacy_save_fullpath = legacy_save_path + self.tfnm + ".txt"
        if os.path.isfile(legacy_save_fullpath):
            file_size = os.path.getsize(legacy_save_fullpath)
            need_size = self.get_filing_size_from_index_page()
            if file_size == need_size:
                with open(legacy_save_fullpath, "rb") as file:
                    buffer = file.read()
                self.logger.info(f"legacy file {legacy_save_fullpath} found, filesize consistent with SEC index. ")
                os.unlink(legacy_save_fullpath)    
            else:
                self.logger.info(f"legacy file found but filesize {file_size} inconsistent with {need_size} from SEC index {legacy_save_fullpath}")        
        else:         
            self.logger.debug(f"legacy file {legacy_save_fullpath} not found")
        return buffer  
    
    def get_raw_filing(self):
        save_path = self.edgar_root_dir + "{}-bycik/{}/{}/".format(self.formtype, self.fcik, self.tfnm)
        save_fullpath = save_path + "{}.txt.gz".format(self.tfnm)
        buffer = bytes()
        if os.path.isfile(save_fullpath):
            buffer = self._read_file(save_fullpath, use_gzip=True)
            self.logger.debug(f"Read existing form filing from file {save_fullpath} with size {len(buffer)}")
            if len(buffer)>100:
                EdgarForm._num_filings_read_from_file +=1
            else:
                self.logger.debug("existing file  {save_fullpath} seems too small with size {len(buffer)}")  
        if len(buffer)<100: #either didnt have file, or have an empty file stored due to HTTP error
            if len(buffer)>0:
                self.logger.debug("existing form filing from file {save_fullpath}  empty probably due to previous download error.  ")
            buffer_from_legacy = self._get_legacy_raw_filing()
            if not buffer_from_legacy:
                filing_raw_txt = ""
                url = "https://www.sec.gov/Archives/edgar/data/{}/{}.txt".format(self.fcik, self.tfnm) 
                EdgarForm._num_filings_downloaded +=1
                buffer = self._retrieve_url(url)
                self.logger.info(f"Downloaded form filing from url {url} with size {len(buffer)}")
            else:
                buffer = buffer_from_legacy 
                if len(buffer)>100:
                    EdgarForm._num_filings_read_from_file +=1
                else:
                    self.logger.debug("existing legacy file  {save_fullpath} seems too small with size {len(buffer)}")    
            if buffer:
                self._write_file(buffer, save_fullpath, use_gzip=True)   
                self.logger.info(f"Got form filing from SEC EDGAR and saved to {save_fullpath}")
            else:     
                self.logger.debug(f"Empty form filing from SEC EDGAR and NOT saved to {save_fullpath}")
                        
        if len(buffer)>100 and self._remove_legacy_files: 
                legacy_save_fullpath = self._legacy_local_edgar_root_old + "/{}-bycik/{}/".format(self.formtype, self.fcik) + self.tfnm + ".txt"
                if os.path.isfile(legacy_save_fullpath):
                    os.unlink(legacy_save_fullpath)    
                    self.logger.info(f"Removed legacy file {legacy_save_fullpath}")
        
        filing_raw_txt = self._decode_buffer(buffer)
        return filing_raw_txt
    @staticmethod
    def _check_is_html_format(data:str):
        """
        data = body_htm
        """
        if re.search(r"</(div|p)>", data, flags=re.I):
            return True
        return False
    
    @staticmethod
    def find_elements_without_children(soup, excl_tags = NON_BREAKING_ELEMENTS):
        #soup = BeautifulSoup(body_htm, "html.parser")
        last_element = None
        for element in soup.find_all():
            if all( child.name in excl_tags or not isinstance(child, bs4.element.Tag)     for child in element.children):
                if last_element is None or not element in last_element.find_all():
                    last_element = element
                    #print(0, str(element))
                    yield element
                #else:
                #    print(1, str(element))
        le_htm = html.unescape(str(last_element))
        soup =   BeautifulSoup(le_htm, "html.parser")      
        if soup.find_all():
            for element in soup.find_all():
                if all( child.name in excl_tags or not isinstance(child, bs4.element.Tag)     for child in element.children):
                    if last_element is None or not element in last_element.find_all():
                        last_element = element
                        #print(0, str(element))
                        yield element

    #@staticmethod
    def html_to_textdf(self, body_htm, strip_tags=['style', 'script', 'code']):
        #soup = BeautifulSoup(html.unescape(re.sub(r"\n", " ", body_htm)), "html.parser")
        body_htm = re.sub(r"^.*?<html", "<html", body_htm, flags=re.I|re.DOTALL)
        soup = BeautifulSoup(body_htm, "html.parser")
        repaired_html = soup.prettify()
        soup = BeautifulSoup(repaired_html, "html.parser")
        #soup = BeautifulSoup(html.unescape(body_htm), "lxml")
        #texts = extract_text_from_divs(element = soup.body)
        for element in soup(strip_tags): 
            element.extract()
        elements = list(self.find_elements_without_children(soup, excl_tags = NON_BREAKING_ELEMENTS))
        texts = [e.get_text(strip=True) for e in elements]
        hs = [ html.unescape(str(e)) for e in elements]
        if len(" ".join(hs)) *5 < len(body_htm):
            #do the other way
            return pd.DataFrame()
            
        body_df = pd.DataFrame( {"htm": hs, "txt": texts } )
        body_df["istable"] = body_df.htm.apply(lambda html: True if re.search(r"<(table|tr|td|th|thead|tbody|tfoot|col|caption)", html, flags=re.I ) else False )
        #body_df.shape
        
        body_df  = body_df.drop_duplicates(subset="txt",keep="first").reset_index(drop=True) #body_df.shape
        #body_df.to_excel("/tmp/df.xlsx")     
        #elements= list(soup.find_all())
        #paragraphs = []; html_blocks = []; istables=[]
        # for element in  find_elements_without_children(soup)   #soup.find_all():
        #     if element.name.lower() not in EdgarForm.NON_BREAKING_ELEMENTS:
        #         if element.get_text(" ") and not "<!-- DONE -->" in str(element):
        #             html_blocks.append(str(element))
        #             paragraphs.append(element.get_text(" "))
        #             istables.append(True if element.name.lower() in ["table", "tr", "td", "th", "thead", "tbody", "tfoot", "col", "colgroup", "caption"] else False )
        #             element.append("<!-- DONE -->")
        #body_df = pd.DataFrame({ "htm": html_blocks, "txt": paragraphs, "istable": istables})
        return body_df
    @staticmethod
    def html_to_text(body_htm, strip_tags=['style', 'script', 'code']):
        """
        body_df.to_excel('/tmp/b.xlsx')
        """
        body_htm = re.sub(r"\n", " ", body_htm)
        soup = BeautifulSoup(html.unescape(body_htm), "html.parser")
        for element in soup(strip_tags): 
            element.extract()
        for element in soup.find_all():
            if element.name.lower() not in EdgarForm.NON_BREAKING_ELEMENTS:
                element.append('\n') if element.name == 'br' else element.append('\n\n')        
        return soup.get_text()
    def _htm_to_df(self, body_htm: str):
        """ body_htm = self.get_document_i(1); len(body_htm)
        open("/tmp/ex0.htm", "w").write(body_htm)
        open("/tmp/md.htm", "w").write(md_htm)
        open("/tmp/ex.htm", "w").write(body_htm)
        body_df.to_csv("/tmp/ex.csv")
        """
        body_txt = body_htm
        if  self._check_is_html_format(body_htm):
            #body_df = pd.DataFrame(); htmlines=[]; txtlines=[]; stylines = []
            #soup = BeautifulSoup(body_htm, 'lxml')
            #body_txt = soup.get_text("\n\n")
            #paragraphs = [p.strip() for p in body_txt.split("\n\n") if p]
            body_df = self.html_to_textdf(body_htm)
            if body_df.empty: 
                #body_df = self._htm_to_df_depr(body_htm)
                soup = BeautifulSoup(body_htm, 'lxml')
                body_txt = soup.get_text("\n\n")
                #Then next if not self._check_is_html_format(body_txt) is False
        if not self._check_is_html_format(body_txt) :
            paragraphs = []
            if re.search(r"\n\n", body_txt):
                #body_htm = re.sub("</(p|P)>", "\n\n</p>", body_htm)
                paragraphs = re.split("\n\n|</(p|P)>", body_txt) #len(paragraphs)
                body_df = pd.DataFrame({ "htm": None, "txt": paragraphs, "istable": None})
            else:    
                #body_htm = markdown.markdown(body_htm)
                body_htm=markdown.markdown(re.sub("<[^>]*?>", "", body_htm))#
                body_df = self.html_to_textdf(body_htm)
        elif body_df.empty:
            body_htm=markdown.markdown(re.sub("<[^>]*?>", "", body_htm))#
            body_df = self.html_to_textdf(body_htm)
        body_df = self._df_remove_lineno_toc(body_df)
        #body_df.to_csv("/tmp/body_df.csv", sep="|")
        #body_df.to_excel("/tmp/d.xlsx")
        #open("/tmp/d.txt", "w").write("\n".join(body_df.txt.to_list()))
        return body_df     
    def _htm_to_df_depr(self, html_value: str):
        """ html_value = self.get_document_i(1, True); len(html_value)"""
        if not self._check_is_html_format(html_value):
            html_value = markdown.markdown(html_value)
        body_df = pd.DataFrame(); htmlines=[]; txtlines=[]; stylines = []
        soup = BeautifulSoup(html_value, 'lxml')
        #body_txt = soup.get_text("\n\n")
        #paragraphs = body_txt.split("\n\n")
        previous_processed_element = ""
        this_htm, this_txt, this_sty = "", "", ""
        _included = []; _dropped=[]
        #list_descendants= list(soup.descendants)
        list_descendants = [element for element in soup.descendants if element.name]
        #l2 = [l1[ie]  for ie in _dropped if l1[ie].name and str(l1[ie].name).lower() in self.DEFINED_SEP_TAGS and "\n" in str(l1[ie]) ]
        for ie, element in enumerate(list_descendants):
            if str(element.name).lower() in self.DEFINED_SEP_TAGS and not element.get_text(strip=True) in previous_processed_element:
                ele_txt = element.get_text("\n").strip()    
                next_txt = list_descendants[ie+1].get_text("\n").strip() 
                if not ele_txt in next_txt:
                    ele_txt = re.sub(r"\n+", " ", element.get_text(strip=True))
                    this_htm += re.sub(r"\n+", " ", str(element))
                    this_txt += ele_txt
                    #if this_txt.startswith("Bank Credit Lines"):
                    #    print(ie)
                    if len(this_txt) > 0: 
                        #print(str(element.name).lower(), len(this_txt))
                        #print(this_txt)
                        this_soup = BeautifulSoup(this_htm, 'lxml')
                        this_style = "; ".join ( set([e.name + " "+ e.get("style") for e in this_soup.descendants if e.name and e.get("style")]))
                        htmlines.append(this_htm)
                        txtlines.append(this_txt)
                        stylines.append(this_style)
                        previous_processed_element = this_txt
                        this_htm, this_txt = "", ""
                        _included.append(ie)
            else:
                #print(str(element))        
                _dropped.append(ie)
        body_df = pd.DataFrame({"sty": stylines, "htm": htmlines, "txt": txtlines})
        #body_df.to_csv("/tmp/body_to_df.csv", index=True, sep="|")
        #open("/tmp/d.htm", "w").write("\n".join(body_df.htm.to_list()))
        #body_df.loc[body_df.txt.str.contains("NOTE")]
        body_df = self._df_remove_lineno_toc(body_df)
        return body_df
    @staticmethod
    def _check_num_title_format(text: str):
        """ 
        Try to find the Numbered Title Pattern
        
        text = "10 Income Taxes"
        
        _check_num_title_format(text = "Note 1. Something"): return 'Note[num].'
        
        
        self._check_num_title_format(text = "NOTE 8  INCOME TAXES")
        _check_num_title_format(text = "ITEM I. Business"): return 'ITEM[abc].'
        _check_num_title_format(text = "ITEM 1A. Risk Factors"): return 'ITEM[abc].'
        _check_num_title_format(text = "II. Business"): return '[ivx].'
        _check_num_title_format(text = "1. Business"): return '[num].'
        _check_num_title_format(text = "Apple Inc. | 2021 Form 10-K | 60"):
        _check_num_title_format(text = "16,406,397,000shares of common stock were issu")
        _check_num_title_format(text="(1)As of September 25, 2021, the Company was a")
        _check_num_title_format(text="ITEM 14. CONTROL AND PROCEDURES.")
        NoteTitleFormat("Note 1.") => "Note [num]."
        To resolve this "inconsistency" within single 10-k format, 
            equal     seq1[0:11] --> seq2[0:11]             'Note [num] ' --> 'Note [num] '
            replace   seq1[11:12] --> seq2[11:12]                       '–' --> '-'
            equal     seq1[12:13] --> seq2[12:13]                       ' ' --> ' '    
            NoteTitleFormat(" 3.1 Articles of Amendment and Restatement of Sunstone Hotel Investors, Inc. (incorporated by reference to Exhibit 3.1 to the registration statement on Form S-11 (File No. 333-117141) filed by the Company).")
        """
        format="other"
        rem_note_num = re.match(r"\s*(Note|NOTE|ITEM|Item)([^0-9A-Za-z]{0,9})\d+[AB]?(\W+)[A-Z]", text   )
        rem_note_ivx = re.match(r"\s*(Note|NOTE|ITEM|Item)([^0-9A-Za-z]{0,9})[IVX]{2,}([^A-Z]+)\W[A-Z]", text   )
        rem_note_abc = re.match(r"\s*(Note|NOTE|ITEM|Item)([^0-9A-Za-z]{0,9})[A-Z]([^A-Z]+)\W[A-Z]", text   )
        rem_num = re.match(r"\s*([^0-9A-Za-z]{0,5})\d{1,2}[AB]?([^0-9A-Za-z]{0,9})\W[A-Z]", text   )
        rem_ivx = re.match(r"\s*([^0-9A-Za-z]{0,5})[IVX]{2,}(\W+)\W[A-Z]", text   )
        rem_abc = re.match(r"\s*([^0-9A-Za-z]{0,5})[A-Z](\W+)\W[A-Z]", text   )
        rem_cap = re.match(r"^[^a-z]+$", text   )
        re_exception_10k = re.search("10-?K", text)
        if re_exception_10k:
                this_note_no = "None"
        elif rem_note_num:
            format = rem_note_num.group(1) + rem_note_num.group(2) +"[num]"+ rem_note_num.group(3)
        elif rem_note_abc:
            format = rem_note_abc.group(1) + rem_note_abc.group(2) +"[abc]" + rem_note_abc.group(3)
        elif rem_note_ivx:
            format = rem_note_ivx.group(1) + rem_note_ivx.group(2) +"[ivx]" + rem_note_ivx.group(3)
        elif rem_num:
            format = rem_num.group(1) +"[num]"+ rem_num.group(2)
            rem_num_exception  = re.match(r""" \s*     ([^0-9A-Za-z]{0,9})        \d+    (\D)    \d""", text   , flags=re.X)
            if rem_num_exception:
                format = rem_num_exception.group(1) +"[num]"+ rem_num_exception.group(2)
        elif rem_abc:    
            format = rem_abc.group(1) +"[abc]"+ rem_abc.group(2)
        elif rem_ivx:
            format = rem_ivx.group(1) +"[ivx]"+ rem_ivx.group(2)
        elif rem_cap:
            format = "CAP"    
        format = re.sub("[^A-Za-z\[\]\(\)\.]","", format)
        return format        
    #@staticmethod
    def _get_num_title_number(self, text):
        """
        self._get_num_title_number(text)
        _get_num_title_number(text = "Item 1A.¬†¬†¬†¬†Risk Factors")
        """
        text = str(text)
        this_note_no=0
        res_enumerat  = re.match(r"""\s*
                        (?: (?:NOTE|Note|ITEM|Item)  [^0-9A-Za-z]{0,5}  )?    
                        ((?: \d\d? | \( \d\d?  \)   | \[  \d\d? \] | < \d\d?>  | \s [A-Z]  | \s [IVX]+  ) [AB]?)\W
                    """, text, flags=re.X)        
        rem_cap = re.match(r"^[^a-z]+$", text   )
        if res_enumerat :
            re_exception_10k = re.search("10-?K", text)
            res_note_no  = re.search(r"\d+[AB]?", res_enumerat.group(1))
            res_note_ab1 = re.search(r"(?:(?:NOTE|Note|ITEM|Item)\W+)([A-Z])[^a-zA-Z0-9]", res_enumerat.group(0))   #with    'Note' keyword
            res_note_ab2 = re.search(                  r"\b([A-Z])[^a-zA-Z0-9\s]", res_enumerat.group(0)) #without 'Note' keyword         
            res_note_ivx = re.search(                  r"\b([IVX]+[AB]?)[^a-zA-Z0-9\s]", res_enumerat.group(0))
            if re_exception_10k:
                this_note_no = "None"
            elif res_note_no:
                this_note_no = res_note_no.group(0)
            elif res_note_ab1: 
                this_note_no = ord(res_note_ab1.group(1)[-1]) - 64 # A is 65 - 64 =1
            elif res_note_ab2: 
                this_note_no = ord(res_note_ab2.group(1)[-1]) - 64 # A is 65 - 64 =1            
            elif res_note_ivx: 
                this_note_sgn = res_note_ivx.group(1)
                try:
                    roman.fromRoman(this_note_sgn)
                except:
                    pass    
        elif rem_cap:
            self._internal_note_number +=1
            this_note_no = self._internal_note_number    
        return str(this_note_no)
    @staticmethod
    def _get_filing_header(filing_raw_txt):
        headertxt="";    
        res_headerend = re.search(r"<SEC-HEADER>(.*?)</SEC-HEADER>", str(filing_raw_txt)[:3000],  re.DOTALL|re.VERBOSE )
        if res_headerend: 
            headertxt = res_headerend.group(1)
        return headertxt
    def get_documents(self, filing_raw_txt, force_rewrite=False, get_xml = False ):
        """This func separates 10-k filing's multiple <DOCUMENT> ... </DOCUMENT> and returns rawdocs as a list of txt;
        
        See https://github.com/LexPredict/openedgar/blob/master/lexpredict_openedgar/openedgar/parsers/edgar.py for more
        """
        rawdocs = list(); docs = []
        #doc1_save_filepath         = self.edgar_root_dir + "{}-bycik/{}/{}/".format(self.formtype, self.fcik, self.tfnm) + "doc001_{}".format(self.tfnm) + ".txt.gz"
        #the_rest_doc_save_filepath = self.edgar_root_dir + "{}-bycik/{}/{}/".format(self.formtype, self.fcik, self.tfnm) + "doc002_{}".format(self.tfnm) + ".txt.gz"
        save_path = os.path.join(self.edgar_root_dir, "{}-bycik/{}/{}/".format(self.formtype, self.fcik, self.tfnm))
        doc1_path = os.path.join(save_path, "doc001_{}.txt.gz".format(self.tfnm))
        doc2_path = os.path.join(save_path, "doc002_{}.txt.gz".format(self.tfnm))
        #old names:         
        doc1_save_filepath = os.path.join(save_path, "doc001_{}.txt.gz".format(self.tfnm))
        the_rest_doc_save_filepath = os.path.join(save_path, "doc002_{}.txt.gz".format(self.tfnm))
                
        if force_rewrite:
            for file in [doc1_save_filepath, the_rest_doc_save_filepath]:
                if os.path.isfile(file):
                    os.unlink(file)

        # 1. FAST PATH: Read from Cleaned Files
        # If both exist and we aren't forcing a rewrite, load them directly.
        if os.path.isfile(doc1_path) and os.path.isfile(doc2_path) and not force_rewrite:
            try:
                # Read the stripped-down text files
                txt_1 = self._read_file(doc1_path, use_gzip=True).decode('utf-8', errors='ignore')
                txt_2 = self._read_file(doc2_path, use_gzip=True).decode('utf-8', errors='ignore')
                
                # Combine them. This simulates the filing but WITHOUT the heavy graphics/zips
                combined_clean_txt = txt_1 + "\n" + txt_2
                
                # Recursively call logic on the clean text
                # We pass force_rewrite=True (internal flag) or handle logic to avoid infinite loop
                # But easiest is just to split the string here since it's small now.
                docs = combined_clean_txt.split('<DOCUMENT>')
                docs = [d for d in docs if d]
                
                # Parse the clean docs to rebuild the dictionary list
                for txt_doc in docs:
                    txt_doc = "<DOCUMENT>\n" + txt_doc
                    # Extract Metadata (Regex is fast on small files)
                    doc_type = re.findall(r"<TYPE>(.+)", txt_doc[:1000], re.IGNORECASE)
                    doc_desc = re.findall(r"<DESCRIPTION>(.+)", txt_doc[:1000], re.IGNORECASE)
                    doc_filename = re.findall(r"<FILENAME>(.+)", txt_doc[:1000], re.IGNORECASE)
                    
                    rawdocs.append({
                        "doc_type": doc_type,
                        "doc_description": doc_desc,
                        "doc_file_name": doc_filename,
                        "doc_txt": txt_doc
                    })
                
                return rawdocs
            except Exception as e:
                self.logger.warning(f"Failed to read cached documents for {self.tfnm}, falling back to raw: {e}")

        # 2. SLOW PATH: Process Raw Filing (First run or cache missing)
        if not filing_raw_txt:
            # If we didn't pass text and cache failed, we must load raw now
            filing_raw_txt = self.get_raw_filing()
        
        if not os.path.isfile(doc1_path):
            self.logger.debug(f" CIK {self.fcik} {self.tfnm} running get_documents(): Beginning")
            os.makedirs(save_path, exist_ok=True)    

        
        docs = filing_raw_txt.split('<DOCUMENT>')
        docs = [d for d in docs if d]
        txt_doc0_1 = docs[0] + ("\n<DOCUMENT>\n" + docs[1] ) if len(docs)>1 else ""
        txt_doc_2_to_end = ""
        
        if not os.path.isfile(the_rest_doc_save_filepath) or not rawdocs:
            for idoc, txt_doc in enumerate(docs):
                #idoc=0; txt_doc = docs[idoc]
                #if idoc<1:
                #   continue
                txt_doc = "<DOCUMENT>\n"  + txt_doc
                doc_text_head = txt_doc[0:4096]
                doc_text_head_upper = doc_text_head.upper()
                doc_type = re.findall("<TYPE>(.+)", doc_text_head_upper)
                doc_sequence = re.findall("<SEQUENCE>(.+)", doc_text_head_upper)
                doc_file_name = re.findall("<FILENAME>(.+)", doc_text_head_upper)
                doc_description = re.findall("<DESCRIPTION>(.+)", doc_text_head_upper)
                is_uuencoded = False
                if "<PDF>" in doc_text_head_upper:
                    is_uuencoded = True
                    content_type = "application/pdf"
                elif  "<TYPE>XML" in doc_text_head_upper  and idoc>5:
                    content_type = "application/xml"
                elif "<HTML" in doc_text_head_upper:
                    content_type = "text/html"
                elif "<XML" in doc_text_head_upper or "<?XML" in doc_text_head_upper:
                    content_type = "application/xml"    
                elif doc_text_head.find("<TEXT>\nbegin ") > 0:
                    is_uuencoded = True
                    if len(doc_file_name) > 0:
                        content_type = mimetypes.guess_type(os.path.basename(doc_file_name[0]))
                        if content_type is None:
                            content_type = "application/octet-stream"
                        else:
                            content_type = content_type[0]
                    else:
                        content_type = "application/octet-stream"
                elif re.search(r"</SEC-HEADER>", txt_doc):
                    content_type = "sec-header"
                else:
                    content_type = "text/plain"
                this_raw_doc = {"doc_type": doc_type,  "doc_sequence": doc_sequence, "doc_file_name": doc_file_name, "doc_description": doc_description, "content_type": content_type, "doc_txt": txt_doc}
                #rawdocs.append(this_raw_doc)
                if (content_type in ["text/plain", "text/html"] and not is_uuencoded): # and  idoc >1:
                    txt_doc_2_to_end += "\n" + txt_doc
                    rawdocs.append(this_raw_doc)
                elif get_xml and re.search("xbrl|xml|ex-101", " ".join([x for x in doc_type + doc_file_name + doc_description if x]), flags=re.I) :
                    rawdocs.append(this_raw_doc)
                elif content_type in ["application/xml"]:
                    pass    
                
            if force_rewrite or not os.path.isfile(doc1_save_filepath):
                self._write_file(txt_doc0_1.encode('utf-8'), doc1_save_filepath, use_gzip=True)       
            if force_rewrite or not os.path.isfile(the_rest_doc_save_filepath):
                self._write_file(txt_doc_2_to_end.encode('utf-8'), the_rest_doc_save_filepath, use_gzip=True)   
        # Instead of saving each individual document as a single file, to save dropbox connection/isfile time,
        # Combine doc0 (header) to the beginning of doc1, and the rest of docs (of which are text/plain) together into doc2
        # First, save only doc1 the form body
        # Second, save doc2_n the other exhibits combined into one
        self.logger.debug(   f" CIK {self.fcik} {self.tfnm} running get_documents(): Done")
        return rawdocs 
    def get_document_i(self, i: int, force=False):
        thisdoc_raw_txt = ""; filing_raw_txt = ""
        save_path = os.path.join(self.edgar_root_dir, "{}-bycik/{}/{}/".format(self.formtype, self.fcik, self.tfnm))
        if not os.path.isdir(save_path):
            os.makedirs(save_path, exist_ok=True)

        doc1_save_filepath = os.path.join(save_path, "doc001_{}.txt.gz".format(self.tfnm))
        the_rest_doc_save_filepath = os.path.join(save_path, "doc002_{}.txt.gz".format(self.tfnm))                    

        #doc1_save_filepath         = self.edgar_root_dir + "{}-bycik/{}/{}/".format(self.formtype, self.fcik, self.tfnm) + "doc001_{}".format(self.tfnm) + ".txt.gz"
        #the_rest_doc_save_filepath = self.edgar_root_dir + "{}-bycik/{}/{}/".format(self.formtype, self.fcik, self.tfnm) + "doc002_{}".format(self.tfnm) + ".txt.gz"
        docfiles = [ doc1_save_filepath, the_rest_doc_save_filepath]
        if not i in [1, 2]: 
            self.logger.critical("get_document_i(i in [1, 2]) cannot take other i values")
            return ""
        this_doc_rel_path = docfiles[i-1]
        if force or not os.path.isfile(this_doc_rel_path):
            filing_raw_txt = self.get_raw_filing()  #len(filing_raw_txt)
            rawdocs = self.get_documents(filing_raw_txt, force_rewrite=True)
            thisdoc_raw_txt = rawdocs[i-1]["doc_txt"] #len(thisdoc_raw_txt)
        else: 
            thisdoc_buffer = self._read_file(this_doc_rel_path, use_gzip=True)
            thisdoc_raw_txt = self._decode_buffer(thisdoc_buffer)
            if len(thisdoc_raw_txt)< 100:
                filing_raw_txt = self.get_raw_filing() 
                rawdocs = self.get_documents(filing_raw_txt, force_rewrite=True)
                thisdoc_raw_txt = rawdocs[i-1]["doc_txt"]
            #logger.debug(f"Read document {i} from existing file {this_doc_rel_path}")
        return thisdoc_raw_txt
    @staticmethod
    def _df_remove_lineno_toc(body_df):
        try:
            body_df["txt_remove_num"] = body_df.txt.apply(lambda txt: re.sub(r"\d|\s", "", str(txt)))
            _count_txt_remove_num =  body_df["txt_remove_num"].value_counts()
            lineno_toc_candidates = _count_txt_remove_num.loc[_count_txt_remove_num>20].index.to_list()
            lineno_toc_candidates = [c for c in lineno_toc_candidates if c.lower().find("item")==-1]
            lineno_toc_candidates = [c for c in lineno_toc_candidates if c.lower().find("note")==-1]
            body_df = body_df.loc[~body_df.txt_remove_num.isin(lineno_toc_candidates)].reset_index(drop=True)
            body_df.drop("txt_remove_num", axis=1, inplace=True)#.reset_index(drop=True)
        except:
            pass    
        #body_df[["txt", "item_format", "item_number", "txt_remove_num"]].to_csv("/tmp/body_df2.csv", index=True, sep="|")
        return body_df
    def get_xbrl_ins(self, force_rewrite=False):
        """
        Get the XBRL instance single file
        """
        doc_xbrl_ins = ""
        filing_local_path = self.edgar_root_dir + "{}-bycik/{}/{}/".format(self.formtype, self.fcik, self.tfnm)
        if not os.path.isdir(filing_local_path):
            os.makedirs(filing_local_path, exist_ok=True)
            
        xbrl_ins_save_filepath         = self.edgar_root_dir + "{}-bycik/{}/{}/".format(self.formtype, self.fcik, self.tfnm) + "xbrl_ins_{}".format(self.tfnm) + ".xml.gz"
        if os.path.isfile(filing_local_path) and not force_rewrite:
            thisdoc_buffer = self._read_file(filing_local_path, use_gzip=True)
            doc_xbrl_ins = self._decode_buffer(thisdoc_buffer)
        else:
            filing_raw_txt = self.get_raw_filing() 
            rawdocs = self.get_documents(filing_raw_txt, force_rewrite=force_rewrite, get_xml=True)
            df_docs = pd.DataFrame(rawdocs)
            if not df_docs.empty:
                df_docs["doc_txt_beg"] = df_docs.doc_txt.apply( lambda x: [str(x)[:3000]])
                df_docs["meta"] = (df_docs.doc_type + df_docs.doc_file_name + df_docs.doc_description   ).apply(lambda x: " ".join(x))
                df_docs["is_xbrl_ins"] = df_docs.apply(lambda row: True if re.search("instance|ins", str(row.meta), flags=re.I  ) or re.search("xbrl.{1,20}instance", str(row.doc_txt_beg), flags=re.I  )  else False, axis=1)
                if not df_docs.loc[df_docs["is_xbrl_ins"]].empty:
                    doc_xbrl_ins = df_docs.loc[df_docs["is_xbrl_ins"]].iloc[0].doc_txt
                    if sum(df_docs["is_xbrl_ins"])>0 and force_rewrite:
                        self._write_file(doc_xbrl_ins.encode('utf-8'), xbrl_ins_save_filepath, use_gzip=True)              
        return doc_xbrl_ins    
    def get_ixbrl(self):
        """
        Get the integrated form 10-K xbrl (iXBRL) file 
        """
        body_htm = self.get_document_i(1)
        if re.search(r"<.?xbrl",body_htm, flags=re.I):
            return body_htm
        else:
            return ""
    def _clean_broken_htm(self, x):
        x= re.sub("(?:html|meta|head|body|div|font|\bp\b|\btr|\btd|table)(\s+[\w-]*=\".*?\")*?", "", x, flags=re.I)
        x = re.sub(r'\b\w+\W+\w+="[^"]*?"', '', str(x), flags=re.I ) 
        x = re.sub(r'\b\w+="[^"]*?"', '', str(x), flags=re.I ) 
        x = re.sub(r'\b\w+=[0-9]*?', '', str(x), flags=re.I ) 
        x = re.sub(r'\/\w+', '', str(x), flags=re.I ) 
        x = re.sub(r'<.*?>', '', str(x), flags=re.I ) 
        x = re.sub(r'[^\x00-\x7F]', '', str(x), flags=re.I ) 
        return x

    def get_xbrl_textblock_headline_dicts(self, doc_xbrl_ins):
        #parser = etree.XMLParser(remove_blank_text=True, remove_comments=True, remove_pis=True, recover=True)
        parser = etree.XMLParser(recover=True)
        doc_xbrl_ins = re.sub("^.*?<XBRL", "<XBRL", doc_xbrl_ins, flags=re.I|re.DOTALL)
        doc_xbrl_ins = re.sub(r"</XBRL>.*$", r"</XBRL>", doc_xbrl_ins, flags=re.I|re.DOTALL)
        if not doc_xbrl_ins:
            return {}
        try:
            soup = BeautifulSoup(doc_xbrl_ins, "lxml")
            textblocks = []; otherblocks = []; ixblocks = []
            textblockeles = []
            #tree = etree.fromstring(doc_xbrl_ins, parser)
            #for element in tree.iter():
            #    element = textblockeles[0]; etree.tostring(element)
            #    etree.tostring(textblockeles[2])
            for element in soup.find_all():
                if re.search("textblock", str(element.name), flags=re.I):
                #if re.search("textblock", str(element.tag), flags=re.I): # old textblock
                    tag_name = element.name #element.tag
                    try: 
                        text_content = html.unescape(element.get_text(strip=True)) #html.unescape(element.text)
                    except:
                        pass
                    text_content = self._clean_broken_htm(text_content) #element.text)
                    if text_content is not None:
                        textblockeles.append(element)
                        textblocks.append((tag_name, text_content))
                #elif re.search("nonNumeric", str(element.tag), flags=re.I):  #new ix inline xbrl
                elif re.search("nonNumeric", str(element.name), flags=re.I):  #new ix inline xbrl        
                    tag_name = element.name #element.get("name")        
                    tag_string =element.get_text(strip=True)  #etree.tostring(element, encoding="unicode")
                    if self._check_is_html_format(tag_string):
                        elem_df = self.html_to_textdf(tag_string)
                        text_content = "\n".join(elem_df.txt.to_list())
                        try: 
                            text_content = html.unescape(text_content)
                        except Exception as e:
                            pass
                    else:
                        try:
                            text_content = html.unescape(element.get_text(strip=True)) if element.get_text(strip=True) else None  #html.unescape(element.text) if element.text else None
                        except Exception as e:
                            #logger.warning(f"{str(element.text)}: {str(e)}: {str(self.fcik)}/{str(self.tfnm)}")
                            logger.warning(f"{str(element.get_text(strip=True))}: {str(e)}: {str(self.fcik)}/{str(self.tfnm)}")
                            text_content = element.get_text(strip=True)
                    if re.search("textblock", str(tag_name), flags=re.I) and re.search("gaap", str(tag_name), flags=re.I):        
                        if text_content is not None:
                            text_content = self._clean_broken_htm(text_content)
                            textblocks.append((tag_name, text_content))
                            textblockeles.append(element)
                #else:
                #    otherblocks.append((tag_name, text_content))    
            #df_otherblocks = pd.DataFrame(otherblocks, columns = ["tag", "value"])        
            #df_ix = pd.DataFrame(ixblocks, columns = ["tag", "value"])        
            df_textblocks = pd.DataFrame(textblocks, columns = ["tag0", "htm"])
            df_textblocks["tag"] = df_textblocks["tag0"].apply(lambda x: re.sub( r"^{[^}]*?}(.*)TextBlock$", "\g<1>", x))
            #df_textblocks["htm"] = df_textblocks["htm"].apply(lambda x: re.sub(r"\n+", " ", x))
            try:
                df_textblocks["htm"] = df_textblocks["htm"].apply(html.unescape)
            except:
                pass   
            #_htmls = " ".join(df_textblocks["htm"].to_list())
            #_clean_broken_htm(self, x)
            #df_textblocks["txt"] = df_textblocks["htm"].apply(self._clean_broken_htm)
            df_textblocks["txt"] = df_textblocks["htm"].str.strip()
            #Try to remove common beginning part in strings
            _common_prefix = os.path.commonprefix(df_textblocks["txt"].to_list())
            _try_prefix = ""
            if not _common_prefix:
                strings = df_textblocks["txt"].to_list()
                strings = [st for st in strings if st]
                if strings:
                    st = ""
                    for st in strings:
                        _try_prefix = st[0]
                        if sum([s.startswith(_try_prefix) for s in strings])>1:
                            break
                    shortest = min(strings, key=len)    
                    if st and shortest and len(_try_prefix)>1:     
                        for l in range(2, len(shortest)):
                            _try_prefix = st[0:l]    
                            if not sum([s.startswith(_try_prefix) for s in strings])>3:
                                break
                if _try_prefix:
                    _common_prefix = _try_prefix[0:len(_try_prefix)-1]            
            if _common_prefix:    
                df_textblocks["txt"] = df_textblocks["txt"].str.replace(_common_prefix, "", regex=False)
            df_textblocks["line1"] = df_textblocks["txt"].apply(lambda x: re.split(r"\n+", str(x))[0])
            df_textblocks["fcik"] = self.fcik
            df_textblocks["tfnm"] = self.tfnm
            #df_textblocks.sort_values(by="line1", inplace=True); df_textblocks.to_excel("/tmp/a.xlsx")
            return df_textblocks[["fcik", "tfnm", "tag", "htm", "txt", "line1"]].to_dict(orient='records')
        except Exception as e:
            traceback.print_exc()
            logger.warning(f"Error on line {traceback.extract_tb(e.__traceback__)[0].lineno}: {str(e)}")        
            logger.warning(f"Failed to process xbrl for {str(self)}")    
        return {}


def titles_simple_sim(note_title, note_title_lag):
    #score = 0 
    #note_title, note_title_lag = row.note_title, row.note_title_lag
    words_title = re.split(r"\W", note_title.lower())
    words_title_lag = re.split(r"\W", note_title_lag.lower())
    common_words = set([w for w in words_title if w in words_title_lag])
    score = len(common_words)/max(len(set(words_title + words_title_lag)),1)
    return score 


def build_xbrl_textblock_title(filename, days_recent=0, _edgar_root_dir="/text/edgar/", allow_write = True):
    """
    filename="882184/0000950123-10-106707.txt"
    filename="766829/0000766829-12-000007.txt"
    filename="801904/0001376474-10-000029.txt"
    filename = "320193/0000320193-23-000106.txt"
    filename="1001679/0001001679-09-000002.txt"
    filename = "101829/0001193125-09-024624.txt"
    filename="864328/0001193125-09-240380.txt"
    filename="1333274/0001564590-20-004625.txt"
    
    filename="1003201/0001558370-19-001956.txt"
    
    filename= "edgar/data/1374881/0001477932-22-001151.txt" # king going concern
    """
    _to_list = filename.split("/")
    cik = _to_list[-2]
    tfnm = _to_list[-1].split(".")[0]
    data_save_filepath  = f"{_edgar_root_dir}10k-bycik/{cik}/{tfnm}/xbrl_textblock_title_{tfnm}.p.gz"
    if is_file_recent(data_save_filepath, days_recent = days_recent): 
        #xbrl_textblock_headline_dicts = gzip_to_df(data_save_filepath)
        dfx3 = pd.read_pickle(data_save_filepath, compression="gzip")
    else:    
        #logger.info(f"Starting for {cik}/{tfnm}")
        #import importlib, edgarform
        #importlib.reload(edgarform); from edgarform import *
        try: 
            my_form10k = EdgarForm10K(cik, tfnm, edgar_root_dir = _edgar_root_dir) #self=my_form10k
            doc_xbrl_ins = my_form10k.get_ixbrl() #len(doc_xbrl_ins)
            is_ix = bool(doc_xbrl_ins)
            doc_xbrl_ins = my_form10k.get_xbrl_ins(force_rewrite=False) if not doc_xbrl_ins else doc_xbrl_ins
            xbrl_textblock_headline_dicts = my_form10k.get_xbrl_textblock_headline_dicts(doc_xbrl_ins)
            dfx0 = pd.DataFrame(xbrl_textblock_headline_dicts)
            dfx0["is_ix"] = is_ix
            #if not is_ix:
            df_note = pd.DataFrame(my_form10k.form10k_get_note_by_keyword(get_note_lists=True))
            df_note.rename(columns={"txt": "note_txt"}, inplace=True)
            if dfx0.empty:
                dfx3 = pd.DataFrame()
            else:
                dfx1 = pd.merge(dfx0, df_note[["fcik", "tfnm", "note_number", "note_title", "note_txt"]], how='outer', on=["fcik", "tfnm"])
                dfx1["xbrl_match_score"] = dfx1.apply(lambda row: max(titles_simple_sim(row.txt, row.note_title), titles_simple_sim(row.txt, row.note_txt)), axis=1)
                
                dfx2 = dfx1.sort_values(by=["tag", "xbrl_match_score"], ascending=False).groupby("tag").head(1)
                dfx3 = dfx2.sort_values(by=["note_title", "xbrl_match_score"], ascending=[True, False]).groupby("note_title").head(1)
            #df_y.txt = df_y.txt.apply(html.unescape)
            
                if allow_write:
                    #df_to_gzip(dfx3, data_save_filepath)
                    dfx3.to_pickle(data_save_filepath, compression="gzip")
                    logger.info(f"Done for {cik}/{tfnm}\n")
        except Exception as e:
            traceback.print_exc()
            logger.warning(f"Error on line {traceback.extract_tb(e.__traceback__)[0].lineno}: {str(e)}")        
        
            logger.critical("build_xbrl_textblock_title: " + str(filename)+ str(e))
            dfx3 = pd.DataFrame()
    return dfx3

def get_form_10k_note_titles(filename:str ="edgar/data/104169/0000104169-23-000020.txt", edgar_root_dir = "/text/edgar/", overwrite_existing=False):
    """
    auliti_2122_notes step 0
    filename="edgar/data/1024628/0000950005-97-000351.txt"
    """
    _to_list = filename.split("/")
    fcik = _to_list[-2]
    tfnm = _to_list[-1].split(".")[0]
    my_form10k = EdgarForm10K(fcik, tfnm, edgar_root_dir = edgar_root_dir); #self=my_form10k
    try:
        #body_htm = my_form10k.get_document_i(1)
        body_df = my_form10k.form10k_get_items_df(overwrite_existing=overwrite_existing)
        noti_df = my_form10k.form10k_get_note_by_keyword(body_df)
        #result_dict["note_txt"] = str(my_form10k.form10k_get_note_by_keyword(body_df, keyword_re))
        return noti_df
    except Exception as e:
        traceback.print_exc()
        logger.warning(f"Error on line {traceback.extract_tb(e.__traceback__)[0].lineno}: {str(e)}  {str(filename)}")                
    return pd.DataFrame() 


def get_filename_notes_list(filename, edgar_root_dir="/text/edgar/"):
    """
    filename = "edgar/data/1004400/0000912057-00-021749.txt"
    # To construct a dataset at cik-datadate-note level 
    # Ok write a function, take FileName as input, return a list of dictionaries, note_num, note_txt, note_title, note_tag

    """
    _to_list = filename.split("/")
    fcik = _to_list[-2]
    tfnm = _to_list[-1].split(".")[0]
    my_form10k = EdgarForm10K(fcik, tfnm, edgar_root_dir = edgar_root_dir); #self = my_form10k
    body_df = my_form10k.form10k_get_items_df(overwrite_existing=False)


class EdgarForm10K(EdgarForm):
    ITEM_KEYWORDS = ["Item", "Business", r"Risk\sFactors?", r"Unresolved\sStaff\sComment",
    "property", "Properties", r"Legal\s+Proceedings?", r"Mine\sSafety",
    r"Submission\sof\sMatters\sto\sa\sVote", r"Market\sfor\s.*?Common\sEquity",
    r"Selected\s+Financial\s+Data", r"Management.{0,10}\sDiscussion\s.{2,10}\sAnalysis", r"Market\sRisk", r"Financial\sStatements?", r"Change\sin\sAccountants", r"Changes\sin\s.{0,10}\sDisagreements\sWith\sAccountants",
    r"Directors.{2,10}Executive\sOfficers", 
    r"Executive\sCompensation", r"Ownership", r"Securities\sOwnership\sof\sCertain",
    r"Relationships?\s.{2,10}\sTransactions", r"Certain\sRelationships?\s+.{2,10}\sRelated\sTransactions",
    r"Controls?\s.{2,10}Procedures", r"Exhigits.{1,2}Schedules", r"Exhibit.{1,10}Financial\sStatements?\sSchedule", r"Exhibits",
    r"Principal\sAccountant\sFees"]
    
    MDA_KEYWORDS = ["sale", "revenue", "demand", "expense", "cost", "grow", "overview", "highlight", "opportunities", "outlook", "summary",  "increase", "decrease", "during", "year"]
    
    FSN_KEYWORDS = ["consolidated", "statement", "financial", "audit", "balance", "sheet", "position", "income", r"cash\s+flow", "equity", "asset", "liabilit"]
    
    FNT_KEYWORDS = [r"accounting\s+polic", "basis", r"related.*?party]\s+transaction", "investment|propert|plant|fixed|equipment|depreciat", "leasing|lease", "employ", "stock|share|equity|option|compensation|benefit", r"income\s+tax", "acquisition|merger",  "loan|debt|borrowing|note", "insurance|saving|retirement|pension|employment", "equity", "litigation", "organization|combination|disposition|disposal|restur", "consolidat|subsidiar|affiliate|segment|foreign", "Contingenc|commitment|guarantee|warrant", r"quarterly\s+financial", "partnership", "dividend", "provision", "inventor", r"working\s+capital", r"fair\s+value|instrument|derivative|hedging|financial|measurement|trading|holding|method", "goodwill|intangible|impair", "reinsurance", "inventor", 
    "payable|accrued|deposit|liabilit|other", "oil|gas|natural|exploration|production", "disposal|discontinu", "estate", "receivable|allowance|credit|loss", "research|development|software",
    "cash|equivalent|securit", "legal|litigation", "minority|variable|interest|entity", "revenue|defer|contract|customer|client|program", "preferred|treasury|partner", "current|asset",
    "transfer|servic|repurchase|resale","agreement|Arrangement"]
    
    REF_KEYWORDS = ["refer", "see", "page", "from", "to", "through", "by", "reference", "exhibit", "incorporated", "included", "filed", "attached", "found", "herein", "therein", "thereto", "hereto", "part\sof", r"F-\d"]
    __version__ = "1.20.231220"
    def __init__(self, *args, **kwargs):
        if not "formname" in kwargs:
            formname = "10-K"
            kwargs['formname'] = formname
        super().__init__(*args, **kwargs)
        #print("Positional arguments:", args)
        #print("Keyword arguments:", kwargs)
        
    def _check_body_df(self, body_df):
        """
        This function is not used yet
        For item 7 MD&A and 8 Financial Statements Notes, check if it is inline, if not, dig into doc002
        body_df = self.form10k_get_items_df()
        """
        _need_repair = False
        
        df_item7 = body_df.loc[body_df.item_number=="7"]
        if df_item7.shape[0] <20: 
            _need_repair = True
        else:
            txt_item7 =  "\n\n".join(df_item7.txt.to_list())
            numwords_item7 = len(txt_item7.split())
            num_ref_words_item7 = len(re.findall( "("+ "|".join(self.REF_KEYWORDS) + ")" , txt_item7, flags=re.I) )
            num_key_words_item7 = len(re.findall( "("+ "|".join(self.MDA_KEYWORDS) + ")" , txt_item7, flags=re.I) )
            num_numerics_item7  = len(re.findall("\d+\.\d+|,\d\d\d", txt_item7 ))
            #isHtmUnlikelyItem8 = (numwords_item8< 500 and len(res_reference)> 2) or len(item8_txt)==0 or (num_numerics<10  and numkeyws_item8<10)
            if (numwords_item7<500 and num_ref_words_item7 >3) or (num_numerics_item7<10 and num_key_words_item7<10 ):
                _need_repair = True

        df_item8 = body_df.loc[body_df.item_number=="8"]
        if df_item8.shape[0] <5: 
            _need_repair = True
        else:
            txt_item8 =  "\n\n".join(df_item8.txt.to_list())
            numwords_item8 = len(txt_item8.split())
            num_ref_words_item8 = len(re.findall( "("+ "|".join(self.REF_KEYWORDS) + ")" , txt_item8, flags=re.I) )
            num_key_words_item8 = len(re.findall( "("+ "|".join(self.FSN_KEYWORDS) + ")" , txt_item8, flags=re.I) )
            num_numerics_item8  = len(re.findall("\d+\.\d+|,\d\d\d", txt_item8 ))
            #isHtmUnlikelyItem8 = (numwords_item8< 500 and len(res_reference)> 2) or len(item8_txt)==0 or (num_numerics<10  and numkeyws_item8<10)
            if (numwords_item8<500 and num_ref_words_item8 >3) or (num_numerics_item8<10 and num_key_words_item8<10 ):
                _need_repair = True

        if _need_repair:
            # read doc2
            doc2_htm = self.get_document_i(2) #open("/tmp/doc2.htm", "w").write(doc2_htm)       
            docs = self.get_documents(doc2_htm)
            df_docs = pd.DataFrame(docs)
            df_ex13 = df_docs.loc[df_docs.doc_type.apply(lambda x: True if re.search(r"ex.*13", str(x), flags=re.I) else False)]
            # doc_txt = df_ex13.doc_txt.to_list()[0]
            if df_ex13.shape[0] >0:
                pass
            else:
                df_docs["num_poswords"] = df_docs.doc_txt.apply(lambda doc_txt: len(re.findall( "("+ "|".join(self.FSN_KEYWORDS + self.MDA_KEYWORDS) + ")" , doc_txt, flags=re.I) ))
                df_docs["num_words"]    = df_docs.doc_txt.apply(lambda doc_txt:  len(doc_txt.split()))
                df_docs["num_numwords"] = df_docs.doc_txt.apply(lambda doc_txt: len(re.findall("\d+\.\d+|,\d\d\d", doc_txt )))
                df_docs["pct_poswords"] = df_docs["num_poswords"]  / df_docs["num_words"]
                df_fsnote = df_docs.loc[
                    df_docs.doc_txt.apply(lambda doc_txt: True if
                        len(re.findall( "("+ "|".join(self.FSN_KEYWORDS + self.MDA_KEYWORDS) + ")" , doc_txt, flags=re.I) ) > 100 and
                        len(doc_txt.split()) > 3000 and  
                        len(re.findall("\d+\.\d+|,\d\d\d", doc_txt )) > 300 else False
                    )
                ]
            #for i in range(0, len(docs)):
            #    with open(f"/tmp/doc{str(i)}.htm", "w") as f:
            #        f.write(docs[i]["doc_txt"])       

        return _need_repair        

    def _get_exhibit_df(self, exhibit_number="13" ):
        f"""
        
        open("/tmp/doc.txt", "w").write(doc_txt)
        
        From "doc002_{self.tfnm}.txt.gz", try to find the exhibit
        
        Form 10-K exhibits are explained in detail under Item 601 of Regulation S-K. 

        Exhibit 13: This is typically the annual report to shareholders for the last fiscal year if it's being provided with the Form 10-K. Note that not all companies use this exhibit number for the annual report to shareholders, and it's not a requirement that it be included as an exhibit if it's not being sent to the SEC with the 10-K.
        """
        doc2_htm = self.get_document_i(2) #open("/tmp/doc2.htm", "w").write(doc2_htm)       
        docs = self.get_documents(doc2_htm)
        df_docs = pd.DataFrame(docs)
        exhibit_number = str(exhibit_number)
        df_exhibit = df_docs.loc[df_docs.doc_type.apply(lambda x: True if re.search(f"ex.*{exhibit_number}", str(x), flags=re.I) else False)]
        if df_exhibit.shape[0] ==1:
            #got it
            doc_txt = df_exhibit.doc_txt.to_list()[0]
            exhibit_df = self._htm_to_df(doc_txt)
            if exhibit_df.shape[0]>0:
                return exhibit_df
        return None # if didnt got ex13 then return None

    def RepairItem8Note(self, body_df):
        """Locate the Notes to consolidated financial statements by checking rawdoc[0]'s item 8, 
           if there are less than 200 words, and if there are _reference_ keywords at least twice, 
           then instead check rawdocs[1:], and pick the one with most notes keywords """
        
        item8_txt  = "\n".join(body_df.loc[body_df.item_number=="8"].txt.to_list())
        
        numwords_item8 = len(item8_txt.split())
        numkeyws_item8 = len(re.findall(r"consolidated|statement|financial|notes|balance|sheet|income|cash\s+flows|equity|assets|liabilities|equity",         item8_txt.lower() ))
        num_numerics = len(re.findall("\d+\.\d+|,\d\d\d", item8_txt ))
        item8_begin = " ".join(item8_txt.split()[:100])
        res_reference = re.findall(r"""refer|see|page|from|to|through|by|reference|exhibit|which|incorporated|included|filed|attached|found|herein|therein|thereto|hereto|part\sof|F-\d""", item8_begin, flags=re.I|re.X)
        # if item 8 is too short, check if it contains a reference instead of content,         
        isHtmUnlikelyItem8 = (numwords_item8< 500 and len(res_reference)> 2) or len(item8_txt)==0 or (num_numerics<10  and numkeyws_item8<10)  #if at least two such reference words are found or the whole item is empty

        if isHtmUnlikelyItem8:            # maybe find it somewhere, such as item 15 or every doc in self.rawdocs after [0]

            #filing_raw_txt = self.get_raw_filing() 
            #rawdocs = self.get_documents(filing_raw_txt, force_rewrite=True)
            #docs_to_search =  [rawdoc for rawdoc in rawdocs if len(re.findall(r"consolidated|statement|financial|notes|balance|sheet|income|cash\s+flows|equity|assets|liabilities|equity", rawdoc["doc_txt"].lower() ))>5  ]
            search_df = body_df.loc[body_df.item_number.isin(["7", "9", "14", "15", "Ex13"])]
            by_item = search_df.groupby("item_number").txt.agg(" ".join).reset_index()
            by_item["hits"]  = by_item.txt.apply(lambda txt: len(re.findall(r"consolidated|statement|financial|notes|balance|sheet|income|cash\s+flows|equity|assets|liabilities|equity", txt.lower() ) ))
            
            finding_items = by_item.loc [ by_item["hits"] == by_item["hits"].max()].item_number.to_list()    
            if finding_items:
                found_item = finding_items[0] 
                body_df.loc[body_df.item_number == found_item, "item_number"] = "8"
        return body_df    
        
    @timeout(300.0)    
    def form10k_get_items_df(self, overwrite_existing = False): #bf10k
        """This method identifies the Item sections of the form 10-K
        and combines the form body with a selected list of exhibits (exhibit 13 for financial report, exhibit 21 for subsidiaries)
        
        #body_df.groupby("item_format").txt.count()
        #body_df.loc[body_df.item_format.str.startswith('Item[num].')][["txt", "item_format", "num_item_keywords"]]
        #body_df.loc[body_df.item_format.str.startswith('ITEM[num].')][["txt", "item_format", "num_item_keywords"]]
        #body_df1[["txt", "item_format", "item_number", "note_number"]].to_csv("/tmp/body_df0.csv", index=True, sep="|")
        ex13_df.to_csv("/tmp/ex_13.csv", index=True, sep="|")
        body_df["item_number"].value_counts().sort_index().reset_index().to_dict()
        
        #open("/tmp/a.htm", "w").write(body_htm)        
        open("/tmp/a.txt", "w").write(body_txt)
        
        """
        body_df_save_filepath         = self.edgar_root_dir + "{}-bycik/{}/{}/".format(self.formtype, self.fcik, self.tfnm) + "body_df_{}".format(self.tfnm) + ".txt.gz"
        if os.path.isfile(body_df_save_filepath) and not overwrite_existing:
            try:
                body_df = pd.read_csv(body_df_save_filepath, sep="|", compression='gzip')
                body_df.item_number =  body_df.item_number.apply(lambda x: str(int(x)) if pd.notna(x) and isinstance(x, float) or isinstance(x, int) else str(x))
                return body_df
            except:
                if os.path.isfile(body_df_save_filepath):
                    os.unlink(body_df_save_filepath)
                self.logger.critical(f"Unable to read {body_df_save_filepath}")
        #
        body_htm = self.get_document_i(1, force=overwrite_existing) #len(body_htm)
        body_df  = self._htm_to_df(body_htm) #body_df.shape
        body_df = body_df.reset_index(drop=True)
        body_df.fillna({"txt":""}, inplace=True)
        body_df = body_df.loc[body_df.txt.str.contains("[0-9a-zA-Z]")].reset_index(drop=True)
        body_df['txt_combined'] = np.where(body_df['txt'].str.match(r'.*[A-Z]{3,}\W?$'), body_df['txt'],
                body_df['txt'] +" "+ body_df['txt'].shift(-1).fillna("").apply(lambda x: " ".join(x.split()[:5])))  
        body_df.fillna( {"txt_combined": ""}, inplace=True)
        body_df["item_format"] = body_df.txt_combined.apply(self._check_num_title_format)  #body_df["item_format"].value_counts()
        body_df["num_item_keywords"] = body_df.txt_combined.apply(lambda text:
            len(re.findall( "("+ "|".join(self.ITEM_KEYWORDS) + ")" , text, flags=re.I) )
        )
        # body_df.loc[body_df.item_format.str.lower() =="item[num]."][["num_item_keywords", "txt"]]
        body_df["item_number"] = "" #np.nan
        if body_df["item_format"].nunique()>1:  #body_df["item_format"].value_counts()
            try: 
                #20240419 changed `item_format_found` to its .lower() to solve only identifying Table of Contents
                item_format_found = body_df.loc[body_df.item_format.apply(lambda x: True if re.search(r"item|num|abc|ivx", x, flags=re.I) else False) ].groupby("item_format").num_item_keywords.sum().idxmax().lower()
                #item_format_found = body_df.loc[body_df.item_format != "other"].groupby("item_format").num_item_keywords.sum().idxmax()
                self.logger.debug(f"Found (globally) Item format: {item_format_found}")
                body_df["item_number"] = np.where(body_df.item_format.str.lower() == item_format_found, 
                    body_df.txt.apply(self._get_num_title_number), np.nan)
                body_df['item_number'] = np.where(body_df['item_number'].isna(), body_df['item_number'].ffill(), body_df['item_number'])
                body_df = body_df.reset_index(drop=True)
                #body_df= body_df[["htm", "txt", "item_number", "istable"]].reset_index(drop=True)
                #body_df["item_number"].value_counts()
                #print("\n".join(body_df.loc[body_df["item_number"]=="7"].txt.to_list()))
            except:
                self.logger.debug("Failed to identify item format globally. Fall back to old way")
        if body_df["item_number"].nunique()<5 or not all( item in list(body_df["item_number"].unique()) for item in ["1", "7", "8"]  ):
            #Didnt find the item format with the DataFrame approach. Try the old paragraph by paragraph approah instead
            soup = BeautifulSoup(body_htm, 'html.parser')
            body_txt = soup.get_text("\n\n")
            paragraphs = body_txt.split("\n\n")
            ref_indicators = ["by", "within", "from", "also", "both", "this", "such", "title", "section", "forth", "read", "regarding", "follow", "include", "describe", "described","found", "particular", "under", "discussed", "defined", "below", "above","pursuant", "included", "exhibit", "presented", "see", "refer", "listed", "set", "formerly", "filed", "under", "outlined", "v.", "instances", "reported", "meets", "referred", "appearing", "explained", "appears"]
            l_item_number, l_txt = [], []
            item_number=""
            for paragraph in paragraphs:
                regex_item = re.search(r"\s*Item\s+([0-9]{1,2}[AB]?)", paragraph, flags = re.I)
                if regex_item:
                    regex_around_item  = re.search(r"(.*?)Item\s+[0-9]{1,}\.{0,1}[ab]{0,1}\.{0,1}(.*)", paragraph, re.I)
                    words_around = re.sub("[^0-9a-zA-Z ]","",regex_around_item.group(1)).lower().split()[-20:] + re.sub("[^0-9a-zA-Z ]","",regex_around_item.group(2)).lower().split()[:20]
                    if not any(i in words_around for i in ref_indicators):
                        item_number = regex_item.group(1)
                        if item_number in ["1", "1A", "7", "8"]:
                            self.logger.debug(f"Found (locally)  item number: {item_number}")
                        # If we find the item line/paragraph, then following paragraphs all belong to this item until finding the next item paragraph/line
                l_txt.append(paragraph)
                l_item_number.append(item_number)
            body_df = pd.DataFrame({ "htm": None, "txt": l_txt, "item_number": l_item_number, "istable": None})
            body_df = self._df_remove_lineno_toc(body_df)
            if body_df["item_number"].nunique()<5 or not all( item in list(body_df["item_number"].unique()) for item in ["1", "7", "8"]  ):
                # still not found. Try 3rd way
                l_item_number, l_txt = [], []
                item_number=""
                for ipa, paragraph in enumerate(paragraphs):
                    paragraph_txt = paragraph
                    re_item_1st  = re.search (r"^\s*Item\s*([0-9IVX]{1,2}[IVX]{0,5}[AB]{0,1})", paragraph_txt, flags=re.I)
                    re_item_2nd  = re.search (r"""^\s*(
                        (1|I)         \W{1,6}  Business                              |
                        (1A)          \W{1,6}  Risk\s+Factor                           |
                        (1B)          \W{1,6}  Unresolved\s+Staff\s+Comment            |
                        (2|II)        \W{1,6}  Properties                              |
                        (3|III)       \W{1,6}  Legal\s+Proceedings?                     |
                        (4|IV)        \W{1,6}  Mine\s+Safety                         |
                        (7|VII)       \W{1,6}  Management.{1,10}Discussion.{1,10}Analysis |
                        (7A|VIIA)     \W{1,6}  Quantitative.{1,10}Qualitative\s+Disclosures?\s+.{1,10}Market\s+Risk |
                        (8|VIII)      \W{1,6}  Financial\s+Statements?.{1,10}Supplementary|
                        (9|VIX)       \W{1,6}  Changes\s+in\s+.{1,10}Disagrement\s+with\s+Accountant|
                        (9B)          \W{1,6}  Other\s+Information|
                        (11|XI)       \W{1,6}  Executive\s+Compensation                      |
                        (14|XIV)      \W{1,6}  Principal\s+Accountant\s+Fee                  |
                        (15|XV)       \W{1,6}  Exhibit.{1,10}Financial\s+Statements?\s+Schedule 
                    )""", paragraph_txt, flags=re.I|re.VERBOSE)    
                    if re_item_1st or re_item_2nd:
                        itemtxt_found = re_item_1st.group(0) if re_item_1st else re_item_2nd.group(0)
                        next_paragraph = paragraphs[ min(ipa+1, len(paragraphs)-1)][:20] # If next line/paragraph is "ITEM...", it must be table of contents
                        check_afteritem  = paragraph+" " + next_paragraph
                        itemtxt_found = re.sub("[\(\)\[\]\{\}]",".", itemtxt_found) #otherwise in itemtxt_found can make an error "unbalanced parenthesis"
                        text_afteritem = re.sub(itemtxt_found, "", check_afteritem)
                        words_after_item = text_afteritem.lower().split()[:10]
                        ref_indicators = ["item", "by", "within", "from", "also", "both", "not", "this", "such", "title", "section", "forth", \
                                "read", "regarding", "follow", "include", "describe", "described", "found", "particular", "under", "discussed", \
                                "defined", "below", "above", "pursuant", "included", "exhibit", "see", "see,", "listed", "set"] + [str(x) for x in range(1, 16)]
                        if not any(w in words_after_item for w in ref_indicators):
                            item_number = re.search(r"^\s*(Item[^0-9I]{0,5})?([0-9IVXAB]{1,5})", itemtxt_found, flags=re.I).group(2)
                            res_roman_number = re.search(r"[IVX]+", item_number)
                            if res_roman_number:
                                item_number = roman.fromRoman(res_roman_number.group(0))
                            if item_number in ["1", "1A", "7", "8"]:    
                                self.logger.debug(f"Found (3rd attempt)  item number: {item_number}")    
                    l_txt.append(paragraph)
                    l_item_number.append(item_number)            
            body_df = pd.DataFrame({ "htm": None, "txt": l_txt, "item_number": l_item_number})  
            body_df = self._df_remove_lineno_toc(body_df)  
            body_df['txt_combined'] = np.where(body_df['txt'].str.match(r'.*[A-Z]{3,}\W?$'), body_df['txt'],
                body_df['txt'] + body_df['txt'].shift(-1).fillna("").apply(lambda x: " ".join(x.split()[:5])))  
            body_df.fillna( {"txt_combined": ""}, inplace=True)
            body_df["item_format"] = body_df.txt_combined.apply(self._check_num_title_format)  #body_df["item_format"].value_counts()
        if body_df["item_number"].nunique()>=5 and  all( item in list(body_df["item_number"].unique()) for item in ["1", "7", "8"]  ):
            self.logger.debug("ITEMS FOUND:" + str( body_df["item_number"].astype(str).value_counts().sort_index().to_dict()  ))
        else:
            found_items_str = ", ".join(  [str(i) for i in list(body_df["item_number"].unique())]  )
            self.logger.debug(f"Failed to identify items from form 10-K {self.fcik} {self.tfnm}, found these items only { found_items_str  } ")    
        ex13_df  = self._get_exhibit_df(exhibit_number="13" )
        if isinstance(ex13_df, pd.DataFrame) :
            ex13_df["item_number"] = "Ex13"
            ex13_df['txt_combined'] = np.where(ex13_df['txt'].str.match(r'.*[A-Z]{3,}\W?$'), ex13_df['txt'],
                ex13_df['txt'] + ex13_df['txt'].shift(-1).fillna("").apply(lambda x: " ".join(x.split()[:5])))  
            ex13_df.txt_combined.fillna("", inplace=True)
            ex13_df["item_format"] = ex13_df.txt_combined.apply(self._check_num_title_format)  
            ex13_df["istable"] = None
            #ex13_df["item_format"].value_counts()
            #ex13_df.to_csv("/tmp/ex_13.csv", index=True, sep="|")
            body_df = pd.concat([body_df, ex13_df], ignore_index=True)#.reset_index(drop=True)
            body_df['txt_combined'] = np.where(body_df['txt'].str.match(r'.*[A-Z]{3,}\W?$'), body_df['txt'],
                    body_df['txt'] + body_df['txt'].shift(-1).fillna("").apply(lambda x: " ".join(x.split()[:5])))  
            body_df.fillna( {"txt_combined": ""}, inplace=True)#body_df.txt_combined.fillna("", inplace=True)
            body_df["item_format"] = body_df.txt_combined.apply(self._check_num_title_format)  #body_df["item_format"].value_counts()
        body_df.reset_index(drop=True, inplace=True)
        #body_df = body_df.applymap(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x).reset_index(drop=True)
        body_df = body_df.apply(lambda x: x if not isinstance(x, str) else x.replace('\n', ' ')).reset_index(drop=True)
        # Getting Note Number
        _formats = body_df.item_format.value_counts().index.to_list()
        _num_formats = [_f for _f in _formats if ("num" in _f.lower() or "abc" in _f.lower() or "ivx" in _f.lower() ) and not "item" in _f.lower()]
        if len(_num_formats)>0:
            _note_title_df = body_df.loc[body_df.item_format.isin(_num_formats)].reset_index(drop=True)
            _note_title_df["num_item_keywords"] = _note_title_df.txt_combined.apply(lambda text:
                len(re.findall( "("+ "|".join(self.FNT_KEYWORDS) + ")" , text, flags=re.I) ) / (len(text.split())+1)
            )    
            item_format_found, item_number_found = None, None
            if  _note_title_df.groupby("item_format").num_item_keywords.sum().shape[0]>0:
                item_format_found = _note_title_df.groupby("item_format").num_item_keywords.sum().idxmax()
            if _note_title_df.groupby("item_number").num_item_keywords.sum().shape[0]>0:
                item_number_found = _note_title_df.groupby("item_number").num_item_keywords.sum().idxmax()
            if item_format_found and item_number_found:
                self.logger.debug(f"Found (globally) Item format: {item_format_found}")
                #body_df["note_number"] = np.where( (body_df.item_format == item_format_found) & (body_df.item_number == item_number_found), body_df.txt.apply(self._get_num_title_number), np.nan)
                #body_df['note_number'] = np.where(body_df['note_number'].isna(), body_df['note_number'].ffill(), body_df['note_number'])
        body_df = self.RepairItem8Note(body_df)
        if not "istable" in body_df.columns:
            body_df["istable"] = None

        body_df.item_number =  body_df.item_number.apply(lambda x: str(int(x)) if pd.notna(x) and isinstance(x, float) or isinstance(x, int) else str(x))
        body_df[["item_number", "item_format", "txt", "htm", "istable"]].to_csv(body_df_save_filepath, sep="|", index=False, use_gzip=True)
        return body_df[["item_number", "item_format", "txt", "htm", "istable"]]

    def form10k_get_note_by_keyword(self, body_df = None, keyword_re = None, get_note_lists=False):
        """
        body_df.to_csv("/tmp/d.csv", sep="|")
        keyword_re = r"income\W+?tax"
        debug: filename = "edgar/data/1000209/0001564590-21-013216.txt"
        """
        this_note_txt = ""
        if not isinstance(body_df, pd.DataFrame):
            body_df = self.form10k_get_items_df(overwrite_existing=True)
        _formats = body_df.item_format.value_counts().index.to_list()
        _num_formats = [_f for _f in _formats if ("num" in _f.lower() or "abc" in _f.lower() or "ivx" in _f.lower()  ) and not "item" in _f.lower()]
        #or "cap" in _f.lower()
        if len(_num_formats)>0:
            body_df['txt_combined'] = np.where(body_df['txt'].str.match(r'.*[A-Z]{3,}\W?$'), body_df['txt'],
                    body_df['txt'] +" "+ body_df['txt'].shift(-1).fillna("").apply(lambda x: " ".join(x.split()[:5])))  
            body_df.txt_combined.fillna("", inplace=True)
            body_df["num_item_keywords"] = body_df.txt_combined.apply(lambda text:
                len(re.findall( "("+ "|".join(self.FNT_KEYWORDS) + ")" , 
                              " ".join (re.split(r"\W+", text)[:10]), 
                               flags=re.I) ) / (len(text.split())+1)
            )    

            _note_title_df = body_df.loc[body_df.item_format.isin(_num_formats)]
            if _note_title_df.loc[_note_title_df.txt_combined.notna()].empty:
                return None
            _note_title_df = _note_title_df.loc[_note_title_df.txt_combined.str.strip().apply(bool)].reset_index(drop=True)
            item_format_found, item_number_found = None, None
            if _note_title_df.groupby("item_number").num_item_keywords.sum().shape[0]>0:
                item_number_found = _note_title_df.groupby("item_number").num_item_keywords.sum().idxmax()
                # this is the 10K item number that contains the most hit for FNT_KEYWORDS
            #_note_body_df = _note_title_df.loc[(_note_title_df.item_number == item_number_found)]
            _note_title_df = _note_title_df.loc[(_note_title_df.item_number == item_number_found)]


            if _note_title_df.groupby("item_format").num_item_keywords.sum().shape[0]>0:
                item_format_found = _note_title_df.groupby("item_format").num_item_keywords.sum().idxmax()
            _note_title_df = body_df.loc[(body_df.item_format == item_format_found) & (body_df.item_number == item_number_found)].copy()      
            _note_title_df["note_number"] = _note_title_df.txt.apply(self._get_num_title_number)
            _note_title_df["note_title"] = _note_title_df.txt
            _note_body_df = body_df.loc[ (body_df.item_number == item_number_found)].copy()      
            _note_body_df = _note_body_df.loc[_note_title_df.index.min():_note_body_df.loc[_note_body_df.item_format=="other"].index.max()]
            
            _note_body_df = pd.merge(_note_body_df, _note_title_df[["txt", "note_number", "note_title"]], on="txt", how="left"  )
            _note_body_df["note_number"] = _note_body_df["note_number"].ffill()
            _note_body_df["note_title"] = _note_body_df["note_title"].ffill()
            
            if not keyword_re:
                if  not get_note_lists:
                    _note_title_df = _note_title_df.reset_index(drop=True)
                    _note_title_df["fcik"] = self.fcik
                    _note_title_df["tfnm"] = self.tfnm
                    return _note_title_df
                else: 
                    _note_body_df = _note_body_df.loc[_note_body_df.txt.notna()]
                    #notes_df = pd.DataFrame(_note_body_df.groupby(["note_number", "note_title"]).txt.apply(lambda x: "\n\n".join(x))).reset_index()
                    notes_df =  _note_body_df.groupby(["note_number", "note_title"], group_keys=False).txt.apply(lambda x: "\n\n".join(x)).reset_index()

                    notes_df["fcik"] = self.fcik
                    notes_df["tfnm"] = self.tfnm

                    notes_list = notes_df.to_dict(orient="records")
                    return notes_list
            _note_title = _note_title_df.loc[_note_title_df.txt_combined.apply(lambda txt: True if 
                re.search(keyword_re, " ".join (re.split(r"\W+", txt)[:10]), flags=re.I) else False)]
            if _note_title.shape[0]>0:
                # found the line matching numbered format and keyword_re!
                _note_number = _note_title.note_number.to_list()[0]
                _note_title = _note_title.txt.to_list()[0]
                this_note_df = body_df.loc[body_df['note_number'] == _note_number]
                this_note_txt = "\n\n".join(this_note_df.txt.to_list()) + "\n\n"
                self.logger.debug(f"Found note {_note_title}")
                EdgarForm._num_filings_get_note +=1
                if keyword_re:
                    return this_note_txt 
        # except Exception as e:
        #     self.logger.critical(str(e))    
        EdgarForm._num_filings_get_note +=1
        return None

    def __str__(self):
        return f"cik {self.fcik}, tfnm: {self.tfnm}, formname: {self.formname}"



class EdgarForm8K(EdgarForm):
    """
    Subclass of EdgarForm specifically for processing Form 8-K.
    Inherits downloading and file handling capabilities from EdgarForm.
    Adds specific methods to parse SEC Headers and detect Non-GAAP/Earnings content.

    Implements Bentley et al. (2018) sentence-level logic.
    """
    # --- REGEX DEFINITIONS ---

    # 1. Earnings Announcement (EA) Scoring Patterns
    # We use these to find the "best" sentence confirming this is an EA
    RE_EA_STRONG = re.compile(r"(?i)(item\s+2\.02|results\s+of\s+operations|financial\s+condition)")
    RE_EA_MEDIUM = re.compile(r"(?i)(financial\s+results|press\s+release|earnings\s+release)")
    RE_EA_CONTEXT = re.compile(r"(?i)(quarter|year)\s+ended")

    # Pre-compile regex patterns for performance
    RE_EARNINGS_WORDS = re.compile(
        r"(?i)\b(earnings?|net\s+income|income|net\s+loss(?:es)?|loss(?:es)?|EPS|profit)\b"
    )
    # 2. NON_GAAP WORDS
    # 2.1. Building Blocks (Strings, not compiled yet)
    # We define these as raw strings so we can combine them into complex proximity patterns
    
    # "Earnings Words" (The Nouns)
    S_EARNINGS = r"(?:basis|earnings?|net\s+income|income|net\s+loss(?:es)?|loss(?:es)?|earned|lost|EPS|profit)"
    
    # "Trigger Words" (The Modifiers)
    # Note: We still keep the negative lookahead (?!\s+limitation) as a safety net
    S_TRIGGERS = r"(?:adjust(?:ed|ments?)?|exclud(?:e|es|ing)|pro-?forma|non-?GAAP|remove|without(?!\s+limitation)|except\s+for|absent)"    
    # 2.2 Proximity Regex (Criterion 2a Refined)
    # Logic: Match TRIGGER ... (0-10 words) ... EARNINGS  OR  EARNINGS ... (0-10 words) ... TRIGGER
    # \W+ matches punctuation/spaces
    # (?:\w+\W+){0,10} matches 0 to 10 "words" in between
    RE_2A_PROXIMITY = re.compile(
        fr"(?i)\b{S_TRIGGERS}\b\W+(?:\w+\W+){{0,3}}\b{S_EARNINGS}\b|"
        fr"(?i)\b{S_EARNINGS}\b\W+(?:\w+\W+){{0,3}}\b{S_TRIGGERS}\b"
    )

    
    # 2a. Direct Trigger Words
    # Note: "remove", "without", "absent" are very common words. 
    # Bentley implies these indicate non-GAAP when used in financial context sentences.
    RE_2A_DIRECT = re.compile(
        r"(?i)\b(adjust(?:ed|ments?)?|exclud(?:e|es|ing)|pro-?forma|non-?GAAP|remove|without|except\s+for|absent)\b"
    )

    # 2b. Conditional Words (Must be followed by earnings word or 'basis')
    # Words: cash, normalized, base, ongoing, core
    # Earnings Words (Bentley Footnote 32 + logic): earnings, net income, income, net loss, loss, EPS, profit, results
    #RE_2B_CONDITIONAL = re.compile(
    #    r"(?i)\b(cash|normalized|base|ongoing|core)\s+"
    #    r"(basis|earnings?|net\s+income|income|net\s+loss(?:es)?|loss(?:es)?|earned|lost|EPS|profit|results)\b")
    RE_2B_CONDITIONAL = re.compile(
        r"(?i)\b(cash|normalized|base|ongoing|core)\s+" + S_EARNINGS + r"\b"
    )
    # 2c. Phrases (Operating earnings etc)
    # These require the sentence to ALSO contain "non-GAAP" or "proforma"
    RE_2C_PHRASES = re.compile(
        r"(?i)\b(operating\s+earnings|earnings\s+from\s+operations|"
        r"operating\s+income|operating\s+loss|income\s+from\s+operations|loss\s+from\s+operations|"
        r"earnings\s+from\s+continuing\s+operations|earnings\s+before\s+discontinued\s+operations|"
        r"loss\s+from\s+continuing\s+operations|loss\s+before\s+discontinued\s+operations|"
        r"income\s+from\s+continuing\s+operations|income\s+before\s+discontinued\s+operations|"
        r"loss\s+prior\s+to\s+discontinued\s+operations|"
        r"income\s+prior\s+to\s+discontinued\s+operations|"
        r"earnings\s+prior\s+to\s+discontinued\s+operations)\b"
    )
    
    RE_NONGAAP_PROFORMA_CHECK = re.compile(r"(?i)\b(non-?GAAP|pro-?forma)\b")
    RE_RECONCILIATION_CHECK = re.compile(r"(?i)\b(reconcili(?:a|e|ation))\b")

    # 7. Safe Harbor Filter
    RE_SAFE_HARBOR = re.compile(
        r"(?i)\b(forward-looking\s+statements|actual\s+results\s+.*?\s+differ|statements\s+are\s+neither\s+promises\s+nor\s+guarantees|"
        r"may|might|can|could|should|shall|materially|risky|risk|uncertain|substantial|deviate)\b"
    )
    # Class-level variable to hold the segmenter
    _segmenter = None

    def __init__(self, *args, **kwargs):
        # Force formname to 8-K
        if "formname" not in kwargs:
            kwargs['formname'] = "8-K"
        super().__init__(*args, **kwargs)

    @classmethod
    def get_segmenter(cls):
        """
        Lazy loader for pysbd.Segmenter.
        Initializes it only the first time it is called in a process.
        """
        if cls._segmenter is None:
            # clean=False is faster and sufficient since we cleaned HTML previously
            cls._segmenter = pysbd.Segmenter(language="en", clean=False)
        return cls._segmenter

    def clean_and_segment_text(self, raw_html):
        # 1. HTML to Text
        soup = BeautifulSoup(raw_html, "lxml")
        for script in soup(["script", "style", "xml"]):
            script.extract()

        # 2. Handle Tables: Extract content and replace with unique markers
        # We only want to process "top-level" tables (tables that are not inside another table)
        # to prevent double-counting or fragmenting nested tables.
        tables_map = {}
        table_counter = 0
        
        # Find all tables that do not have a parent table
        top_level_tables = [t for t in soup.find_all("table") if t.find_parent("table") is None]

        for tbl in top_level_tables:
            # Extract text: separator=" " prevents cells merging (e.g. "$100" and "2023" becoming "$1002023")
            # strip=True cleans up leading/trailing whitespace in cells
            tbl_text = tbl.get_text(separator=" ", strip=True)
            
            # Skip empty tables to keep noise down
            if len(tbl_text) < 2: 
                tbl.extract()
                continue

            # Create a unique marker that Pysbd won't accidentally split
            marker = f"|||TABLE_MARKER_{table_counter}|||"
            tables_map[marker] = tbl_text
            
            # Replace the HTML table element with our marker
            # We add newlines to ensure the marker sits on its own line after get_text
            tbl.replace_with(f"\n{marker}\n")
            table_counter += 1

        # 3. Get the remaining text (which now contains markers instead of tables)
        # separator="\n" ensures paragraphs don't merge
        text_blob = soup.get_text(separator="\n", strip=True)

        # 4. Process line by line
        all_sentences = []
        
        # Access the singleton segmenter via the class method (Optimized as discussed)
        seg_func = self.get_segmenter().segment

        # Split the blob into lines. 
        # Because we wrapped markers in \n, they should appear as distinct lines.
        # Get Text with Double Newline Separators
        # separator="\n\n" forces a clear break between div/p/br elements
        text_blob = soup.get_text(separator="\n\n", strip=True)

        # FIX LINE WRAPS: Replace single newlines with space
        # This handles cases where older text files wrapped at 80 chars.
        # Since we used \n\n for structural breaks, single \n are likely just formatting.
        text_blob = text_blob.replace('\n', ' ')
        paragraphs = text_blob.split('\n\n')

        for p in paragraphs:
            clean_p = p.strip()
            if not clean_p: 
                continue
            
            # Check if this line is one of our Table Markers
            if clean_p in tables_map:
                # It's a table: Append the original table text as a SINGLE unit (no segmentation)
                all_sentences.append(tables_map[clean_p])
            else:
                # It's normal text: Apply sentence segmentation
                #if len(line) > 20: # Skip tiny artifacts
                all_sentences.extend(seg_func(clean_p))
                            
        return all_sentences

    def get_header_items(self, header_txt):
        """
        Extracts 'ITEM INFORMATION' lines from the SEC Header.
        Example Header Line: ITEM INFORMATION:		Regulation FD Disclosure
        """
        # Regex to find lines starting with ITEM INFORMATION:
        # We use re.MULTILINE (m) to match ^ at start of lines
        items = re.findall(fr"^ITEM.{{0,10}}?INFORMATION.{{0,10}}?:\s*?(\S.+)$", header_txt, flags=re.MULTILINE)
        return [item.strip() for item in items if item.strip()]

    def get_filing_date(self, header_txt):
        """
        Extracts FILED AS OF DATE from header.
        """
        match = re.search(fr"FILED.{{0,10}}?AS.{{0,10}}?OF.{{0,10}}?DATE.{{0,10}}?:\s*?(\d{{8}})", header_txt)
        return match.group(1) if match else None

    def check_earnings_announcement(self, text, items_list):
        """
        Determines if the 8-K is an Earnings Announcement.
        Criteria:
        1. Header contains 'Item 2.02' (Results of Operations and Financial Condition).
        2. Robust Regex search in body for Item 2.02 or specific financial result phrases.
        """
        # Check Header Items first (High Confidence)
        # Note: In header text, it usually says "Results of Operations..." not "Item 2.02" explicitly
        # But commonly we look for the text associated with 2.02
        for item in items_list:
            #if "Results of Operations" in item or "Financial Condition" in item:
            if re.search(r"(?i)results\s+of\s+operations", item, flags=re.I) or re.search(r"(?i)financial\s+condition", item, flags=re.I):
                return 1
        
        # Robust Regex for Body
        # Looking for "Item 2.02" specifically in the text
        if re.search(r"(?i)item\s+2\.02", text, flags=re.I):
            return 1
            
        # Fallback: Look for strong combination of "Financial Results" AND "Quarter/Year Ended"
        if re.search(r"(?i)financial\s+results", text, flags=re.I) and \
           re.search(r"(?i)(quarter|year)\s+ended", text, flags=re.I):
            return 1
            
        return 0
        
    def analyze_sentences_for_verification(self, raw_txt):
        """
        Segments text and scans for:
        1. Non-GAAP presence (Boolean)
        2. Representative Non-GAAP Sentence (Verification)
        3. Representative EA Sentence (Verification)
        
        Returns: (has_ng_flag, ea_sentence, ng_sentence)
        """
        sentences = self.clean_and_segment_text(raw_txt)
        
        has_ng = 0
        best_ng_sentence = ""
        best_ea_sentence = ""
        
        # Scores to track the "Best" sentence
        max_ea_score = 0
        max_ng_score = 0
        
        for sentence in sentences:
            if len(sentence) < 5: 
                continue
            # 1. Filter out Boilerplate
            if self.RE_SAFE_HARBOR.search(sentence):
                continue                
            # --- 1. Score for Earnings Announcement (EA) ---
            current_ea_score = 0
            if self.RE_EA_STRONG.search(sentence):
                current_ea_score += 10
            if self.RE_EA_MEDIUM.search(sentence):
                current_ea_score += 5
            if self.RE_EA_CONTEXT.search(sentence):
                current_ea_score += 3
            
            # Update best EA sentence
            if current_ea_score > max_ea_score:
                max_ea_score = current_ea_score
                best_ea_sentence = sentence.strip()

            # --- 2. Score for Non-GAAP (NG) ---
            # We only check for NG if we haven't found a definitive one, 
            # OR if we want to find the "best" one for verification.
            current_ng_score = 0
            is_ng_match = False


            # Check 2a: Proximity-based Trigger
            # This replaces the old "if search(earnings) AND search(trigger)" logic
            if self.RE_2A_PROXIMITY.search(sentence):
                current_ng_score += 10
                is_ng_match = True
            
            # Check 2b: Conditional (Core/Normalized + Earnings)
            elif self.RE_2B_CONDITIONAL.search(sentence):
                current_ng_score += 8
                is_ng_match = True
            
            # Check 2c: Phrases + Context
            elif self.RE_2C_PHRASES.search(sentence):
                if self.RE_NONGAAP_PROFORMA_CHECK.search(sentence):
                    current_ng_score += 8
                    is_ng_match = True
                    
                elif self.RE_RECONCILIATION_CHECK.search(sentence):
                    current_ng_score += 6
                    is_ng_match = True
                    
            # Update state
            if is_ng_match:
                has_ng = 1 # Found at least one
                if current_ng_score > max_ng_score:
                    max_ng_score = current_ng_score
                    best_ng_sentence = sentence.strip()

        return has_ng, best_ea_sentence, best_ng_sentence
        
    def check_non_gaap(self, raw_txt):
        """
        THIS FUNCTION IS DEPRECATED. USE analyze_sentences_for_verification() INSTEAD.
        Checks for Non-GAAP performance measures based on Bentley et al. (2018) Appendix B.
        
        Criteria:
        1. Trigger words: adjust, exclude, proforma, non-GAAP, etc.
        2. Conditional words: cash, normalized, core + (earnings|income|etc).
        
        Checks criteria 2a, 2b, 2c on a PER SENTENCE basis.
        Returns 1 if ANY sentence matches the criteria.        

        https://aistudio.google.com/prompts/1nI5o7ARqROW8-28_2sj3Qt2oXsoe4fra
        """
        # Get list of sentences from the document
        sentences = self.clean_and_segment_text(raw_txt)
        has_ng = 0        
        for sentence in sentences:
            # Optimization: Skip extremely short sentences
            if len(sentence) < 5: 
                continue
            if self.RE_EARNINGS_WORDS.search(sentence):
                # Check 2a: Direct Trigger Words
                # "adjust", "exclude", "proforma", "non-GAAP", etc.
                if self.RE_2A_DIRECT.search(sentence):
                    # Optional: Add strict check for Criterion 1 (Numbers) here if desired
                    # For now, we assume trigger word existence in a sentence is sufficient
                    has_ng = 1
                    break
                
                # Check 2b: Conditional Words
                # "core earnings", "normalized income", etc.
                elif self.RE_2B_CONDITIONAL.search(sentence):
                    has_ng = 1
                    break
                
            # Check 2c: Operating Earnings Phrase
            # Must contain "operating earnings" etc AND ("non-GAAP" OR "proforma")
                
            if self.RE_NONGAAP_PROFORMA_CHECK.search(sentence):
                if self.RE_2C_PHRASES.search(sentence):
                    # Note: Bentley also mentions checking that GAAP EPS is NOT in the sentence
                    # Implementing that requires regex for numbers, which we skip for broad detection
                    has_ng = 1
                    break
                elif self.RE_RECONCILIATION_CHECK.search(sentence):
                    has_ng = 1
                    break
        return has_ng

    def process_filing(self):
        """
        Orchestrates the reading and parsing of the 8-K file.
        Efficiently splits documents and targets relevant text sections.
        
        # to test:
        cik = "1000015"; tfnm = "0001104659-04-002799"
        processor = EdgarForm8K(fcik=cik, tfnm=tfnm)
        self = processor
        data = processor.process_filing()
        """
        try:
            raw_txt = self.get_raw_filing()
        except Exception as e:
            logger.error(f"Failed to retrieve filing {self.fcik}/{self.tfnm}: {e}")
            return None

        if not raw_txt:
            return None

        # 1. Extract Header & Metadata
        header_txt = self._get_filing_header(raw_txt)
        items_list = self.get_header_items(header_txt)
        filing_date = self.get_filing_date(header_txt)
        
        # 2. Split into Documents
        # Use the parent class method get_documents() to parse <DOCUMENT> tags
        # This returns a list of dictionaries with 'doc_type', 'doc_txt', etc.
        try:
            documents = self.get_documents(raw_txt, force_rewrite=False)
        except Exception as e:
            # Fallback if splitting fails: treat whole file as one doc
            logger.warning(f"Document splitting failed for {self.fcik}/{self.tfnm}, falling back to raw text: {e}")
            documents = [{'doc_type': ['8-K'], 'doc_txt': raw_txt}]

        # 3. Identify Relevant Content
        # We want to check:
        #   A. The "8-K" body (for Item 2.02 declaration)
        #   B. "EX-99" exhibits (Press releases, often contain the Non-GAAP numbers)
        # We ignore XBRL (EX-101), GRAPHIC, ZIP, XML, JSON
        
        relevant_text = ""
        is_ea = 0
        
        # Priority types to search
        target_types_re = re.compile(r"8-K|EX-99|PRESS", re.IGNORECASE)
        
        for doc in documents:
            # doc_type is a list like ['8-K'] or ['EX-99.1']
            if doc.get('doc_type', ['']):
                dtype = doc.get('doc_type', [''])[0] 
            else:
                dtype = ''
            content = doc.get('doc_txt', '')
            
            # Skip empty or binary/xbrl documents early
            if not content or len(content) < 50: continue
            if re.search(r"XML|JSON|ZIP|GRAPHIC|EX-101", dtype, re.IGNORECASE): continue
            
            # If it's a target document (8-K or EX-99), append to analysis buffer
            if target_types_re.search(dtype):
                relevant_text += "\n" + content

        # 4. Run Analysis on Filtered Text
        # Check Earnings Announcement status
        # Note: We pass items_list from header + the text body
        is_ea = self.check_earnings_announcement(relevant_text, items_list)
        
        has_ng = 0; 
        ea_sentence = ""
        ng_sentence = ""
        if is_ea:
            # Only perform expensive segmentation/regex check if it is an EA
            #has_ng = self.check_non_gaap(relevant_text)
            has_ng, ea_sentence, ng_sentence = self.analyze_sentences_for_verification(relevant_text)

        return {
            "fcik": self.fcik,
            "tfnm": self.tfnm,
            "filing_date": filing_date,
            "item_name": items_list,
            "is_earnings_announcement": is_ea,
            "has_non_gaap": has_ng,
            "ea_sentence": ea_sentence,
            "ng_sentence": ng_sentence
        }

#@timeout(30.0)


def remove_aurep(text):
    aurep=""



def get_form_10k_note_list(filename:str ="edgar/data/104169/0000104169-23-000020.txt"):
    #filename="edgar/data/104169/0000104169-23-000020.txt"
    _to_list = filename.split("/")
    fcik = _to_list[-2]
    tfnm = _to_list[-1].split(".")[0]
    my_form10k = EdgarForm10K(fcik = fcik, tfnm = tfnm); #self = my_form10k
    body_df = my_form10k.form10k_get_items_df()
    notes_list = my_form10k.form10k_get_note_by_keyword(body_df, get_note_lists=True)
    return notes_list

def get_form_10k_note(keyword_re: str = "income_tax", num_total_jobs=0, req_started_at= round(time.time() ), 
                      filename:str ="edgar/data/104169/0000104169-23-000020.txt"):
    #keyword_re = r"\W+?".join (re.split(r"[^a-zA-Z0-9]", keyword_re))
    result_dict = {"FileName": filename, "note_txt": ""}
    _to_list = filename.split("/")
    fcik = _to_list[-2]
    tfnm = _to_list[-1].split(".")[0]
    my_form10k = EdgarForm10K(fcik = fcik, tfnm = tfnm)
    
    try:
        body_df = my_form10k.form10k_get_items_df(overwrite_existing=True)
        
        result_dict["note_txt"] = str(my_form10k.form10k_get_note_by_keyword(body_df, keyword_re))
        return result_dict
    except Exception as e:
        traceback.print_exc()
        logger.warning(f"Error on line {traceback.extract_tb(e.__traceback__)[0].lineno}: {str(e)}")        
        
        logger.critical("get_form_10k_note "+ str(e))
    _hours_used = (time.time()  - req_started_at) / 3600        
    total_processed = EdgarForm._num_filings_get_note
    logger.info(f"After {_hours_used} hours, already processed {total_processed} of {num_total_jobs} filings. Speed: {round(total_processed/(_hours_used+0.00001), 1)} filings per hour\n")    
    return result_dict 


    

def is_file_recent(file_path, days_recent =32.0):
    if not os.path.isfile(file_path):
        return False
    
    try:
        # Get the modification time of the file
        modification_time = os.path.getmtime(file_path)
        
        # Calculate the age of the file in seconds
        file_age_seconds = time.time() - modification_time
        
        # Calculate the age of the file in days
        file_age_days = file_age_seconds / (24 * 60 * 60)
        
        # Check if the file is less than 7 days old
        if file_age_days <= days_recent:
            return True
        else:
            return False
    except Exception as e:
        print(f"{str(e)}")
        traceback.print_exc()
        logger.warning(f"Error on line {traceback.extract_tb(e.__traceback__)[0].lineno}: {str(e)}")        
        
        return False
####@timeout(90.0)

def os_path_isfile(filename):
    """
    replace the original os.path.isfile() and make it case insensitive to filenames
    """
    found = None
    paths = filename.split('/')
    folder = '/'.join(paths[:-1])
    try:
        all_files = sorted(os.listdir(folder))
    except:
        return None
    fn = paths[-1]
    fn_std = unidecode(fn.lower()) #unicodedata.normalize('NFKD', fn.lower()).encode('ASCII', 'ignore').decode() 
    fn_std_part = ".".join(fn_std.split(".")[:-1])
    for fl in all_files:
        fl_std = unidecode( fl.lower()) #unicodedata.normalize('NFKD', fl.lower()).encode('ASCII', 'ignore').decode()
        if fn_std == fl_std: 
            found = f"{folder}/{fl}"
            return found
    for fl in all_files:    
        if fl_std[:len(fn_std_part)] == fn_std_part and "conflicted copy" in fl:
            return found
    return found

def cosine_similarity_numpy(A, B):
    # Calculate dot product
    if type(A) == np.ndarray and type(B) == np.ndarray:
        if A.shape[0] != B.shape[0]:
            return 0.0
        A = np.squeeze(A)
        B = np.squeeze(B)        
        dot_product = np.dot(A, B)
        magnitude_A = np.linalg.norm(A)
        magnitude_B = np.linalg.norm(B)
        # Check for zero magnitude
        if magnitude_A == 0 or magnitude_B == 0:
            return 0.0
        else:
            #return dot_product / (magnitude_A * magnitude_B)
            similarity = dot_product / (magnitude_A * magnitude_B)
            return np.clip(similarity, -1.0, 1.0) 
    elif sum([int(type(A) == np.ndarray),  int(type(B) == np.ndarray)]) == 1: # most dissimilar
        if type(A) == np.ndarray: # current year is not missing, lag year is missing, change =1, 1- change =0
            return 0.0
        else: # current year is missing, lag year is not missing, change = -1, 1- change =2
            return 2.0
    else: # current year is missing, lag year is  missing, change = 0, 1- change =1
        return 1.0
    
def get_bodydf_emb_by_filename(FileName, edgar_root_dir="/text/edgar/", model_shortname=None):
    """
    This is a read-only (i.e., non-BERT-calculating) version of bmc_us_2121_yoybert.get_bodydf_emb_by_filename().


    files_to_compare = {'FileName': 'edgar/data/109471/0000910680-07-000324.txt', 'FileName_lag': 'edgar/data/109471/0000109471-06-000007.txt', 'gvkey': '001173', 'datadate': pd.Timestamp('2007-01-31 00:00:00')}
    FileName, FileName_lag = files_to_compare["FileName"], files_to_compare["FileName_lag"]
    
    files_to_compare = {'index': 171, 'FileName': 'edgar/data/93384/0000093384-98-000007.txt', 'FileName_lag': 'edgar/data/93384/0000950168-97-001522.txt', 'gvkey': '009999', 'datadate': pd.Timestamp('1998-02-28 00:00:00')}
    FileName = files_to_compare["FileName_lag"]
    
    fcik="19704"; tfnm = "0000019704-96-000005"

    FileName="edgar/data/1018332/0000927016-97-000922.txt"
    """
    model_shortname = 'yft'
    _to_list = FileName.split("/")
    fcik = _to_list[-2]
    tfnm = _to_list[-1].split(".")[0]
    #my_form10k = EdgarForm10K(fcik = fcik, tfnm = tfnm, edgar_root_dir=edgar_root_dir) #self = my_form10k
    embd_df_read_filepath = f"{edgar_root_dir}/10k-bycik/{fcik}/{tfnm}/embd_df_{model_shortname}_{tfnm}.p.gz"
    if os_path_isfile(embd_df_read_filepath) and is_file_recent(embd_df_read_filepath, 900):
        try:
            embd_df = pd.read_pickle(embd_df_read_filepath, compression="gzip")
        except Exception as e: 
            logger.warning(f"{str(e)} {embd_df_read_filepath} cannot be read")    
            embd_df = pd.DataFrame()
        if embd_df.empty:
            logger.warning(f"Existing embedding from {embd_df_read_filepath} but empty: {FileName}")
    else:
        logger.warning(f"No existing embedding for {FileName} from file {embd_df_read_filepath}")
        embd_df = pd.DataFrame()
    return embd_df    

def get_form_10k_item(  keyword_re: str="Management.{1,10}Discussion.{1,10}Analysis", exclude_aup=False, body_df = pd.DataFrame()):
    """

    if keyword_re is None, return full 10K
    head_df.to_csv("/tmp/h1.csv")
    """
    result_dict = {"item_txt": ""}
    
    if body_df.empty:
        return result_dict

    #body_df.loc[body_df["ar_begin"]]
    #body_df.loc[body_df["ar_end"]]
    if exclude_aup:
        if not body_df.loc[body_df["ar_begin"]].empty and not body_df.loc[body_df["ar_end"]].empty:
            ar_begins = body_df.loc[body_df["ar_begin"]].index.to_list()
            ar_begin = ar_begins[0]
            ar_ends = body_df.loc[body_df["ar_end"]].index.to_list()
            ar_ends = [
                min([x for x in ar_ends if x > ar_begin+2]) if [x for x in ar_ends if x > ar_begin+2] else None for ar_begin in ar_begins
            ]
            ar_end = ar_ends[-1]
            if ar_begin and ar_end:
                body_df = pd.concat([body_df.iloc[0:ar_begin-1], body_df.iloc[ar_end+1:]])
                body_df = body_df.reset_index(drop=True)
                
    try:
        
        if keyword_re is None:
            item_df = body_df.reset_index(drop=True)
        else:
            head_df = body_df.groupby("item_number").head(5).sort_values(by=["item_number", "index"])
            #To preserve the previous behavior, use, group_keys=False
            search_head = pd.DataFrame(head_df.groupby("item_number", group_keys=False).txt.apply(lambda x: 1 if re.search(keyword_re, str(x), flags=re.I) else 0 )).reset_index()
            search_head = search_head.loc[search_head.txt>0]
            if search_head.shape[0]>0:
                item_num_found = search_head.item_number.iloc[0]
                item_df = body_df.loc[body_df.item_number==item_num_found].reset_index(drop=True)
            else:
                item_df = body_df.reset_index(drop=True)
        
        item_df.fillna({"txt": ""}, inplace=True)                
        #result_dict["item_txt"] = "\n".join(item_df.txt.to_list())
        result_dict["item_txt"] = "\n".join(item_df.iloc[item_df.item_number.first_valid_index():].txt.to_list())
        item_df = item_df.loc[item_df.embd.notna()].reset_index(drop=True)        
        item_df = item_df.loc[item_df.embd.apply(lambda emb: True if not np.isnan(emb).any() else False )]
        weights = [len(sentence.split()) for sentence in item_df.txt.to_list()]
        total_weight = sum(weights)
        embeddings = item_df.embd.to_list()
        weighted_embeddings = [np.array(emb) * weight for emb, weight in zip(embeddings, weights)]
        # Handle empty groups or groups with no embeddings
        weighted_average = np.sum(weighted_embeddings, axis=0) / total_weight if total_weight > 0 \
            else np.zeros_like(np.array(embeddings[0])) if len(embeddings) > 0 else None  
        result_dict["wembd"] =  weighted_average
        if keyword_re is None:
            result_dict["embs"] = []
            result_dict["ws"] = []
        else:
            result_dict["embs"] = item_df.embd.to_list()
            result_dict["ws"] = weights
    except Exception as e:
        traceback.print_exc()
        logger.warning(f"Error on line {traceback.extract_tb(e.__traceback__)[0].lineno}: {str(e)}")                
        logger.critical(f"get_form_10k_item  critical: "+str(e))

    return result_dict        

from sklearn.metrics.pairwise import cosine_similarity as sk_cossim

def measure_10k_byitems_yoy(files_to_compare, model_shortname=None):
    """
    By 10-K item:
      - Cohen et al 2020 JF paper:  
          * MD&A (Item 7)
          * Legal Proceedings (Item 3)
          * Q and Q Disclosure about Market Risk (Item 7A)
          * Risk Factors (Item 1A)
          * Other Information (9B)
       - Hobert and Philipp 2016 JPE paper:
          * Business (Item 1)
       - Czerney, Sivadasan, 2021 WP:
          * Item 8 Financial Statement and Exhibits
             
    To be implemented later: By Note item (Burke Hoitash Hoitash 2021 TAR paper found they change with CAM):
        Accounting Policy
        Revenue
        M&A
        Intangibles
        Income Tax
        Other
        
    df_byitem_yoy.to_csv("/tmp/b2.csv")    
    
    # to test
    model_shortname = "yft"
    files_to_compare = {'FileName': 'edgar/data/109471/0000910680-07-000324.txt', 'FileName_lag': 'edgar/data/109471/0000109471-06-000007.txt', 'gvkey': '001173', 'datadate': pd.Timestamp('2007-01-31 00:00:00')}

    files_to_compare={'FileName': 'edgar/data/727739/0000892569-97-001135.txt', 'FileName_lag': 'edgar/data/727739/0000892569-96-000436.txt', 'gvkey': '003122', 'datadate': pd.Timestamp('1997-01-31 00:00:00')}

    ValueError: Input contains NaN.
    files_to_compare={'FileName': 'edgar/data/725549/0000725549-97-000003.txt', 'FileName_lag': 'edgar/data/725549/0000725549-96-000004.txt', 'gvkey': '003063', 'datadate': pd.Timestamp('1997-01-31 00:00:00')}
    files_to_compare={'FileName': 'edgar/data/1002044/0000950135-97-005232.txt', 'FileName_lag': 'edgar/data/1002044/0000950135-96-005373.txt', 'gvkey': '061573', 'datadate': pd.Timestamp('1997-09-30 00:00:00')}
    """
    #init empty return value:
    import sim_measures as sim

    #import importlib
    #importlib.reload(sim)

    df_byitem_yoy = pd.DataFrame()
    
    if isinstance(files_to_compare, str):
        files_to_compare = json.loads(files_to_compare)
    
    FileName, FileName_lag = files_to_compare["FileName"], files_to_compare["FileName_lag"]


    # Try to read existing file # moved up on July 16, 2023
    _to_list = FileName.split("/")
    fcik = _to_list[-2]
    tfnm = _to_list[-1].split(".")[0]
    my_form10k = EdgarForm10K(fcik = fcik, tfnm = tfnm)

    byitem_yoy_save_filepath         = my_form10k.edgar_root_dir + "{}-bycik/{}/{}/".format(my_form10k.formtype, my_form10k.fcik, my_form10k.tfnm) + "byitem_yoy_{}".format(my_form10k.tfnm) + ".csv.gz"
    
    df_byitem_yoy_existing = pd.DataFrame()
    
    # if os.path.isfile(byitem_yoy_save_filepath) and is_file_recent(byitem_yoy_save_filepath, -1): #183
    #     try:
    #         df_byitem_yoy_existing = pd.read_csv(byitem_yoy_save_filepath, compression="gzip", sep="|")
    #         if sum(df_byitem_yoy_existing.cos.notnull())>2 and df_byitem_yoy_existing.shape[0]==11 and "chwds" in df_byitem_yoy_existing.columns : # the most recently updated version, return
    #             logger.debug(f"Returning existing data from file {byitem_yoy_save_filepath}")
    #             return df_byitem_yoy_existing[["gvkey", "datadate", "FileName", "FileName_lag", "item_number", "item_title_keyword", 'cos', 'jac', 'medt', 'simple', 'cos5', "chwds", "dcos", "dcos5", "djcd"]]
    #         else:
    #             logger.debug(f"Incomplete existing data from file {byitem_yoy_save_filepath}")
    #             # existing data incomplete
    #             # try to complete it
    #     except Exception as e:
    #         traceback.print_exc()
    #         logger.warning(f"Error on line {traceback.extract_tb(e.__traceback__)[0].lineno}: {str(e)}")        
            
    #         # probably didnt save csv successfully last time
    #         logger.error(f"File {byitem_yoy_save_filepath} raised error: " + str(e))
    #         os.unlink(byitem_yoy_save_filepath)
    #         df_byitem_yoy_existing = pd.DataFrame()
    # else:
    #     df_byitem_yoy_existing = pd.DataFrame()
    
    df_byitem_yoy = pd.DataFrame([
        {"item_number": "1",      "item_title_keyword": r"Business"},
        {"item_number": "1A",     "item_title_keyword": r"Risk.{1,5}Factor"},
        {"item_number": "2",      "item_title_keyword": r"Property|Properties"},
        {"item_number": "3",      "item_title_keyword": r"Legal.{1,5}Proceeding"},
        {"item_number": "4",      "item_title_keyword": r"Mine.{1,5}Safety"},
        {"item_number": "5",      "item_title_keyword": r"Registrant.{1,10}Common.{1,5}Equity"},
        {"item_number": "7",      "item_title_keyword": r"Management.{1,10}Discussion.{1,10}Analysis"},
        {"item_number": "7B",     "item_title_keyword": r"Qua\w+.{1,10}Qua\w+.{1,5}Disclosure|market.{1,5}risk"},
        {"item_number": "8",      "item_title_keyword": r"Financial\s+Statements?.{1,10}Supplementary\s+Data"},
        {"item_number": "8X",     "item_title_keyword": r"Financial\s+Statements?.{1,10}Supplementary\s+Data"},
        {"item_number": "9A",     "item_title_keyword": r"Controls?.{1,5}Procedures?"},
        {"item_number": "9B",     "item_title_keyword": r"Other.{1,5}Information"},
        {"item_number": "10",     "item_title_keyword": r"Directors.{1,5}Executive.{1,5}Officers?.{1,10}Corporate.{1,5}Governance"},
        {"item_number": "11",     "item_title_keyword": r"Executive.{1,5}Compensation"},
        {"item_number": "12",     "item_title_keyword": r"Certain.{1,5}Beneficial.{1,5}Owner"},
        {"item_number": "13",     "item_title_keyword": r"Related.{1,5}Transactions"},
        {"item_number": "14",     "item_title_keyword": r"Principal.{1,5}Accounting.{1,5}Fees?"},
        {"item_number": "15",     "item_title_keyword": r"Exhibits?.{1,10}Financial\s+Statement\s+Schedule"},
        {"item_number": "_all_",  "item_title_keyword": None},
        {"item_number": "_alX_",  "item_title_keyword": None},
    ])
    df_byitem_yoy["gvkey"]          = files_to_compare["gvkey"]
    df_byitem_yoy["datadate"]       = files_to_compare["datadate"]
    df_byitem_yoy["FileName"]       = FileName
    df_byitem_yoy["FileName_lag"]   = FileName_lag
    
    if not df_byitem_yoy_existing.empty:
        df_byitem_yoy = pd.merge(df_byitem_yoy, df_byitem_yoy_existing[["item_number", "item_text", "item_text_lag"]], on="item_number", how="left")
        del df_byitem_yoy_existing
    else:
        df_byitem_yoy["item_text"] = np.NaN
        df_byitem_yoy["item_text_lag"] = np.NaN

    for filename in [FileName_lag, FileName]:
        #filename=FileName
        _to_list = filename.split("/")
        fcik = _to_list[-2]
        tfnm = _to_list[-1].split(".")[0]
        my_form10k = EdgarForm10K(fcik = fcik, tfnm = tfnm)
        body_df_save_filepath         =  f"{my_form10k.edgar_root_dir}/10k-bycik/{fcik}/{tfnm}/body_df_{tfnm}.txt.gz"
        body_df = pd.DataFrame()
        if not is_file_recent(body_df_save_filepath, 999):
            # when existing body_df file is older than two weeks, likely of older version, generate again:
            try:
                # filename = "edgar/data/1566373/0001193125-21-100530.txt"
                logger.debug(f"Overwriting old-version items_df for {filename}")
                #body_df = my_form10k.form10k_get_items_df(overwrite_existing=True)
                sub_result = subprocess.run(["python3", "edgarform.py", "-getbodydf", filename], capture_output=True, text=True, check=True)
                _text = sub_result.stdout
                body_df= pd.DataFrame.from_records (json.loads (_text))
                #body_df = my_form10k.form10k_get_items_df(overwrite_existing=False)
            except subprocess.CalledProcessError as se:
                logger.error(f"edgarform.py -getbodydf error: {str(se)}")    
            except Exception as e :
                try:
                    body_df = my_form10k.form10k_get_items_df(overwrite_existing=True)
                except Exception as e2 :
                    traceback.print_exc()
                    logger.warning(f"Error on line {traceback.extract_tb(e2.__traceback__)[0].lineno}: {str(e2)}")        
                    
                    logger.error(f"Failed to run form10k_get_items_df for filename {filename}: {str(e2)}")
                    body_df =  pd.DataFrame()    
        else: 
            # when existing body_df file is newer than two weeks, likely of current version. Try to use:
            try:
                body_df = my_form10k.form10k_get_items_df(overwrite_existing=False)
            except:    
                #body_df = my_form10k.form10k_get_items_df(overwrite_existing=True)
                try:
                    sub_result = subprocess.run(["python3", "edgarform.py", "-getbodydf", filename], capture_output=True, text=True, check=True)
                    body_df= pd.DataFrame.from_records (json.loads (sub_result.stdout))
                except subprocess.CalledProcessError as e:
                    logger.error(f"edgarform.py -getbodydf error: {str(e)}")


        body_df.reset_index(inplace=True)
        if not model_shortname is None:
            emb_df = get_bodydf_emb_by_filename(filename)
            if not emb_df.empty and "embd" in emb_df.columns:
                body_df = pd.merge(body_df, emb_df[["item_number", "txt", "embd"]], on=["item_number", "txt"], how="left")
            else:
                body_df["embd"] = np.NaN
        else:
            emb_df = pd.DataFrame()    
            body_df["embd"] = np.NaN

        body_df.drop_duplicates(subset=["index"], inplace=True)
        body_df["ar_begin"] = (body_df.txt.fillna("").apply(lambda text: True if \
          #re.search(r"REPORTs? \s+ OF  \s+ INDEPENDENT  \s+  REGISTERED  \s+ PUBLIC  \s+ ACCOUNTING  \s+ FIRM", text, flags=re.I|re.X) 
          #INDEPENDENT AUDITOR'S REPORT
          #REPORT OF INDEPENDENT REGISTERED PUBLIC ACCOUNTANTS
          len(re.findall(r"\bREPORTs?\b", text, flags=re.I|re.X))>=1 and \
          len(re.findall(r"INDEPENDENT|REGISTERED|Certified|Public", text, flags=re.I|re.X))>=1 and \
          len(re.findall(r"ACCOUNTING\s+FIRM|ACCOUNTANT|AUDITOR", text, flags=re.I|re.X))>=1 and \
          not re.search(r"refer|see|reference|exhibit|incorporated|included|attached|found|herein|therein|thereto|hereto", text, flags=re.I|re.X)  \
        else False)) & (body_df.htm.fillna("").apply(lambda htm: True if not re.search(r"<a\shref", htm, flags=re.I|re.X) else False ))
        body_df["ar_end"] = body_df.txt.fillna("").apply(lambda text: True if \
            (re.search(r"\/S\/|LLP|L\.L\.P\.|CPA|C.P.A.|Accountant", text, flags=re.I|re.X)  and len(text.split( )) < 15) or \
            (re.match(r"\s*(\w+\W+){0,2}(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d+\D{1,5}\d\d\d\d", text, flags=re.I|re.X) and len(text.split( )) <= 5)    
                 else False)

        if filename == FileName:
            item_text_col = "item_text"
            wemb_col = "wembd"       
            getitem_col = "getitem"
            embs_col = "embs"
            ws_col = "ws"
        elif filename == FileName_lag:
            item_text_col = "item_text_lag"
            wemb_col = "wembd_lag"
            getitem_col = "getitem_lag"
            embs_col = "embs_lag"
            ws_col = "ws_lag"
        df_byitem_yoy[getitem_col] = df_byitem_yoy.apply(lambda row: get_form_10k_item(
                row["item_title_keyword"], True if "X" in row["item_number"] else False, body_df), axis=1)       
        df_byitem_yoy[item_text_col] = df_byitem_yoy[getitem_col].apply(lambda getitem_col: getitem_col["item_txt"])    
        df_byitem_yoy[wemb_col] = df_byitem_yoy[getitem_col].apply(lambda getitem_col: getitem_col["wembd"])    
        df_byitem_yoy[embs_col] = df_byitem_yoy[getitem_col].apply(lambda getitem_col: getitem_col["embs"])    
        df_byitem_yoy[ws_col] = df_byitem_yoy[getitem_col].apply(lambda getitem_col: getitem_col["ws"])    
        del body_df
            

    try:    
        item_pair_list = list(zip(
            df_byitem_yoy["item_number"].to_list(), df_byitem_yoy["item_number"].to_list(),
            df_byitem_yoy["item_text"].to_list(), df_byitem_yoy["item_text_lag"].to_list(),
            df_byitem_yoy["wembd"].to_list(), df_byitem_yoy["wembd_lag"].to_list(),
            df_byitem_yoy["embs"].to_list(), df_byitem_yoy["embs_lag"].to_list(),
            df_byitem_yoy["ws"].to_list(), df_byitem_yoy["ws_lag"].to_list()
        ))

        sim_measure_results = []       
        dembs = []; ws_all = []
        dembs_X = []; ws_alX = []
        for item_number, item_number_lag, item_text, item_text_lag, wembd, wembd_lag, embs, embs_lag, ws, ws_lag in item_pair_list:
            #item_number, item_number_lag, item_text, item_text_lag, wembd, wembd_lag, embs, embs_lag, ws, ws_lag = item_pair_list[1]
            #print(item_number)
            try:
                this_sim_measure = sim.sim_measures(item_text, item_text_lag)
                #cosine_similarity_numpy(wembd, wembd_lag)
                this_sim_measure["dmue"] = 1- cosine_similarity_numpy(wembd, wembd_lag)
                
            except Exception as e:
                traceback.print_exc()
                logger.warning(f"Error on line {traceback.extract_tb(e.__traceback__)[0].lineno}: {str(e)}")        
                logger.exception("sim_measure error" +str(e))
                this_sim_measure = {}
                logger.warning(f"files_to_compare={str(files_to_compare)}")
            if not (item_number == "_all_" or item_number == "_alX_"):
                try:
                    #df_paras = pd.DataFrame(ws, columns=["ws"])
                    if len(embs)> 0 and len(embs_lag)> 0 and not wembd is None:
                        totalwords = sum(ws)
                        bsim_matrix = sk_cossim(embs, embs_lag)
                        told_max_bsim = bsim_matrix.max(axis=1).round(4)
                        this_sim_measure["demb"] = 1-sum([e*w for e, w in list(zip(told_max_bsim.tolist(), ws))])/ totalwords
                        dembs.append(this_sim_measure["demb"])
                        ws_all.append(totalwords)
                        if not "X" in item_number:
                            dembs_X.append(this_sim_measure["demb"])
                            ws_alX.append(totalwords)
                    else:
                        totalwords = 0
                        this_sim_measure["demb"] = np.NaN
                    
                except Exception as e:
                    traceback.print_exc()
                    logger.warning(f"Error on line {traceback.extract_tb(e.__traceback__)[0].lineno}: {str(e)}")        
                    logger.exception("sim_measure error" +str(e))
                    logger.warning(f"files_to_compare={str(files_to_compare)}")
            elif item_number == "_all_":
                if sum(ws_all) > 0:
                    this_sim_measure["demb"] = sum( [e*w for e, w in list(zip(dembs, ws_all))] ) / sum(ws_all)
                else:
                    this_sim_measure["demb"] = np.NaN
            elif item_number == "_alX_":        
                if sum(ws_alX) > 0:
                    this_sim_measure["demb"] = sum( [e*w for e, w in list(zip(dembs_X, ws_alX))] ) / sum(ws_alX)
                else:
                    this_sim_measure["demb"] = np.NaN
            sim_measure_results.append(this_sim_measure)        
        del item_pair_list
        
        #result_dict = {"cos": cos_sim_value, "jac": sim_jacard_value, "medt": sim_min_edit, "simple": sim_simple_value, "cos5":gram5_cos_sim_value, "chwds": change_words, 
        #"dcos": 1-cos_sim_value, "dcos5": 1-gram5_cos_sim_value, "djcd":1-sim_jacard_value}

        for key in ['cos', 'jac', 'medt', 'simple', 'cos5', "chwds", "dcos", "dcos5", "djcd", "dmue", "demb"]:
            df_byitem_yoy[key] = pd.Series([sim_measure_result.get(key, None) for sim_measure_result in sim_measure_results], name = key)

        del sim_measure_results

        df_byitem_yoy.to_csv(byitem_yoy_save_filepath, sep="|", index=False, use_gzip=True)
        return df_byitem_yoy[["gvkey", "datadate", "FileName", "FileName_lag", "item_number", "item_title_keyword", 'cos', 'jac', 'medt', 'simple', 'cos5', "chwds", "dcos", "dcos5", "djcd", "dmue", "demb"]]

    except Exception as e:
        traceback.print_exc()
        logger.warning(f"Error on line {traceback.extract_tb(e.__traceback__)[0].lineno}: {str(e)}")        

        my_form10k = EdgarForm10K(fcik = "", tfnm = "")
        del my_form10k
        logger.error(f"Failed to get item_yoy measures for filename pairs {FileName} and {FileName_lag} \nfiles_to_compare={str(files_to_compare)}\ndue to {e}")
        #raise
        return pd.DataFrame()    


"""
Failed to get item_yoy measures for filename pairs edgar/data/827052/0000827052-00-000050.txt and edgar/data/827052/0000827052-99-000048.txt due to

Failed to get item_yoy measures for filename pairs edgar/data/83402/0000950116-03-004989.txt and edgar/data/83402/0000950116-02-002869.txt 
files_to_compare={'FileName': 'edgar/data/83402/0000950116-03-004989.txt', 'FileName_lag': 'edgar/data/83402/0000950116-02-002869.txt', 'gvkey': '009083', 'datadate': Timestamp('2003-09-30 00:00:00')}
due to 


ata/945394/0000908737-99-000108.txt 
files_to_compare={'FileName': 'edgar/data/945394/0000908737-00-000099.txt', 'FileName_lag': 'edgar/data/945394/0000908737-99-000108.txt', 'gvkey': '061188', 'datadate': Timestamp('1999-12-31 00:00:00')}
due to 
 80%|███████████████████████████████████████████████████▍            | 5514/6854 [03:11<03:14,  6.88it/s]
sequence
"""        

if False: # to test class
    final_df.loc[final_df.CIK==320193][["DateFiled", "FileName"]]
    self = EdgarForm10K("100320", '0001144204-07-014274', '10KSB')
    filing_raw_txt = self.get_raw_filing()
    rawdocs = self.get_documents(filing_raw_txt)
    
    #51143 IBM  
    final_df.loc[final_df.CIK==51143][["DateFiled", "FileName"]]
    self = EdgarForm10K(51143, "0001558370-23-002376", edgar_root_dir = _edgar_root_dir, formname = "10-K", mo=mo, logger=logger)
    body_df = self.form10k_get_items_df()
    
    self = EdgarForm10K("51143", '0001558370-22-001584', edgar_root_dir = "/text/edgar"); filing_raw_txt = self.get_raw_filing()
    
    self = EdgarForm10K(51143, "0001558370-23-002376", edgar_root_dir =  "/text/edgar", formname = "10-K", mo=mo, logger=logger)
    self.form10k_get_items_df()
    
    _edgar_root_dir = "/text/edgar"
    my_form10k = EdgarForm10K(51143, "0001558370-23-002376", edgar_root_dir = _edgar_root_dir, formname = "10-K", mo=mo, logger=logger)

    my_form10k.form10k_get_items_df()
    51143
    #apple ( with gz, disk storage reduced from 194M to 24M, or to 12%)
    final_df.loc[final_df.CIK==320193][["DateFiled", "FileName", "IndexPage"]]
    self = EdgarForm10K("320193", '0000320193-22-000108', edgar_root_dir = _edgar_root_dir)
    filing_raw_txt = self.get_raw_filing()
    rawdocs = self.get_documents(filing_raw_txt)
    html_value = self.get_document_i(1)
    
    #Walmart 104169
    self = EdgarForm10K("104169", '0000104169-23-000020', edgar_root_dir = "/text/edgar/", mo=mo, logger=logger); filing_raw_txt = self.get_raw_filing()


    self = EdgarForm10K("104169", "0000104169-22-000012", edgar_root_dir = _edgar_root_dir, formname = "10-K", user_agent=_http_user_agent, mo=mo, logger=logger)
    body_df_lag = self.form10k_get_items_df()
    
    # 1000235
    final_df.loc[final_df.CIK==1000232][["DateFiled", "FileName", "IndexPage"]]
    self = EdgarForm10K("1000232", '0001000232-01-000001', edgar_root_dir = _edgar_root_dir)
    filing_raw_txt = self.get_raw_filing()
    rawdocs = self.get_documents(filing_raw_txt)
    # to test zero index file due to url download error:
    self = EdgarForm10K("1057051", '0000950136-07-002098', edgar_root_dir = _edgar_root_dir); filing_raw_txt = self.get_raw_filing()

    # sample 5 ciks
    # final_df.CIK.sample(n=5).to_list()
    # [804753, 850083, 1020859, 1086484, 819479]
    # 804753,850083,1020859,1086484,819479,100320,320193,1000232,1057051
    
    # problematic ones
    self = EdgarForm10K("1004411", '0000950136-03-000581', edgar_root_dir = _edgar_root_dir); filing_raw_txt = self.get_raw_filing(); df  =self.form10k_get_items_df()
    
    # 1002664/0001002664-03-000005
    self = EdgarForm10K("1002664", '0001002664-03-000005', edgar_root_dir = _edgar_root_dir); filing_raw_txt = self.get_raw_filing(); df  =self.form10k_get_items_df()
    
    # /17206/0000017206-94-000007
    # 60512/0000060512-94-000006


def get_header_df(years = range(1996, 2026), _edgar_root_dir="/mnt/text/edgar/"):
    header_dfs = []
    for year in years:
        #Read output from `form_10k_header_busaddr.py`
        header_dfs.append(pd.read_csv( f"{_edgar_root_dir}10k-df/header_buzaddr_{str(year)}.csv.gz", compression="gzip", sep="|"))
    header_df = pd.concat(header_dfs); header_df.shape[0]
    header_df['Conformed_Period_of_Report'] = pd.to_datetime(header_df['Conformed_Period_of_Report'], format='%Y%m%d')
    header_df['datadate_m'] = header_df['Conformed_Period_of_Report'].apply(lambda x: datetime.date( x.date().year, x.date().month, 1) if isinstance(x, pd.Timestamp) else None)    
    #267029 -> 287088
    #header_df.DateFiled.str[:4].value_counts().sort_index()
    return header_df

def get_funda_10k_df_linked(years = range(1996, 2026), _edgar_root_dir="/mnt/text/edgar/",
    pickfilename=None):
    """
    To link comp-funda with EDGAR Form 10-K by cik
    (edgarform.py version).
    
    To use: 
    funda_10k = get_funda_10k_df_linked(years = range(1996, 2026), pickfilename=None)
    """
    if pickfilename is None:
        #pickfilename = f"funda_10k_h_20240127.p.gz"
        #pickfilename = f"funda_10k_h_20251024.p.gz"
        pickfilename = f"funda_10k_h_20250409.p.gz"
        pickfilepath = f"{_edgar_root_dir}10k-df/{pickfilename}"
    else:
        pickfilepath = pickfilename    
    if os.path.isfile(pickfilepath):
        funda_10k_g = pd.read_pickle(pickfilepath, compression="gzip")
        print(f"Loaded funda_10k_h from {pickfilepath}")
        #funda_10k_g.datadate.dt.year.value_counts().sort_index()
        return funda_10k_g
    
    header_dfs = []
    
    for year in years:
        #Read output from `form_10k_header_busaddr.py`
        header_dfs.append(pd.read_csv( f"{_edgar_root_dir}10k-df/header_buzaddr_{str(year)}.csv.gz", compression="gzip", sep="|"))
    
    header_df = pd.concat(header_dfs); header_df.shape[0]
    #267029 -> 287088
    header_df.DateFiled.str[:4].value_counts().sort_index()
    
    #del header_dfs
    
    header_df = header_df[['CIK', 'CompanyName', 'FormType', 'DateFiled', 'FileName',
           'Form_Name', 'Conformed_Period_of_Report',
           'State_of_Incorporation', 'Standard_Industrial_Classification',
           'SIC_Code',  'BUSIADDR_STATE']].reset_index(drop=True)
    
    header_df['Conformed_Period_of_Report'] = pd.to_datetime(header_df['Conformed_Period_of_Report'], format='%Y%m%d')
    header_df['datadate_m'] = header_df['Conformed_Period_of_Report'].apply(lambda x: datetime.date( x.date().year, x.date().month, 1) if isinstance(x, pd.Timestamp) else None)
    #header_df["cik"] = header_df["CIK"].astype(str).str.zfill(10)    
    header_df.rename(columns={"CIK":"cik_10k"}, inplace=True)
    header_df["cik"] = header_df["cik_10k"]
    
    #try Tyco
    header_df.loc[header_df.CompanyName.str.startswith("TYCO INTERNATIONAL")][["cik_10k", "DateFiled", "Form_Name", "CompanyName"]]
    
    #Read output from `build_cik_gvkey.py`
    #fundau2 = pd.read_pickle(f"{WRDS_ROOT}/fundau0_ciklink.pkl")
    #fundau2 = gzip_to_df("/data/edgar/fundau2_ciklink_20231128.p.gz")
    fundau2 = pd.read_pickle("/data/wrds/edgar/fundau2_ciklink_20250929.p.gz", compression="gzip")
    #292258
    #fundau2.groupby("datadate").gvkey.value_counts().sort_index()
    #728_159
    
    fundau2["datadate"] = pd.to_datetime(fundau2["datadate"])
    
    fundau2['datadate_m'] = fundau2['datadate'].apply(lambda x: datetime.date( x.date().year, x.date().month, 1) if isinstance(x, pd.Timestamp) else None)
    
    #fundau2.loc[fundau2.gvkey=="010787"].sort_values(by=["cik2","datadate"])[["datadate","cik2"]].iloc[0:60]
    
    fundau2.loc[fundau2.cik.notna()].shape[0]
    #513_030 -> 1_475_340 ->263750 has cik
    
    fundau2.loc[fundau2.cik2.notna()].shape[0]
    #511_696 ->1_473_803 -> 262905 has cik2
    
    #view
    fundau2.loc[fundau2.gvkey=="010787"][["conm", "datadate", "gvkey", "permno", "cik", "cik2", "cusip6"]].sort_values(by="datadate").sort_values(by=["cik2", "datadate"]).iloc[0:60]
    
    fundau2 = fundau2[["gvkey", "cusip", "sich", "datadate", "cik_comp", "cik_link_from", "conm", "conm_w1", "comnam", "comnam_w1", "cik", "cik2", "datadate_m"]]
    
    funda_10k_a = pd.merge(fundau2.loc[fundau2.cik.notna()], header_df[['cik_10k', 'CompanyName', 'FormType', 'DateFiled', 'FileName','Form_Name', 'Conformed_Period_of_Report', 
           'datadate_m', 'cik']], on = ["cik", "datadate_m"], how="inner")
    
    funda_10k_a.drop("cik2", axis=1, inplace=True)
    
    funda_10k_a["cik_link_from"] = funda_10k_a["cik_link_from"].apply(lambda x: "comp_1st_link_2nd_from" + str(x))
    
    funda_10k_a["cik_link_from"].value_counts()
    funda_10k_a = funda_10k_a.reset_index(drop=True)
    #477320 -> 1126989 ->170248
    
    funda_10k_a.loc[funda_10k_a.cik_10k.notna()].cik.nunique()
    #18_375 ->18806 unique ciks
    
    funda_10k_a.loc[funda_10k_a.cik_10k.notna()].drop_duplicates(subset=["gvkey", "datadate"])
    #157_724 ->170075 unique firm-year found 10-K
    funda_10k_a["cik_link_from"].value_counts()
    
    header_df["cik2"] =header_df["cik"]
    
    funda_10k_b = pd.merge(fundau2.loc[fundau2.cik2.notna()], header_df[['cik_10k', 'CompanyName', 'FormType', 'DateFiled', 'FileName','Form_Name', 'Conformed_Period_of_Report', 
           'datadate_m', 'cik2']], left_on = ["cik2", "datadate_m"], right_on = ["cik2", "datadate_m"], how="inner")
    funda_10k_b["cik"] = funda_10k_b["cik2"]
    funda_10k_b.drop("cik2", axis=1, inplace=True)
    
    funda_10k_b["cik_link_from"] = funda_10k_b["cik_link_from"].apply(lambda x: "link_1st_from" + str(x) + "_comp_2nd")
    
    funda_10k_b = funda_10k_b.reset_index(drop=True)
    #466_811 total; -> 1118420  ->169154
    
    funda_10k_b.loc[funda_10k_b.cik_10k.notna()].drop_duplicates(subset=["gvkey", "datadate"])
    #157_179 unique firm-year found 10-K -> 157872 ->168984
    
    funda_10k_b["cik_link_from"].value_counts()
    
    
    funda_10k_a.loc[funda_10k_a.gvkey=="010787"][["gvkey", "datadate", "conm", "CompanyName", "cik_10k", "cik", "cik_link_from"]].sort_values(by="datadate")
    funda_10k_b.loc[funda_10k_b.gvkey=="010787"][["gvkey", "datadate", "conm", "CompanyName", "cik_10k", "cik", "cik_link_from"]].sort_values(by="datadate")
    #2009--2015
    
    
    funda_10k_b.loc[funda_10k_b.cik_10k.notna()].cik.nunique()
    #18_459 unique ciks ->18504 ->18741
    
    funda_10k_a = funda_10k_a.loc[funda_10k_a.cik_10k.notna()].reset_index(drop=True)
    funda_10k_b = funda_10k_b.loc[funda_10k_b.cik_10k.notna()].reset_index(drop=True)
    
    funda_10k_ab = pd.concat([funda_10k_a, funda_10k_b], ignore_index=True)
    #944_131 ->2245409 -> 339402
    
    funda_10k_ab["num10kmatched"] = funda_10k_ab.groupby(by=["gvkey", "cik"]).FileName.transform("count")
    funda_10k_ab.loc[funda_10k_ab.gvkey=="010787"].sort_values(by="datadate")[["cik", "FileName", "num10kmatched"]]
    funda_10k_ab.loc[funda_10k_ab.cik_10k.notna()].drop_duplicates(subset=["gvkey", "datadate"]).shape[0]
    #157868 ->170193 unique firm-years have cik_10k matched
    funda_10k_c = funda_10k_ab.sort_values(["gvkey", "datadate", "cik_10k", "num10kmatched"], ascending=[True, True, True, False]).groupby(by=["gvkey", "datadate"]).nth(0).reset_index()
    
    ##261_977 from funda0
    ##157_868 matched with 10K
    
    # For unmatched, fill
    
    # fundau0 = local_raw_sql(r"""
    # select distinct gvkey, cusip, sich, datadate, fyear, cik as cik_comp,
    #   at, ceq, csho, dltt, sstk, dltis, ppegt, ppent, rect, act, che, lct, dlc, dd1, 
    #   ni, ib, ibc, epspx, epsfi, sale, capx,
    #   oancf, xidoc, recch, 
    #   prcc_f,  emp, au, fic, conm
    # from comp.funda 
    # where fyear>1994 and (indfmt='INDL') and (datafmt='STD') and (popsrc='D') and (consol='C') and at >0
    # and ceq is not null
    # ;""", "fundau0", "../data/proj/auliti/wrds/", "trrreli", use_gzip=True, force_rewrite=False)
    
    force_rewrite = False
    from util_wrds import local_raw_sql, wrds_ccm
    fundau0 = local_raw_sql(r"""
        select distinct gvkey, cusip, sich, datadate, cik as cik_comp,
          at, ceq, csho, dltt, sstk, dltis, ppegt, ppent, rect, act, che, lct, dlc, dd1, 
          ni, ib, ibc, epspx, epsfi, sale, capx,
          oancf, xidoc, recch, 
          prcc_f,  emp, au, fic, conm
        from comp.funda 
        where fyear>1994 and (indfmt='INDL') and (datafmt='STD') and (popsrc='D') and (consol='C') and at >0
        and ceq is not null
        ;""", "fundau0_build_cik_gvkey_20250929", "/data/wrds/edgar/", "trrreli", use_gzip= True, force_rewrite=False)

    #261977->263375 -> 278834
    fundau0["cusip6"] = fundau0["cusip"].str[:6]
    fundau0["datadate"] = pd.to_datetime(fundau0["datadate"])
    
    #fundau0.loc[fundau0.gvkey=="010787"].sort_values(by="datadate").datadate
    #fundau1.loc[fundau1.gvkey=="010787"].sort_values(by="datadate").datadate
    
    ccmlink = local_raw_sql(""" select *
        from crsp.ccmxpf_linktable 
        where  linkprim in ('P', 'C')""", 
        "ccmxpf_linktable_20250929", "/data/wrds/edgar/", "reliunc", use_gzip= True, force_rewrite=False)
    
    fundau0ccm = wrds_ccm(fundau0, ccmlink, datevar='datadate', linktype=["LU","LC"])
    
    funda_10k_d = pd.merge(fundau0ccm, funda_10k_c[["gvkey", "datadate", "cik", "cik_link_from"]], how="left", on=["gvkey", "datadate"])
    #261977 ->263375
    
    funda_10k_d["datadate_m"] = funda_10k_d['datadate'].apply(lambda x: datetime.date( x.date().year, x.date().month, 1) if isinstance(x, pd.Timestamp) else None)
    funda_10k_d["cik_link_from"].value_counts()    
    """
    cik_link_from
    comp_1st_link_2nd_frombycusip     79610 -> 74763 -> 84234 ->135227
    comp_1st_link_2nd_frombypermno    76872 -> 82195 -> 76776 ->34507
    link_1st_frombycusip_comp_2nd      1319 -> 1602 -> 1659 ->403
    link_1st_frombypermno_comp_2nd       67 -> 73  ->79   -> 51"""
    funda_10k_d.loc[funda_10k_d["cik_link_from"].isna()].shape[0]
    #104109 -> 104742 ->100627 ->108646
    
    funda_10k_d["cik_link_from"] = funda_10k_d["cik_link_from"].fillna("extended")
    
    funda_10k_d.loc[funda_10k_d.cik.isna()].shape[0]
    #104742 ->108646
    
    funda_10k_d.loc[funda_10k_d.cik.notna()].shape[0]
    #158633 ->162748 ->170188
    
    funda_10k_d_bygvkey = funda_10k_d.loc[funda_10k_d.cik.notna()].drop_duplicates(subset="gvkey")
    funda_10k_d_bygvkey["cik_fill"] = funda_10k_d_bygvkey["cik"]
    # Fill within groups
    funda_10k_e = pd.merge(funda_10k_d, funda_10k_d_bygvkey[["gvkey", "cik_fill"]], on="gvkey", how="left")
    funda_10k_e["cik"] = np.where(funda_10k_e["cik"].notna(), funda_10k_e["cik"], funda_10k_e["cik_fill"])
    
    funda_10k_e.loc[funda_10k_e.cik.notna()].shape[0]
    #now 193629 ->194416 ->204548
    
    funda_10k_e.loc[funda_10k_e.gvkey=="010787"].sort_values(by="datadate")[["gvkey", "datadate","cik"]]
    #WHY THEY ARE ALL MISSING NOW?
    
    # #if do nothing:
    funda_10k_e["cik_comp"] = funda_10k_e["cik_comp"].astype(float)
    funda_10k_comp = pd.merge(funda_10k_e[["gvkey", "cusip", "sich", "datadate", "cik_comp", "cik_link_from", "conm",  "cik", "datadate_m", "cik_link_from"]], header_df[['cik_10k', 'CompanyName', 'FormType', 'DateFiled', 'FileName','Form_Name', 'Conformed_Period_of_Report',  'datadate_m', 'cik']], left_on = ["cik_comp", "datadate_m"], right_on = ["cik", "datadate_m"], how="inner")
    # #154_453 or 59.0% of 261977
    # 167527 or 82% of 204548
    funda_10k_comp = funda_10k_comp.loc[funda_10k_comp.cik_10k.notna()].reset_index(drop=True).drop_duplicates(subset=["gvkey", "datadate"])
    # #154315 -?167376
    funda_10k_f = pd.merge(funda_10k_e[["gvkey", "cusip", "sich", "datadate", "cik_comp", "conm",  "cik", "datadate_m", "cik_link_from"]], header_df[['cik_10k', 'CompanyName', 'FormType', 'DateFiled', 'FileName','Form_Name', 'Conformed_Period_of_Report', 
           'datadate_m', 'cik']], on = ["cik", "datadate_m"], how="left")
    
    funda_10k_f.loc[funda_10k_f.cik_10k.notna()].shape[0]
    #159124 of 263522, or 60.38%
    #170695
    
    funda_10k_g = funda_10k_f.loc[funda_10k_f.cik_10k.notna()].reset_index(drop=True).drop_duplicates(subset=["gvkey", "datadate"])
    #158977 ->170540
    
    #now compare funda_10k_g with funda_10k_comp
    funda_10k_comp["cik_fromcomp_orig"] = True
    funda_10k_h = pd.merge(funda_10k_g, funda_10k_comp[["gvkey", "datadate", "cik_fromcomp_orig"]], on=["gvkey", "datadate"], how="left")
    
    funda_10k_h["cik_fromcomp_orig"] = funda_10k_h["cik_fromcomp_orig"].fillna(False)
    
    funda_10k_h.loc[funda_10k_h.cik_fromcomp_orig, "cik_link_from"] = "comp_orig"
    
    funda_10k_h.cik_link_from.value_counts()
    funda_10k_h.groupby("cik_link_from").cik_10k.nunique()
    funda_10k_h.loc[funda_10k_h.cik_link_from=="comp_orig"].cik_10k.nunique()  # 18132 ->18566
    funda_10k_h.loc[funda_10k_h.cik_link_from!="comp_orig"].cik_10k.nunique()  #   769 ->598
    
    """                           #firm_year    #unique cik  
       comp_orig                         154315          18083
       comp_1st_link_2nd_frombycusip       2461            542
       link_1st_frombycusip_comp_2nd        918             27
       extended                             411            175
       comp_1st_link_2nd_frombypermno       144            109
       link_1st_frombypermno_comp_2nd        30             27
    """
    
    # Comp Funda LINK with EDGAR 10K COMPLETED!
    #funda_10k_h.to_pickle(f"{WRDS_ROOT}/funda_10k_h.pkl")
    #df_to_gzip(funda_10k_h, pickfilename)
    #pickfilepath = f"{_edgar_root_dir}10k-df/{pickfilename}"
    funda_10k_h.to_pickle(pickfilepath, compression="gzip"); funda_10k_h.shape# 170540
    return funda_10k_h

def get_funda_10k_df_lagged(funda_10k):
    
    funda_10k["datadate_year"] = funda_10k["datadate"].apply(lambda x: int(x.date().year))
    
    funda_10k_lag = funda_10k.copy().reset_index(drop=True)
    
    funda_10k_lag["datadate_year"] = funda_10k_lag["datadate_year"].apply(lambda x: int(x) + 1 if int(x)>1900 else 1900)
    
    #funda_10k.loc[funda_10k.gvkey=="010787"].datadate_year
    #funda_10k_lag.loc[funda_10k_lag.gvkey=="010787"].datadate_year
    
    funda_10k_ext = pd.merge(
        funda_10k[[    "gvkey", "cik", "datadate", "datadate_year", "Conformed_Period_of_Report", "DateFiled", "CompanyName", "Form_Name", "FileName"]], 
        funda_10k_lag[["gvkey", "cik", "datadate", "datadate_year", "Conformed_Period_of_Report", "DateFiled", "CompanyName", "Form_Name", "FileName"]], on=["gvkey", "datadate_year"], how="left", suffixes=["", "_lag"])
    funda_10k_ext.shape[0] #163314
    
    funda_10k_ext = funda_10k_ext.loc[(funda_10k_ext.FileName.notna()) & (funda_10k_ext.FileName_lag.notna())].reset_index(drop=True); funda_10k_ext.shape[0] 
    #141003
    funda_10k_ext = funda_10k_ext.loc[((funda_10k_ext.datadate - funda_10k_ext.datadate_lag)/ np.timedelta64(1, 'D')/30.44).apply(round)==12].reset_index(drop=True); funda_10k_ext.shape[0]
    #140040
    funda_10k_ext.sort_values(["gvkey", "datadate", "DateFiled", "datadate_lag", "DateFiled_lag"])
    funda_10k_ext.drop_duplicates(subset=["gvkey", "datadate"], inplace=True); funda_10k_ext.shape[0]
    #140040
    #funda_10k_ext.loc[funda_10k_ext.gvkey=="010787", ["cik", "datadate_year", "datadate", "datadate_lag"]]
    #funda_10k_ext.loc[funda_10k_ext.gvkey=="010787"].to_csv("/tmp/010787ext.csv")
    
    return funda_10k_ext
    

#self.tenkFile = os.path.expanduser(config_apn.edgar10kFolder + self.fcik + "/" + self.tfnm + ".txt")

def build_body_df_by_filename(filename, days_recent = 0): #bf10k
    _to_list = filename.split("/")
    cik = _to_list[-2]
    tfnm = _to_list[-1].split(".")[0]
    body_df_save_filepath  = f"{_edgar_root_dir}10k-bycik/{cik}/{tfnm}/body_df_{tfnm}.txt.gz"
    if not is_file_recent(body_df_save_filepath, days_recent = days_recent): 
        try:
            my_form10k = EdgarForm10K(cik, tfnm)
            body_df = my_form10k.form10k_get_items_df(overwrite_existing=True)
            logger.info(f"Created successfully body df for {str(my_form10k)}: npara: {body_df.shape[0]}")
        except Exception as e:
            traceback.print_exc()
            logger.warning(f"Error on line {traceback.extract_tb(e.__traceback__)[0].lineno}: {str(e)}")        
            
            logger.warning(f"Failed to create body df for {str(my_form10k)}: {str(e)}")    
    return True

def get_mo(args):
    _use_localfs = True if args.use_localfs else False    
    _use_dropbox = True if args.use_dropbox else False
    _edgar_root_dir = args.edgar_root_dir
    _sync_if_missing_file = args.sync_if_missing_file
    if _sync_if_missing_file: 
        if not _use_localfs or not _use_dropbox:
            _use_dropbox = True;            _use_localfs = True
    
    if  _use_localfs and  _use_dropbox:
        if  _sync_if_missing_file:
            pass
        else: 
            logger.info("""Both local Filesystem and Dropbox are On, but not --sync-if-missing-file. 
            When reading data, Will first try to find file in local FS; if unfound then check Dropbox; 
            if still unfound then build dataset and save to both FSs.""")

    if not _use_localfs and not _use_dropbox:
        logger.info("Neither local Filesystem nor Dropbox Turned on. Assume local FS is on from now on")    
        _use_localfs = True

    #if not args.dropbox_app_key:
    #    dropbox_app_key = "homjxj5nz71v3bc"#, dropbox_app_secret = "hfuk3lftvk4u9bk"
            
    mo = ManageOvercloud(use_localfs = _use_localfs, use_dropbox = _use_dropbox, 
                      local_prefix = args.localfs_rootfolder, cloud_prefix = args.cloudfs_rootfolder,
                      dropbox_app_key = args.dropbox_app_key, dropbox_app_secret = args.dropbox_app_secret,
                      sync_if_missing_file=_sync_if_missing_file)

    for mypath in [_edgar_root_dir, _edgar_root_dir+"full-index/", _edgar_root_dir+"by-index/", _edgar_root_dir+"10k-bycik/", _edgar_root_dir+"10k-df/"]:
        if not mo.path_isdir(mypath, check_both = _sync_if_missing_file):
            logger.info(f"Folder {mypath} not exist in at least one location, attempt to create")
            try:
                mo.makedirs(mypath)
            except Exception as e: 
                logger.critical(f"While trying to create useful folder {mypath}, an exception occurred" + str(e))
            traceback.print_exc()
            logger.warning(f"Error on line {traceback.extract_tb(e.__traceback__)[0].lineno}: {str(e)}")        

    return mo                  

def get_bodydf_by_filename(filename, days_recent = 9999):
    _to_list = filename.split("/"); fcik = _to_list[-2]; tfnm = _to_list[-1].split(".")[0]
    my_form10k = EdgarForm10K(fcik = fcik, tfnm = tfnm)
    body_df = my_form10k.form10k_get_items_df(overwrite_existing=False); #print(body_df.to_json(orient="records"))
    return body_df


def setup_logging(args):
    """Sets up logging configuration."""
    # Logger is already configured globally in the script, 
    # but we can add specific handlers or levels based on args here if needed.
    logger.debug("Beginning arguments:")
    for arg in vars(args):
        logger.info(f"{arg}: {getattr(args, arg)}")

def get_years_to_process(args):
    """Determines the list of years to process based on arguments."""
    download10k_arg = args.download_10k_years
    build_10k_arg = args.build_10k_formbody_years
    years_arg = args.years if args.years else download10k_arg if download10k_arg else build_10k_arg if build_10k_arg else ""
    
    logger.info(f"Value of -years: {years_arg}")
    
    res_range = re.search(r"^(\d{4})\.\.(\d{4})$", years_arg)
    res_single = re.search(r"^\d{4}$", years_arg)
    
    if res_single:
        years = [int(res_single.group(0))]
    elif res_range:
        years = [y for y in range(int(res_range.group(1)), int(res_range.group(2)) + 1)]
    else:
        years = [y for y in range(1993, datetime.date.today().year + 1)]
        
    logger.info(f"Running over years {years}")
    return years

def run_build_edgar_index(args, edgar_root_dir):
    """Handles downloading and building the EDGAR master index."""
    if args.build_edgar_index:
        myei = EdgarIndex(user_agent=args.http_user_agent, edgar_root_dir=edgar_root_dir, logger=logger)
        myei.get_index_df()
        logger.info("EDGAR Index ready")

def run_get_bodydf(args):
    """Handles generating a body DF for a specific single file."""
    if args.get_bodydf_by_filename:
        filename = args.get_bodydf_by_filename
        logger.debug(f"Got filename: {filename}")
        parts = filename.split("/")
        fcik = parts[-2]
        tfnm = parts[-1].split(".")[0]
        
        my_form10k = EdgarForm10K(fcik=fcik, tfnm=tfnm)
        body_df = my_form10k.form10k_get_items_df(overwrite_existing=True)
        print(body_df.to_json(orient="records"))
        exit()

def process_notes_by_keyword(args, years, edgar_root_dir, myei):
    """Extracts notes from 10-Ks based on a keyword regex."""
    keyword = args.get_note_10k_by_keyword
    if not keyword:
        return

    req_started_at = round(time.time())
    keyword_re = r"\W+?".join(re.split(r"[^a-zA-Z0-9]", keyword))
    
    for year in years:
        idx_df = myei.get_index_df([year], form_re="^(10-K|10K)").reset_index(drop=True)
        idx_df = idx_df.loc[~idx_df.FormType.str.endswith("A")].reset_index(drop=True)
        
        if args.test:
            idx_df = idx_df.iloc[:20].reset_index(drop=True)
            
        filenames = idx_df.FileName.to_list()
        num_jobs = len(filenames)
        logger.info(f"About to process {num_jobs} form 10-k's for notes extraction.")

        save_filename = os.path.join(edgar_root_dir, f"10k-df/note_{keyword}_{year}.csv.gz")
        
        partial_func = functools.partial(get_form_10k_note, keyword_re, num_jobs, req_started_at)
        num_threads = 14
        
        with Pool(num_threads) as pool:
            results = pool.map(partial_func, filenames, chunksize=1 + int(len(filenames) / num_threads / 8))
            
        result_df = pd.DataFrame(results)
        final_df = pd.merge(idx_df, result_df, how="left", on="FileName")
        
        # Save results locally
        os.makedirs(os.path.dirname(save_filename), exist_ok=True)
        final_df.to_csv(save_filename, sep="|", compression="gzip")
        
        logger.info(f"edgarform get note {keyword} {year} done")

def process_xbrl_textblock(args, years, myei, edgar_root_dir):
    """Extracts XBRL text blocks."""
    if not args.get_xbrl_textblock_title:
        return

    for year in years:
        idx_df = myei.get_index_df([year], form_re="^(10-K|10K)").reset_index(drop=True)
        idx_df = idx_df.loc[~idx_df.FormType.str.endswith("A")].reset_index(drop=True)
        
        if args.test:
            idx_df = idx_df.iloc[:100]
            
        filenames = idx_df.FileName.to_list()
        num_jobs = len(filenames)
        logger.info(f"About to extract xbrl textblock {num_jobs} form 10-k's for year {year}")
        
        # Parallel processing using p_umap (assumed imported)
        results = p_umap(build_xbrl_textblock_title, filenames, num_cpus=max(1, int(cpu_count()/2)))
        
        save_path = os.path.join(edgar_root_dir, f"10k-df/xbrl_textblock{year}.p.gz")
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        #df_to_gzip(results, save_path)
        results.to_pickle(save_path, compression="gzip")

def download_specific_forms(args, years, myei, edgar_root_dir):
    """Downloads specific form types (8-K, 10-Q, etc) based on arguments."""
    if not args.downloadform:
        return

    form_arg = re.sub("[^a-z0-9]", "", str(args.downloadform).lower())
    form_map = {
        "8k": r"^8-K", "def14a": r"^DEF\s14A", "proxy": r"^DEF\s14A",
        "10q": r"^10-?Q", "10k": r"^10-?K", "13d": r"^13D", "13g": r"^13G"
    }
    form_re = form_map.get(form_arg)
    
    if not form_re:
        logger.warning(f"No regex found for form {args.downloadform}")
        return

    filenames = []
    # Build index for all requested years first
    forms_idx_df = myei.get_index_df(years, form_re=form_re, allow_download=True)
    forms_idx_df["cik"] = forms_idx_df["CIK"].astype(str)
    forms_idx_df["YearFiled"] = forms_idx_df["DateFiled"].str[0:4]
    
    # Save aggregate index locally
    idx_save_path = os.path.join(edgar_root_dir, f"forms_idx_df_{form_arg}_{years[0]}_{years[-1]}.csv.gz")
    forms_idx_df.to_csv(idx_save_path, index=False, sep="|", compression="gzip")
    
    for year in years:
        filenames += forms_idx_df.loc[forms_idx_df.YearFiled == str(year)].sort_values(by=["cik", "DateFiled"]).FileName.to_list()

    if args.test:
        filenames = filenames[:100]

    for filename in tqdm(filenames, desc=f"Downloading files for {form_arg}"):
        parts = filename.split("/")
        cik, tfnm = parts[-2], parts[-1].split(".")[0]
        
        my_form = EdgarForm(
            cik, tfnm, 
            edgar_root_dir=edgar_root_dir, 
            formname=form_arg, 
            user_agent=args.http_user_agent, 
            rmold=args.remove_legacy_files, 
            logger=logger
        )
        _ = my_form.get_raw_filing()

def process_10k_forms(args, years, myei, edgar_root_dir):
    """Handles downloading 10-Ks or building 10-K body DFs."""
    run_download = bool(args.download_10k_years)
    run_build = bool(args.build_10k_formbody_years)
    
    if not (run_download or run_build):
        return

    req_started_at = round(time.time())
    
    if run_build and not run_download:
        # Use parallel processing for building bodies if only building
        for year in years:
            idx_df = myei.get_index_df([year], form_re="^(10-K|10K)").reset_index(drop=True)
            idx_df = idx_df.loc[~idx_df.FormType.str.endswith("A")].reset_index(drop=True)
            
            if args.test:
                idx_df = idx_df.iloc[:100]
                
            filenames = idx_df.FileName.to_list()
            logger.info(f"About to process {len(filenames)} form 10-k's for year {year}")
            
            p_umap(build_body_df_by_filename, filenames, num_cpus=max(1, int(cpu_count()/2)))
        return

    # Sequential processing for download (to respect rate limits) or mixed operations
    idx_df = myei.get_index_df(years, form_re="^(10-K|10K)")
    idx_df = idx_df.loc[~idx_df.FormType.str.endswith("A")].reset_index(drop=True)
    
    filenames = idx_df.FileName.to_list()
    logger.info(f"About to process {len(filenames)} form 10-k's")
    
    if args.test:
        filenames = filenames[:100]

    total_processed = 0
    for filename in filenames:
        parts = filename.split("/")
        cik, tfnm = parts[-2], parts[-1].split(".")[0]
        
        my_form10k = EdgarForm10K(
            cik, tfnm, 
            edgar_root_dir=edgar_root_dir, 
            formname="10-K", 
            user_agent=args.http_user_agent, 
            rmold=args.remove_legacy_files, 
            logger=logger
        )

        if run_download:
            if args.remove_legacy_files:
                my_form10k.get_raw_filing()
            else:
                # Triggers caching logic
                my_form10k.get_document_i(1)

        if run_build:
            try:
                save_path = os.path.join(edgar_root_dir, f"10k-bycik/{cik}/{tfnm}/body_df_{tfnm}.txt.gz")
                if not is_file_recent(save_path, 14):
                    my_form10k.form10k_get_items_df(overwrite_existing=True)
            except Exception as e:
                logger.exception(f"run_build_10k_formbody error for {tfnm}: {e}")

        total_processed += 1
        hours_used = (time.time() - req_started_at) / 3600
        logger.info(f"Processed {total_processed}/{len(filenames)}. Speed: {round(total_processed/(hours_used+1e-5), 1)}/hr")

def _worker_process_8k(row, edgar_root_dir, user_agent):
    """
    Worker function to process a single 8-K row.
    Must be top-level to be picklable by multiprocessing.
    """
    try:
        # FileName format: edgar/data/{cik}/{accession}.txt
        parts = row['FileName'].split('/')
        cik = parts[-2]
        tfnm = parts[-1].replace('.txt', '')
        
        # Initialize 8K Processor
        # We assume logger is configured globally or handled within the class
        processor = EdgarForm8K(
            fcik=cik,
            tfnm=tfnm,
            edgar_root_dir=edgar_root_dir,
            user_agent=user_agent
        )
        
        # Extract Data
        data = processor.process_filing()
        
        if data:
            data['conformed_period'] = row.get('DateFiled', '')
            return data
            
    except Exception as e:
        # Avoid passing logger objects across processes, just print specific errors if needed
        # or rely on the class's internal error handling
        print(f"Error processing {row.get('FileName', 'unknown')}: {e}")
        
    return None

def process_8k_filings(args, years, myei, edgar_root_dir):
    """
    Downloads and processes 8-K filings to extract metadata, 
    check for Earnings Announcements (Item 2.02), and check for Non-GAAP metrics.
    Output: Saves a CSV file with the analysis results.
    """
    if not args.process_8k_metadata:
        return

    # 1. Build Index for 8-Ks
    logger.info("Retrieving 8-K Index...")
    # Use CPU count or a fixed number (e.g., 10 or 20)
    # Note: parsing is CPU intensive, so don't exceed cpu_count() by much
    num_workers = min(10, multiprocessing.cpu_count())
        
    # This ensures we have the master index for the requested years
    for year in years:
        df_index = myei.get_index_df(years=[year], form_re="^8-?K", allow_download=True)
        
        if df_index.empty:
            logger.warning(f"No 8-K filings found for year {year}")
            return

        # Filter for testing if needed
        if args.test:
            logger.info("Test mode: Processing first 50 filings only.")
            df_index_test = df_index.loc[df_index.CompanyName.str.startswith("Apple Inc.")].reset_index(drop=True)
            files_to_process = df_index_test.to_dict('records')[-10:]
        else:
            files_to_process = df_index.to_dict('records')

        results = []
        logger.info(f"Processing {len(files_to_process)} 8-K filings...")
        
        # Define output filename based on years
        year_str = str(year)
        output_file_meta = os.path.join(edgar_root_dir, f"8k_metadata_{year_str}.csv.gz")
        output_file_eang = os.path.join(edgar_root_dir, f"8k_eangdata_{year_str}.csv.gz")

    
        # Prepare Multiprocessing
        num_files = len(files_to_process)
        

        
        results = []
        
        # Calculate the step size for 10% increments
        milestone_step = max(1, int(num_files / 10))
        processed_count = 0

        logger.info(f"Starting multiprocessing with {num_workers} workers on {num_files} files...")

        # Define the partial function to pass constant arguments
        worker_func = partial(_worker_process_8k, 
                            edgar_root_dir=edgar_root_dir, 
                            user_agent=args.http_user_agent)

        # Execute
        with multiprocessing.Pool(processes=num_workers) as pool:
            # imap_unordered is faster as it yields results as soon as they finish
            # chunksize helps performance by grouping tasks
            for result in pool.imap_unordered(worker_func, files_to_process, chunksize=10):
                processed_count += 1
                
                if result:
                    results.append(result)
                
                # Print Progress every 10%
                if processed_count % milestone_step == 0 or processed_count == num_files:
                    percent = int((processed_count / num_files) * 100)
                    logger.info(f"Progress (year {year_str}):  {percent}% ({processed_count}/{num_files})")

        # ... continue to save CSV ...
            
        # Processing Loop
        # for row in tqdm(files_to_process, desc="Analyzing 8-Ks"):
        #     try:
        #         # FileName format: edgar/data/{cik}/{accession}.txt
        #         parts = row['FileName'].split('/')
        #         cik = parts[-2]
        #         tfnm = parts[-1].replace('.txt', '')
                
        #         # Initialize 8K Processor
        #         # Note: We rely on the parent EdgarForm logic to handle local caching/downloading
        #         processor = EdgarForm8K(
        #             fcik=cik,
        #             tfnm=tfnm,
        #             edgar_root_dir=edgar_root_dir,
        #             logger=logger,
        #             user_agent=args.http_user_agent
        #         )
                
        #         # Extract Data
        #         data = processor.process_filing()
                
        #         if data:
        #             # Add index-level metadata for context
        #             data['conformed_period'] = row.get('DateFiled', '') # Fallback if header parsing fails
        #             results.append(data)
            
        #     except Exception as e:
        #         logger.error(f"Error processing {row.get('FileName', 'unknown')}: {e}")

        # Save Results
        if results:
            df_results = pd.DataFrame(results)
            
            # Clean item_name list for CSV storage (pipe separated)
            df_results['item_name'] = df_results['item_name'].apply(lambda x: "|".join(x) if isinstance(x, list) else str(x))
            
            # Save
            df_results[['fcik', 'tfnm', 'filing_date', 'item_name', 'is_earnings_announcement', 'has_non_gaap']].to_csv(output_file_meta, index=False, sep='|', compression='gzip')
            logger.info(f"8-K Processing complete. Saved {len(df_results)} rows to {output_file_meta}")
            df_ea = df_results.loc[df_results['is_earnings_announcement'] == 1].reset_index(drop=True)
            df_ea[['fcik', 'tfnm', 'filing_date', 'item_name', 'is_earnings_announcement', 'has_non_gaap', 'ea_sentence', 'ng_sentence']].to_csv(output_file_eang, index=False, sep='|', compression='gzip')
            logger.info(f"8-K Processing complete. Saved {len(df_ea)} rows to {output_file_eang}")
        else:
            logger.warning("No 8-K results generated.")

if __name__ == "__main__":
    #from edgarform import *
    parser = create_parser()
    args = parser.parse_args()
    setup_logging(args)
    
    edgar_root_dir = args.edgar_root_dir
    
    # 1. Build Index
    run_build_edgar_index(args, edgar_root_dir)
    
    # 2. Get Single Body DF (Exit after)
    run_get_bodydf(args)
    
    # 3. Determine Years
    years = get_years_to_process(args)
    
    # Initialize Shared Indexer
    myei = EdgarIndex(user_agent=args.http_user_agent, edgar_root_dir=edgar_root_dir, logger=logger)
    
    # 4. Run Tasks
    if args.process_8k_metadata:
        process_8k_filings(args, years, myei, edgar_root_dir)    
    elif args.get_note_10k_by_keyword:
        process_notes_by_keyword(args, years, edgar_root_dir, myei)
    elif args.get_xbrl_textblock_title:
        process_xbrl_textblock(args, years, myei, edgar_root_dir)
    elif args.downloadform:
        download_specific_forms(args, years, myei, edgar_root_dir)
    else:
        # Default fallback to 10K download/build
        process_10k_forms(args, years, myei, edgar_root_dir)


