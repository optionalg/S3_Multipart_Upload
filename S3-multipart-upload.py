#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author : Jash Lee
# May 19, 2015

import boto
import sys
import os
import math
import logging
import psutil
import signal
from filechunkio import FileChunkIO
from datetime import datetime
from multiprocessing import Pool

"""
Always multipart upload, must run as root

Requirements:
 - boto v2.38.0
 - filechunkio 1.6
 - psutil 2.2.1

/etc/boto.cfg - for site-wide settings that all users on this machine will use
~/.boto       - for user-specific settings

Usage : python S3-multipart-upload.py xxxxx xxxxx/xxxxx/ /logs gz 5

"""

# Global variables
LOG_FILENAME = '/var/log/S3-multipart-upload.log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO)
try:
    BUCKET_NAME = sys.argv[1]
    SUB_FOLDER = sys.argv[2]
    SOURCE_PATH = sys.argv[3]
    FILE_TYPE = sys.argv[4]
    WORKERS = sys.argv[5] # Default = 4
except:
    pass

def readPidContent(pidfile):
    with file(pidfile) as f:
        pid = f.read()
        if psutil.pid_exists(int(pid)): # Check if the process/pid in pidfile is actually running
            return True
        else:
            return False

def allowOneInstance():  # Make sure only one instance running
    pid = str(os.getpid()) # Get current pid
    base_name = os.path.basename(__file__) # Get the script name (S3-multipart-upload.py) or whatever
    pidfile = "/tmp/{0}.pid".format(base_name) # Create pid/lock file
    if (os.path.isfile(pidfile) and readPidContent(pidfile)): # If pidfile exist and pid actually running
        msg = "{0} {1} already exists, exiting".format(str(datetime.now()), pidfile)
        logging.error(msg)
        sys.exit()
    else:
        file(pidfile, 'w').write(pid) # Otherwise create pidfile with pid number
        return pidfile

def releasePidfile(pidfile): # Release pidfile && exit program
    os.unlink(pidfile)
    sys.exit()

def validArguments():  # Valid arguments
    if len(sys.argv) < 5:
        msg = "{0} Usage : {1} bucket sub_folder source_path file_type workers".format(str(datetime.now()), sys.argv[0])
        logging.error(msg)
        sys.exit()

def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def _upload_part(bucket, multipart_id, part_num, source_file, offset, bytes, pidfile, amount_of_retries=10):
    """
    Uploads a part with retries. retry retry retry ...
    """
    def _upload(retries_left=amount_of_retries):
        try:
            conn = boto.connect_s3()  # Connect to S3
            try:
                s3Uploader = conn.get_bucket(bucket)
            except Exception as e:
                msg = "{0} Can't not reach S3 bucket : {1}".format(str(datetime.now()), bucket)
                logging.info(msg)
                releasePidfile(pidfile)
                sys.exit()

            msg = "{0} Start uploading part #{1} ...".format(str(datetime.now()), part_num)
            logging.info(msg)

            """
            Send the file parts, using FileChunkIO to create a file-like object
            that points to a certain byte range within the original file. We
            set bytes to never exceed the original file size.
            """
            for mp in s3Uploader.get_all_multipart_uploads():
                if mp.id == multipart_id:
                    with FileChunkIO(source_file, 'r', offset=offset,
                        bytes=bytes) as fp:
                        mp.upload_part_from_file(fp=fp, part_num=part_num)
                    break
        except Exception, exc:
            if retries_left:
                _upload(retries_left = retries_left - 1) # Recursive function fuck ya
            else:
                msg = "{0} YOU FUCKED UP ... Failed uploading part #{1} ...".format(str(datetime.now()), part_num)
                logging.info(msg)
                raise exc
        else:
            msg = "{0} ... Uploaded part #{1} ...".format(str(datetime.now()), part_num)
            logging.info(msg)

    _upload()

def s3MultipartUpload(bucket, sub_folder, source_path, file_type, pidfile, workers):
    conn = boto.connect_s3()  # Connect to S3
    try:
        s3Uploader = conn.get_bucket(bucket)
    except Exception, exc:
        msg = "{0} Can't not reach S3 bucket : {1}".format(str(datetime.now()), bucket)
        logging.info(msg)
        releasePidfile(pidfile)
        sys.exit()

    # Find all files in source_path directory with extension file_type
    file_list = []
    for file in os.listdir(source_path):
        if file.endswith(".{0}".format(file_type)):
            file_list.append(file)

    if not file_list:  # If no files to upload empty list
        msg = "{0} No files to upload, exit program".format(str(datetime.now()))
        logging.info(msg)
        releasePidfile(pidfile)
        sys.exit()

    for file in file_list:
        source_file = str(source_path) + str(file) # Get file path + name
        source_size = os.stat(source_file).st_size # Get file info
        s3Key = str(sub_folder) + str(file)  # Create s3Key name for file
        mp = s3Uploader.initiate_multipart_upload(s3Key) # Create a multipart upload request
        chunk_size = 52428800  # Use a chunk size of 50 MiB (feel free to change this)
        chunk_count = int(math.ceil(source_size / float(chunk_size)))
        msg = "{0} Uploading {1} to {2}/{3} now ...".format(str(datetime.now()), source_file, bucket, sub_folder)
        logging.info(msg)
        pool = Pool(workers, init_worker)  # Multi-Processing uploading
        for i in range(chunk_count):
            offset = chunk_size * i
            remaining_bytes = source_size - offset
            bytes = min(chunk_size, remaining_bytes)
            part_num = i + 1
            pool.apply_async(_upload_part, [bucket, mp.id, part_num, source_file, offset, bytes, pidfile])
        try:
            pass
        except KeyboardInterrupt:
            msg = "{0} Caught KeyboardInterrupt, terminating workers".format(str(datetime.now()))
            logging.info(msg)
            pool.terminate()
            pool.join()
            mp.cancel_upload()
            releasePidfile(pidfile)
            sys.exit()
        else:
            pool.close()
            pool.join() # Finish multi-thread

        if len(mp.get_all_parts()) == chunk_count:
            """
            Complete the MultiPart Upload operation.  This method should
            be called when all parts of the file have been successfully
            uploaded to S3.
            :rtype: :class:`boto.s3.multipart.CompletedMultiPartUpload`
            :returns: An object representing the completed upload.
            """
            msg = "{0} Upload {1} Successfully".format(str(datetime.now()), source_file)
            logging.info(msg)
            mp.complete_upload() # Finish the upload
            os.remove(source_file) # Remove uploaded file
        else:
            """
            Cancels a MultiPart Upload operation.  The storage consumed by
            any previously uploaded parts will be freed. However, if any
            part uploads are currently in progress, those part uploads
            might or might not succeed. As a result, it might be necessary
            to abort a given multipart upload multiple times in order to
            completely free all storage consumed by all parts.
            """
            mp.cancel_upload() # Clean up shits, because uploading fucked up
            msg = "{0} Uploading {1} failed, exit program".format(str(datetime.now()), source_file)
            logging.info(msg)
            sys.exit()

if __name__ == "__main__":
    # Show Time
    pidfile = allowOneInstance()
    validArguments()

    # Do some actual work here
    if len(sys.argv) == 5:
        WORKERS = 4

    s3MultipartUpload(BUCKET_NAME, SUB_FOLDER, SOURCE_PATH, FILE_TYPE, pidfile, WORKERS)

    # Job Done
    releasePidfile(pidfile)
