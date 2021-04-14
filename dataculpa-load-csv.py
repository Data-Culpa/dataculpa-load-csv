#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# dataculpa-load-csv.py
# Data Culpa CSV Loader 
#
# Copyright (c) 2020-2021 Data Culpa, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to 
# deal in the Software without restriction, including without limitation the 
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS 
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
# DEALINGS IN THE SOFTWARE.
#

import argparse
import dotenv
import json
import logging
import os
import uuid
import sqlite3
import sys

from dateutil.parser import parse as DateUtilParse

from dataculpa import DataCulpaValidator

for logger_name in ['urllib3']:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.WARN)


class Config:
    def __init__(self):
        # Data Culpa parameters
        self.pipeline_name      = os.environ.get('DC_PIPELINE_NAME')
        self.pipeline_env       = os.environ.get('DC_PIPELINE_ENV', 'default')
        self.pipeline_stage     = os.environ.get('DC_PIPELINE_STAGE', 'default')
        self.pipeline_version   = os.environ.get('DC_PIPELINE_VERSION', 'default')
        self.dc_host            = os.environ.get('DC_HOST')
        self.dc_port            = os.environ.get('DC_PORT')
        self.dc_protocol        = os.environ.get('DC_PROTOCOL')
        self.dc_secret          = os.environ.get('DC_SECRET')


def NewDataCulpaHandle(pipeline_stage=None):
    if pipeline_stage is None:
        pipeline_stage = gConfig.pipeline_stage

    dc = DataCulpaValidator(gConfig.pipeline_name,
                            pipeline_environment=gConfig.pipeline_env,
                            pipeline_stage=pipeline_stage,
                            pipeline_version=gConfig.pipeline_version,
                            protocol=gConfig.dc_protocol, 
                            dc_host=gConfig.dc_host, 
                            dc_port=gConfig.dc_port)

    return dc

gConfig = None

def main():

    ap = argparse.ArgumentParser()
    ap.add_argument("-e", "--env",  help="Use provided env file instead of default .env")
    ap.add_argument("-f", "--csv",  help="CSV file to load.")
    ap.add_argument("-m", "--meta", help="Metadata structure in JSON to store with the data.")

    args = ap.parse_args()

    env_path = ".env"
    if args.env:
        env_path = args.env
    if not os.path.exists(env_path):
        sys.stderr.write("Error: missing env file at %s\n" % env_path)
        os._exit(1)
        return
    # endif

    if not args.csv:
        sys.stderr.write("Error: no csv file specified to load.");
        os._exit(2)
        return
    # endif

    dotenv.load_dotenv(env_path)
    
    d = os.environ
    for k, v in d.items():
        if k.startswith("DC_"):
            print("%20s -> %s" % (k, v))

    global gConfig
    gConfig = Config()

    meta_decode = None
    if args.meta:
        meta_decode = json.loads(args.meta)
    # endif

    dc = NewDataCulpaHandle()
    
    if meta_decode is not None:
        dc.queue_metadata(meta_decode)
    # endif

    worked = dc.load_csv_file(args.csv)
    if not worked:
        sys.stderr.write("worked: %s\n" % worked)
        os._exit(2)

    dc.queue_commit()
    if dc.has_errors():
        sys.stderr.write("Errors while processing data:\n")
        for e in dc.get_errors():
            sys.stderr.write("%s\n" % e)
        os._exit(3)
    # endif

    return


if __name__ == "__main__":
    main()

