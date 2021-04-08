#!/usr/bin/env python
# coding: utf-8

import subprocess, os, json
import logging
if __name__ == '__main__':

    """
    the index is named "kimun"
    current settings are based on a temporary index named 'kimun_version4'
    settings are saved at `setting_path`
    make sure the mappings are compatible with the data to be uploaded
    """

    setting_path = "/home/eileen/nsf_data_ingestion/nsf_data_ingestion/kimun_loader/settings.json"
    with open(setting_path, "r") as fp:
        settings = "'" + fp.read() + "'"  # reading index settings
    header = "'Content-Type: application/json'"
    command = 'curl -XPUT "http://128.230.247.186:9201/kimun_jim" -H ' + header + ' -d '+ settings

    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()

    logging.info("Create index result : \n",out)
    print("Create index result : \n",out)
    logging.info("\n\n\nCreate index error : \n",err)
    print("\n\n\nCreate index error : \n",err)
