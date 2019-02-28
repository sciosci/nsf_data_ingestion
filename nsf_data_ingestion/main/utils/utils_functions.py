import os
import shutil
import subprocess
from nsf_data_ingestion.main.config.config import source_path


def get_archive_file_list():
    list1 = ['comm_use.A-B.xml.tar.gz','comm_use.C-H.xml.tar.gz','comm_use.I-N.xml.tar.gz','comm_use.O-     Z.xml.tar.gz','non_comm_use.A-B.xml.tar.gz','non_comm_use.C-H.xml.tar.gz','non_comm_use.I-N.xml.tar.gz','non_comm_use.O-Z.xml.tar.gz']
    list = ['non_comm_use.O-Z.xml.tar.gz']
    return list



def pull():
    os.chdir(source_path)
    output = subprocess.check_output(["git", "pull", "origin", "airflow_model"])
print(os.curdir)