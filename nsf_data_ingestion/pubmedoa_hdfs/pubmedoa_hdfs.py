import os
from glob import glob
from shutil import copyfile
import subprocess
import sys

def list_xml_path(path_dir):
    fullpath = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(path_dir)) for f in fn]
    path_list = [folder for folder in fullpath if os.path.splitext(folder)[-1] in ('.nxml', '.xml')]
    return path_list

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

if __name__ == '__main__':
    path_to_oa = 'pubmedoa' # path to all folder of pubmed oa
    path_chunk = 'pubmedoa_to_hdfs' # path to move chunk file in there
    subprocess.call(['mkdir', path_chunk])
    paths = list_xml_path(path_to_oa)
    n_chunk = 5000 # size of each chunk
    sample_paths_chunk = chunks(paths, n_chunk) # sample paths
    for i, sample_paths in enumerate(sample_paths_chunk):
        print("Chunk ", i)
        for p in glob(os.path.join(path_chunk , '*.nxml')):
            os.remove(p)
        for p in sample_paths:
            fname = p.split('/')[-1]
            copyfile(p, os.path.join(path_chunk, fname))
        subprocess.call(['tar', '-zcf', 'pubmedoa_to_hdfs.tar.gz', '-C', path_chunk, '.'])
        subprocess.call(['hdfs', 'dfs', '-put', 'pubmedoa_to_hdfs.tar.gz',
                        os.path.join(sys.argv[1], 'data/raw/pubmed_oa', str(i) + '.tar.gz')])
