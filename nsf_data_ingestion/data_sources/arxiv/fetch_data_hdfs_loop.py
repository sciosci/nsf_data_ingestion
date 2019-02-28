from datetime import datetime
import urllib.request
import time
from subprocess import call
import sys
import os

retry_time = 45


# save_path = ""

def get_raw_data(path):
    start = time.clock()
    query = "http://export.arxiv.org/oai2?verb=ListRecords&metadataPrefix=oai_dc"
    print("request: %s" % (query))
    request = urllib.request.Request(query)
    response = urllib.request.urlopen(request).read().decode('utf-8')
    rawfile = open('papers_0.xml', 'w')
    rawfile.write(response)
    rawfile.close()

    save_to_hdfs("papers_0.xml", path)

    end = time.clock()
    print("takes: %f s" % (end - start))

    pos_start = response.rfind('<resumptionToken')
    pos_end = response.rfind('</resumptionToken')
    if pos_end > 0 and pos_end > pos_start:
        pos = response.rfind('>', pos_start, pos_end)
        resume_token = response[pos + 1:pos_end]
        print("request_resume: %s" % (resume_token))
        get_resume(resume_token, path)


def get_resume(token, path):
    repeat = 0
    time.sleep(retry_time)
    start = time.clock()
    filen = 'papers_%s.xml' % (token.replace('|', '_'))
    rawfile = open(filen, 'w')
    try:
        query = "http://export.arxiv.org/oai2?verb=ListRecords&resumptionToken=%s" % (token)
        request = urllib.request.Request(query)
        response = urllib.request.urlopen(request).read().decode('utf-8')
        rawfile.write(response)
        rawfile.close()
        save_to_hdfs(filen, path)

        end = time.clock()
        print("takes: %f s" % (end - start))

        pos_start = response.rfind('<resumptionToken')
        pos_end = response.rfind('</resumptionToken')
        if pos_end > 0 and pos_end > pos_start and repeat < 20:
            pos = response.rfind('>', pos_start, pos_end)
            resume_token = response[pos + 1:pos_end]
            print("request_resume: %s" % (resume_token))
            get_resume(resume_token, path)
            repeat += 1
    except Exception as err:
        print(err)
        print("retry resume_token: %s" % (token))
        time.sleep(30)
        get_resume(token)


def save_to_hdfs(filename, path):
    print(">>>>>>>", path)
    file = os.path.join(path, filename)
    try:
        out = call("hdfs dfs -test -e %s" % (file), shell=True)
        if out == 0:
            call("hdfs dfs -rm %s" % (file), shell=True)
            print("file %s exists, delete old one" % (file))
        call(['hdfs', 'dfs', '-put', filename, path])
        print("send %s to hdfs" % (filename))
    except Exception as e:
        print("save %s to hdfs failed" % (filename))


if __name__ == '__main__':
    get_raw_data(path=sys.argv[1])
