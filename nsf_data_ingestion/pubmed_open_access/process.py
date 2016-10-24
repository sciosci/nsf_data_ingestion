# Script to Scrapp the data Pubmed Open Access in to .csv files

import csv
import xml.etree.ElementTree as ET
import os
import pubmed_parser as pp
import random
import string
import json
import uuid
import glob

#directory_path = '/Users/kartik/Documents/NSF_GRANT_PROJECT/bulk_data/non_comm_use.A-B.xml/3_Biotech'
#directory_path = '/Users/kartik/Documents/NSF_GRANT_PROJECT/bulk_data/'
directory_path = '/users/kanagre'

def randomword(length):
   return ''.join(random.choice(string.lowercase) for i in range(length))

def main():
    print "This is main"
    #file_name = "sample.xml"
    #for filename in glob.glob(os.path.join(directory_path, '*.nxml')):
        #dict_out = pp.parse_pubmed_xml(filename)
        #xml_json = json.dumps(dict_out, ensure_ascii=False)
        #document_info = parse_document(dict_out)
        #parse_scientist(dict_out, document_info)

    for subdir, dirs, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.nxml'):
                print(file)
                filename = os.path.join(subdir, file)
                dict_out = pp.parse_pubmed_xml(filename)
                xml_json = json.dumps(dict_out, ensure_ascii=False)
                document_info = parse_document(dict_out)
                parse_scientist(dict_out, document_info)

def parse_document(dict_out):
    document_csv = open("document.csv", 'a')
    if os.stat("document.csv").st_size == 0:
        writer = csv.writer(document_csv)
        writer.writerow(["id", "title", "summary", "year", "pubmed_id", "journal", "pubmed_central_id"])

    id = uuid.uuid1()
    summary = dict_out['abstract'].encode('utf-8').strip()
    title = dict_out['full_title'].encode('utf-8').strip()
    year = dict_out['publication_year'].encode('utf-8').strip()
    pubmed_id = dict_out['pmid'].encode('utf-8').strip()
    journal = dict_out['journal'].encode('utf-8').strip()
    pubmed_central_id = dict_out['pmc'].encode('utf-8').strip()

    writer = csv.writer(document_csv)
    writer.writerow([id, title, summary, year, pubmed_id, journal, pubmed_central_id])
    document_csv.close()
    document_info = {"document_id": id, "document_pubmed_id": pubmed_id, "document_pubmed_central_id": pubmed_central_id}
    return document_info

def parse_scientist(dict_out, document_info):
    document_id = document_info['document_id']
    document_pubmed_id = document_info['document_pubmed_id']
    document_pubmed_central_id = document_info['document_pubmed_central_id']
    affiliation_array = []

    scientist_csv = open("scientist.csv", 'a')
    organization_csv = open("organization.csv", 'a')
    scientist_organization_csv = open("scientist_organization.csv", 'a')

    if os.stat("scientist.csv").st_size == 0:
        scientist_writer = csv.writer(scientist_csv)
        scientist_writer.writerow(["id", "first_name", "last_name", "document_id", "document_pubmed_id", "document_pubmed_central_id"])

    if os.stat("organization.csv").st_size == 0:
        organization_writer = csv.writer(organization_csv)
        organization_writer.writerow(["id", "name"])

    if os.stat("scientist_organization.csv").st_size == 0:
        scientist_organization_writer = csv.writer(scientist_organization_csv)
        scientist_organization_writer.writerow(["scientist_id", "organization_id"])

    affiliation_list = dict_out['affiliation_list']
    for affiliation in affiliation_list:
        if affiliation[0] is not None:
            affiliation_name = affiliation[0].encode('utf-8').strip()
        if affiliation[1] is not None:
            affiliation_organization = affiliation[1].encode('utf-8').strip()
        affiliation_organization_id = uuid.uuid1()
        organization_writer = csv.writer(organization_csv)
        organization_writer.writerow([affiliation_organization_id, affiliation_organization])
        affiliation_object = {"organization_id": affiliation_organization_id, "affiliation_name": affiliation_name, "organization": affiliation_organization}
        affiliation_array.append(affiliation_object)
    #print affiliation_array
    #print "-----"

    affiliation_list = dict_out['affiliation_list']
    #print affiliation_list
    author_list = dict_out['author_list']
    #print author_list

    for author in dict_out['author_list']:
        scientist_writer = csv.writer(scientist_csv)
        scientist_id = uuid.uuid1()

        if author[1] is not None:
            first_name = author[1].encode('utf-8').strip()
        if author[0] is not None:
            last_name = author[0].encode('utf-8').strip()
        if author[2] is not None:
            author_affiliation = author[2]
        scientist_writer.writerow([scientist_id, first_name, last_name, document_id, document_pubmed_id, document_pubmed_central_id])

        for affiliation in affiliation_array:
            if affiliation['affiliation_name'] is not None:
                if affiliation['affiliation_name'] == author_affiliation:
                    scientist_organization_writer = csv.writer(scientist_organization_csv)
                    scientist_organization_writer.writerow([scientist_id, affiliation['organization_id']])

    scientist_csv.close()
    organization_csv.close()
    scientist_organization_csv.close()

main()
