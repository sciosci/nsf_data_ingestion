import xml.etree.ElementTree as ET
import findspark
import os
import sys
findspark.init('/opt/spark-2.1.0-bin-cdh5.9.1/')
from pyspark.sql import SparkSession

def process(file):
	prefix = "{http://www.openarchives.org/OAI/2.0/}"
	prefix2 = "{http://purl.org/dc/elements/1.1/}"

	root = ET.fromstring(file)
	records = root.find(prefix + "ListRecords")
	data = []
	for record in records.findall(prefix + "record"):
		identifier = record[0][0].text
		datastamp = record[0][1].text
		subjects = []
		for subject in record[0].findall(prefix + "setSpec"):
			subjects.append(subject.text)
		if len(subjects) == 0:
			subjects.append("none")

		dc = record[1][0]
		title = dc[0].text
		authors = []
		for author in dc.findall(prefix2 + "creator"):
			authors.append(author.text)
		abstract = dc.find(prefix2 + "description").text

		d = {"id":identifier, "datastamp":datastamp, "title":title, "subjects":subjects, "authors":authors, "abstract":abstract}
		data.append(d)
	return data

if __name__ == '__main__':
  xmlpath = sys.argv[1]
  parpath = sys.argv[2]
	spark = SparkSession.builder.getOrCreate()
	sc = spark.sparkContext
  filep = os.path.join(xmlpath, "papers_*.xml")
	xmlfiles = sc.binaryFiles(filep)
	xmldf = xmlfiles.flatMap(process).toDF()
	xmldf.write.parquet(parpath)
