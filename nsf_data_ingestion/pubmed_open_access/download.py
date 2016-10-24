# Script to download the Data from Pubmed Open Access
import urllib
urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/comm_use.A-B.xml.tar.gz", filename="/users/kanagre/pubmed_data/comm_use.A-B.xml.tar.gz")
urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/comm_use.C-H.xml.tar.gz", filename="/users/kanagre/pubmed_data/comm_use.C-H.xml.tar.gz")
urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/comm_use.I-N.xml.tar.gz", filename="/users/kanagre/pubmed_data/comm_use.I-N.xml.tar.gz")
urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/comm_use.O-Z.xml.tar.gz", filename="/users/kanagre/pubmed_data/comm_use.O-Z.xml.tar.gz")
urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/non_comm_use.A-B.xml.tar.gz", filename="/users/kanagre/pubmed_data/non_comm_use.A-B.xml.tar.gz")
urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/non_comm_use.C-H.xml.tar.gz", filename="/users/kanagre/pubmed_data/non_comm_use.C-H.xml.tar.gz")
urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/non_comm_use.I-N.xml.tar.gz", filename="/users/kanagre/pubmed_data/non_comm_use.I-N.xml.tar.gz")
urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/non_comm_use.O-Z.xml.tar.gz", filename="/users/kanagre/pubmed_data/non_comm_use.O-Z.xml.tar.gz")
