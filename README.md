## How to access the SOS cluster

[Documentation](https://github.com/sciosci/nsf_data_ingestion/wiki/Accessing-and-running-jobs-in-the-SOS-cluster)

## Data ingestion scripts for NSF EAGER grant

Scripts for retrieving, cleaning, and creating data from multiple repositories.

The structure of the repository should be as follows:

```
/nsf_data_ingestion
 /nber
 /arxiv
 /medline
```

```
# Package Structure - 
/nsf_data_ingestion
  # Airflow Package 
  /airflow
  # Source Systems - Each System consists of Download and Parquet Write Process Scripts
  /arxiv
  /medline
  /federal_reporter
  /pubmed
  # Config - Config Files
  /config
    /nsf_config
    /spark_config
  # Objects - Params particular to Source System
  /objects
    /data_source_params
  # Utils - Common Functions and Utilities
  /utils
    /utils_functions
  # Models - Models used by recommendation System (Aggregation)
  /models
    /tfdif_aggregate  
 ```
 Additional Systems can be added by including parameters in config file such as paths, urls etc....
 Include setters in data__source_params to reflect these config parameters.
 Access data_source_params from the scripts by passing source system as argument.
  
 # Airflow
  Airflow folder conatins dags for scheduling these processes.
  Include Tasks or Seperate Dags.

 # Libraries Folder
  The libraries folder contains libraries that are required to be passed to the worker nodes.
   
  
  
 
  
