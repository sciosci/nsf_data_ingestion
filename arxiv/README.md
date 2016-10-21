
To download the arxiv data, run download.py
You can specify how many papers you want by passing the value in the query function.
E.g: query(start_index=0, max_index=10000) , this will fetch the first 10000 values. 

process.py converts the downloaded dataframe in arxiv.py which is in a dataframe (pandas) to parquet.
