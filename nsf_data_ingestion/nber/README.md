# NBER

Run following script to download around 7k papers from NBER.
You can also run the commented script to scrape all the links.

```bash
python download_nber.py
```

We also put Pandas pickle file on S3 (with Python 3).

```
wget https://s3-us-west-2.amazonaws.com/science-of-science-bucket/nber/nber_article_details_full.pickle
```
