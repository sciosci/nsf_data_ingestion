# NBER

Run following script to download example 4k papers from NBER. The full version is
shown below.

```bash
python download_nber.py
```

We also put Pandas pickle file on AWS S3 (saved using Python 3). This contains around
20k papers and 10k authors.

```bash
wget https://s3-us-west-2.amazonaws.com/science-of-science-bucket/nber/nber_article_details_full.pickle
wget https://s3-us-west-2.amazonaws.com/science-of-science-bucket/nber/nber_authors.pickle
```

## Script

Scrape all 20k NBER papers (a little hacky way). Simply grab function from
`download_nber.py` script

```python
article_details = list()
for i in range(22792):
    url = 'http://www.nber.org/papers/w%i' % i
    try:
        article_details.append(parse_article_details(url))
    except:
        print(url)
df = pd.DataFrame(article_details)
df.to_pickle('nber_article_details_full.pickle')
```

Here if you download pickle from AWS S3, run the following to parse author information

```python
df = pd.read_pickle('nber_article_details_full.pickle')
df_author = pd.DataFrame(np.vstack([a for a in list(df.authors) if len(a) > 0]), columns=['name', 'url'])
df_author.drop_duplicates(subset='url', keep='last', inplace=True)
authors = list()
for url in list(df_author.url):
    try:
        authors.append(parse_author_details(url))
    except:
        print(url)
author_df = pd.DataFrame(authors)
author_df.to_pickle('nber_authors.pickle')
```
