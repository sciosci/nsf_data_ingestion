import sys
import pandas as pd
from bs4 import BeautifulSoup
if sys.version_info[0] == 3:
    from urllib.request import urlretrieve, urlopen
else:
    from urllib import urlretrieve, urlopen


def parse_new_archive_links(url='http://papers.nber.org/new_archive/'):
    """
    Parse all links from http://papers.nber.org/new_archive/
    """
    page = urlopen(url).read()
    soup = BeautifulSoup(page, 'html.parser')
    soup_sel = soup.find('td', attrs={'id' : 'mainContentTd'})

    nber_links = list()
    for link in soup_sel.findAll('p')[:-4]:
        if link.a is not None:
            if not ('/' in link.a['href']):
                nber_links.append(url + link.a['href'])
            else:
                nber_links.append(link.a['href'])
    return nber_links

def parse_paper_links(url='http://papers.nber.org/new_archive/'):
    """
    Parse and return article links under http://papers.nber.org/new_archive/
    """
    nber_links = parse_new_archive_links(url)
    paper_links = list()
    program_links = dict()

    for nber_link in nber_links[0:10]:
        page_release = urlopen(nber_link).read()
        soup = BeautifulSoup(page_release, 'html.parser')
        soup_sel = soup.find('td', attrs={'id' : 'mainContentTd'})

        for link in soup_sel.findAll('a'):
            if link.has_attr('href') or link.has_attr('vhref'):
                if link.has_attr('href'):
                    v = link['href']
                elif link.has_attr('vhref'):
                    v = link['vhref']
                k = link.text

                if (not 'http://' in v) and (not 'mailto' in v):
                    paper_links.append([k, 'http://www.nber.org' + v])
                elif len(k) >= 4:
                    paper_links.append([k, v])
                else:
                    if k not in program_links.keys():
                        program_links[k] = v # available at http://www.nber.org/papersbyprog/
    return paper_links

def parse_article_details(url):
    """
    Give url e.g. 'http://www.nber.org/papers/w22766'
    parse details and give the dictionary output

    To Do: add function to prevent if providing wrong url
    """
    page = urlopen(url).read()
    soup = BeautifulSoup(page, 'html.parser')
    soup_sel = soup.find('td', attrs={'id' : 'mainContentTd'})

    if soup_sel is not None:
        title = soup_sel.find('h1', attrs={'class':'title'}).text
        names = soup_sel.find('h2', attrs={'class':'bibtop'})

        authors = list()
        try:
            for name in names.findAll('a'):
                author_name = name.text
                author_link = name['href']
                authors.append([author_name, 'http://www.nber.org' + author_link])
        except:
            authors = None

        abstract = soup_sel.findAll('p')[1].text.replace('\n', ' ').strip()

        pdf = soup_sel.find('table', attrs={'class': 'bibformatsicons'})
        if pdf is not None:
            pdf_link = pdf.find('p').a['href']
        else:
            pdf_link = ''
    else:
        title = ''
        abstract = ''
        authors = None
        pdf_link = ''

    dict_out = {'title': title,
                'abstract': abstract,
                'authors': authors,
                'pdf': pdf_link,
                'url': url}

    return dict_out

def parse_author_details(url):
    """
    Parse details of given author url link
    example link: http://www.nber.org/people/e._kathleen_adams
    """
    page = urlopen(url).read()
    soup = BeautifulSoup(page, 'html.parser')
    soup_sel = soup.find('td', attrs={'id' : 'mainContentTd'})
    name = soup_sel.find('h1').find('span', attrs={'itemprop': 'name'}).text or ''
    address = soup_sel.find('span', attrs={'itemprop': 'address'}).text or ''
    working_paper = 'http://www.nber.org/authors_papers' + soup_sel.find('a')['href']

    dict_author = {'name': name,
                   'url': url,
                   'address': address,
                   'working_paper': working_paper}
    return dict_author

if __name__ == '__main__':
    paper_links = parse_paper_links(url='http://papers.nber.org/new_archive/')
    paper_detail_links = pd.unique(list(map(lambda x: x[1], paper_links)))

    article_details = list()
    for p in paper_detail_links:
        try:
            article_details.append(parse_article_details(p))
        except:
            print(p)
    df = pd.DateFrame(article_details)
    df.to_pickle('nber_article_details.pickle')
