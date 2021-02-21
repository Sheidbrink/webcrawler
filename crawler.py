import argparse
import bs4
import logging
import queue
import re
import requests
import sqlite3
import threading
import time
import urllib

from functools import partial

def create_table(db):
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS site (url, search, content)')
    conn.commit()
    conn.close()

def insert_into_paste_table(db, url, match):
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute('INSERT INTO site VALUES (?, ?, ?)', (url, str(match.re), match.string))
    conn.commit()
    conn.close()

def crawl_domain(startUrl, proc_site, db):
    marked_visited = set()

    to_visit   = queue.Queue()
    to_process = queue.Queue()

    crawler_t   = threading.Thread(target=crawler, args=(to_visit, to_process))
    crawler_t.start()

    to_visit.put(startUrl)
    marked_visited.add(startUrl)
    while True:
        try:
            res, soup = to_process.get(timeout=10)

            new_links    = get_links_in_soup(soup, res.url)
            domain_links = filter_domain_links(startUrl, new_links)

            matches = proc_site(res, soup)
            for match in matches:
                print(f'{res.url} matches')
                insert_into_paste_table(db, res.url, match)

            for link in domain_links:
                if link not in marked_visited:
                    marked_visited.add(link)
                    to_visit.put(link)
        except queue.Empty:
            print('No more sites to process')
            break


def crawler(to_visit, to_process):
    while True:
        try:
            url = to_visit.get(timeout=10)
            print(f'[*] Downloading {url}: {to_visit.qsize()} remaining')
            res, soup = get_site(url)
            to_process.put((res,soup))
            to_visit.task_done()
            time.sleep(1)
        except queue.Empty:
            print('No more sites to crawl.')
            break


def get_site(url):
    res = requests.get(url)
    soup = bs4.BeautifulSoup(res.text, features='html.parser')
    return res, soup


def get_links_in_soup(soup, base_url):
    links = soup.select('a[href]')
    links = [link.get('href') for link in links]
    # urljoin shouldn't overwrite anything if link is already absolute url
    links = [urllib.parse.urljoin(base_url, link) for link in links]
    return links


def filter_domain_links(filter_url, urls):
    domain = urllib.parse.urlparse(filter_url)
    for url in urls:
        urldomain = urllib.parse.urlparse(url)
        if domain.netloc in urldomain.netloc or\
           urldomain.netloc in domain.netloc:
            yield url


def proc_site(res, soup, regexes=[]):
    return search_for_text(res.text, regexes)


def search_for_text(text, regexes):
    matches = []
    for regex in regexes:
        match = re.search(regex, text, flags=re.IGNORECASE)
        if match is not None:
            matches.append(match)
    return matches


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Crawl website.')
    parser.add_argument('url', metavar='<website>', type=str, help='Site to start on.')
    #  parser.add_argument('--urls', type=str, help='Filename of sites to crawl.')
    parser.add_argument('--db', type=str, default='matches.db', help='Database name to use.')
    parser.add_argument('--match', metavar='FILE', type=str, required=True, help='Filename of regexes to match.')
    args = parser.parse_args()

    with open(args.match, 'r') as f:
        regexes = f.read().splitlines()

    create_table(args.db)
    crawl_domain(args.url, partial(proc_site, regexes=regexes), args.db)
