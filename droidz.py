import argparse
import bs4
import datetime
import os
import re
import requests
import sqlite3
import subprocess
import sys
import types

from voussoirkit import pathclass
from voussoirkit import ratelimiter
from voussoirkit import sqlhelpers
from voussoirkit import winwhich

CATEGORIES = [
    'stickmen',
    'stickpacks',
    'vehicles',
    'weapons',
    'objects',
    'random',
    'effects',
    'backgrounds',
]

DB_INIT = '''
BEGIN;
CREATE TABLE IF NOT EXISTS sticks(
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT,
    description TEXT,
    date INT,
    author TEXT,
    download_link,
    category TEXT,
    downloads INT,
    version TEXT,
    vote_score INT,
    usage_rating TEXT,
    retrieved INT
);
CREATE INDEX IF NOT EXISTS index_sticks_id ON sticks(id);
COMMIT;
'''

SQL_COLUMNS = sqlhelpers.extract_table_column_map(DB_INIT)

sql = sqlite3.connect('sticks.db')
sql.executescript(DB_INIT)

USERAGENT = '''Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)
Chrome/79.0.3945.130 Safari/537.36'''.replace('\n', ' ').strip()

HEADERS = {
    'User-Agent': USERAGENT
}

REQUEST_RATELIMITER = ratelimiter.Ratelimiter(allowance=1, period=1)
DOWNLOAD_RATELIMITER = ratelimiter.Ratelimiter(allowance=1, period=5)

WINRAR = winwhich.which('winrar')

def get_now():
    return datetime.datetime.now(datetime.timezone.utc).timestamp()

def request(url):
    REQUEST_RATELIMITER.limit()
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response

def id_from_direct_url(direct_url):
    id = direct_url.split('/direct/')[-1]
    id = id.split('/')[0].split('?')[0]
    return id

def insert_id(id):
    cur = sql.cursor()
    cur.execute('SELECT 1 FROM sticks WHERE id == ?', [id])
    existing = cur.fetchone()
    if not existing:
        data = {'id': id}
        columns = SQL_COLUMNS['sticks']
        (qmarks, bindings) = sqlhelpers.insert_filler(columns, data, require_all=False)

        query = f'INSERT INTO sticks VALUES({qmarks})'
        cur.execute(query, bindings)

    status = types.SimpleNamespace(id=id, is_new=not existing)
    return status

# SCRAPE
################################################################################
def scrape_direct(id):
    url = f'http://droidz.org/direct/{id}'
    print(url)
    response = request(url)
    text = response.text

    # I had a weird issue where some brs were not self-closing and they
    # contained a bunch of other elements. This whitespace replacement fixed
    # the issue but I didn't quite understand why.
    text = re.sub(r'<\s*br\s*/\s*>', '<br/>', text)
    soup = bs4.BeautifulSoup(text, 'html.parser')

    for br in soup.find_all('br'):
        br.replace_with('\n')

    stick_info = soup.select('.content')[1].get_text()
    author = soup.find('a', href=re.compile(r'search\.php\?searchq=')).get_text()
    vote_score = int(re.search(r'Vote Score: ([-\d]+)\s*$', stick_info, flags=re.M).group(1))
    downloads = int(re.search(r'Downloads: (\d+)\s*$', stick_info, flags=re.M).group(1))
    category = re.search(r'Category: (.+?)\s*$', stick_info, flags=re.M).group(1)
    version = re.search(r'Version: (.+?)\s*$', stick_info, flags=re.M).group(1)
    usage_rating = re.search(r'Usage Rating: (.+?)\s*$', stick_info, flags=re.M).group(1)
    date = re.search(r'Date Submitted: (.+?)\s*$', stick_info, flags=re.M).group(1)
    date = datetime.datetime.strptime(date, '%B %d, %Y')
    date = date.timestamp()

    name = soup.select_one('.section .top h2').get_text().strip()
    description = soup.select_one('.section .content').get_text().strip()
    if description == f'{author}, has left no comments for this submission.':
        description = None
    else:
        description = description.replace(f'{author} says, ', '')
    download_link = soup.find('a', href=re.compile(r'/resources/grab\.php\?file='))['href']
    retrieved = int(get_now())

    data = {
        'id': id,
        'name': name,
        'description': description,
        'date': date,
        'author': author,
        'download_link': download_link,
        'category': category,
        'downloads': downloads,
        'version': version,
        'vote_score': vote_score,
        'usage_rating': usage_rating,
        'retrieved': retrieved,
    }

    insert_id(id)

    cur = sql.cursor()
    (qmarks, bindings) = sqlhelpers.update_filler(data, 'id')
    query = f'UPDATE sticks {qmarks}'
    cur.execute(query, bindings)
    sql.commit()

    return data

def scrape_category(category):
    page = 1
    all_directs = set()
    while True:
        url = f'http://droidz.org/stickmain/{category}.php?page={page}'
        print(url)
        response = request(url)
        soup = bs4.BeautifulSoup(response.text, 'html.parser')
        this_directs = soup.find_all('a', href=re.compile(r'/direct/\d+'))
        prev_count = len(all_directs)
        all_directs.update(this_directs)
        if len(all_directs) == prev_count:
            break
        page += 1
        for direct in this_directs:
            id = id_from_direct_url(direct['href'])
            insert_id(id)

        sql.commit()

    print(f'Got {len(all_directs)} directs.')
    return all_directs

def scrape_latest():
    url = 'http://droidz.org/stickmain/'
    print(url)
    response = request(url)
    soup = bs4.BeautifulSoup(response.text, 'html.parser')
    h2s = soup.find_all('h2')
    for h2 in h2s:
        if 'Latest 50 Accepted' in h2.get_text():
            latest_50_h2 = h2
            break

    div = latest_50_h2.parent
    directs = div.find_all('a', href=re.compile(r'/direct/\d+'))
    for direct in directs:
        id = id_from_direct_url(direct['href'])
        status = insert_id(id)
        if not status.is_new:
            break

    return status

# UPDATE
################################################################################
def incremental_update():
    status = scrape_latest()
    if status.is_new:
        print('The Latest box didn\'t contain everything.')
        print('Need to check the categories for new sticks.')
        for category in CATEGORIES:
            scrape_category(category)

    cur = sql.cursor()
    cur.execute('SELECT id FROM sticks WHERE retrieved IS NULL')
    ids = [row[0] for row in cur.fetchall()]

    for id in ids:
        scrape_direct(id)

def full_update():
    for category in CATEGORIES:
        scrape_category(category)

    cur = sql.cursor()
    cur.execute('SELECT id FROM sticks ORDER BY retrieved ASC')
    ids = [row[0] for row in cur.fetchall()]

    for id in ids:
        scrape_direct(id)

# DOWNLOAD
################################################################################
def download_stick(id, overwrite=False, extract=False):
    directory = pathclass.Path('download').with_child(id)
    if directory.exists and not overwrite:
        return directory

    cur = sql.cursor()
    cur.execute('SELECT download_link FROM sticks WHERE id == ?', [id])
    download_link = cur.fetchone()[0]
    filename = re.search(r'file=(.+)', download_link).group(1)
    filepath = directory.with_child(filename)

    DOWNLOAD_RATELIMITER.limit()
    print(f'Downloading {id}')
    response = request(download_link)

    os.makedirs(directory.absolute_path, exist_ok=True)
    with open(filepath.absolute_path, 'wb') as handle:
        handle.write(response.content)

    if extract and filepath.extension == 'zip':
        # As much as I would like to use Python's zipfile module, I found that
        # some of the .zips on the site are actually rars.
        command = [
            WINRAR, 'x',
            '-o+', '-ibck',
            filepath.absolute_path,
            '*.*',
            directory.absolute_path + os.sep,
        ]
        subprocess.run(command)
        os.remove(filepath.absolute_path)

    return directory

def download_all(overwrite=False, extract=False):
    cur = sql.cursor()
    cur.execute('SELECT id FROM sticks')
    ids = [row[0] for row in cur.fetchall()]
    for id in ids:
        download_stick(id, overwrite=overwrite, extract=extract)

# COMMAND LINE
################################################################################
from voussoirkit import betterhelp

DOCSTRING = '''
Scrape sticks from droidz.org.

{update}

{download}

TO SEE DETAILS ON EACH COMMAND, RUN
> droidz.py <command> --help
'''.lstrip()

SUB_DOCSTRINGS = dict(
update='''
update:
    Update the database with stick info.

    > droidz.py update

    flags:
    --full:
        Re-scrape all categories and all sticks to get fresh info.
        Otherwise, only new sticks will be scraped.
'''.strip(),

download='''
download:
    Download the stick files.

    > droidz.py download all
    > droidz.py download [ids]

    flags:
    --overwrite:
        Re-download any existing files. Otherwise they'll be skipped.

    --extract:
        Extract downloaded zip files.
        NOTE: Some files on the site are labeled as .zip but are actually rars,
        so this extraction process requires you to have winrar on your PATH.
        Sorry.
'''.strip(),
)

DOCSTRING = betterhelp.add_previews(DOCSTRING, SUB_DOCSTRINGS)

def update_argparse(args):
    if args.full:
        return full_update()
    else:
        return incremental_update()

def download_argparse(args):
    if args.extract and not WINRAR:
        raise Exception('The --extract flag requires you to have winrar on your path.')
    if len(args.ids) == 1 and args.ids[0] == 'all':
        return download_all(overwrite=args.overwrite, extract=args.extract)
    else:
        for id in args.ids:
            return download_stick(id, overwrite=args.overwrite, extract=args.extract)

parser = argparse.ArgumentParser(description=__doc__)
subparsers = parser.add_subparsers()

p_update = subparsers.add_parser('update')
p_update.add_argument('--full', dest='full', action='store_true')
p_update.set_defaults(func=update_argparse)

p_download = subparsers.add_parser('download')
p_download.add_argument('ids', nargs='+', default=None)
p_download.add_argument('--overwrite', dest='overwrite', action='store_true')
p_download.add_argument('--extract', dest='extract', action='store_true')
p_download.set_defaults(func=download_argparse)

@betterhelp.subparser_betterhelp(parser, main_docstring=DOCSTRING, sub_docstrings=SUB_DOCSTRINGS)
def main(argv):
    args = parser.parse_args(argv)
    return args.func(args)

if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
