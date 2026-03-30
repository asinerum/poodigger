import re
import sys
import duckdb
import jinja2
import requests
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from formatize import parse_date

def wormsave (fromdate: str, todate: str, **kwargs):
  posts = wormdig(fromdate, todate, **kwargs)
  if not posts: return print('Nothing saved')
  if not this.TEMPLATES: load_templates() ## Load from CDN
  target = kwargs.get('target') or kwargs.get('dir') or 'test'
  print('==================================')
  print('============= SAVING =============')
  print('==================================')
  for post in posts:
    file = post['file']
    if (kwargs.get('pte') or kwargs.get('author')) and not post.get('pte'):
      print(f'No content for {file}')
      continue
    html = render_template('HTML_FORM', {
      'head': this.TEMPLATES['HTML_STYLE'],
      'body': render_template('POST_FORM', post),
    })
    try:
      print(f'Saving {file}')
      save_file(html, f'{target}/{file}')
    except Exception as e:
      print(f'File error: {e}')
  print('Done')

def wormdig (fromdate: str, todate: str, **kwargs) -> list:
  posts = duckdig(fromdate, todate)
  if not posts: return print('No data available')
  page = int(kwargs.get('page') or kwargs.get('pageno') or 1)
  pages = int(kwargs.get('pages') or 0) ## Total pages from [page]
  wormhole = kwargs.get('wormhole') or 'comments' ## Can be customized on use
  pte = kwargs.get('pte') or kwargs.get('author') ## Private comment as of Bloggerize JS module
  posts = [x for x in posts if (p:=int(x['pageno'])) >= page and (p < page+pages or pages == 0)]
  if pte:
    for post in posts:
      soup = BeautifulSoup(post[wormhole], 'html.parser')
      coms = soup.find_all('comdiv')
      for com in coms:
        authorurl = com.find('a', {'class': 'comment-authorurl'})['href']
        if not re.search(r'' + pte, authorurl, re.IGNORECASE): com.decompose()
      if soup.find('comdiv'): post['pte'] = True
      post[wormhole] = str(soup)
  return posts

def duckdig (fromdate: str, todate: str) -> list:
  try:
    conn = db_connect()
    df = db_load(conn, fromdate, todate)
    db_reform(conn, df)
    print('Duck dig done')
    return db_select(conn)
  except Exception as e:
    return print(f'Error occurred: {e}')

def db_reform (conn, df: pd.DataFrame):
  db_drop(conn)
  limit = len(this.COLUMNS)
  for i in range(0, int(len(df)/limit)):
    db_insert(conn, df, i)
  print('Temporary data table created')

def db_load (conn, fromdate: str, todate: str, parloc: str=None) -> pd.DataFrame:
  fromdate, todate, parquet = arrange_dates(fromdate, todate, parloc)
  if not parquet: return None
  df = db_parquet(conn, parquet, parloc)
  limit = len(this.COLUMNS)
  startindex = None
  endindex = None
  for i in range(0, len(df), limit):
    dated = df[i][:10]
    if dated < fromdate:  continue
    if startindex == None: startindex = i
    if dated > todate: break
    endindex = i
  if None in [startindex, endindex]: return None
  subdf = df[startindex:endindex+limit]
  subdf.reset_index(drop=True, inplace=True)
  return subdf

def db_select (conn, query: str='SELECT * FROM temp', values: list=[]) -> list:
  cursor = conn.cursor()
  cursor.execute(query, values)
  rows = cursor.fetchall()
  columns = [desc[0] for desc in cursor.description]
  return [dict(zip(columns, row)) for row in rows]

def db_insert (conn, df: pd.DataFrame, pindex: int=0):
  db_temp(conn)
  limit = len(this.COLUMNS)
  offset = pindex * limit
  placeholders = ','.join(['?' for _ in this.COLUMNS])
  query = f'INSERT INTO temp ({",".join(this.COLUMNS)}) VALUES ({placeholders})'
  conn.execute(query, [df[x] for x in range(offset, offset + limit)])

def db_drop (conn):
  query = f'DROP TABLE IF EXISTS temp'
  conn.execute(query)

def db_temp (conn):
  cols = []
  for col in this.COLUMNS:
    if this.COLUMNS.index(col) == 0:
      cols.append(f'{col} text unique')
    else:
      cols.append(f'{col} text')
  query = f'CREATE TABLE IF NOT EXISTS temp ({",".join(cols)})'
  conn.execute(query)

def db_parquet (conn, parfile: str, parloc: str=None, tolist: bool=False, dfraw: bool=False):
  print('Loading data archive..')
  parquet_url = (parloc or this.STORAGE) + parfile
  cursor = conn.execute(f"SELECT * FROM read_parquet('{parquet_url}')")
  print('Data archive loaded')
  if tolist: return cursor.fetchall()
  df = cursor.fetchdf()
  if dfraw: return df
  return df[df.columns[0]]

def db_connect (source: str=':memory:') -> duckdb.DuckDBPyConnection:
  conn = duckdb.connect(source)
  conn.execute('INSTALL httpfs')
  conn.execute('LOAD httpfs')
  return conn

def arrange_dates (fromdate: str, todate: str, parloc: str=None) -> tuple:
  fromdate = parse_date(fromdate)
  todate = parse_date(todate)
  if fromdate > todate:
    print('Invalid date range')
    return None, None, None
  ranges = date_ranges(parloc)
  for range in ranges:
    if todate <= range[1]:
      if fromdate > range[0]:
        return fromdate, todate, f'{range[1]}.parquet'
      else:
        print('Invalid starting date, using default')
        return range[0], todate, f'{range[1]}.parquet'
  print('Invalid ending date, using default')
  return ranges[-1][0], ranges[-1][1], f'{ranges[-1][1]}.parquet'

def date_ranges (parloc: str=None) -> list:
  dates = list_parquets(parloc)
  ranges = []
  old = '2008-05-20'
  for date in dates:
    ranges.append((old, date))
    old = date
  return ranges

def list_parquets (parloc: str=None) -> list:
  try:
    response = requests.get((parloc or this.STORAGE) + 'parquets.txt')
    return response.text.splitlines()
  except:
    return []

def load_git_parquet (parfile: str, parloc: str=None) -> pd.DataFrame:
  return load_parquet((parloc or this.STORAGE) + parfile)

def dig_parquet (parquet: str, column: str, page: int=1):
  df = load_parquet(parquet)
  return dig_poo_data(df, column, page)

def dig_poo_data (df: pd.DataFrame, column: str, page: int=1):
  try:
    return df[len(this.COLUMNS) * (page-1) + this.COLUMNS.index(column)]
  except:
    return None

def load_parquet (parquet: str) -> pd.DataFrame:
  print('Loading remote parquet archive..')
  df = pd.read_parquet(parquet) ## Path/URL
  print('Parquet archive loaded')
  return df[df.columns[0]]

def parquet_files (files: list, name: str=None, source: str=None, target: str=None):
  name = name or files[-1].split('.')[0]
  source = source or folder_source()[0]
  target = target or folder_target()[0]
  files = pack_files(files, source)
  files = files.to_frame(name=name)
  files.to_parquet(f'{target}/{name}.parquet', index=False, compression='gzip')
  print('Done')

def pack_files (files: list, filedir: str) -> pd.DataFrame:
  dfs = []
  for file in files:
    print('Process', file)
    dfs.append(pack_file(file, filedir))
  return pd.concat(dfs) ## ignore_index=False

def pack_file (filename: str, filedir: str) -> pd.DataFrame:
  return pd.DataFrame(load_file(filename, filedir)).data

def save_file (content: str, output: str, usesoup: bool=True):
  if usesoup:
    soup = BeautifulSoup(content, 'html.parser')
    html = soup.prettify(encoding='utf-8')
  else:
    html = content
  output = Path(output)
  output.parent.mkdir(parents=True, exist_ok=True)
  with open(output, 'wb') as file: file.write(html)
  print('File saved')

def load_file (filename: str, filedir: str, full: bool=False) -> dict:
  soup = read_file(filename, filedir)
  if not soup: return {'data': {}, 'html': None}
  if full: return {'data': {}, 'html': str(soup)}
  data = soup.find('body')
  load = {'data': {}, 'html': None}
  load['data'][this.COLUMNS[0]] = filename
  load['data'][this.COLUMNS[1]] = data.find('a', {'id': 'bloggerEntryURL'})['href']
  load['data'][this.COLUMNS[2]] = data.find('h2', {'id': 'bloggerEntryTitle'}).get_text()
  load['data'][this.COLUMNS[3]] = data.find('h3', {'id': 'bloggerEntryAuthor'}).get_text()
  load['data'][this.COLUMNS[4]] = data.find('span', {'id': 'bloggerEntryLabel'}).get_text()
  load['data'][this.COLUMNS[5]] = data.find('span', {'id': 'bloggerEntryGMTDate'}).get_text()
  load['data'][this.COLUMNS[6]] = str(data.find('span', {'id': 'bloggerEntryContent'}))
  load['data'][this.COLUMNS[7]] = data.find('span', {'id': 'bloggerEntryTotalComments'}).get_text()
  load['data'][this.COLUMNS[8]] = data.find('span', {'id': 'bloggerTotalCommentPages'}).get_text()
  load['data'][this.COLUMNS[9]] = data.find('span', {'id': 'bloggerCommentPageNo'}).get_text()
  load['data'][this.COLUMNS[10]] = str(data.find('span', {'id': 'bloggerEntryComment'}))
  return load

def read_file (filename: str, filedir: str) -> BeautifulSoup:
  try:
    with open(f'{filedir}/{filename}', 'r', encoding='utf-8') as file:
      page_cont = file.read()
    return BeautifulSoup(page_cont, 'html.parser')
  except FileNotFoundError:
    print('No file found')
    return None

def read_list (filelist: str='list.txt') -> list:
  try:
    with open(filelist, 'r') as file:
      list_cont = file.read()
    page_list = list_cont.split('\n')
    return list(filter(None, page_list))
  except FileNotFoundError:
    print('No list available')
    return []

def folder_target (path: str='parquet.target.txt') -> list:
  return read_list(path)

def folder_source (path: str='parquet.source.txt') -> list:
  return read_list(path)

def render_template (temp_var: str, data: dict={}) -> str:
  if temp_var not in this.TEMPLATES: return None
  template = jinja2.Template(this.TEMPLATES[temp_var])
  return template.render(**data)

def load_templates (cdn_url: str=None) -> dict:
  js_code = read_cdn(cdn_url)
  this.TEMPLATES['HTML_FORM'] = extract_var(js_code, 'HTML_FORM')
  this.TEMPLATES['POST_FORM'] = extract_var(js_code, 'POST_FORM')
  this.TEMPLATES['COMMENT_FORM'] = extract_var(js_code, 'COMMENT_FORM')
  this.TEMPLATES['HTML_STYLE'] = extract_var(js_code, 'HTML_STYLE')
  return this.TEMPLATES

def extract_cdn (text_var: str, cdn_url: str=None) -> str:
  return extract_var(read_cdn(cdn_url), text_var)

def extract_var (js_code: str, text_var: str) -> str:
  if not js_code: return None
  res = re.search(text_var + r'\s=\s(`|\')(.*?)(`|\')', js_code, re.DOTALL)
  if not res: return None
  return res[2]

def read_cdn (cdn_url: str=None) -> str:
  try:
    response = requests.get(cdn_url or this.JSCDN)
    return response.text
  except:
    return None

COLUMNS = [
  'file',
  'entry',
  'title',
  'author',
  'label',
  'stamp',
  'content',
  'total',
  'pages',
  'pageno',
  'comments'
]

STORAGE = 'https://raw.githubusercontent.com/asinerum/poodigger/refs/heads/main/data/'
JSCDN = 'https://cdn.jsdelivr.net/gh/asinerum/bloggerize/src/htmls.js'
TEMPLATES = {}

this = sys.modules[__name__]

def reconfig (**kwargs):
  for key, value in kwargs.items():
    setattr(this, key, value)
