from .parquet import *

import re
import duckdb
import pandas as pd
from bs4 import BeautifulSoup

## Vector search over parquet data source
def poome (query: str, parquet: str) -> pd.DataFrame:
  return duckify(db_connect(), query=query, source=f"read_parquet('{parquet}')")

def parquetize (conn: duckdb.DuckDBPyConnection, file: str, source: str='temp_embeddings'):
  df = conn.sql(f"SELECT * FROM {source}").fetchdf()
  df.to_parquet(f'{file}.parquet', index=False, compression='gzip')
  print('Data saved as parquet')

def memorize (conn: duckdb.DuckDBPyConnection, file: str):
  conn.sql(f"ATTACH '{file}.db' AS {file}")
  conn.sql(f"COPY FROM DATABASE memory TO {file}")
  conn.sql(f"DETACH {file}")
  print('Data saved as file')

def vectorify (conn: duckdb.DuckDBPyConnection, lmdim: int=None):
  print('Vectorizing archive..')
  conn.execute("DROP FUNCTION IF EXISTS text2vector")
  conn.create_function('text2vector', embed)
  conn.execute(f"DROP TABLE IF EXISTS temp_embeddings")
  conn.execute(f"CREATE TABLE temp_embeddings (id text, embedding FLOAT[{lmdim or LMID['dim']}])")
  conn.execute(f"INSERT INTO temp_embeddings SELECT id, text2vector(text) AS embedding FROM temp_texts")
  print('Data table temp_embeddings created')

def embed (text: str) -> list[float]:
  vectors, x = vectorize([text])
  return vectors.tolist()[0]

## Create intermediate data table (plain text) from original parquet
def textify (parquet: str, select: str=None, output: str=None) -> duckdb.DuckDBPyConnection:
  conn = db_connect()
  print('Extracting archive..')
  db_reform(conn, load_parquet(parquet))
  conn.create_function('html2text', decompose)
  conn.execute(f"DROP TABLE IF EXISTS temp_texts")
  conn.execute(f"CREATE TABLE temp_texts AS SELECT {select or TEXTSELECT} FROM temp")
  conn.execute(f"DROP TABLE IF EXISTS temp")
  conn.execute(f"INSTALL vss; LOAD vss;")
  print('Data table temp_texts created')
  if output: memorize(conn, output)
  return conn

## Convert HTML to plain text
def decompose (html: str) -> str:
  soup = BeautifulSoup(html, 'html.parser')
  for br in soup.find_all(['br', 'br/']): br.replace_with('\n')
  for styletag in soup(['style', 'script']): styletag.decompose()
  return re.sub(r'\n+', '\n', soup.get_text().replace('\xa0', ''))

## Process cosine similarity search over DuckDB connection
## Parameter <source> can be <read_parquet('embeddings.parquet')>
def duckify (conn, query: str, source: str='text_embeddings', limit: int=10) -> pd.DataFrame:
  embedding, sizes = vectorize([query])
  query_embedding = embedding[0].tolist()
  conn.execute("INSTALL vss; LOAD vss;")
  return conn.execute(f"""
    SELECT id, array_cosine_similarity(embedding::FLOAT[{sizes[0]}], {query_embedding}::FLOAT[{sizes[0]}]) AS similarity_score
    FROM {source} ORDER BY similarity_score DESC LIMIT {limit}"""
  ).fetchdf()

## Store vectorized dataframe into DuckDB data table
def framify (df: pd.DataFrame, storage: str=None, lmdim: int=None) -> duckdb.DuckDBPyConnection:
  conn = duckdb.connect(storage or ':memory:')
  conn.execute("DROP TABLE IF EXISTS text_embeddings")
  conn.execute(f"CREATE TABLE text_embeddings (id VARCHAR, embedding FLOAT[{lmdim or LMID['dim']}])")
  conn.execute(f"INSERT INTO text_embeddings SELECT * FROM df")
  return conn

## Dataframe for vectorized chunks
def framize (corpus: list) -> pd.DataFrame:
  embeddings, x = vectorize(corpus)
  df = pd.DataFrame({
    'id': corpus,
    'embedding': list(embeddings),
  })
  return df

## Vectorize text chunks (corpus)
def vectorize (corpus: list, method: str=None, lmid: str=None) -> tuple:
  if method == 'bow': ## BagOfWords
    from sklearn.feature_extraction.text import CountVectorizer
    vectorizer = CountVectorizer()
    X = vectorizer.fit_transform(corpus)
    return X.toarray(), vectorizer.get_feature_names_out()
  elif method == 'tfidf': ## TermFrequency-InverseDocumentFrequency
    from sklearn.feature_extraction.text import TfidfVectorizer
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(corpus)
    return X.toarray(), vectorizer.get_feature_names_out()
  else: ## Using MiniLM
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(lmid or LMID['id'])
    embeddings = model.encode(corpus, normalize_embeddings=True)
    return embeddings, [len(x) for x in embeddings]

LMID = {'id': 'all-MiniLM-L6-v2', 'dim': 384}
TEXTSELECT = "file AS id, CONCAT(title,'\n',label,'\n',html2text(content),'\nCOMMENTS\n',html2text(comments)) AS text"
