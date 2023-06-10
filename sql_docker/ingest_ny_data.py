import pandas as pd
from sqlalchemy import create_engine
import argparse
import os


def main(params):
	# params
	user = params.user
	password = params.password
	host = params.host
	port = params.port
	database = params.database
	table = params.table
	url = params.url
	parquet_file = 'yellow_tripdata_2023-01.parquet'

	# bash cmd to download parquet file
	os.system(f'curl {url} -o {parquet_file}')
	df = pd.read_parquet(f'{parquet_file}')

	# connect to postgres db
	engine = create_engine(
		f'postgresql://{user}:{password}@{host}:{port}/{database}')
	engine_postgre = engine.connect()
	sql = pd.io.sql.get_schema(df, f'{table}', con=engine)
	engine.execute(sql)

	# insert data
	print(f'Ingest data to table: {table}')
	df.to_sql(name=f'{table}', if_exists='append',
			  con=engine_postgre, index=False, chunksize=100000)


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Ingest data to Posgres')
	parser.add_argument('--user', help='user name for Postgres')
	parser.add_argument('--password', help='password for Postgres')
	parser.add_argument('--host', help='host for Postgres')
	parser.add_argument('--port', help='port for Postgres')
	parser.add_argument('--database', help='database name for Postgres')
	parser.add_argument(
		'--table', help='table name for Postgres use to store data')
	parser.add_argument('--url', help='url of Parquet file')
	args = parser.parse_args()
	main(args)
