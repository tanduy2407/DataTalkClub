import pandas as pd
from sqlalchemy import create_engine, text
import logging as l


class Database:
    def connect_db(self):
        host = 'localhost'
        port = '5432'
        database = 'dbt_production'
        user = 'postgres'
        password = 'tanduy2407'
        url = f'postgresql://{user}:{password}@{host}:{port}/{database}'
        engine = create_engine(url, isolation_level="AUTOCOMMIT")
        return engine


    def create_table(self, engine, table):
        df = pd.read_parquet(
            f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet')
        sql = pd.io.sql.get_schema(df, table, con=engine)
        with engine.connect() as conn:
            try:
                conn.execute(text(sql))
                print('Create table succesfully')
            except Exception as err:
                pass


    def ingest_data(self, engine, table):
        for i in range(1, 13):
            df = pd.read_parquet(f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-{i:02}.parquet')
            df.to_sql(name=table, if_exists='append',
                    con=engine, index=False)
            print(f'Insert data 2020-{i:02} successfully!')


db = Database()
conn = db.connect_db()
db.create_table(conn, 'ny_yellow_trip_2020')
db.ingest_data(conn, 'ny_yellow_trip_2020')