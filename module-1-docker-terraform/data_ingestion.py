#!/usr/bin/env python
# coding: utf-8

import os
import requests
import pandas as pd
import pyarrow.parquet as pq

from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click




dtype = {
    'VendorID': 'Int64',
    'lpep_pickup_datetime': 'datetime64[ns]',
    'lpep_dropoff_datetime': 'datetime64[ns]',
    'store_and_fwd_flag': 'str',
    'RatecodeID': 'Int64',
    'PULocationID': 'Int64',
    'DOLocationID': 'Int64',
    'passenger_count': 'Int64',
    'trip_distance': 'float64',
    'fare_amount': 'float64',
    'extra': 'float64',
    'mta_tax': 'float64',
    'tip_amount': 'float64',
    'tolls_amount': 'float64',
    'ehail_fee': 'float64',
    'improvement_surcharge': 'float64',
    'total_amount': 'float64',
    'payment_type': 'Int64',
    'trip_type': 'Int64',
    'congestion_surcharge': 'float64',
    'cbd_congestion_fee': 'float64'
}


@click.command()
@click.option('--pg-user', default='root', show_default=True, help='Postgres user')
@click.option('--pg-password', default='root', show_default=True, help='Postgres password')
@click.option('--pg-host', default='localhost', show_default=True, help='Postgres host')
@click.option('--pg-port', default='5432', show_default=True, help='Postgres port')
@click.option('--pg-db', default='ny_taxi', show_default=True, help='Postgres database')
@click.option('--month', default=11, show_default=True, type=int, help='Month (1-12)')
@click.option('--year', default=2025, show_default=True, type=int, help='Year')
@click.option('--target-table', default='green_taxi', show_default=True, help='Target table name')
def run(pg_user, pg_password, pg_host, pg_port, pg_db, month, year, target_table):
    prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    taxi_url = f'{prefix}/green_tripdata_{year}-{month:02d}.parquet'
    zone_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'

    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    # download parquet file and load into sql
    file_name = download_parquet_file(taxi_url, month, year)
    pq_file = pq.ParquetFile(file_name)

    target_table = f'{target_table}_{year}_{month:02d}'
    load_trip_data_to_sql(pq_file, engine, target_table)
    
    # get zone data and load into sql
    load_zone_data_to_sql(zone_url, engine)
    

def download_parquet_file(url: str, month: int, year: int) -> str:
    file_name = f'green_trip_data_{year}-{month:02d}.parquet'
    # check if file exists
    if not os.path.exists(file_name):
        print(f'Downloading {url}')
        response = requests.get(url)

        if response.status_code==200:
            with open(file_name, 'wb') as f:
                f.write(response.content)
            print('Parquet download complete!')
        else:
            print(f'Failed to download. Status code: {response.status_code}')
    else:
        print(f'File: {file_name} already exists. Skipping download.')

    return file_name

def load_trip_data_to_sql(pq_file: pq.ParquetFile, engine: str, target_table: str) -> None:
    make_taxi_table = True
    pq_iter = pq_file.iter_batches(batch_size=10000)
    for chunk in tqdm(pq_iter):
        df = chunk.to_pandas()

        #create table schema for taxi data if one does not exist
        if make_taxi_table:
            df.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace'
            )
            make_taxi_table = False
            print(f'Table name: {target_table} created')

        #transform data types and load to sql
        print(f"Processing a chunk of {len(df)} rows...")
        df = df.astype(dtype)
        df.to_sql(name=target_table, con=engine, if_exists='append')

def load_zone_data_to_sql(url: str, engine: str):
    df = pd.read_csv(url)
    df.to_sql(name='zones', con=engine, if_exists='replace')
    print(f'Loaded {len(df)} rows to zones')



if __name__ == '__main__':
    run()