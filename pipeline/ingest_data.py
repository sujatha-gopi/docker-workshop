#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


@click.command()
@click.option('--pguser', default='root', show_default=True, help='Postgres user')
@click.option('--pgpass', default='root', show_default=True, help='Postgres password')
@click.option('--pghost', default='localhost', show_default=True, help='Postgres host')
@click.option('--pgport', default=5432, show_default=True, type=int, help='Postgres port')
@click.option('--pgdb', default='ny_taxi', show_default=True, help='Postgres database')
@click.option('--target-table', 'target_table', default='yellow_taxi_trips', show_default=True, help='Target table name')
@click.option('--year', default=2021, show_default=True, type=int, help='Year for dataset')
@click.option('--month', default=1, show_default=True, type=int, help='Month for dataset (1-12)')
@click.option('--chunksize', default=100000, show_default=True, type=int, help='CSV read chunksize')
def run(pguser, pgpass, pghost, pgport, pgdb, target_table, year, month, chunksize):
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'
    engine = create_engine(f'postgresql://{pguser}:{pgpass}@{pghost}:{pgport}/{pgdb}')

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists='replace')
            first = False

        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')


if __name__ == '__main__':
    run()

