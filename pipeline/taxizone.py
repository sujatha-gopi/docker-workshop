#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
import wget
from sqlalchemy import create_engine

def run(pguser, pgpass, pghost, pgport, pgdb, table_name):
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
    file_path = wget.download(url)
    taxizone_df = pd.read_csv(file_path)
    engine = create_engine(f'postgresql://{pguser}:{pgpass}@{pghost}:{pgport}/{pgdb}')
    taxizone_df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

@click.command()
@click.option('--pguser', default='root', help='Postgres username')
@click.option('--pgpass', default='root', help='Postgres password')
@click.option('--pghost', default='localhost', help='Postgres host')
@click.option('--pgport', default='5433', help='Postgres port')
@click.option('--pgdb', default='ny_taxi', help='Postgres database name')
@click.option('--table-name', 'table_name', default='taxi_zones', help='Table name to write to')

def main(pguser, pgpass, pghost, pgport, pgdb, table_name):
    run(pguser, pgpass, pghost, pgport, pgdb, table_name)

if __name__ == '__main__':
    main()