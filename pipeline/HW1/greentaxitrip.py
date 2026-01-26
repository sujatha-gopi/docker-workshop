#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
import click
import wget
from sqlalchemy import create_engine


def run(pguser, pgpass, pghost, pgport, pgdb, table_name):

    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"

    file_path = wget.download(url)
    greentaxi_df = pd.read_parquet(file_path)
    engine = create_engine(f'postgresql://{pguser}:{pgpass}@{pghost}:{pgport}/{pgdb}')

    print(pd.io.sql.get_schema(greentaxi_df, name=table_name, con=engine))

    greentaxi_df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    greentaxi_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)


@click.command()
@click.option('--pguser', default='root', help='Postgres username')
@click.option('--pgpass', default='root', help='Postgres password')
@click.option('--pghost', default='pgdatabase', help='Postgres host')
@click.option('--pgport', default='5432', help='Postgres port')
@click.option('--pgdb', default='ny_taxi', help='Postgres database name')
@click.option('--table-name', 'table_name', default='greentaxi', help='Table name to write to')
def main(pguser, pgpass, pghost, pgport, pgdb, table_name):
    run(pguser, pgpass, pghost, pgport, pgdb, table_name)


if __name__ == '__main__':
    main()