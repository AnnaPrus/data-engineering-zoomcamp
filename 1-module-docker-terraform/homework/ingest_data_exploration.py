#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
import click
def ingest_data(url1, url2, engine, target_table1, target_table2):
    df1 = pd.read_parquet(url1)
    df2 = pd.read_csv(url2)

    df1.head(0).to_sql(
        name=target_table1,
        con=engine,
        if_exists="replace")

    df2.head(0).to_sql(
        name=target_table2,
        con=engine,
        if_exists="replace")

    df1.to_sql(
            name=target_table1,
            con=engine,
            if_exists="append"
        )
    df2.to_sql(
            name=target_table2,
            con=engine,
            if_exists="append"
        )
    print("Ingestion finished successfully")



@click.command()
@click.option('--pg-user', default='root', show_default=True, help='Postgres user')
@click.option('--pg-pass', default='root', show_default=True, help='Postgres password')
@click.option('--pg-host', default='localhost', show_default=True, help='Postgres host')
@click.option('--pg-port', default='5432', show_default=True, help='Postgres port')
@click.option('--pg-db', default='trip_data', show_default=True, help='Postgres database')
@click.option('--table-trips', default='green_trip_data_2025', show_default=True)
@click.option('--table-zones', default='taxi_zone_lookup', show_default=True)
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, table_trips, table_zones):
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    url1 = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
    url2 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    ingest_data(
        url1=url1,
        url2=url2,
        engine=engine,
        target_table1=table_trips,
        target_table2=table_zones
    )


if __name__ == '__main__':
    main()