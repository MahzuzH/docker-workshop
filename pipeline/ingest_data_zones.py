#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

# Taxi Zone Lookup columns (TLC)
# Typical columns: LocationID, Borough, Zone, service_zone
zone_dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string",
}

def run(
    pg_user: str = 'root',
    pg_pass: str = 'root',
    pg_host: str = 'localhost',
    pg_port: int = 5432,
    pg_db: str = 'ny_taxi',
    target_table: str = 'zones',
    chunksize: int = 100000,
    if_exists: str = "replace",  # replace or append
):
    # DataTalksClub hosts the TLC taxi zone lookup here (CSV)
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

    engine = create_engine(
        f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    )

    # The file is small; but we can still support chunking safely.
    df_iter = pd.read_csv(
        url,
        dtype=zone_dtype,
        iterator=True,
        chunksize=chunksize,
    )

    first = True
    table_name = target_table

    for df_chunk in tqdm(df_iter):
        if first:
            # Create schema
            df_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
            )
            first = False
            print(f"Table created: {table_name}")

        # Insert data
        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append" if if_exists in ("replace", "fail") else if_exists,
            index=False,
        )
        print("Inserted:", len(df_chunk))

@click.command()
@click.option('--pg-user', default='root', show_default=True, help='Postgres user')
@click.option('--pg-pass', default='root', show_default=True, help='Postgres password')
@click.option('--pg-host', default='localhost', show_default=True, help='Postgres host')
@click.option('--pg-port', default=5432, show_default=True, type=int, help='Postgres port')
@click.option('--pg-db', default='ny_taxi', show_default=True, help='Postgres database')
@click.option('--target-table', default='zones', show_default=True, help='Target DB table')
@click.option('--chunksize', default=100000, show_default=True, type=int, help='CSV read chunksize')
@click.option('--if-exists', 'if_exists', default='replace', show_default=True,
              type=click.Choice(['replace', 'append', 'fail'], case_sensitive=False),
              help='What to do if table exists')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, chunksize, if_exists):
    run(
        pg_user=pg_user,
        pg_pass=pg_pass,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_db=pg_db,
        target_table=target_table,
        chunksize=chunksize,
        if_exists=if_exists.lower(),
    )

if __name__ == '__main__':
    main()
