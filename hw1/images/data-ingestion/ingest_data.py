#!/usr/bin/env python
# coding: utf-8

import argparse
import os
from time import time

import pandas as pd
from sqlalchemy import create_engine

CHUNK_SIZE = 100000
DATA_PATH = "yellow_tripdata_2021-01.csv"
DATA_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"  # noqa: E501
ZONES_DATA_PATH = "taxi_zone_lookup.csv"
ZONES_DATA_URL = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"


def ingest_data(args) -> None:
    # Download data
    os.system(f"wget {args.url} -O {DATA_PATH}")

    # Connect to PostgreSQL
    db_url = f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.db}"
    engine = create_engine(url=db_url)
    engine.connect()

    # Read chunk of data to RAM and infer schema
    df = pd.read_csv(DATA_PATH, compression="gzip", nrows=100)
    print(pd.io.sql.get_schema(df, name=args.table_name, con=engine))

    # Create table in DB
    df.head(n=0).to_sql(name=args.table_name, con=engine, if_exists="replace")

    # Populate table by chunk-wise iteration over dataset
    df_iterator = pd.read_csv(DATA_PATH, compression="gzip", iterator=True, chunksize=CHUNK_SIZE)
    for chunk in df_iterator:
        t_start = time()
        chunk.lpep_pickup_datetime = pd.to_datetime(chunk.lpep_pickup_datetime)
        chunk.lpep_dropoff_datetime = pd.to_datetime(chunk.lpep_dropoff_datetime)
        chunk.to_sql(name=args.table_name, con=engine, if_exists="append")
        t_end = time()
        print(f"Ingested another chunk...\nIt took {(t_end - t_start):.3f} seconds")

    print("Question 3: How many taxi trips were totally made on January 15?")
    query = (
        f"SELECT COUNT(*) FROM {args.table_name} "
        "WHERE lpep_pickup_datetime::date = '2019-01-15' "
        "AND lpep_dropoff_datetime::date = '2019-01-15';"
    )
    result: pd.DataFrame = pd.read_sql(sql=query, con=engine)
    print(f'Query: {query}\nAnswer: {result["count"].item()}')  # 20530

    print(
        "Question 4: Which was the day with the largest trip distance? "
        "Use the pick-up time for your calculations."
    )
    query = (
        f"SELECT lpep_pickup_datetime::date FROM {args.table_name} "
        f"ORDER BY trip_distance DESC LIMIT 1;"
    )
    result: pd.DataFrame = pd.read_sql(sql=query, con=engine)
    print(f'Query: {query}\nAnswer: {result["lpep_pickup_datetime"].item()}')

    print("Question 5: In 2019-01-01 how many trips had 2 and 3 passengers?")
    query = (
        f"SELECT COUNT(*) FROM {args.table_name} "
        f"WHERE lpep_pickup_datetime::date = '2019-01-01' AND passenger_count = 2;"
    )
    result_1: pd.DataFrame = pd.read_sql(sql=query, con=engine)
    query = (
        f"SELECT COUNT(*) FROM {args.table_name} "
        f"WHERE lpep_pickup_datetime::date = '2019-01-01' AND passenger_count = 3;"
    )
    result_2: pd.DataFrame = pd.read_sql(sql=query, con=engine)
    print(f'Query: {query}\nAnswer: 2: {result_1["count"].item()}, 3: {result_2["count"].item()}')

    print(
        "Question 6: For the passengers picked up in the Astoria Zone "
        "which was the drop-off zone that had the largest tip?"
    )
    os.system(f"wget {ZONES_DATA_URL} -O {ZONES_DATA_PATH}")
    df = pd.read_csv("taxi_zone_lookup.csv")
    astoria_zone_id = df[df["Zone"] == "Astoria"]["LocationID"].item()
    query = (
        f'SELECT "DOLocationID" FROM {args.table_name} WHERE "PULocationID" = {astoria_zone_id} '
        "ORDER BY tip_amount DESC LIMIT 1;"
    )
    result: pd.DataFrame = pd.read_sql(sql=query, con=engine)
    drop_off_location_id = result["DOLocationID"].item()
    drop_off_zone = df[df["LocationID"] == drop_off_location_id]["Zone"].item()
    print(f"Query: {query}\nAnswer: {drop_off_zone}")


def setup_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--user", default="postgres", help="Username for Postgres")
    parser.add_argument("--password", default="postgres", help="Password for Postgres")
    parser.add_argument("--host", default="postgres-db", help="Host for Postgres")
    parser.add_argument("--port", default=5432, help="Port for Postgres")
    parser.add_argument("--db", default="postgres_db", help="Database name for Postgres")
    parser.add_argument(
        "--table_name",
        default="yellow_taxi_trips",
        help="Name of the table where we will write the results to",
    )
    parser.add_argument("--url", default=DATA_URL, help="URL of the dataset file")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NY Yellow Taxi Trip data to Postgres")
    setup_parser(parser)
    args: argparse.Namespace = parser.parse_args()

    ingest_data(args)


if __name__ == "__main__":
    main()
