from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import pandas as pd
import boto3
import urllib.request
import sqlite3


class PreProcessToS3Operator(BaseOperator):
    ui_color = "#d0e0e3"

    @apply_defaults
    def __init__(self, aws_credentials_id="", aws_region="", *args, **kwargs):
        super(PreProcessToS3Operator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.aws_region = aws_region

    def execute(self, context):
        self.log.info("Begin Preprocessing")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        session = boto3.Session(
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            region_name="us-east-2",
        )

        s3 = session.client("s3")

        # Define the S3 bucket
        bucket_name = "awsbucket3323"
        # Read our weather CSV file directly into a DataFrame
        weather_file_path = "capstone/raw-data/GlobalLandTemperaturesByCity.csv"
        df = pd.read_csv(f"s3://{bucket_name}/{weather_file_path}")
        df["dt"] = pd.to_datetime(df["dt"])
        # Filter the DataFrame to include only rows between 2000 and 2013
        start_date = "2000-01-01"
        end_date = "2013-12-31"
        filtered_df = df[(df["dt"] >= start_date) & (df["dt"] <= end_date)]
        filtered_df["date_year"] = filtered_df["dt"].dt.year
        filtered_df["date_month"] = filtered_df["dt"].dt.month
        filtered_df["date_day"] = filtered_df["dt"].dt.day
        s3.put_object(
            Bucket=bucket_name,
            Key="capstone/filtered_data/filtered_weather_city_data.csv",
            Body=filtered_df.to_csv(index=False).encode("utf-8"),
        )
        self.log.info("File Processed: filtered_weather_city_data")

        country_file_path = "capstone/raw-data/GlobalLandTemperaturesByCountry.csv"
        df = pd.read_csv(f"s3://{bucket_name}/{country_file_path}")
        df["dt"] = pd.to_datetime(df["dt"])
        # Filter the DataFrame to include only rows between 2000 and 2013
        start_date = "2000-01-01"
        end_date = "2013-12-31"
        filtered_df = df[(df["dt"] >= start_date) & (df["dt"] <= end_date)]
        filtered_df["date_year"] = filtered_df["dt"].dt.year
        filtered_df["date_month"] = filtered_df["dt"].dt.month
        filtered_df["date_day"] = filtered_df["dt"].dt.day
        s3.put_object(
            Bucket=bucket_name,
            Key="capstone/filtered_data/filtered_weather_country_data.csv",
            Body=filtered_df.to_csv(index=False).encode("utf-8"),
        )
        self.log.info("File Processed: filtered_weather_country_data")

        # S3 URL for the SQLite file
        s3_url = (
            "https://awsbucket3323.s3.amazonaws.com/capstone/raw-data/database.sqlite"
        )
        # Choose destination filepath
        temp_file_path = "database.sqlite"
        # Download the file
        urllib.request.urlretrieve(s3_url, temp_file_path)
        # Connect to the SQLite database
        conn = sqlite3.connect(temp_file_path)
        # locating the table names within are sqlite db and saves them to S3 as csv files
        query = "SELECT * FROM League"
        df = pd.read_sql_query(query, conn)
        s3.put_object(
            Bucket=bucket_name,
            Key="capstone/filtered_data/League.csv",
            Body=df.to_csv(index=False).encode("utf-8"),
        )
        self.log.info("File Processed: League")

        query = "SELECT * FROM Country"
        df = pd.read_sql_query(query, conn)
        s3.put_object(
            Bucket=bucket_name,
            Key="capstone/filtered_data/Country.csv",
            Body=df.to_csv(index=False).encode("utf-8"),
        )
        self.log.info("File Processed: Country")

        query = "SELECT * FROM Team"
        df = pd.read_sql_query(query, conn)
        s3.put_object(
            Bucket=bucket_name,
            Key="capstone/filtered_data/Team.csv",
            Body=df.to_csv(index=False).encode("utf-8"),
        )
        self.log.info("File Processed: Team")

        query = "SELECT * FROM Player_Attributes"
        df = pd.read_sql_query(query, conn)
        columns_to_drop = [
            "preferred_foot",
            "attacking_work_rate",
            "defensive_work_rate",
            "volleys",
            "curve",
            "long_passing",
            "acceleration",
            "agility",
            "balance",
            "shot_power",
            "jumping",
            "long_shots",
            "interceptions",
            "vision",
            "marking",
            "standing_tackle",
            "sliding_tackle",
            "gk_diving",
            "gk_handling",
            "gk_kicking",
            "gk_positioning",
            "gk_reflexes"
        ]

        df = df.drop(columns=columns_to_drop)
        df["date"] = pd.to_datetime(df["date"])
        s3.put_object(
            Bucket=bucket_name,
            Key="capstone/filtered_data/Player_Attributes.csv",
            Body=df.to_csv(index=False).encode("utf-8"),
        )
        self.log.info("File Processed: Player_Attributes")

        query = "SELECT * FROM Match"
        df = pd.read_sql_query(query, conn)
        columns_to_drop = [
            "stage",
            "season",
            "home_player_X1",
            "home_player_X2",
            "home_player_X3",
            "home_player_X4",
            "home_player_X5",
            "home_player_X6",
            "home_player_X7",
            "home_player_X8",
            "home_player_X9",
            "home_player_X10",
            "home_player_X11",
            "away_player_X1",
            "away_player_X2",
            "away_player_X3",
            "away_player_X4",
            "away_player_X5",
            "away_player_X6",
            "away_player_X7",
            "away_player_X8",
            "away_player_X9",
            "away_player_X10",
            "away_player_X11",
            "home_player_Y1",
            "home_player_Y2",
            "home_player_Y3",
            "home_player_Y4",
            "home_player_Y5",
            "home_player_Y6",
            "home_player_Y7",
            "home_player_Y8",
            "home_player_Y9",
            "home_player_Y10",
            "home_player_Y11",
            "away_player_Y1",
            "away_player_Y2",
            "away_player_Y3",
            "away_player_Y4",
            "away_player_Y5",
            "away_player_Y6",
            "away_player_Y7",
            "away_player_Y8",
            "away_player_Y9",
            "away_player_Y10",
            "away_player_Y11",
            "goal",
            "shoton",
            "shotoff",
            "foulcommit",
            "card",
            "cross",
            "corner",
            "possession",
            "B365H",
            "B365D",
            "B365A",
            "BWH",
            "BWD",
            "BWA",
            "IWH",
            "IWD",
            "IWA",
            "LBH",
            "LBD",
            "LBA",
            "PSH",
            "PSD",
            "PSA",
            "WHH",
            "WHD",
            "WHA",
            "SJH",
            "SJD",
            "SJA",
            "VCH",
            "VCD",
            "VCA",
            "GBH",
            "GBD",
            "GBA",
            "BSH",
            "BSD",
            "BSA",
        ]

        df.drop(columns=columns_to_drop, inplace=True)
        df["date"] = pd.to_datetime(df["date"])
        df["date_year"] = df["date"].dt.year
        df["date_month"] = df["date"].dt.month
        df["date_day"] = df["date"].dt.day
        s3.put_object(
            Bucket=bucket_name,
            Key="capstone/filtered_data/Match.csv",
            Body=df.to_csv(index=False).encode("utf-8"),
        )
        self.log.info("File Processed: Match")

        query = "SELECT * FROM Team_Attributes"
        df = pd.read_sql_query(query, conn)
        columns_to_drop = [
            "defenceTeamWidth",
            "defenceTeamWidthClass",
            "defenceDefenderLineClass"
        ]
        df.drop(columns=columns_to_drop, inplace=True)
        df["date"] = pd.to_datetime(df["date"])
        s3.put_object(
            Bucket=bucket_name,
            Key="capstone/filtered_data/Team_Attributes.csv",
            Body=df.to_csv(index=False).encode("utf-8"),
        )
        self.log.info("File Processed: Team_Attributes")

        query = "SELECT * FROM Player"
        df = pd.read_sql_query(query, conn)
        s3.put_object(
            Bucket=bucket_name,
            Key="capstone/filtered_data/Player.csv",
            Body=df.to_csv(index=False).encode("utf-8"),
        )
        self.log.info("File Processed: Player")

        conn.close()
        s3.close()

        self.log.info("Preprocessing Complete!")
