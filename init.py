import cProfile
import datetime as dt
import io
import os
import pstats
import subprocess
import sys
import tempfile
import time
import types
from typing import List
from urllib.parse import quote_plus

import altair as alt
import duckdb
import pandas as pd
import plotly.express as px
import requests
import streamlit as st
import streamlit.components.v1 as components
from bs4 import BeautifulSoup
from dateutil import parser
from google_images_search import GoogleImagesSearch
from googleapiclient.errors import HttpError
from nba_api.live.nba.endpoints import scoreboard
from pandas import DataFrame
from streamlit_modal import Modal
from streamlit_searchbox import st_searchbox
from streamlit_theme import st_theme

from etl import check_file_exists, download_csv_from_bucket, upload_to_bucket

START_YEAR = 2005
END_YEAR = 2023
DB_NAME = "stats.duckdb"


def init_db():
    conn = duckdb.connect(database=DB_NAME, read_only=False)
    for year in range(START_YEAR, END_YEAR + 1):
        players_stats_csv = download_csv_from_bucket(
            "nba_dashboard_files", f"regular_dataset_{year - 1}_{year}.csv"
        )
        players_stats_ranked_csv = download_csv_from_bucket(
            "nba_dashboard_files", f"ranked_dataset_{year - 1}_{year}.csv"
        )

        players_stats = pd.read_csv(io.BytesIO(players_stats_csv))
        players_stats_ranked = pd.read_csv(io.BytesIO(players_stats_ranked_csv))
        # Create tables in DuckDB
        # print(players_stats)
        conn.register("players_stats_" + str(year) + "_" + str(year + 1), players_stats)
        conn.register(
            "players_stats_ranked_" + str(year) + "_" + str(year + 1),
            players_stats_ranked,
        )

        # Get the list of all players
        all_players_query = conn.execute(
            f"SELECT player FROM players_stats_{year}_{year + 1} ORDER BY player"
        ).fetchall()
        all_players = [row[0] for row in all_players_query]

        all_players_df = pd.DataFrame({"player": all_players})
        conn.register("all_players_" + str(year) + "_" + str(year + 1), all_players_df)

    return conn


if __name__ == "__main__":
    time.sleep(1)
