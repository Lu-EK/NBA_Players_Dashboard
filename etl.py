## Libraraies and modules

import os
import re
import string
import tempfile
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from io import StringIO
from urllib.request import urlopen

import duckdb
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import storage

from Categorization import defensive_profile, offensive_profile

START_YEAR = 2005
END_YEAR = 2025


# def create_yearly_dataframes(year):
#     # File paths
#     regular_dataset_path = f"datasets/regular/regular_dataset_{year}_{year + 1}.csv"
#     advanced_dataset_path = f"datasets/advanced/advanced_dataset_{year}_{year + 1}.csv"
#     shooting_splits_path = f"datasets/shootings_splits/SP_{year}_{year + 1}.csv"

#     # Read CSV files
#     regular_df = pd.read_csv(regular_dataset_path)
#     advanced_df = pd.read_csv(advanced_dataset_path)
#     shooting_splits_df = pd.read_csv(shooting_splits_path)

#     return regular_df, advanced_df, shooting_splits_df

## Functions


class get_data:
    def dataset_players(self, season, mode):
        url = f"https://www.basketball-reference.com/leagues/NBA_{season}_{mode}.html"
        try:
            time.sleep(5)
            response = urlopen(url)
            table_web = BeautifulSoup(response, "html.parser").findAll("table")

            df = pd.read_html(StringIO(str(table_web)))[0]
            # df = df.drop(df[df.Player == 'Rk'].index)
            # df = df.drop('Rk', axis=1)
            df.insert(0, "Season", season)
            df = df.apply(pd.to_numeric, errors="coerce").fillna(df)
            return df
        except HttpError as e:
            if e.resp.status == 429:
                st.error("Request quota exceeded. Please try again later.")
            sys.exit("Something went wrong. Sorry !")


def create_yearly_dataframes(year):
    # File paths
    get_data_obj = get_data()
    regular_df = get_data_obj.dataset_players(year, "per_game")
    advanced_df = get_data_obj.dataset_players(year, "advanced")
    shooting_splits_df = get_data_obj.dataset_players(year, "shooting")

    # Read CSV files
    # regular_df = pd.read_csv(regular_dataset_path)
    # advanced_df = pd.read_csv(advanced_dataset_path)
    # shooting_splits_df = pd.read_csv(shooting_splits_path)

    return regular_df, advanced_df, shooting_splits_df


def process_glossary_file(input_file, output_file):
    # Read the contents of the input file
    with open(input_file, "r") as file:
        lines = file.readlines()

    # Modify each line to add a return at the end and a newline character
    modified_lines = [line.strip() + "\n \n" for line in lines]

    # Write the modified contents to the output file
    with open(output_file, "w") as file:
        file.writelines(modified_lines)


def transform_data(regular_dataset, advanced_dataset, shooting_splits):
    # In the case of multiples occurrences of a player, keep only the first occurrence
    regular_dataset = regular_dataset[
        ~regular_dataset["Player"].duplicated(keep="first")
    ]
    advanced_dataset = advanced_dataset[
        ~advanced_dataset["Player"].duplicated(keep="first")
    ]

    # Concatenate the two tables, erasing the recurrent columns
    start_columns = advanced_dataset.columns.get_loc("PER")
    columns_to_concat = advanced_dataset.columns[start_columns:]
    full_dataset_raw = pd.concat(
        [regular_dataset, advanced_dataset[columns_to_concat]], axis=1
    )

    # Clear the DataFrame from duplicated columns
    duplicated_columns = full_dataset_raw.columns[full_dataset_raw.columns.duplicated()]
    full_dataset = full_dataset_raw.drop(columns=duplicated_columns)
    full_dataset = full_dataset.drop(columns=["Unnamed: 24", "Unnamed: 19"])

    # Transform shooting percentages to concatenate with the full dataset
    shooting_splits.columns = shooting_splits.columns.get_level_values(1)
    shooting_splits = shooting_splits.drop(shooting_splits.index[0])
    shooting_splits.reset_index(drop=True, inplace=True)
    shooting_splits.drop_duplicates(
        subset=shooting_splits.columns[2], keep="first", inplace=True
    )
    shooting_splits = shooting_splits.loc[
        :, ~shooting_splits.columns.str.startswith("Unnamed")
    ]
    shooting_splits.reset_index(drop=True, inplace=True)

    # shooting_splits = shooting_splits[(shooting_splits['MP'].astype(int) / shooting_splits['G'].astype(int) >= 24) & (shooting_splits['G'].astype(int) >= 25)].reset_index()
    # Remove players that played less than 24MPG so far, and less than 25 games
    full_dataset["MP"] = pd.to_numeric(full_dataset["MP"], errors="coerce")
    full_dataset["G"] = pd.to_numeric(full_dataset["G"], errors="coerce")
    full_dataset = full_dataset[(full_dataset["MP"] >= 24) & (full_dataset["G"] >= 25)]

    # Round numbers
    full_dataset.iloc[:, 5:] = full_dataset.iloc[:, 5:].round(2)
    full_dataset = full_dataset.reset_index()

    # Merge the full dataset with shooting splits
    full_dataset = pd.merge(
        full_dataset, shooting_splits, on="Player", how="left"
    ).reset_index()

    stats_to_keep = ["Pos_x", "3P%_x", "Tm_x", "G_x"]

    cols_to_drop = [
        col
        for col in full_dataset
        if col not in stats_to_keep
        and isinstance(col, str)
        and (col.endswith("_x") or col.endswith("_y"))
    ]

    full_dataset = full_dataset.drop(columns=cols_to_drop)
    cols_to_keep = []

    for col in full_dataset.columns:
        if col not in cols_to_keep:
            cols_to_keep.append(col)

    full_dataset = full_dataset[cols_to_keep].dropna(axis=1, how="all")
    full_dataset.rename(
        columns={
            "3P%_x": "3P%",
            "G_x": "G",
            "Tm_x": "Team",
            "Dist.": "Average distance (ft.) of FGA",
            "-9999": "player_code",
            "0-3": "%FGA 0-3",
            "3-10": "%FGA 3-10",
            "10-16": "%FGA 10-16",
            "16-3P": "%FGA 16-3P",
            "Pos_x": "Pos",
        },
        inplace=True,
    )
    # full_dataset.drop(
    #    columns=["#", "%FGA", "%3PA", "Att.", "player_code"], inplace=True
    # )
    full_dataset = full_dataset.loc[:, ~full_dataset.columns.duplicated()]
    full_dataset.iloc[:, -4:] = full_dataset.iloc[:, -4:].apply(
        lambda x: x.astype(float) * 100
    )
    #full_dataset["%FGA 3P"] = 1 - full_dataset[
    #    ["%FGA 0-3", "%FGA 3-10", "%FGA 10-16", "%FGA 16-3P"]
    #].sum(axis=1)

    # Select only the columns that are numeric for mean calculation
    numeric_columns = full_dataset.iloc[:, :].select_dtypes(include="number")
    # Calculate the mean
    # avg = numeric_columns.mean().round(1)
    # full_dataset.loc['Average'] = avg

    # add AST/TOV
    full_dataset.insert(21, "AST/TOV", (full_dataset["AST"] / full_dataset["TOV"]))
    # full_dataset["AST/TOV"] = round(full_dataset["AST/TOV"], 1)
    full_dataset["AST/TOV"] = full_dataset["AST/TOV"].astype(float).round(1)

    # Add std_areas_FGA
    fga_area_columns = full_dataset[
        ["%FGA 0-3", "%FGA 3-10", "%FGA 10-16", "%FGA 16-3P", "%3PA"]
    ]

    full_dataset["std_areas_FGA"] = fga_area_columns.apply(
        lambda row: np.std(row), axis=1
    )
    full_dataset["std_areas_FGA"] = round(full_dataset["std_areas_FGA"], 2)

    full_dataset.iloc[5:, :] = full_dataset.iloc[5:, :].round(1)
    full_dataset = full_dataset.drop(["", "Att.", "#"], axis=1)
    # Create a copy of the original DataFrame
    full_dataset_ranked = full_dataset.copy()

    # Select columns from the 4th onwards
    columns_to_rank = full_dataset_ranked.columns[3:]

    # Iterate over the selected columns and calculate the ranked values
    for column in columns_to_rank:
        # Convert column values to numeric, ignoring errors
        column_data = pd.to_numeric(full_dataset_ranked[column], errors="coerce")
        # Replace missing or non-numeric values with NaN
        column_data = column_data.fillna(0)
        # Calculate the percentile ranks for the column and reverse the ranking order
        percentile_ranks = 1 - column_data.rank(pct=True)
        # Calculate the range numbers based on the reversed percentile ranks
        range_numbers = (percentile_ranks * 10).astype(int) + 1
        # Create a new column with the ranked values next to the original column
        new_column_name = f"{column} ranked"
        full_dataset_ranked.insert(
            full_dataset_ranked.columns.get_loc(column) + 1,
            new_column_name,
            range_numbers,
        )

    # Add profiles
    full_dataset.insert(3, "Offensive Profile", "")
    full_dataset.insert(4, "Defensive Profile", "")

    for index, row in full_dataset.iterrows():
        # Assign offensive profile to a player
        full_dataset.at[index, "Offensive Profile"] = offensive_profile(
            row, full_dataset_ranked.loc[index]
        )
        # Assign defensive profile to a player
        full_dataset.at[index, "Defensive Profile"] = defensive_profile(
            row, full_dataset_ranked.loc[index]
        )

    return full_dataset, full_dataset_ranked


def export_data_to_csv(start_year, full_dataset, full_dataset_ranked):
    full_dataset.to_csv(
        f"/home/lucas/Data Science/Project NBA/datasets/combined/regular_dataset_{start_year}_{start_year + 1}.csv"
    )
    full_dataset_ranked.to_csv(
        f"/home/lucas/Data Science/Project NBA/datasets/combined/ranked_dataset_{start_year}_{start_year + 1}.csv"
    )


# def export_csv_to_google_cloud(start_year, full_dataset, full_dataset_ranked, bucket):

#     client = storage.Client()
#     bucket = client.get_bucket(bucket_name)

#     blob_full_dataset = bucket.blob(f"regular_dataset_{start_year}_{start_year + 1}.csv")
#     blob_full_dataset.upload_from_string(full_dataset.to_csv(index=False), content_type='text/csv')

#     blob_full_dataset_ranked = bucket.blob(f"ranked_dataset_{start_year}_{start_year + 1}.csv")
#     blob_full_dataset_ranked.upload_from_string(full_dataset_ranked.to_csv(index=False), content_type='text/csv')

def download_duckdb_database(bucket_name, db_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(db_name)
    print(blob)
    temp_dir = tempfile.TemporaryDirectory()
    file_path = os.path.join(temp_dir.name, db_name)
    print('path etl =', file_path)
    blob.download_to_filename(file_path)
    
    return file_path

def download_csv_from_bucket(bucket_name, source_blob_name):
    """Downloads a file from a Google Cloud Storage bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    #if os.path.exists(destination_file_name):
        #os.remove()
    file = blob.download_as_string()

    return file


def upload_to_bucket(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the Google Cloud Storage bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    if blob.exists():
        blob.delete()
    blob.upload_from_filename(source_file_name, content_type="text/csv")

def check_file_exists(bucket_name, source_file_name):
    """Checks if a file already exists in the bucket"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_file_name)
    return blob.exists()

if __name__ == "__main__":
    # Functions to assign the offensive and defensive roles
    subprocess.run(["python3", "Categorization.py"])

    # Create directory if it doesn't exist
    directory = "/home/lucas/Data Science/Project NBA/datasets/combined"
    if not os.path.exists(directory):
        os.makedirs(directory)

    for year in range(START_YEAR, END_YEAR):
        regular_df, advanced_df, shooting_splits_df = create_yearly_dataframes(year - 1)
        full_dataset, full_dataset_ranked = transform_data(
            regular_df, advanced_df, shooting_splits_df
        )

        upload_to_bucket(
            "nba_dashboard_files",
            f"/home/lucas/Data Science/Project NBA/datasets/combined/regular_dataset_{year - 1}_{year}.csv",
            f"regular_dataset_{year - 1}_{year}.csv",
        )
        upload_to_bucket(
            "nba_dashboard_files",
            f"/home/lucas/Data Science/Project NBA/datasets/combined/ranked_dataset_{year - 1}_{year}.csv",
            f"ranked_dataset_{year - 1}_{year}.csv",
        )

    process_glossary_file("docs/glossary.txt", "docs/filtered_glossary.txt")

    print("Extraction and transformation executed")
