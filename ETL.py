## Libraraies and modules

import os
import subprocess

import duckdb
import numpy as np
import pandas as pd

from Categorization import defensive_profile, offensive_profile

START_YEAR = 2020
END_YEAR = 2024

# Functions to assign the offensive and defensive roles
subprocess.run(["python3", "Categorization.py"])

# Create directory if it doesn't exist
directory = "/home/lucas/Data Science/Project NBA/datasets/combined"
if not os.path.exists(directory):
    os.makedirs(directory)


def create_yearly_dataframes(year):
    # File paths
    regular_dataset_path = f"datasets/regular/regular_dataset_{year}_{year + 1}.csv"
    advanced_dataset_path = f"datasets/advanced/advanced_dataset_{year}_{year + 1}.csv"
    shooting_splits_path = f"datasets/shootings_splits/SP_{year}_{year + 1}.csv"

    # Read CSV files
    regular_df = pd.read_csv(regular_dataset_path)
    advanced_df = pd.read_csv(advanced_dataset_path)
    shooting_splits_df = pd.read_csv(shooting_splits_path)

    return regular_df, advanced_df, shooting_splits_df


## Functions


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
    regular_dataset = regular_dataset[~regular_dataset["Rk"].duplicated(keep="first")]
    advanced_dataset = advanced_dataset[
        ~advanced_dataset["Rk"].duplicated(keep="first")
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
    shooting_splits.columns = shooting_splits.iloc[0]
    shooting_splits = shooting_splits.drop(shooting_splits.index[0])
    # shooting_splits = shooting_splits[(shooting_splits['MP'].astype(int) / shooting_splits['G'].astype(int) >= 24) & (shooting_splits['G'].astype(int) >= 25)].reset_index()

    # Remove players that played less than 24MPG so far, and less than 25 games
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
    full_dataset.drop(
        columns=["#", "%FGA", "%3PA", "Att.", "player_code"], inplace=True
    )
    full_dataset = full_dataset.loc[:, ~full_dataset.columns.duplicated()]
    full_dataset.iloc[:, -4:] = full_dataset.iloc[:, -4:].apply(
        lambda x: x.astype(float) * 100
    )
    full_dataset["%FGA 3P"] = 100 - full_dataset.iloc[:, -4:].sum(axis=1)

    # Select only the columns that are numeric for mean calculation
    numeric_columns = full_dataset.iloc[:, :].select_dtypes(include="number")
    full_dataset.iloc[5:, :] = full_dataset.iloc[5:, :].round(1)
    # Calculate the mean
    # avg = numeric_columns.mean().round(1)
    # full_dataset.loc['Average'] = avg

    # add AST/TOV
    full_dataset.insert(21, "AST/TOV", (full_dataset["AST"] / full_dataset["TOV"]))
    full_dataset["AST/TOV"] = round(full_dataset["AST/TOV"], 1)

    # Add std_areas_FGA
    fga_area_columns = full_dataset.iloc[:, -5:]
    full_dataset["std_areas_FGA"] = fga_area_columns.apply(
        lambda row: np.std(row), axis=1
    )
    full_dataset["std_areas_FGA"] = round(full_dataset["std_areas_FGA"], 1)

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
    full_dataset.insert(3, "Offensive Profile", 0)
    full_dataset.insert(4, "Defensive Profile", 0)

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

    # return full_dataset, full_dataset_ranked


for year in range(START_YEAR, END_YEAR):
    regular_df, advanced_df, shooting_splits_df = create_yearly_dataframes(year)
    # Assigning DataFrames with custom names
    # globals()[f'regular_dataset_{year}_{year+1}'] = regular_df
    # globals()[f'advanced_dataset_{year}_{year+1}'] = advanced_df
    # lobals()[f'shooting_splits_{year}_{year+1}'] = shooting_splits_df
    full_dataset, full_dataset_ranked = transform_data(
        regular_df, advanced_df, shooting_splits_df
    )
    export_data_to_csv(year, full_dataset, full_dataset_ranked)

process_glossary_file("docs/glossary.txt", "docs/filtered_glossary.txt")

print("Extraction and transformation executed")
