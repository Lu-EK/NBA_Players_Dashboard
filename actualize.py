import datetime
import io
import os

import etl

year = datetime.date.today()
month = datetime.datetime.now()
current_year = year.year
current_month = month.month

file_path_ranked = f"/home/lucas/Data Science/Project NBA/datasets/combined/ranked_dataset_{current_year - 1}_{current_year}.csv"
file_path_regular = f"/home/lucas/Data Science/Project NBA/datasets/combined/regular_dataset_{current_year - 1}_{current_year}.csv"
blob_name_ranked = f"ranked_dataset_{current_year - 1}_{current_year}.csv"
blob_name_regular = f"regular_dataset_{current_year - 1}_{current_year}.csv"


## To be sure the stats on the November 10th 2025 are labelled with 2025-2026, as well as the stats on the March 10th 2026
if current_month >= 8:
    current_year = current_year + 1
regular_df, advanced_df, shooting_splits_df = etl.create_yearly_dataframes(current_year)
full_dataset, full_dataset_ranked = etl.transform_data(
    regular_df, advanced_df, shooting_splits_df
)

if os.path.exists(file_path_regular):
    os.remove(file_path_regular)

if os.path.exists(file_path_ranked):
    os.remove(file_path_ranked)

etl.export_data_to_csv(current_year - 1, full_dataset, full_dataset_ranked)

etl.upload_to_bucket(
    "nba_dashboard_files",
    file_path_regular,
    blob_name_regular,
)

etl.upload_to_bucket(
    "nba_dashboard_files",
    file_path_ranked,
    blob_name_ranked,
)

print(f"upload for the year {current_year} done")
