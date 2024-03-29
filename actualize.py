import datetime
from ETL import dataset_players, create_yearly_dataframes, upload_to_bucket

year = datetime.date.today()
month = datetime.datetime.now()
current_year = year.year
current_month = month.month

## To be sure the stats on the November 10th 2025 are labelled with 2025-2026, as well as the stats on the March 10th 2026
if current_month >= 8:
    current_year = current_year + 1
regular_df, advanced_df, shooting_splits_df = create_yearly_dataframes(current_year)
full_dataset, full_dataset_ranked = transform_data(
    regular_df, advanced_df, shooting_splits_df
)
upload_to_bucket(
    "nba_dashboard_files",
    f"/home/lucas/Data Science/Project NBA/datasets/combined/regular_dataset_{current_year - 1}_{current_year}.csv",
    f"regular_dataset_{current_year - 1}_{current_year}.csv",
)
upload_to_bucket(
    "nba_dashboard_files",
    f"/home/lucas/Data Science/Project NBA/datasets/combined/ranked_dataset_{current_year - 1}_{current_year}.csv",
    f"regular_dataset_{current_year - 1}_{current_year}.csv",
)
