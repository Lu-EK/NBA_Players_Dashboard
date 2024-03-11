#!/usr/bin/env python
# coding: utf-8

# In[23]:


import pandas as pd
import numpy as np
import duckdb
get_ipython().run_line_magic('run', 'Categorization.ipynb')

# in comparison to today's stats, statistics may have vary due to the season being on-going
pd.set_option('display.max_columns', None)

START_YEAR = 2020
END_YEAR = 2024

def create_yearly_dataframes(year):
    # File paths
    regular_dataset_path = f'datasets/regular/regular_dataset_{year}_{year + 1}.csv'
    advanced_dataset_path = f'datasets/advanced/advanced_dataset_{year}_{year + 1}.csv'
    shooting_splits_path = f'datasets/shootings_splits/SP_{year}_{year + 1}.csv'
    
    # Read CSV files
    regular_df = pd.read_csv(regular_dataset_path)
    advanced_df = pd.read_csv(advanced_dataset_path)
    shooting_splits_df = pd.read_csv(shooting_splits_path)

    return regular_df, advanced_df, shooting_splits_df

for year in range(START_YEAR, END_YEAR):
    regular_df, advanced_df, shooting_splits_df = create_yearly_dataframes(year)
    # Assigning DataFrames with custom names
    globals()[f'regular_dataset_{year}_{year+1}'] = regular_df
    globals()[f'advanced_dataset_{year}_{year+1}'] = advanced_df
    globals()[f'shooting_splits_{year}_{year+1}'] = shooting_splits_df


# In[12]:


# Functions to assign the offensive and defensive roles
get_ipython().run_line_magic('run', 'Categorization.ipynb')

# Display settings
pd.set_option('display.max_columns', None)


# In[13]:


## Functions

# Get the glossary of statistics
def get_glossary(glossary_file, columns):
    with open(glossary_file, 'r') as file:
        glossary_lines = file.readlines()
        
    filtered_lines = []

    for line in glossary_lines:
        if any(line.startswith(column) for column in columns):
            filtered_lines.append(line.strip() + '\n')  # Add newline character
        else:
            filtered_lines.append(line.strip() + '\n')  # Add newline character for all lines
        
    return filtered_lines

def add_return_to_lines(glossary_file):
    # Read the contents of the file
    with open(glossary_file, 'r') as file:
        lines = file.readlines()

    # Modify each line to add a return at the end
    modified_lines = [line.strip() + '\n' for line in lines]

    # Write the modified contents back to the file
    with open(glossary_file, 'w') as file:
        file.writelines(modified_lines)
        
def write_filtered_glossary_to_file(filtered_glossary, output_file):
    with open(output_file, 'w') as file:
        for line in filtered_glossary:
            file.write(line)



# In[14]:


## DataFrames transformation

# In the case of multiples occurrence of a player, we only keep
regular_dataset_2023 = regular_dataset_2023[~regular_dataset_2023['Rk'].duplicated(keep='first')]
advanced_dataset_2023 = advanced_dataset_2023[~advanced_dataset_2023['Rk'].duplicated(keep='first')]

# concatenate the two tables, erasing the reccurent columns
start_columns = advanced_dataset_2023.columns.get_loc('PER')
columns_to_concat = advanced_dataset_2023.columns[start_columns:]
columns_to_concat
full_dataset_2024_raw = pd.concat([regular_dataset_2023, advanced_dataset_2023[columns_to_concat]], axis=1)

# clear the dataframe from duplicated columns
duplicated_columns = full_dataset_2024_raw.columns[full_dataset_2024_raw.columns.duplicated()]
full_dataset_2024 = full_dataset_2024_raw.drop(columns=duplicated_columns)
full_dataset_2024 = full_dataset_2024.drop(columns=['Unnamed: 24', 'Unnamed: 19'])
full_dataset_2024.columns

filtered_glossary = get_glossary('glossary.txt', full_dataset_2024.columns)
write_filtered_glossary_to_file(filtered_glossary, 'filtered_glossary.txt')


#Tranform shootings %FGA to cocnat with the full dataset

shooting_splits_2023.columns = shooting_splits_2023.iloc[0]
shooting_splits_2023 = shooting_splits_2023.drop(shooting_splits_2023.index[0])
shooting_splits_2023 = shooting_splits_2023[(shooting_splits_2023['MP'].astype(int) / shooting_splits_2023['G'].astype(int) >= 24) & (shooting_splits_2023['G'].astype(int) >= 25)].reset_index()
shooting_splits_2023


# In[15]:


# Remove players that played less than 24MPG so far, and less than 25 games
full_dataset_2024 = full_dataset_2024[(full_dataset_2024['MP'] >= 24) & (full_dataset_2024['G'] >= 25)]

# Round numbers
full_dataset_2024.iloc[:, 5:] = full_dataset_2024.iloc[:, 5:].round(2)

full_dataset_2024 = full_dataset_2024.reset_index()


# In[16]:


# Create the concatenated dataset with standard and advanced statistics. 

full_dataset_2024v2 = pd.merge(full_dataset_2024, shooting_splits_2023, on ='Player', how='left').reset_index()

stats_to_keep = ['Pos_x', '3P%_x']

cols_to_drop = [col for col in full_dataset_2024v2 if col not in stats_to_keep and isinstance(col, str) and (col.endswith('_x') or col.endswith('_y'))]

full_dataset_2024v2 = full_dataset_2024v2.drop(columns=cols_to_drop)
cols_to_keep = []

for col in full_dataset_2024v2.columns:
    if col not in cols_to_keep:
        cols_to_keep.append(col)
        
full_dataset_2024v2 = full_dataset_2024v2[cols_to_keep].dropna(axis=1, how='all')
full_dataset_2024v2.rename(columns={'3P%_x' : '3P%', 'Dist.' : 'Average distance (ft.) of FGA', '-9999' : 'player_code', '0-3' : '%FGA 0-3', '3-10' : '%FGA 3-10', '10-16' : '%FGA 10-16', '16-3P' : '%FGA 16-3P', 'Pos_x' : 'Pos'}, inplace=True)
full_dataset_2024v2.drop(columns=['#', '%FGA', '%3PA', 'Att.', 'player_code'],inplace=True)
full_dataset_2024v2 = full_dataset_2024v2.loc[:, ~full_dataset_2024v2.columns.duplicated()]
full_dataset_2024v2.iloc[:, -4:] = full_dataset_2024v2.iloc[:, -4:].apply(lambda x : x.astype(float) * 100)
full_dataset_2024v2['%FGA 3P'] = 100 - full_dataset_2024v2.iloc[:, -4:].sum(axis=1)

# add AST/TOV
full_dataset_2024v2.insert(21, 'AST/TOV', (full_dataset_2024v2['AST']/full_dataset_2024v2['TOV']))
full_dataset_2024v2['AST/TOV'] = round(full_dataset_2024v2['AST/TOV'], 1)
full_dataset_2024v2

# Select only the columns that are numeric for mean calculation
numeric_columns = full_dataset_2024v2.iloc[:, :].select_dtypes(include='number')

# Calculate the mean
avg = numeric_columns.mean().round(1)

full_dataset_2024v2.loc['Average'] = avg

# Add std_areas_FGA.
# if std_areas_FGA is low -> shots are more likely to be evenly splitted
# into the 5 distances than is the std_areas_FGA is high

fga_area_columns = full_dataset_2024v2.iloc[:, -5:]
full_dataset_2024v2['std_areas_FGA'] = fga_area_columns.apply(lambda row:np.std(row), axis=1)
full_dataset_2024v2['std_areas_FGA'] = round(full_dataset_2024v2['std_areas_FGA'], 1)
full_dataset_2024v2


# In[ ]:





# In[17]:


# Create a copy of the original DataFrame
full_dataset_ranked_2024v2 = full_dataset_2024v2.copy()

# Select columns from the 4th onwards
columns_to_rank = full_dataset_ranked_2024v2.columns[3:]

# Iterate over the selected columns and calculate the ranked values
for column in columns_to_rank:
    # Convert column values to numeric, ignoring errors
    column_data = pd.to_numeric(full_dataset_ranked_2024v2[column], errors='coerce')
    
    # Replace missing or non-numeric values with NaN
    column_data = column_data.fillna(0)
    
    # Calculate the percentile ranks for the column and reverse the ranking order
    percentile_ranks = 1 - column_data.rank(pct=True)
    
    # Calculate the range numbers based on the reversed percentile ranks
    range_numbers = (percentile_ranks * 10).astype(int) + 1
    
    # Create a new column with the ranked values next to the original column
    new_column_name = f'{column} ranked'
    full_dataset_ranked_2024v2.insert(full_dataset_ranked_2024v2.columns.get_loc(column) + 1, new_column_name, range_numbers)


# In[18]:


## Add profiles

full_dataset_2024v2.insert(3, 'Offensive Profile', 0)
full_dataset_2024v2.insert(4, 'Defensive Profile', 0)

for index, row in full_dataset_2024v2.iterrows():
    # Assign offensive profile to a player
    full_dataset_2024v2.at[index, 'Offensive Profile'] = offensive_profile(row, full_dataset_ranked_2024v2.loc[index])
    # Assign defensive profile to a player    
    full_dataset_2024v2.at[index, 'Defensive Profile'] = defensive_profile(row, full_dataset_ranked_2024v2.loc[index])


# In[19]:


#Export data

full_dataset_2024v2.to_csv('full_dataset_2024v2.csv')
full_dataset_ranked_2024v2.to_csv('full_dataset_ranked_2024v2.csv')


