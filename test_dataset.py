#!/usr/bin/env python
# coding: utf-8

# In[77]:


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



# In[78]:


# In the case of multiples occurrence of a player, we only keep
regular_dataset_2023 = regular_dataset_2023[~regular_dataset_2023['Rk'].duplicated(keep='first')]
advanced_dataset_2023 = advanced_dataset_2023[~advanced_dataset_2023['Rk'].duplicated(keep='first')]


# In[79]:


regular_dataset_2023


# In[80]:


advanced_dataset_2023


# In[81]:


regular_dataset_2023.columns


# In[82]:


advanced_dataset_2023.columns


# In[83]:


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


# In[84]:


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


# In[85]:


def add_return_to_lines(glossary_file):
    # Read the contents of the file
    with open(glossary_file, 'r') as file:
        lines = file.readlines()

    # Modify each line to add a return at the end
    modified_lines = [line.strip() + '\n' for line in lines]

    # Write the modified contents back to the file
    with open(glossary_file, 'w') as file:
        file.writelines(modified_lines)


# In[86]:


filtered_glossary = get_glossary('glossary.txt', full_dataset_2024.columns)
filtered_glossary


# In[87]:


def write_filtered_glossary_to_file(filtered_glossary, output_file):
    with open(output_file, 'w') as file:
        for line in filtered_glossary:
            file.write(line)

write_filtered_glossary_to_file(filtered_glossary, 'filtered_glossary.txt')


# In[88]:


# Remove players that played less than 24MPG so far, and less than 25 games
full_dataset_2024 = full_dataset_2024[(full_dataset_2024['MP'] >= 24) & (full_dataset_2024['G'] >= 25)]

# Round numbers
full_dataset_2024.iloc[:, 5:] = full_dataset_2024.iloc[:, 5:].round(2)


# In[89]:


full_dataset_2024 = full_dataset_2024.reset_index()
full_dataset_2024


# - Multiples entries for a single player --> Happens when a player has been traded, line with ['Tm'] = TOT keeped among all lines
# - Removed player MPG < 24 and G < 25
# - Concatenated standard stats with advanced stats 

# In[90]:


#Extract and tranform shootings %FGA to cocnat with the full dataset

shooting_splits_2023 = pd.read_csv('shooting_splits_2023.csv')
shooting_splits_2023.columns = shooting_splits_2023.iloc[0]
shooting_splits_2023 = shooting_splits_2023.drop(shooting_splits_2023.index[0])
shooting_splits_2023 = shooting_splits_2023[(shooting_splits_2023['MP'].astype(int) / shooting_splits_2023['G'].astype(int) >= 24) & (shooting_splits_2023['G'].astype(int) >= 25)].reset_index()
shooting_splits_2023


# In[91]:


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
full_dataset_2024v2


# In[92]:


full_dataset_2024v2[full_dataset_2024v2['Player'] == 'Kevin Durant']


# In[93]:


# Get the mean of every value for analysis needs

# Select only the columns that are numeric for mean calculation
numeric_columns = full_dataset_2024v2.iloc[:, :].select_dtypes(include='number')

# Calculate the mean
avg = numeric_columns.mean().round(1)

# Assign the calculated mean to a new row called 'Average'
full_dataset_2024v2.loc['Average'] = avg

# Display the updated DataFrame
full_dataset_2024v2


# In[94]:


# check if the mean is correct with an other sample
full_dataset_2024v2.insert(21, 'AST/TOV', (full_dataset_2024v2['AST']/full_dataset_2024v2['TOV']))
full_dataset_2024v2['AST/TOV'] = round(full_dataset_2024v2['AST/TOV'], 1)
full_dataset_2024v2


# In[95]:


# if std_areas_FGA is low -> shots are more likely to be evenly splitted into the 5 distances than is the std_areas_FGA is high

fga_area_columns = full_dataset_2024v2.iloc[:, -5:]
full_dataset_2024v2['std_areas_FGA'] = fga_area_columns.apply(lambda row:np.std(row), axis=1)
full_dataset_2024v2['std_areas_FGA'] = round(full_dataset_2024v2['std_areas_FGA'], 1)
full_dataset_2024v2


# In[96]:


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

# Display the updated dataset
full_dataset_ranked_2024v2


# In[97]:


full_dataset_ranked_2024v2[full_dataset_ranked_2024v2['USG% ranked'] == 1].sort_values(by=['USG%'], ascending=False)


# In[98]:


full_dataset_2024v2[full_dataset_2024v2['%FGA 0-3'] > 50]


# In[99]:


full_dataset_2024v2.insert(3, 'Offensive Profile', 0)
full_dataset_2024v2.insert(4, 'Defensive Profile', 0)
# Round numbers
full_dataset_2024.iloc[:, 5:] = full_dataset_2024.iloc[:, 5:].round(2)
full_dataset_2024v2


# In[ ]:





# In[100]:


# Iterate over each row in the DataFrame
for index, row in full_dataset_2024v2.iterrows():
    # Call offensive_profile function and assign the result to 'Offensive Profile' column
    full_dataset_2024v2.at[index, 'Offensive Profile'] = offensive_profile(row, full_dataset_ranked_2024v2.loc[index])
    # Call defensive_profile function and assign the result to 'Defensive Profile' column
    full_dataset_2024v2.at[index, 'Defensive Profile'] = defensive_profile(row, full_dataset_ranked_2024v2.loc[index])


# In[101]:


full_dataset_2024v2


# In[102]:


full_dataset_2024v2[full_dataset_2024v2['Offensive Profile'] == 'Creator/Facilitator']


# In[103]:


full_dataset_ranked_2024v2[full_dataset_ranked_2024v2['Player'] == 'Klay Thompson']


# In[104]:


full_dataset_2024v2[full_dataset_2024v2['Offensive Profile'] == 'Pure Shooter/Stretcher']


# In[105]:


full_dataset_2024v2[full_dataset_2024v2['Offensive Profile'] == 'Paint Threat']


# In[106]:


full_dataset_2024v2[full_dataset_2024v2['Offensive Profile'] == 'Slasher']


# Paint Threat et Slasher en concurrence 

# In[107]:


full_dataset_2024v2[full_dataset_2024v2['Offensive Profile'] == 'Versatile Scorer']


# In[108]:


full_dataset_2024v2[full_dataset_2024v2['Offensive Profile'] == 'No significant offensive role']


# In[109]:


full_dataset_2024v2[full_dataset_2024v2['Player'] == 'Klay Thompson']


# In[110]:


full_dataset_2024v2.to_csv('full_dataset_2024v2.csv')
full_dataset_ranked_2024v2.to_csv('full_dataset_ranked_2024v2.csv')


# In[111]:


ranking_east_2024 = pd.read_csv("ranking_east_2023.csv")
ranking_west_2024 = pd.read_csv("ranking_west_2023.csv")


# In[112]:


ranking_west_2024


# In[ ]:




