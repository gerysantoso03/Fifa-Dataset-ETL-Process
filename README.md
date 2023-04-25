
# ETL Process Fifa Dataset

In this project i will show you my journey in learning ETL process with python on the messy Fifa dataset. 

## About the dataset
The dataset used for this project is Fifa 21 Data. It was sourced from Kaggle and can be accessed [here](https://www.kaggle.com/datasets/yagunnersya/fifa-21-messy-raw-dataset-for-cleaning-exploring). The datasets was obtained in its raw state with web scrapping from sofia.com. It contains the details of football player along with their performance, updated until 2021. The datasets had 77 column and 18979 row of football player data, but on this project i only use 16 Column that represent the most frequently observed values from football players, based on me 😄. 

## Columns used
```
- ID 
- playerUrl 
- Nationality  
- Age 
- Height 
- Weight 
- Club 
- OVA
- POT 
- BOV 
- Best Position
- Contract
- Value
- Wage
- Release Clause
- Hits
```

## Objective

From the datasets, here are the objectives to be achieved from the ETL process performed:
-  Ensure that all the columns have correct data type
- Numerical columns should be in correct format for further calculation and analysis
- Remove null values from the columns
- Uniform value format inside columns
- Remove special characters
- Store clean dataset into database

## ETL Process

### 1. Install and Load Library
I have two library options to load the datasets, ```Pandas``` and ```Polars```. But in this project, i preferred to use ```pandas``` as the main library to work with DataFrame. 

#### Install Library
```
# Install package
!pip install polars
!pip install pyarrow
```

#### Import Library
```
# Import package
import pandas as pd
import numpy as np
import sqlite3 
import polars as pl # Second alternative for pandas DataFrame
```

### 2. Load Datasets

```
# Extract data with pandas
raw_data = pd.read_csv('Files/fifa21 raw data v2.csv',
                       dtype="str",
                       usecols=['ID', 'playerUrl', 'Nationality', 'Age', '↓OVA', 'POT', 'Club', 'Contract', 'Height', 'Weight', 
                                 'BOV', 'Best Position', 'Value', 'Wage', 'Release Clause', 'Hits'])
```

### 3. Take 500 raw sample from the dataset

```
fifa_df = raw_data.sample(500)
```

### 4. ID Column
ID column represent the unique value of each football player. Because the DataFrame contain 500 sample from the raw data, the ID column doesn't sequential. So ```reset_index``` method will be fit to reset the index, therefor index column will show in ascending order. 

```
# Reset index of DataFrame
fifa_df.reset_index(inplace=True, drop=True)
```





