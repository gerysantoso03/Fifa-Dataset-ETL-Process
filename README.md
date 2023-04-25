
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

##  Technology Used
```
- Pandas
- Numpy
- Sqlite3
- Polars
- Jupyter Notebook
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

### 2. Extract Datasets

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

### 5. Obtain Football Player FullName 

If we look into playerUrl column, we can observe every URl inside this column acccomodate fullname of each football player. When we look at the original dataset, it had ```Longname``` and ```Name``` columns. Using these two columns to obtain fullname of the players seems not a good idea, because ```Name``` column had shortened first name of the players and ```LongName``` had special characters on the value. Hence, ```playerUrl``` column was a great idea to achieve fullname of every players. ```playerUrl``` column was split using ```/``` and then we retained and reformat the values.

```
# Show playerUrl column
fifa_df['playerUrl']

# Split value into multiple columns based on '/' character
fifa_df['playerUrl'] = fifa_df['playerUrl'].str.split('/', expand=True).iloc[:, 5]

# Remove '-' between player name and capitalize each word
fifa_df['playerUrl'] = fifa_df['playerUrl'].str.replace('-', " ").str.title()

# Rename playerUrl column into FullName 
fifa_df.rename(columns = {'playerUrl': 'FullName'}, inplace=True)

# Show DataFrame
display(fifa_df)
```

### 6. Height & Weight Columns

```Height``` and ```Weight``` columns both already had consistent value, but we must correct the data type of the columns and uniform the format. ```Height``` column had ```cm``` as the main format, so if height value is not in ```cm```, we must convert it. As well as ```Weight``` column, this column has ```kg``` as the main format, so if we obtained value that doesn't fit the main format, the value must be convert into ```kg```. 

#### Height Column
```
# Show Height column
fifa_df['Height']

# Convert feet and inch into cm 
def convert_height(height):
    if "'" in height:
        feet, inch = height.split("'")
        total_height = (int(feet) * 12 + int(inch[:-1])) * 2.54
    else:
        total_height = int(height.replace("cm", ""))
    return total_height
    
fifa_df['Height'] = fifa_df['Height'].apply(convert_height)

fifa_df['Height'] = fifa_df['Height'].astype('int64')

fifa_df['Height']
```

#### Weight Column
```
# Show Weight column
fifa_df['Weight']

# Convert weight from lbs into kg
def convert_weight(weight):
    if weight.endswith('lbs') == True:
        total_weight = round(int(weight.replace('lbs', '')) * 0.454, 1)
    else:
        total_weight = int(weight.replace('kg', ''))
    return total_weight
    
fifa_df['Weight'] = fifa_df['Weight'].apply(convert_weight)

fifa_df['Weight']
```

### 7. Club Column

```Club``` column had ```/n``` as a prefix of every values. So to get rid of it, we can use ```pd.str.strip``` method. 

```
# Remove /n inside Club column
fifa_df['Club'] = fifa_df['Club'].str.strip('\n')

# Show club column
fifa_df['Club']
```

### 8. OVA, BOT, POT Columns

```BOV, POT, OVA``` columns had string as the data type, because these columns had numerical data type as the value, we must reformat the data type into numeric and in this process we convert those columns into percentage value  

```
# Show OVA,POT,BOV Columns
fifa_df[['↓OVA', 'POT', 'BOV']]

# Change data type for those 3 columns
fifa_df[['↓OVA', 'BOV', 'POT']] = fifa_df[['↓OVA', 'BOV', 'POT']].astype('int64')

# Rename column
fifa_df.rename(columns = {'↓OVA': 'OVA'}, inplace=True)

# Divide values inside these columns by 100 to show percentage
fifa_df.loc[:, ['OVA', 'POT', 'BOV']] = fifa_df.loc[:, ['OVA', 'POT', 'BOV']] / 100
```

### 9. Price Columns
Price columns had ```Value, Wage, and Release Clause``` as the values. To clean these columns, we must get rid of the special characters and convert it into numerical data type and show correct values of each symbol they had. For example K as thousand, M as million, and B as billion. 

```
# Create columns that contain price column
price_columns = ['Value', 'Wage', 'Release Clause']

# Remove '€' inside price columns
for x in price_columns:
    fifa_df[x] = fifa_df[x].str.strip('€')

# Create function to convert price columns
def convert_price(x):
    if x.endswith('M'):
        x = pd.to_numeric(x[:-1]) * 1000000
    elif x.endswith('K'):
        x = pd.to_numeric(x[:-1]) * 1000
    else:
        x
    
    return x

# Convert price columns 
for x in price_columns:
    fifa_df[x] = fifa_df[x].apply(convert_price)

fifa_df[price_columns] = fifa_df[price_columns].astype('int64')
```

### 10. Contract Column
Contract column provide information about contract period of the players. Contract had inconsistent data type, this column also had 3 categories of values in the format '21st June 2021 On Loan', 'Free', and '2017 ~ 2021'. To get rid of this issue, i had split this column with ```~``` into two new columns ```Contract Start``` and ```Contract End```. But this method only work for ```2017 ~ 2021``` this category. Therefor to fix the other two categories, i used ```loc``` method to get all rows that contain ```On Loan``` and ```Free``` value. For ```On Loan``` value, i store the the loan year into ```Contract Start``` column  and give 0 on ```Contract End``` column. For the last category, 0 value was stored to both ```Contract Start``` and ```Contract End``` columns. 

```
# Show Contract columns
fifa_df['Contract']

# Split contract column into 2 columns, we name it Contract Start and Contract End
fifa_df[['Contract Start', 'Contract End']] = fifa_df['Contract'].str.split(' ~ ', expand=True)

# Check Contract Start column that contain 'On Loan' value 
fifa_df.loc[fifa_df['Contract Start'].str.contains('On Loan'), 'Contract Start']

# Check Contract Start column that contain 'Free' value
fifa_df.loc[fifa_df['Contract Start'].str.contains('Free'), 'Contract Start']

fifa_df.loc[fifa_df['Contract End'].isnull(), 'Contract End']

# Fill Contract Start column that contain 'On Loan' value with loan year
fifa_df.loc[fifa_df['Contract'].str.contains('On Loan'), 'Contract Start'] = fifa_df.loc[fifa_df['Contract Start'].str.contains('On Loan'), 'Contract Start'].str[-12:-8]

# Fill Contract Start column that contain 'Free' value with 0 
fifa_df.loc[fifa_df['Contract Start'].str.contains('Free'), 'Contract Start'] = 0

# Check Contract Start column doesn't contain any null value
fifa_df['Contract Start'].isnull().sum()

# Check whether Contract End column contain any null value
fifa_df['Contract End'].isnull().sum()

# Locate index on Contract End column that contain null value
fifa_df.loc[fifa_df['Contract End'].isnull(), 'Contract End']

# Fill null value in Contract End column with 0
fifa_df.loc[fifa_df['Contract End'].isnull(), 'Contract End'] = 0

# Re-check null values inside Contract End column 
fifa_df['Contract End'].isnull().sum()
```

### 11. Contract Category
From ```Contract Start``` and ```Contract End``` columns, i make another one column that provide the category of the contract, so the audience will know type of contract each players had. 

```
# Function to create contract category
def create_contract_category(contract_start, contract_end):
    if contract_start != 0 and contract_end == 0:
        contract_category = 'Loan'
    elif contract_start == 0 and contract_end == 0:
        contract_category = 'Free'
    else:
        contract_category = 'Contract'
    
    return contract_category
    
# Create new column called 'Contract Category' to store the category of contract columns
fifa_df['Contract Category'] = fifa_df.apply(lambda row: create_contract_category(row['Contract Start'], row['Contract End']), axis=1)

# Check if contract category successfully created
display(fifa_df)
```

### 12. Hits Column
Hits column had null values, so i fill it with 0. I think 0 will suit to this case, because hits represent the value for each players and every players must have different hits value based on their performance. So if i fill null value with median or max value of the column, i think it will ruined all of the values and can lead into misscalculation and analysis. 

```
# Show Hits column
fifa_df['Hits']

# Check null value inside Hits column
fifa_df['Hits'].isnull().sum()

# Store 0 into Hits column that contain null value
fifa_df.loc[fifa_df['Hits'].isnull(), 'Hits'] = 0

# Re-check null value inside Hits column
fifa_df['Hits'].isnull().sum()

```


### 13. Load Clean Dataset Into Database
i used ```sqlite3``` library as the Database to store clean dataset that have been processed before.

#### Make Connections and Create Table

```
conn = sqlite3.connect('Database/session.db')

print(conn)

try:
    conn.execute('DROP TABLE IF EXISTS `Fifa`')
except Exception as e:
    raise(e)
finally:
    print('Table dropped')
    
try:
    conn.execute("""
         CREATE TABLE Fifa
        (
            ID    INTEGER PRIMARY KEY,
            Fullname TEXT NOT NULL,
            Height  INTEGER,
            Weight  Float DEFAULT 0, 
            Age  INTEGER DEFAULT 0,
            Nationality TEXT NOT NULL,
            Club TEXT NOT NULL,
            Best_position TEXT NOT NULL,
            BOV Float DEFAULT 0,
            OVA Float DEFAULT 0,
            POT Float DEFAULT 0,
            Value Float DEFAULT 0,
            Wage Float DEFAULT 0,
            Release_clause Float DEFAULT 0,
            Contract_start TEXT NOT NULL,
            Contract_end TEXT NOT NULL,
            Contract_category TEXT NOT NULL
        );
    """)
    print('Table successfully created')
except Exception as e:
    print(str(e))
    print('Table failed to create')
finally:
    conn.close()
```

#### Insert Clean Dataset Into Table

```
# Insert our data into our table 
fifa_df_list = clean_fifa_df.values.tolist()

conn = sqlite3.connect('Database/session.db')

curr = conn.cursor()

try:
    curr.executemany("""
        INSERT INTO Fifa 
        (
            ID, Fullname, Height, Weight, Age, Nationality, Club, Best_position, 
            BOV, OVA, POT, Value, Wage, Release_clause, Contract_start, Contract_end, Contract_category 
        ) 
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, fifa_df_list)
    conn.commit()
    print('Data successfully inserted')
except Exception as e:
    print(str(e))
    print('Data insertion failed')
finally:
    conn.close()
```

#### Check If The Data Successfully Stored Into Database
sql_query = """
    SELECT * FROM Fifa LIMIT 1;
"""

conn = sqlite3.connect('Database/session.db')

cursor = conn.cursor()

try:
    cursor.execute(sql_query)
    data = cursor.fetchall()
    print("Data successfully obtained")
except Exception as e:
    print(str(e))
    print('Data fail to obtained')
finally:
    print(data)
    conn.close()
```

