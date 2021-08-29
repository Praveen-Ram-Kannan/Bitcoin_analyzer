# Bitcoin_analyzer

Analyze successful bitcoin trades using the given dataset provided.

Dataset : https://storage.googleapis.com/zalora-interview-data/bitstampUSD_1-min_data_2012-01-01_to_2020-09-14.csv

## Project Setup

Run sh run_bitcoin_analyzer.sh  

creates and activates the virtual environment bitcoin-venv, which in turn installs the packages
mentioned in the requirements.txt file.

or

Run python src/bit_coin_analysis.py which in turn runs src/data_extraction.py for Data extraction

## Approach

The entire process is break down in to 2 steps.
1. Data Extraction
2. Data Analysis

### Data Extraction

Data Extraction process is implemented in src/data_extraction.py.

#### Approach

1. Split the entire data into 100 MB chunks
2. Since the file is arround 280 MB, used 3 threads to run in parallel that fetches 100 MB of data each
3. Headers used in the get request is created in such a way that the data is not duplicated
4. Once the chunk is fetched, the chunk is written to the output data file in parallel

### Data Analysis

Analyse the find successful trades that can happen with the given Bitcoin dataset

#### Approach

1. After the given dataset is downloaded to the local, a spark dataframe is created with that.

2. Data cleanup - Removal of all the outliers(Data that is not useful for the prediction of
successful trades.

3. Transformations - Transformations needed such as Timestamp to date time(To fetch all the successful
trades at the day level)

4. Data prep - Inorder to find the successful trades, the minimum and maximum price of bitcoin is
captured on the day level

5. Fetch successful trades - The successful trades for the given dataset is captured based on
following assumptions:
i. Bitcoin is purchased at the Min value on the first day and sold if the Max value on the following
days are larger than the inital value.
ii. Bitcoin is purchased at the Min value on the first day and sold if the Min value on the following
days are larger than the inital value.
iii. Bitcoin is purchased at the Max value on the first day and sold if the Max value on the following
days are larger than the inital value.
iv. Bitcoin is purchased at the Max value on the first day and sold if the Min value on the following
days are larger than the inital value.

6. Once the successful trades are captured, it is written to the output csv file

## Architecture

The Arcitecture is developed using python and pyspark.

Since the source dataset is very large, pyspark works well for the analysis of Bigdata.

The spark code is executed on the databricks cluster.

Databricks notebook file reference : src/bit_coin_analysis.dbc