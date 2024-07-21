# Extract: Process to pull data from Source system
# Load: Process to write data to a destination system

# Common upstream & downstream systems
# OLTP Databases: Postgres, MySQL, sqlite3, etc
# OLAP Databases: Snowflake, BigQuery, Clickhouse, DuckDB, etc
# Cloud data storage: AWS S3, GCP Cloud Store, Minio, etc
# Queue systems: Kafka, Redpanda, etc
# API
# Local disk: csv, excel, json, xml files
# SFTP\FTP server

# Databases: When reading or writing to a database we use a database driver. Database drivers are libraries that we can use to read or write to a database.
# Question: How do you read data from a sqlite3 database and write to a DuckDB database?
# Hint: Look at importing the database libraries for sqlite3 and duckdb and create connections to talk to the respective databases

##################################################################### Option 1



print("Question 1 >> Option 1")
# Using dataframe and ingesting is the recommended method
# Fetch data from the SQLite Customer table into df1
import sqlite3
import pandas as pd
import duckdb

# Connect to the SQLite database
sqlite_conn = sqlite3.connect('example.sqlitedb')

# # create cursor object
# sqlite_cur = sqlite_conn.cursor()

# # Fetch data from the SQLite Customer table
# sqlite_cur.execute("SELECT * FROM Customer")
# rows = sqlite_cur.fetchall()
# print(type(rows))

select_query = "SELECT * FROM Customer"
df1 = pd.read_sql_query(select_query, sqlite_conn)

# close the sqlite conn
sqlite_conn.close()


# connect to duckdb example database
duckdb_conn = duckdb.connect(database='example.duckdb', read_only=False)

# Insert data into the DuckDB Customer table from df1
duckdb_conn.execute("INSERT INTO example.sqllite_example.customer SELECT * FROM df1")


# Hint: Look for Commit and close the connections
# Commit tells the DB connection to send the data to the database and commit it, if you don't commit the data will not be inserted

# We should close the connection, as DB connections are expensive
# Verify insertion by querying tableB
result = duckdb_conn.execute("SELECT * FROM example.sqllite_example.customer").df()
print(result)

# Close the DuckDB connection
duckdb_conn.close()


###################################################################### Option 2
print("Question 1 >> Option 2")
import sqlite3 
import duckdb

# Connect to the SQLite database
sqlite_conn = sqlite3.connect('example.sqlitedb')

select_query = "SELECT * FROM Customer"

# Fetch data from the SQLite Customer table
data_from_sqlite = sqlite_conn.execute(select_query).fetchall()


# Close the SQLite connection
sqlite_conn.close()

# create duckdb connection
duckdb_conn = duckdb.connect(database='example.duckdb', read_only=False)


# Insert data into the DuckDB Customer table
insertdata = "INSERT INTO example.sqllite_example.customer VALUES (?, ?, ?, ?, ?, ?)"
duckdb_conn.executemany(insertdata, data_from_sqlite)


# Commit the transaction
duckdb_conn.commit()

# Verify insertion by querying customer table from duckdb
result = duckdb_conn.execute("SELECT * FROM example.sqllite_example.customer").fetchall()
for row in result:
    print(row)

# Close the DuckDB connection
duckdb_conn.close()





# Cloud storage
# Question: How do you read data from the S3 location given below and write the data to a DuckDB database?
# Data source: https://docs.opendata.aws/noaa-ghcn-pds/readme.html station data at path "csv.gz/by_station/ASN00002022.csv.gz"
# Hint: Use boto3 client with UNSIGNED config to access the S3 bucket
# Hint: The data will be zipped you have to unzip it and decode it to utf-8

# AWS S3 bucket and file details
bucket_name = "noaa-ghcn-pds"
file_key = "csv.gz/by_station/ASN00002022.csv.gz"
# Create a boto3 client with anonymous access

# Download the CSV file from S3
# Decompress the gzip data
# Read the CSV file using csv.reader
# Connect to the DuckDB database (assume WeatherData table exists)

# Insert data into the DuckDB WeatherData table

# API
# Question: How do you read data from the CoinCap API given below and write the data to a DuckDB database?
# URL: "https://api.coincap.io/v2/exchanges"
# Hint: use requests library

# Define the API endpoint
url = "https://api.coincap.io/v2/exchanges"

# Fetch data from the CoinCap API
# Connect to the DuckDB database

# Insert data into the DuckDB Exchanges table
# Prepare data for insertion
# Hint: Ensure that the data types of the data to be inserted is compatible with DuckDBs data column types in ./setup_db.py


# Local disk
# Question: How do you read a CSV file from local disk and write it to a database?
# Look up open function with csvreader for python

# Web scraping
# Questions: Use beatiful soup to scrape the below website and print all the links in that website
# URL of the website to scrape
url = 'https://example.com'
