import sqlite3
import csv
import os

'''
Creating a SQLite3 database called example and create a table called customers.
then insert data from the customers.csv file into the customers table.
'''



# delete existing database
def del_existing_db(db_path):
    # Delete file if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"{db_path} has been deleted.")
    else:
        print(f"{db_path} does not exist.")


del_existing_db("example.sqlitedb")


# Create a connection to the database, 
con = sqlite3.connect('example.sqlitedb')

# Create a cursor object using the cursor() method
cur = con.cursor()

# Create a table in the example.db database
# these are the columns in the table : customer_id,zipcode,city,state_code,datetime_created,datetime_updated
cur.execute('''
              CREATE TABLE IF NOT EXISTS Customer (
                customer_id INTEGER PRIMARY KEY,
                zipcode     TEXT,
                city        TEXT,
                state_code  TEXT,
                datetime_created TEXT,
                datetime_updated TEXT
            )
            ''')


# Insert data into the Customer table in the example.db database from customer.csv file in data folder
# Function to read CSV and insert data into the table
def insert_data_from_csv(csv_file):
    with open(csv_file, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            cur.execute(
                """
                INSERT INTO Customer (customer_id, zipcode, city, state_code, datetime_created, datetime_updated)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    row["customer_id"],
                    row["zipcode"],
                    row["city"],
                    row["state_code"],
                    row["datetime_created"],
                    row["datetime_updated"],
                ),
            )
    con.commit()

# # get current directory
# print(os.getcwd())

# Insert data into the Customer table
insert_data_from_csv("./data/customers.csv")

# # Fetch data from the SQLite Customer table
# cur.execute("SELECT * FROM Customer")
# rows = cur.fetchall()

# for r in rows:
#     print(r)

# con.close()