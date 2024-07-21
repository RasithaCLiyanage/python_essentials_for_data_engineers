import duckdb 
import os

# delete existing database
def del_existing_db(db_path):
    # Delete file if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"{db_path} has been deleted.")
    else:
        print(f"{db_path} does not exist.")

del_existing_db("example.duckdb")

# Create a DuckDB database called exampledb in persistent storage
con = duckdb.connect(database='example.duckdb', read_only=False)
print("database called example was created")

# create a cursor object
cursor = con.cursor()
print("cursor object created")




# create a schema called sqllite_example
con.sql("use example")
con.sql("CREATE SCHEMA sqllite_example")
print("schema called sqllite_example created")

# Create a table called customer in the example database
con.sql("USE sqllite_example")
con.sql(
    """
    CREATE TABLE IF NOT EXISTS Customer (
    customer_id INTEGER,
    zipcode TEXT,
    city TEXT,
    state_code TEXT,
    datetime_created TIMESTAMP,
    datetime_updated TIMESTAMP
    )
    """
)

print("show database connected")
con.sql("show databases").show()


# Query to list all schemas
query = "SELECT * FROM information_schema.schemata"
# Execute the query
result = con.sql(query).show()


print("################################################################")


con.sql("SELECT current_database()").show()


con.sql("SELECT current_schema()").show()


con.sql("show tables").show()

print("rows in the customer table")
con.sql("SELECT * FROM example.sqllite_example.Customer").show()