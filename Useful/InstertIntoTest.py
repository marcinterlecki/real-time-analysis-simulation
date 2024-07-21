import pyodbc

# Define the server name, database name, username, and password
server = 'xxxxx'
database = 'xxxxx'
username = 'xxxxx'
password = 'xxxxx' 
driver= '{ODBC Driver 18 for SQL Server}'

# Create the connection string
cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)

# Create a cursor from the connection
cursor = cnxn.cursor()

# Define data to be inserted
first_name = 'test'
last_name = 'xxxxx'
email = 'mt@example.com'

# Create insert query
insert_query = f"INSERT INTO Names (first_name, last_name, email) VALUES (?, ?, ?)"

# Execute the query
cursor.execute(insert_query, first_name, last_name, email)

# Commit the transaction
cnxn.commit()

# Close the connection
cnxn.close()
