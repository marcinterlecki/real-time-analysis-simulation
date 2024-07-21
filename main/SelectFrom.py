# -*- coding: utf-8 -*-

import pyodbc

# Define the server name, database name, username, and password
server = 'xxxxx'
database = 'xxxxx'
username = 'xxxxx'
password = 'xxxxx' 
driver= '{ODBC Driver 18 for SQL Server}'

# Create the connection string
cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)

# Create a new cursor from the connection
cursor = cnxn.cursor()

# Define the SQL query and execute it
sql_query = "SELECT * FROM Credit_Decision ORDER BY proc_time ASC" # Replace "table_name" with your actual table name
cursor.execute(sql_query)

# Fetch the rows from the executed SQL query
rows = cursor.fetchall()

# Separate console output
print("\n")
print("\n")
print("\n")

# print lines to separate console output
print("--------------------------------------------------")
print("------------------------------------")
print("------------------------")
print("-------")


# Loop through each row and print it
for row in rows:
    print(row)
    
 
print("-------") 
print("------------------------")
print("------------------------------------")
print("--------------------------------------------------")
 
 
    
# Separate console output
print("\n")
print("\n")
print("\n")
    