import pyodbc

# Define the server name, database name, username, and password
server = 'xxxxx'
database = 'xxxxx'
username = 'xxxxx'
password = 'xxxxx'
driver = '{ODBC Driver 18 for SQL Server}'

# Create the connection string
cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

# Create a cursor from the connection
cursor = cnxn.cursor()

# Define data to be inserted
proc_time = '2023-06-07 16:31:47'
ID = 1
Outstanding_Debt = 1000.50
Credit_Mix = 'Good'
Num_Credit_Card = 3
Interest_Rate = 12.5
Decision = 'Good,0.05'

# Create insert query
insert_query = "INSERT INTO Credit_Decision (proc_time, ID, Outstanding_Debt, Credit_Mix, Num_Credit_Card, Interest_Rate, Decision) VALUES (?, ?, ?, ?, ?, ?, ?)"

# Execute the query
cursor.execute(insert_query, proc_time, ID, Outstanding_Debt, Credit_Mix, Num_Credit_Card, Interest_Rate, Decision)

# Commit the transaction
cnxn.commit()

# Close the connection
cnxn.close()
