import pyodbc
import pandas as pd
import time

# Define the server name, database name, username, and password
server = 'xxxxx'
database = 'xxxxx'
username = 'xxxxx'
password = 'xxxxx'
driver = '{ODBC Driver 18 for SQL Server}'

# Create the connection string
cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

# Create a new cursor from the connection
cursor = cnxn.cursor()

# Define the SQL query
sql_query = "SELECT COUNT(*) AS [LICZBA WIERSZY] FROM Credit_Decision"

while True:
    # Get the current timestamp
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")

    # Execute the SQL query
    cursor.execute(sql_query)

    # Fetch the result
    result = cursor.fetchone()

    # Create a dictionary with the column name and the corresponding value
    result_dict = {'Timestamp': current_time, 'Liczba wnioskow w tabeli': result[0]}

    # Create a dataframe from the dictionary
    df = pd.DataFrame(result_dict, index=[0])

    # Format the output
    output = "\n".join([f"{col}: {value}" for col, value in df.iloc[0].items()])

    # Print the formatted output
    
    # Separate console output
    print("\n")
   
    print(output)
 
    # print lines to separate console output
    print("--------------------------------------------------")   

    time.sleep(5)  # Wait for 10 seconds before the next query
