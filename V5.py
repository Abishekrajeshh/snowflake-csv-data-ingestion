import snowflake.connector as sf
import pandas as pd
import numpy as np
import os


# Establish a connection to Snowflake
conn = sf.connect(
    user='Type Your Credential',
    password='Type Your Credential',
    account='Type Your Credential',
    warehouse='Type Your Credential',
    database='Type Your Credential',
    schema='Type Your Credential'
)


print("sf.connect Completed")


cursor = conn.cursor()
cursor.execute("SELECT current_version()")
sfResults = cursor.fetchall()
print('Snowflake Version: ' + sfResults[0][0])


# Define the table name
table_name = 'Py4'


# Read the CSV file into a DataFrame
csv_file_path = r"C:/Users/krabi/Desktop/New_Python_files/nested2.csv"
df = pd.read_csv(csv_file_path)
print ("csv_file_path",csv_file_path)


# Handling the missing values
df.replace({np.nan: None}, inplace=True)


# Create a Snowflake cursor object
cur = conn.cursor()


# Check if the table exists, and create it if it doesn't
columns = ", ".join([f'"{col}" STRING' for col in df.columns])
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    {columns}
)
"""
print(create_table_query)
cur.execute(create_table_query)


# Put the CSV file into the Snowflake stage
stage_name = 'my_stage'
cur.execute(f"CREATE OR REPLACE STAGE {stage_name}")




# Adjust the file path for the PUT command
csv_file_path = os.path.abspath(csv_file_path).replace('\\', '/')
put_command = f"PUT 'file://{csv_file_path}' @{stage_name}"


try:
    cur.execute(put_command)
    print("PUT command executed successfully.")
except snowflake.connector.errors.ProgrammingError as e:
    print(f"Error during PUT command: {e}")

# copy into command

# Execute COPY INTO command
copy_into_query = f"""
COPY INTO {table_name}
FROM @{stage_name}
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
"""
try:
    cur.execute(copy_into_query)
    print(f"COPY INTO command executed successfully. Data loaded into Snowflake table {table_name}.")
except snowflake.connector.errors.ProgrammingError as e:
    print(f"Error during COPY INTO command: {e}")

# Commit the transaction (optional)
conn.commit()

# Close cursor and connection
cur.close()
conn.close()

#user should input the file name