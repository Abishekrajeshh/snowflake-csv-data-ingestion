import os
import snowflake.connector as sf
import snowflake
import pandas as pd

def upload_to_snowflake(csv_file_path, df_columns):
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

    while True:
        try:
            # Ask user for table and stage names
            table_name = input("Enter the Snowflake table name: ").strip().upper()
            stage_name = input("Enter the Snowflake stage name: ").strip().upper()

            # Create a Snowflake cursor object
            cur = conn.cursor()

            # Check if the table exists
            check_table_query = f"SHOW TABLES LIKE '{table_name}'"
            cur.execute(check_table_query)
            table_exists = cur.fetchone() is not None

            # Ask user for append or overwrite
            mode = input("Enter 'append' to add data to the existing table or 'overwrite' to replace it: ").strip().lower()

            # Create table if it doesn't exist
            if not table_exists:
                columns = ", ".join([f'"{col.upper()}" STRING' for col in df_columns])
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {columns}
                )
                """
                try:
                    cur.execute(create_table_query)
                    print("Table created successfully.")
                except snowflake.connector.errors.ProgrammingError as e:
                    print(f"Error during table creation: {e}")

            # Create or replace the stage
            try:
                cur.execute(f"CREATE OR REPLACE STAGE {stage_name}")
                print("Stage created/replaced successfully.")
            except snowflake.connector.errors.ProgrammingError as e:
                print(f"Error during stage creation: {e}")

            # Adjust the file path for the PUT command
            csv_file_path = os.path.abspath(csv_file_path).replace('\\', '/')
            put_command = f"PUT 'file://{csv_file_path}' @{stage_name}"

            # Execute the PUT command
            try:
                cur.execute(put_command)
                print("PUT command executed successfully.")
            except snowflake.connector.errors.ProgrammingError as e:
                print(f"Error during PUT command: {e}")

            # Define the columns in the correct order
            column_order = ", ".join([col.upper() for col in df_columns])

            # Execute COPY INTO command based on user choice
            try:
                if mode == 'append':
                    copy_into_query = f"""
                    COPY INTO {table_name}({column_order})
                    FROM @{stage_name}
                    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1, ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
                    ON_ERROR = 'CONTINUE'
                    """
                    cur.execute(copy_into_query)
                    print(f"COPY INTO command executed successfully. Data loaded into Snowflake table {table_name}.")
                elif mode == 'overwrite':
                    truncate_table_query = f"TRUNCATE TABLE {table_name}"
                    cur.execute(truncate_table_query)
                    print(f"Table {table_name} truncated successfully.")
                    
                    copy_into_query = f"""
                    COPY INTO {table_name}({column_order})
                    FROM @{stage_name}
                    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1, ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
                    ON_ERROR = 'CONTINUE'
                    """
                    cur.execute(copy_into_query)
                    print(f"COPY INTO command executed successfully. Data loaded into Snowflake table {table_name}.")
                else:
                    print("Invalid mode. Please enter either 'append' or 'overwrite'.")
            except snowflake.connector.errors.ProgrammingError as e:
                print(f"Error during COPY INTO command: {e}")
                print("Please provide the correct information.")

            # If everything is successful, break the loop
            break

        except Exception as e:
            print(f"Error: {e}")
            print("Please enter the correct table or stage name.")

    # Commit the transaction (optional)
    conn.commit()

    # Close cursor and connection
    cur.close()
    conn.close()

if __name__ == '__main__':
    while True:
        try:
            # Taking location and file name as input from the user
            location = input("Enter the directory path where the CSV file is located: ").strip()
            csv_file_name = input("Enter the CSV file name (with .csv extension): ").strip()

            csv_file_path = os.path.join(location, csv_file_name)

            # Load the CSV file to determine its columns
            df = pd.read_csv(csv_file_path)
            df_columns = df.columns

            print(f"CSV file found. Columns: {df_columns}")
            break
        except (FileNotFoundError, OSError) as e:
            print(f"Error: {e}")
            print("Please enter the correct file location and name.")

    # Upload the CSV file to Snowflake
    upload_to_snowflake(csv_file_path, df_columns)
