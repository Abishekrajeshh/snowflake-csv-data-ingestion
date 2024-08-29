import os
import pandas as pd
import snowflake.connector as sf
import snowflake
import time

def upload_to_snowflake(csv_file_path, df_columns, table_name, stage_name, mode):
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

    try:
        # Create a Snowflake cursor object
        cur = conn.cursor()

        # Check if the table exists
        check_table_query = f"SHOW TABLES LIKE '{table_name}'"
        cur.execute(check_table_query)
        table_exists = cur.fetchone() is not None

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
            return False

        # Define the columns in the correct order, handling spaces in column names
        column_order = ", ".join([f'"{col.upper()}"' for col in df_columns])

        # Execute COPY INTO command based on user choice
        try:
            if mode == 'append':
                copy_into_query = f"""
                COPY INTO {table_name}({column_order})
                FROM @{stage_name}
                FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1, ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
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
                FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1, ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
                ON_ERROR = 'CONTINUE'
                """
                cur.execute(copy_into_query)
                print(f"COPY INTO command executed successfully. Data loaded into Snowflake table {table_name}.")
            else:
                print("Invalid mode. Please enter either 'append' or 'overwrite'.")
                return False
        except snowflake.connector.errors.ProgrammingError as e:
            print(f"Error during COPY INTO command: {e}")
            print("Please provide the correct information.")
            return False

    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        # Commit the transaction (optional)
        conn.commit()

        # Close cursor and connection
        cur.close()
        conn.close()

    return True

if __name__ == '__main__':
    while True:
        try:
            # Taking location of CSV files directory as input from the user
            location = input("Enter the directory path where the CSV files are located: ").strip()
            if not os.path.isdir(location):
                raise FileNotFoundError(f"Directory not found: {location}")

            # Get list of CSV files in the directory
            csv_files = [f for f in os.listdir(location) if f.endswith('.csv')]
            if not csv_files:
                raise FileNotFoundError("No CSV files found in the specified directory.")

            # Ask user for table and stage names and mode
            table_name = input("Enter the Snowflake table name: ").strip().upper()
            stage_name = input("Enter the Snowflake stage name: ").strip().upper()
            mode = input("Enter 'append' to add data to the existing table or 'overwrite' to replace it: ").strip().lower()

            if not table_name or not stage_name or not mode:
                raise ValueError("Table name, stage name, and mode cannot be empty.")

            for csv_file_name in csv_files:
                csv_file_path = os.path.join(location, csv_file_name)

                # Load the CSV file to determine its columns
                try:
                    df = pd.read_csv(csv_file_path, encoding='latin1')  # Try with 'latin1' encoding
                    df_columns = df.columns
                except pd.errors.ParserError as e:
                    print(f"Error parsing CSV file {csv_file_name}: {e}")
                    continue

                print(f"Uploading {csv_file_name} with columns: {df_columns}")

                # Upload the CSV file to Snowflake with retries
                success = False
                retries = 3
                while not success and retries > 0:
                    success = upload_to_snowflake(csv_file_path, df_columns, table_name, stage_name, mode)
                    if not success:
                        retries -= 1
                        print(f"Retrying... {retries} attempts left.")
                        time.sleep(2)  # Wait for 2 seconds before retrying

                if not success:
                    print(f"Failed to upload {csv_file_name} after multiple attempts.")

            print("Bulk upload completed.")
            break
        except (FileNotFoundError, OSError) as e:
            print(f"Error: {e}")
            print("Please enter the correct file location and name.")
        except pd.errors.ParserError as e:
            print(f"Error parsing CSV file: {e}")
            print("Please check the file format and contents.")
        except Exception as e:
            print(f"Unexpected error: {e}")
