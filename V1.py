import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
import time

def upload_to_mysql(csv_file_path, df_columns, table_name, mode):
    # Establish a connection to MySQL
    try:
        conn = mysql.connector.connect(
            host='Type Your Credential',  # Change to your MySQL host
            user='Type Your Credential',       # Change to your MySQL user
            password='Type Your Credential',  # Change to your MySQL password
            database='Type Your Credential'  # Use your database name
        )
        if conn.is_connected():
            print("MySQL connection established")

        cursor = conn.cursor()
        # Check if the table exists
        check_table_query = f"SHOW TABLES LIKE '{table_name}'"
        cursor.execute(check_table_query)
        table_exists = cursor.fetchone() is not None

        # Create table if it doesn't exist
        if not table_exists:
            columns = ", ".join([f"`{col}` VARCHAR(255)" for col in df_columns])
            create_table_query = f"""
            CREATE TABLE {table_name} (
                {columns}
            )
            """
            try:
                cursor.execute(create_table_query)
                print("Table created successfully.")
            except Error as e:
                print(f"Error during table creation: {e}")
                return False

        # Load data from CSV file and insert into MySQL table
        df = pd.read_csv(csv_file_path, encoding='latin1')  # Try with 'latin1' encoding
        column_order = ", ".join([f"`{col}`" for col in df_columns])
        placeholders = ", ".join(["%s"] * len(df_columns))
        insert_query = f"INSERT INTO {table_name} ({column_order}) VALUES ({placeholders})"

        if mode == 'overwrite':
            truncate_table_query = f"TRUNCATE TABLE {table_name}"
            cursor.execute(truncate_table_query)
            print(f"Table {table_name} truncated successfully.")

        # Insert data
        for i, row in df.iterrows():
            try:
                cursor.execute(insert_query, tuple(row))
            except Error as e:
                print(f"Error inserting row {i}: {e}")
                continue

        print(f"Data loaded into MySQL table {table_name}.")
        conn.commit()
    except Error as e:
        print(f"Error: {e}")
        return False
    finally:
        if conn.is_connected():
            cursor.close()
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

            # Ask user for table name and mode
            table_name = input("Enter the MySQL table name: ").strip()
            mode = input("Enter 'append' to add data to the existing table or 'overwrite' to replace it: ").strip().lower()

            if not table_name or not mode:
                raise ValueError("Table name and mode cannot be empty.")

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

                # Upload the CSV file to MySQL with retries
                success = False
                retries = 3
                while not success and retries > 0:
                    success = upload_to_mysql(csv_file_path, df_columns, table_name, mode)
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
