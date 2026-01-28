import json
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def main():
    # Fetch config
    with open('config.json', 'r') as file:
        config = json.load(file)

    # Set Snowflake connection args
    sf_user = config.get("sf_user")
    sf_password = config.get("sf_password")
    sf_account = config.get("sf_account")

    # Create Snowflake connection
    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
    )
    print(conn)

    try:
        # csv_file_name = input("Enter csv file name: ") # Metro_Government_Employee_Earnings_Table_view_1971494843601823480

        # Read data
        # earnings_df = pd.read_csv(f"data/{csv_file_name}.csv")
        earnings_df = pd.read_csv(f"data/Metro_Government_Employee_Earnings_Table_view_1971494843601823480.csv")

        print(earnings_df.info())

        for name in earnings_df.columns:
            # Convert to upper case and replace spaces with underscores
            new_name = name.upper().replace(" ", "_")

            # Rename columns
            earnings_df = earnings_df.rename(columns={name: new_name})

        # Convert column to numeric
        earnings_df['BONUSES'] = pd.to_numeric(earnings_df['BONUSES'], errors="coerce")

        print(earnings_df.info())

        # Create warehouse
        conn.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS EMPLOYEES_WH")

        # Create database
        conn.cursor().execute("CREATE DATABASE IF NOT EXISTS EMPLOYEES_DB")

        # Set database for schema
        conn.cursor().execute("USE DATABASE EMPLOYEES_DB")

        # Create schema
        conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS RAW")

        # Set schema for table
        conn.cursor().execute("USE SCHEMA RAW")

        # Create table
        conn.cursor().execute("CREATE TABLE IF NOT EXISTS EMPLOYEE_EARNINGS (OBJECTID VARCHAR(10)," \
                                                                            "EMPLOYEE_NAME STRING," \
                                                                            "HOME_BUSINESS_UNIT STRING," \
                                                                            "JOB_CLASS STRING," \
                                                                            "REGULAR_PAY FLOAT," \
                                                                            "OVERTIME_PAY FLOAT," \
                                                                            "SUPPLEMENTAL_PAY FLOAT," \
                                                                            "LONGEVITY FLOAT," \
                                                                            "BONUSES FLOAT," \
                                                                            "PAYOUTS FLOAT," \
                                                                            "OTHER_PAY FLOAT," \
                                                                            "TOTAL_PAY FLOAT," \
                                                                            "FISCAL_YEAR INT)")

        # Write data to Snowflake table
        write_pandas(conn=conn, table_name="EMPLOYEE_EARNINGS", df=earnings_df)

    except Exception as e:
        import traceback
        print(e)
        print(traceback.format_exc())

    finally:
        # Closing the connection
        conn.close()

main()