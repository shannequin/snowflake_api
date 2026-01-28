import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.core import CreateMode
from snowflake.core.database import Database
from snowflake.core.schema import Schema
from snowflake.core.warehouse import Warehouse
from snowflake.core import Root
# from snowflake.snowpark import Session


def main():
    USER = "sh4ndroid"
    PASSWORD = "12Tomato!sdroid"
    ACCOUNT = "UWBCYVD-VM64505"
    # role = "ACCOUNTADMIN"
    # warehouse = "EMPLOYEES_WH"
    # database = "EMPLOYEES_DB"
    # schema = "EMPLOYEES_SCHEMA"

    earnings_df = pd.read_csv("data/Metro_Government_Employee_Earnings_Table_view_1971494843601823480.csv")
    print(earnings_df.head(1))

    earnings_df.columns = ["OBJECTID", "EMPLOYEE_NAME", "HOME_BUSINESS_UNIT", "JOB_CLASS", "REGULAR_PAY", "OVERTIME_PAY", "SUPPLEMENTAL_PAY", "LONGEVITY", "BONUSES", "PAYOUTS", "OTHER_PAY", "TOTAL_PAY", "FISCAL_YEAR"]
    earnings_df['BONUSES'] = pd.to_numeric(earnings_df['BONUSES'], errors="coerce")

    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        # warehouse=WAREHOUSE,
        # database=DATABASE,
        # schema=SCHEMA
    )
    print(conn)

    try:
        # session = Session.builder.config("connection_name", "myconnection").create()
        # root = Root(session)

        # employees_wh = Warehouse(
        # name="EMPLOYEES_WH",
        # warehouse_size="XSMALL"
        # )
        # root.warehouses.create(my_wh, mode=CreateMode.or_replace)

        # employees_db = Database(
        #     name="EMPLOYEES_DB"
        # )

        # employees_schema = Schema(
        #     name="PYTHON_API_SCHEMA"
        # )

        # from snowflake.core.table import Table, TableColumn

        # my_table = Table(
        # name="my_table",
        # columns=[TableColumn(name="c1", datatype="int", nullable=False),
        #         TableColumn(name="c2", datatype="string")]
        # )
        # root.databases["my_db"].schemas["my_schema"].tables.create(my_table)

        #     CREATE OR REPLACE DATABASE METRO_GOVERNMENT_EMPLOYEE_EARNINGS_DB;

        # CREATE SCHEMA METRO_GOVERNMENT_EMPLOYEE_EARNINGS_DB.RAW;

        conn.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS EMPLOYEES_WH")
        conn.cursor().execute("CREATE DATABASE IF NOT EXISTS EMPLOYEES_DB")
        conn.cursor().execute("USE DATABASE EMPLOYEES_DB")
        conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS RAW")
        conn.cursor().execute("USE SCHEMA RAW")
        conn.cursor().execute("CREATE TABLE IF NOT EXISTS EMPLOYEE_EARNINGS (OBJECTID VARCHAR(10), EMPLOYEE_NAME STRING, HOME_BUSINESS_UNIT STRING, JOB_CLASS STRING, REGULAR_PAY FLOAT, OVERTIME_PAY FLOAT, SUPPLEMENTAL_PAY FLOAT, LONGEVITY FLOAT,BONUSES FLOAT, PAYOUTS FLOAT, OTHER_PAY FLOAT,TOTAL_PAY FLOAT, FISCAL_YEAR INT)")

        write_pandas(conn=conn, table_name="EMPLOYEE_EARNINGS", df=earnings_df)
    except Exception as e:
        import traceback
        print(e)
        print(traceback.format_exc())

    finally:
        # Closing the connection
        conn.close()

main()