import mysql.connector
from mysql.connector import Error
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Args for Data Warehouse Loading Script')

    parser.add_argument('--host',
                        type=str,
                        default='localhost',
                        help='database host', required=True)


    parser.add_argument('--username',
                        type=str,
                        default='test',
                        help='database username', required=True)


    parser.add_argument('--password',
                        type=str,
                        default='test',
                        help='database password', required=True)


    parser.add_argument('--database',
                        type=str,
                        default='dwh_b2b_platform',
                        help='database name', required=True)

    args = parser.parse_args()

    host = args.host
    user = args.username
    password = args.password
    database = args.database

    try:
        connection = mysql.connector.connect(host=host,
                                             database=database,
                                             user=user,
                                             password=password)

        cursor = connection.cursor()
        print("Refreshing Data Warehouse Facts And Dimensions")

        print("Refreshing Dim Companies")
        cursor.callproc('sp__load_dim_companies', )
        print("Refreshing Dim Customers")
        cursor.callproc('sp__load_dim_customers', )
        print("Refreshing Dim Company Products")
        cursor.callproc('sp__load_dim_company_products', )
        print("Refreshing Web Logs")
        cursor.callproc('sp__load_web_logs', )
        print("Refreshing Fact B2B Sales")
        cursor.callproc('sp__load_fact_b2b_sales', )
        print("Refreshing Fact B2C Sales")
        cursor.callproc('sp__load_fact_b2c_sales', )

        print("Data Warehouse Facts And Dimensions have been refreshed")
    except Error as error:
        print("Failed to execute stored procedure: {}".format(error))

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")