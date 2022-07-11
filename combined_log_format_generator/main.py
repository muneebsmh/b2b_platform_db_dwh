import random
import socket
import struct
from time import mktime, strptime, strftime, gmtime
import pandas as pd
import csv
from sqlalchemy import create_engine
from mysql.connector import Error
import argparse
import os

username_ls = []
useragent_ls = []
combined_logs = []

def generate_request():
    request="GET /b2b_platform.htm HTTP/1.1"
    return request

def generate_ip():
    ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
    return ip

def generate_timestamp():
    stime = mktime(strptime("1/Jan/2021 00:00:00 -0700","%d/%b/%Y %H:%M:%S %z"))
    etime = mktime(strptime("1/Jun/2022 00:00:00 -0700","%d/%b/%Y %H:%M:%S %z"))
    random_time = stime + random.random() * (etime - stime)
    return strftime("%d/%b/%Y %H:%M:%S %z", gmtime(random_time))

def generate_username():
    random_user = random.randint(0, len(username_ls) - 1)
    return username_ls[random_user]

def generate_useragent():
    random_agent = random.randint(0,len(useragent_ls)-1)
    return useragent_ls[random_agent]

def generate_url():
    url="www.b2bplatform.com/start.html"
    return url

def generate_status_code():
    codes_ls = ['200','400','403']
    random_code = random.randint(0, len(codes_ls) - 1)
    return codes_ls[random_code]

def generate_request_size():
    random_code = random.randint(100, 4000)
    return random_code

def generate_combined_log():
    log = str(generate_ip()) + " - " + str(generate_username()) + " [" + str(generate_timestamp()) + "] \"" \
          + str(generate_request()) + "\" " + str(generate_status_code()) + " " +str(generate_request_size()) \
          + " \"" + str(generate_url()) + "\" \"" + str(generate_useragent() + "\"")
    return log

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Args for Combined Log Format Generator')
    parser.add_argument('--host',
                        type=str,
                        default='localhost',
                        help='localhost', required=True)

    parser.add_argument('--username',
                        type=str,
                        default='test',
                        help='database username', required=True)

    parser.add_argument('--password',
                        type=str,
                        default='test',
                        help='username password', required=True)

    parser.add_argument('--database',
                        type=str,
                        default='b2b_platform',
                        help='database name', required=True)

    parser.add_argument('--username_csv',
                        type=str,
                        default='/resources/usernames.csv',
                        help='path to the usernames csv to generate sample logs', required=True)

    parser.add_argument('--useragent_csv',
                        type=str,
                        default='/resources/useragents.csv',
                        help='path to the useragents csv to generate sample logs', required=True)

    parser.add_argument('--output_logs',
                        type=str,
                        default='/resources/generated_logs.csv',
                        help='path to the generated sample logs', required=True)

    base_path = os.path.dirname(os.path.realpath(__file__))
    args = parser.parse_args()
    username = args.username
    password = args.password
    host = args.host
    database = args.database
    username_csv_path = base_path + args.username_csv
    useragent_csv_path = base_path + args.useragent_csv
    output_logs_path = base_path + args.output_logs
    print('Generating Combined Log Format logs')
    username_df = pd.read_csv(username_csv_path,
                               index_col=False, header=0, sep='~|~', engine='python')
    for i in username_df.values.tolist():
        for j in i:
            username_ls.append(j)

    useragent_df = pd.read_csv(useragent_csv_path,
                               index_col=False, header=0, sep='~|~', engine='python')
    for i in useragent_df.values.tolist():
        for j in i:
            useragent_ls.append(j)

    for i in range(1,50000):
        combined_logs.append([generate_combined_log()])

    try:
        with open(output_logs_path, "w") as f:
            writer = csv.writer(f)
            writer.writerow(["logs"])
            writer.writerows(combined_logs)

        engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{database}"
                               .format(user=username, pw=password, host=host, database=database))
        df = pd.DataFrame(combined_logs, columns=['logs'])
        df.to_sql(con=engine,name='weblogs',if_exists='append',index=False)

        print('Combined Log Format logs have been generated')

    except Error as e:
        print('exception occured: ' + str(e))

