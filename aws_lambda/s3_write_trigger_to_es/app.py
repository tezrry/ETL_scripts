import sys
import json
import datetime
import dateutil.tz
import dateutil.parser
import pymysql
import boto3

s3 = boto3.resource('s3', region_name='cn-north-1')

db_host = ''
db_port = 3306
db_user = ''
db_password = ''
db_name = ''
time_zone = dateutil.tz.gettz('CST')

event = 'event_5'
sql_format = 'insert into Pirates.Metrics_User (UserId, FirstLoginTime, LoginCount) values ("%s", "%s", 1) on duplicate key update LastLoginTime = values(FirstLoginTime), LoginCount = LoginCount + 1'
sql2_format = 'insert into Pirates.Metrics_UserLogin (UserId, LoginTime, UserFlag) values ("%s", "%s", %d)'

try:
    conn = pymysql.connect(host=db_host, port=db_port, user=db_user, password=db_password, db=db_name, charset='utf8')
except Exception as e:
    print(e)
    sys.exit(1)


def handler(event, context):
    try:
        with conn.cursor() as cur:
            for record in event['Records']:
                bucket = record['s3']['bucket']['name']
                key = record['s3']['object']['key']
                obj = s3.Object(bucket, key)

                str_content = obj.get()['Body'].read().decode('utf-8')
                for line in str_content.split('\n'):
                    if line != '':
                        row = json.loads(line)

                        utc_time = dateutil.parser.parse(row['timestamp'])
                        local_time = utc_time.astimezone(time_zone)
                        time_str = local_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

                        sql = sql_format %(row['5_1'], time_str)
                        new_user = 0
                        if cur.execute(sql) == 1: new_user = 1

                        sql = sql2_format %(row['5_1'], time_str, new_user)
                        cur.execute(sql)

        conn.commit()
    except Exception as e:
        print(e)
        sys.exit(1)
    #conn.close()

    return key + ' is processed successfully'
