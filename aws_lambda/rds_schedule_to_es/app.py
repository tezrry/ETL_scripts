import sys
import pymysql
import datetime
import dateutil.tz
import json
import http.client
import base64


db_host = ''
db_port = 3306
db_user = ''
db_password = ''
db_name = ''

table = 'Metrics_DailyNewUser'
columns = ('FirstLoginDate', 'UserCount', 'Retention_D1', 'Retention_D3', 'Retention_D7')

http_host = ''
http_post_url = ''
http_user = ''
http_password = ''


utc_tz = dateutil.tz.gettz('UTC')

try:
    conn = pymysql.connect(
        host=db_host, 
        port=db_port, 
        user=db_user, 
        password=db_password, 
        db=db_name, 
        charset='utf8'
    )
except Exception as e:
    print(e)
    sys.exit(1)


def get_es_id(row):
    return row[0].strftime('%Y%m%d%H%M%S%f')

def get_es_source(row):
    source = {}
    utc_datetime = datetime.datetime.combine(row[0], datetime.time.min).astimezone(utc_tz)
    source["FirstLoginDate"] = utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    source["UserCount"] = row[1]

    if (row[2] is not None):
        source["Retention_D1"] = row[2]

    if (row[3] is not None):
        source["Retention_D3"] = row[3]

    if (row[4] is not None):
        source["Retention_D7"] = row[4]
    
    return source


def handler(event, context):
    es_data = []
    row_tag = None
    try:
        sql = 'select ' + ','.join(columns) + ' from ' + table
        check_sql = 'select Tag_DATETIME3 from Pirates.Util_CheckPointForDataExport where Name = "%s"'
        set_sql = 'replace into Pirates.Util_CheckPointForDataExport (Name, Tag_DATETIME3) values ("%s", "%s")'

        with conn.cursor() as cur:
            if cur.execute(check_sql %table) > 0:
                for row in cur:
                    sql = sql + ' where FirstLoginDate > "%s"' %(row[0].strftime('%Y-%m-%d'))

            if cur.execute(sql) > 0:
                for row in cur:
                    metadata = {}
                    metadata["_index"] = table.lower()
                    metadata["_type"] = "doc"
                    metadata["_id"] = get_es_id(row)
                    action = {}
                    action["index"] = metadata
                    es_data.append(json.dumps(action))
                    es_data.append(json.dumps(get_es_source(row)))

                    row_tag = row[0]
                

                auth = ('%s:%s' % (http_user,http_password)).encode('utf8')
                headers = {
                    'Authorization': 'Basic %s' % base64.b64encode(auth).decode('utf8'),
                    'content-type': 'application/x-ndjson', 
                    'kbn-xsrf': 'reporting',
                }
                data = '\n'.join(es_data) + '\n'

                http_conn = http.client.HTTPConnection(http_host)
                http_conn.request('POST', http_post_url, data, headers)
                response = http_conn.getresponse()
                body = response.read().decode('utf-8')
                http_conn.close()

                if response.status != 200:
                    sys.exit(1)
                
                res_json = json.loads(body)
                if res_json['errors']:
                    sys.exit(1)

                set_sql = set_sql %(table, row_tag.strftime('%Y-%m-%d'), )
                cur.execute(set_sql)
                conn.commit()
    except Exception as e:
        print(e)
        sys.exit(1)

    if row_tag is None:
        return 'None is processed'

    return row_tag.strftime('%Y-%m-%d %H:%M:%S.%f') + ' is processed successfully'

print(handler(None, None))
conn.close()