import time
from influxdb import InfluxDBClient

INFLUXDB_HOST = "192.168.1.194"
INFLUXDB_PORT = 8086
INFLUXDB_USER = "admin"
INFLUXDB_PASSWD = "admin"
INFLUXDB_NAME = "test_result"

client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USER, password=INFLUXDB_PASSWD, database=INFLUXDB_NAME)

print(client.get_list_database())
acc_record = [{
    "measurement": "accuracy",
    "tags": {
        "server_version": "0.4.3",
        "dataset": "test",
        "index_type": "test",
        "nlist": 12,
        "search_nprobe": 12,
        "top_k": 1,
        "nq": 1
    },
    "time": time.ctime(),
    "fields": {
        "accuracy": 0.1
    }
}]
try:
	res = client.write_points(acc_record)
	print(res)
except Exception as e:
    print(str(e))