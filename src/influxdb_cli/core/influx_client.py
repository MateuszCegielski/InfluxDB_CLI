from influxdb import InfluxDBClient

class InfluxClient(InfluxDBClient):
    def __init__(self, host='localhost', port=8086, username=None, password=None, database=None, ssl=False, verify_ssl=False, timeout=None, retries=None, **kwargs):
        super().__init__(host=host, port=port, username=username, password=password, database=database, ssl=ssl, verify_ssl=verify_ssl, timeout=timeout, retries=retries, **kwargs)