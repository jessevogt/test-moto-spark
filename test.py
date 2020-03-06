import subprocess
import uuid
import boto3
import signal
import os
import socket
import time
from datetime import datetime, timedelta
from typing import Optional
from pyspark.sql import SparkSession, Row
from contextlib import suppress

import unittest


class TestMotoSpark(unittest.TestCase):
    def testGoodRead(self):
        expected = [Row(x=1)]
        path = f"s3a://{self.bucket}/2019-01-01"

        self.spark.createDataFrame(expected).write.parquet(path)
        actual = self.spark.read.parquet(path).collect()

        self.assertEqual(expected, actual)

    def testBadRead(self):
        expected = [Row(x=1)]
        path = f"s3a://{self.bucket}/2019-01-01%2012%3A30%3A00"

        self.spark.createDataFrame(expected).write.parquet(path)
        actual = self.spark.read.parquet(path).collect()

        self.assertEqual(expected, actual)

    def setUp(self):
        self.bucket = str(uuid.uuid4())
        self.client.create_bucket(Bucket=self.bucket)

    @classmethod
    def setUpClass(cls):
        moto_port = 8000
        moto_host = "localhost"
        moto_endpoint = f"http://{moto_host}:{moto_port}"

        aws_access_key = "x"
        aws_access_secret = "x"
        aws_region = "us-west-1"

        cls.moto_proc = subprocess.Popen(["moto_server", "s3", "-p", str(moto_port), "-H", moto_host])
        cls.spark = None

        cls.spark = (
            SparkSession.builder.appName("local-testing-spark")
            .master("local[1]")
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby")
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.default.parallelism", 1)
            .config("spark.task.maxFailures", 1)
            .config("fs.s3a.access.key", aws_access_key)
            .config("fs.s3a.secret.key", aws_access_secret)
            .config("fs.s3a.endpoint", moto_endpoint)
            .config("fs.s3a.region", "us-west-1")
            .config("fs.s3a.connection.ssl.enabled", False)
            .config("fs.s3a.path.style.access", True)
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .getOrCreate()
        )

        wait_for_listen("s3", moto_host, moto_port, 10.0)

        cls.client = boto3.client("s3",
            region_name=aws_region,
            endpoint_url=moto_endpoint,
            use_ssl=False,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_access_secret,
        )

    @classmethod
    def tearDownClass(cls):
        with suppress(Exception):
            if cls.spark is not None:
                spark.stop()

        with suppress(Exception):
            os.kill(cls.moto_proc.pid, signal.SIGTERM)



def wait_for_listen(service: str, host: str, port: int, timeout: float):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    address = host, port
    start_time = time.time()
    timeout_expired = start_time + timeout

    attempt = 0
    last_ex = None
    last_status = 0.0

    while time.time() <= timeout_expired:
        attempt += 1

        res: Optional[int]
        try:
            res = sock.connect_ex(address)
        except Exception as ex:
            last_ex = ex
            res = None

        if res == 0:
            sock.close()
            print(
                f"connected to {service} on attempt {attempt} after {time.time() - start_time}"
            )
            return

        now = time.time()
        if now - last_status > 1.0:
            print(
                f"got result {res} on attempt {attempt} to connect to {service} at {address[0]}:{address[1]}"
            )
            last_status = now

        time.sleep(0.2)

    raise Exception(
        f"Unable to connect to {service} running at {address} after {attempt} attempts. (ex: {last_ex})"
    )
