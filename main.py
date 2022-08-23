import boto3
from botocore import UNSIGNED
from botocore.config import Config
from datetime import datetime, timedelta
import json
from pathlib import Path
from deepdiff import DeepHash
import os
from pprint import pprint

class Cache:
    def __init__(
        self,
        lifetime: timedelta,
        args: dict,
        get_data,
    ):
        self.lifetime = lifetime

        self.args = args
        self.get_data = get_data

        self.dir = DeepHash(self.args)[self.args][0:10]

        self.path = f".cache/{self.dir}"

        self.cache_path = self.path + "/cache.json"
        self.time_path = self.path + "/time"

        self.pathobj = Path(self.path)
        self.cachepathobj = Path(self.cache_path)
        self.timepathobj = Path(self.time_path)

        self.create_path(self.pathobj)

    def cache_exists(self) -> bool:
        return self.cachepathobj.exists() and self.timepathobj.exists()

    def create_path(self, pathobj):
        if not pathobj.exists():
            os.makedirs(self.pathobj)

    def _internal_write(self, data):
        """Write both the cache file and the time file"""
        json_object = json.dumps(data, indent=4, default=str)
        with open(self.cache_path, "w") as outfile:
            outfile.write(json_object)

        with open(self.time_path, "w") as timefile:
            time = datetime.now()
            timefile.write(str(time))

    def _internal_read(self):
        with open(self.cache_path) as file:
            return json.load(file)

    def read_cache_time(self) -> datetime:
        """Read the time from file"""
        with open(self.time_path) as file:
            time = file.read().rstrip()
            return datetime.fromisoformat(time)

    def read(self, force=False):
        if self.cache_exists():
            if force:
                data = self.get_data(**self.args)
                self._internal_write(data)
                return data
            else:
                time = self.read_cache_time()
                if datetime.now() > time + self.lifetime:
                    data = self.get_data(**self.args)
                    self._internal_write(data)
                    return data
                else:
                    return self._internal_read()
        else:
            data = self.get_data(**self.args)
            self._internal_write(data)
            return data


s3 = boto3.client(
    's3',
    'us-west-1',
    config=Config(signature_version=UNSIGNED),
)

# list_objects = Cache(
#     lifetime=timedelta(minutes=15),
#     args={
#         "Bucket": 'noaa-ghe-pds',
#         "Delimiter": '/*',
#     },
#     get_data=s3.get_paginator("list_objects_v2").paginate,
# )

pager = s3.get_paginator("list_objects_v2").paginate(
    Bucket='noaa-ghe-pds',
    Delimiter='/*',
)

for p in pager:
    c = p["Contents"]
    pprint(c[0])
