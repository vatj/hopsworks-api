#
#   Copyright 2024 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

from __future__ import annotations

import concurrent.futures
import functools
import hashlib
import http.client
import json
import logging
import os
import platform
import random
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone
from os.path import expanduser, join


class EnvironmentAttribute:
    def __init__(self):
        self._platform = None
        self._hsml_version = None
        self._hsfs_version = None
        self._hopsworks_version = None
        self._python_version = None
        self._user_id = None
        self._backend_version = None
        self._timezone = None

    def _get_lib_version(self, lib_name):
        try:
            lib = __import__(lib_name)
            return lib.__version__
        except ImportError:
            return ""

    def get_hsml_version(self):
        if self._hsml_version is None:
            self._hsml_version = self._get_lib_version("hsml")
        return self._hsml_version

    def get_hsfs_version(self):
        if self._hsfs_version is None:
            self._hsfs_version = self._get_lib_version("hsfs")
        return self._hsfs_version

    def get_hopsworks_version(self):
        if self._hopsworks_version is None:
            self._hopsworks_version = self._get_lib_version("hopsworks")
        return self._hopsworks_version

    def get_python_version(self):
        if not self._python_version:
            self._python_version = platform.python_version()
        return self._python_version

    def get_user_id(self):
        if not self._user_id:
            hopsworks_dir = _create_hopsworks_dir_if_not_exist()
            user_id_file = join(hopsworks_dir, _USER_ID_FILE)
            if os.path.exists(user_id_file):
                with open(user_id_file, "r") as fr:
                    self._user_id = fr.read().rstrip()
            else:
                with open(user_id_file, "w") as fw:
                    self._user_id = _generate_user_id()
                    fw.write(self._user_id)
        return self._user_id

    def get_platform(self):
        if not self._platform:
            self._platform = platform.platform()
        return self._platform

    def get_backend_host_name(self):
        return _backend_hostname

    def get_backend_version(self):
        return _backend_version

    def get_timezone(self):
        if self._timezone is None:
            self._timezone = datetime.now().astimezone().tzinfo
        return self._timezone

    def json(self):
        return json.dumps(
            {
                "platform": self.get_platform(),
                "hsml_version": self.get_hsml_version(),
                "hsfs_version": self.get_hsfs_version(),
                "hopsworks_version": self.get_hopsworks_version(),
                "user_id": self.get_user_id(),
                "backend_version": self.get_backend_version(),
                "timezone": str(self.get_timezone()),
                "python_version": self.get_python_version(),
            }
        )


class MethodCounter:
    def __init__(self):
        self.method_counts = {}
        random.seed(42)

    def add(self, m):
        s = self._get_method_name(m)
        self.method_counts[s] = self.method_counts.get(s, 0) + 1

    def get_count(self, m):
        s = self._get_method_name(m)
        return self.method_counts.get(s, 0)

    def should_sample(self, m):
        cnt = self.get_count(m)
        if cnt < 100:
            return True
        elif cnt < 1000:
            return random.random() < 0.1
        elif cnt < 10000:
            return random.random() < 0.01
        else:
            return random.random() < 0.001

    def _get_method_name(self, m):
        return m.__module__ + m.__name__


_logger = logging.getLogger(__name__)
_handler = logging.StreamHandler(stream=sys.stdout)
_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
_logger.addHandler(_handler)
_logger.setLevel(logging.INFO)
_conn = http.client.HTTPSConnection("usage.hops.works")
_num_executor = os.environ.get("NUM_HOPSWORKS_USAGE_EXECUTORS", default="2")
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=int(_num_executor))
_env_attr = EnvironmentAttribute()
_method_counter = MethodCounter()
_backend_hostname = None
_backend_version = None
HOPSWORKS_DIR = join(expanduser("~"), ".hopsworks")
_USER_ID_FILE = "user_id"
_is_enabled = os.environ.get("ENABLE_HOPSWORKS_USAGE", default="true").lower() == "true"


def enable():
    global _is_enabled
    _is_enabled = True


def disable():
    global _is_enabled
    _is_enabled = False


def get_env():
    return _env_attr.json()


def init_usage(hostname, backend_version):
    global _backend_hostname, _backend_version, _is_enabled
    _backend_hostname = hostname
    _backend_version = backend_version
    _is_enabled = _is_enabled and _is_target_hostname(hostname)


def _is_target_hostname(hostname):
    target_hostname = {"c.app.hopsworks.ai"}
    return hostname in target_hostname


def _hash_string(input_string):
    if input_string:
        hash_object = hashlib.md5()
        hash_object.update(input_string.encode("utf-8"))
        hashed_string = hash_object.hexdigest()
        return hashed_string
    else:
        return ""


def _create_hopsworks_dir_if_not_exist():
    if not os.path.exists(HOPSWORKS_DIR):
        os.makedirs(HOPSWORKS_DIR)
    return HOPSWORKS_DIR


def _generate_user_id():
    random_uuid = uuid.uuid4()
    # Convert the UUID to a hexadecimal string and remove hyphens
    uuid_hex = random_uuid.hex.replace("-", "")
    short_uuid = "HOPSWORKSID=" + uuid_hex[:16]
    return short_uuid


def _extract_method_name_line_and_file(e):
    tb_frames = traceback.extract_tb(e.__traceback__)

    tb_result = []
    for frame in tb_frames[::-1]:
        method_name = frame.name
        line_number = frame.lineno
        file_name = frame.filename.split("/")[-1]
        tb_result.append(f"{file_name}::{line_number} {method_name}")
    return "\t".join(tb_result)


def method_logger(func):
    # Disable usage BEFORE import hsfs, return function itself
    if not _is_enabled:
        return func

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Disable usage AFTER import hsfs, return function itself
        if not _is_enabled:
            return func(*args, **kwargs)

        start_time = time.perf_counter()
        exception = None
        try:
            # Call the original method
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            exception = e
            raise e
        finally:
            try:
                end_time = time.perf_counter()
                execution_time = end_time - start_time
                _method_counter.add(func)
                # Send log to REST API server
                if exception or _method_counter.should_sample(func):
                    _executor.submit(_send_log, execution_time, func, exception)
            except Exception:
                pass

    return wrapper


def _send_log(execution_time, func, exception):
    tz = _env_attr.get_timezone()
    zoned_datetime = datetime.now(tz=tz)
    log_data = {
        # env
        "user_id": _env_attr.get_user_id(),
        "tz": tz.tzname(zoned_datetime),
        "datetime": zoned_datetime.astimezone(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S %Z"
        ),
        "backend_hostname": _hash_string(_env_attr.get_backend_host_name()),
        "backend_version": _env_attr.get_backend_version(),
        "platform": _env_attr.get_platform(),
        "python_version": _env_attr.get_python_version(),
        "hsml_version": _env_attr.get_hsml_version(),
        "hsfs_version": _env_attr.get_hsfs_version(),
        "hopsworks_version": _env_attr.get_hopsworks_version(),
        # method
        "method_name": func.__name__,
        "module_name": func.__module__,
        "execution_time": int(execution_time * 1000),
        "num_call": _method_counter.get_count(func),
        # error
        "error_message": str(exception) if exception else None,
        "stack_trace": (
            _extract_method_name_line_and_file(exception) if exception else None
        ),
    }
    _send_log_with_data(log_data)


def _send_log_with_data(log_data):
    headers = {"Content-type": "application/json"}
    data = json.dumps({"Data": log_data})
    _logger.debug(f"data: {data}")
    _conn.request("POST", "", body=data, headers=headers)
    _logger.debug(_conn.getresponse().read().decode("utf-8"))
