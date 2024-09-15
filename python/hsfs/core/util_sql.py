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

import asyncio
import logging
from typing import Any, Dict, Optional

from hopsworks_common.core import variable_api
from hsfs.core.constants import HAS_AIOMYSQL, HAS_SQLALCHEMY


if HAS_SQLALCHEMY:
    from sqlalchemy import create_engine
    from sqlalchemy.engine.url import URL, make_url

if HAS_AIOMYSQL:
    from aiomysql.sa import create_engine as async_create_engine


_logger = logging.getLogger(__name__)


def create_mysql_engine(
    online_conn: Any, external: bool, options: Optional[Dict[str, Any]] = None
) -> Any:
    _logger.debug("Creating SQL Alchemy MYSQL engine")
    online_options = online_conn.spark_options()
    sql_alchemy_conn_url = build_database_url(
        online_options=online_options, external=external
    )

    if options is None:
        options = {"pool_recycle": 3600}
    elif isinstance(options, dict):
        if "pool_recycle" not in options:
            options["pool_recycle"] = 3600
    else:
        raise TypeError("options should be a dictionary")

    # default connection pool size kept by engine is 5
    sql_alchemy_engine = create_engine(
        sql_alchemy_conn_url.render_as_string(), **options
    )
    return sql_alchemy_engine


def build_database_url(online_options: Dict[str, str], external: bool) -> URL:
    """Here we are replacing the first part of the string returned by Hopsworks,
    # jdbc:mysql:// with the sqlalchemy one + username and password
    # useSSL and allowPublicKeyRetrieval are not valid properties for the pymysql driver
    # to use SSL we'll have to something like this:
    ```python
    ssl_args = {'ssl_ca': ca_path}
    engine = create_engine("mysql+pymysql://<user>:<pass>@<addr>/<schema>", connect_args=ssl_args)
    ```
    """
    _logger.debug("Building MySQL connection string from online options")
    cleaned_url = (
        online_options["url"]
        .replace("jdbc:mysql://", "mysql+pymysql://")
        .replace("useSSL=false&", "")
        .replace("?allowPublicKeyRetrieval=true", "")
    )
    url = make_url(cleaned_url)
    if external:
        # This only works with external clients.
        # Hopsworks clients should use the storage connector
        url = url.set(
            host=variable_api.VariableApi().get_loadbalancer_external_domain("mysqld")
        )
        _logger.debug("External MySQL host: %s", url.host)
    else:
        _logger.debug("Internal MySQL host: %s", url.host)
    url.set(username=online_options["user"], password=online_options["password"])
    _logger.debug("MySQL connection string: %s", url.render_as_string())

    return url


async def create_async_engine(
    online_conn: Any,
    external: bool,
    default_min_size: int,
    options: Optional[Dict[str, Any]] = None,
) -> Any:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError as er:
        raise RuntimeError(
            "Event loop is not running. Please invoke this co-routine from a running loop or provide an event loop."
        ) from er
    sqlalchemy_url = build_database_url(
        online_options=online_conn.spark_options(), external=external
    )
    if options is None:
        options = {}
    if not HAS_AIOMYSQL:
        raise ImportError(
            "hopsworks_aiomysql is not installed. Please install hopsworks_aiomysql to use this feature."
        )

    # create a aiomysql connection pool
    pool = await async_create_engine(
        host=sqlalchemy_url.host,
        port=sqlalchemy_url.port,
        user=sqlalchemy_url.username,
        password=sqlalchemy_url.password,
        db=sqlalchemy_url.database,
        loop=loop,
        minsize=options.get("minsize", default_min_size),
        maxsize=options.get("maxsize", default_min_size),
        pool_recycle=options.get("pool_recycle", 10),
        autocommit=True,
        **options,
    )
    return sqlalchemy_url.host, pool
