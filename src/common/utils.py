"""
Utilities for Databricks notebooks:
- Structured logging to stdout and stderr
- Catalog and schema detection from Spark errors
- String helpers including snake_case
- Common PySpark UDFs
"""

import logging
import re
import sys
from typing import Optional
from urllib.parse import urljoin

import requests
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T


def logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a configured logger that routes < WARNING to stdout and >= WARNING to stderr.

    The logger is initialized once per name and cached on the logging module.
    """
    if not name:
        name = __name__

    log = logging.getLogger(name)
    if not log.handlers:
        log.setLevel(logging.DEBUG)

        stdout_level = logging.INFO
        stderr_level = logging.WARNING
        print(
            f"creating log handler - name:{name} stdout_level:{stdout_level} stderr_level:{stderr_level}"
        )
        fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

        # Handler for < WARNING -> stdout
        h_stdout = logging.StreamHandler(sys.stdout)
        h_stdout.setLevel(stdout_level)
        h_stdout.addFilter(lambda record: record.levelno < stderr_level)
        h_stdout.setFormatter(fmt)

        # Handler for >= WARNING -> stderr
        h_stderr = logging.StreamHandler(sys.stderr)
        h_stderr.setLevel(stderr_level)
        h_stderr.setFormatter(fmt)

        log.addHandler(h_stdout)
        log.addHandler(h_stderr)

    return log


def config_value(name: str, default: Optional[str] = None):
    dbutils = get_dbutils()

    try:
        value = dbutils.widgets.get(name)
        if value:
            return value
    except Exception:
        pass

    try:
        value = spark.conf.get(name)
        if value:
            return value
    except Exception:
        pass

    if default is not None:
        return default
    raise ValueError(f"Missing configuration value: {name}")


def get_spark():
    global_name = "spark"
    if global_name in globals():
        return globals()[global_name]
    return SparkSession.builder.getOrCreate()


def get_dbutils():
    global_name = "dbutils"
    if global_name in globals():
        return globals()[global_name]
    return DBUtils(get_spark())


def get_notebook_context():
    return get_dbutils().notebook.entry_point.getDbutils().notebook().getContext()


def api_url(path: str = None) -> str:
    url = get_notebook_context().apiUrl().get().rstrip("/")
    if path:
        url = urljoin(url + "/", path.lstrip("/"))
    return url


def api_headers() -> dict:
    token = get_notebook_context().apiToken().get()
    return {"Authorization": f"Bearer {token}"}


def http_request(
        url: str,
        method: str = "GET",
        headers: dict = None,
        params: dict = None,
        json: dict = None,
        *acceptable_status_codes: int
):
    """
    Perform an HTTP request with validation on acceptable status codes.

    Args:
        url (str): Target URL.
        method (str, optional): HTTP method. Defaults to GET.
        headers (dict, optional): Request headers.
        params (dict, optional): Query parameters.
        json (dict, optional): Request body (JSON).
        *acceptable_status_codes (int): Acceptable HTTP status codes. Defaults to (200,).

    Raises:
        RuntimeError: If response status code is not acceptable.

    Returns:
        requests.Response: Response object.
    """
    if not acceptable_status_codes:
        acceptable_status_codes = (200,)
    elif -1 in acceptable_status_codes:
        acceptable_status_codes = None

    if not headers and url.startswith(api_url()):
        headers = api_headers()

    resp = requests.request(
        method=method.upper(),
        url=url,
        headers=headers,
        params=params,
        json=json,
    )

    if acceptable_status_codes and resp.status_code not in acceptable_status_codes:
        raise RuntimeError(
            f"Unexpected status code {resp.status_code}\n"
            f"Headers: {resp.headers}\n"
            f"Body: {resp.text}"
        )

    return resp


def snake_case(s: str) -> str:
    """
    Convert a string to snake_case.

    Steps:
      1. Replace non alphanumeric characters with spaces
      2. Split camelCase and PascalCase boundaries
      3. Join with underscores in lowercase
    """
    cleaned = re.sub(r"[^0-9A-Za-z]+", " ", s)
    split_camel_matches = re.finditer(
        r'.+?(?:(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', cleaned
    )
    parts = []
    for m in split_camel_matches:
        parts.extend(m.group(0).split())
    return "_".join(p.lower() for p in parts if p)


@F.udf(returnType=T.StringType())
def os_path(path: Optional[str]) -> Optional[str]:
    """
    Convert a dbfs-prefixed path to a plain path.

    Examples:
        dbfs:/foo -> /foo
        dbfs://foo -> /foo
    """
    if path is None:
        return None
    return re.sub(r"^dbfs:(//)?", "", path)


if __name__ == "__main__":
    print(snake_case("_thisIsATest#cool_"))
