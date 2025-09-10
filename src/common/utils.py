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


@F.udf(returnType=T.StringType())
def trim_to_none(s: Optional[str]) -> Optional[str]:
    """
    Normalize whitespace and newlines, trimming outer spaces.
    Returns None if the resulting string is empty.
    """
    if s:
        s = re.sub(r"\s+\n", "\n", s)
        s = re.sub(r"\n{3,}", "\n\n", s)
        s = s.strip()
    return s or None


if __name__ == "__main__":
    print(snake_case("_thisIsATest#cool_"))
