from typing import Any

from udp.connectors.base import Connector
from udp.connectors.csv import CsvConnector
from udp.connectors.database import DatabaseConnector
from udp.connectors.excel import ExcelConnector
from udp.connectors.rest_api import RestApiConnector

CONNECTORS: dict[str, Connector[Any, Any]] = {
    "csv": CsvConnector(),
    "database": DatabaseConnector(),
    "excel": ExcelConnector(),
    "rest_api": RestApiConnector(),
}
