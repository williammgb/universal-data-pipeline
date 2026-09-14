from typing import Any

from udp.connectors.base import Connector
from udp.connectors.csv import CsvConnector

CONNECTORS: dict[str, Connector[Any, Any]] = {"csv": CsvConnector()}
