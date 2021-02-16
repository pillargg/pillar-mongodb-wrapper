from typing import List, Callable

from dbclient.dbclient import DBClient
from dbclient.lib import duration_to_int

name: str = "dbclient"

__all__: List[Callable] = [DBClient, duration_to_int]
