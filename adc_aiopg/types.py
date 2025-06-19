from dataclasses import dataclass
from typing import List, Mapping


class Pagination:
    limit: int
    offset: int
    total: int


class PaginatedResponse:
    items: List[Mapping]
    pagination: Pagination
