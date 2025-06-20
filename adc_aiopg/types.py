from typing import List


class Pagination:
    limit: int
    offset: int
    total: int


class PaginatedResponse[T]:
    items: List[T]
    pagination: Pagination
