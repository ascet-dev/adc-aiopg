from datetime import datetime, UTC
from logging import getLogger
from typing import Optional, Union, List
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.sql import Select

logger = getLogger(__name__)
dialect = PGDialect_asyncpg(paramstyle='pyformat')


def compile_query(query):
    compiled = query.compile(dialect=dialect, compile_kwargs={"render_postcompile": True})
    compiled_params = sorted(compiled.params.items())
    mapping = {key: '$' + str(number) for number, (key, _) in enumerate(compiled_params, start=1)}
    new_query = compiled.string % mapping
    new_params = [val for key, val in compiled_params]
    logger.debug('\n%s', compiled.string % compiled.params)
    return new_query, new_params


def create(table: sa.Table, payload):
    return sa.insert(table).values(payload).returning(table)


def count(table: sa.Table):
    return sa.select(sa.func.count()).select_from(table)


def search(
    table: sa.Table,
    order_by: Union[List, str] = None,
    limit: Optional[int] = None,
    offset: int = 0,
):
    query = sa.select(table)

    if order_by:
        if isinstance(order_by, List):
            for order in order_by:
                query = _add_order_to_query(query, order, sa.column)
        else:
            query = _add_order_to_query(query, order_by, sa.column)

    if offset:
        query = query.offset(offset)
    if limit is not None:
        query = query.limit(limit)
    return query


def _add_order_to_query(query: Select, order_by: str, column_getter) -> Select:
    if order_by.startswith('-'):
        order_by_column = sa.desc(column_getter(order_by[1:]))
    else:
        order_by_column = column_getter(order_by)
    query = query.order_by(order_by_column)
    return query


def get_by_id(table: sa.Table, entity_id: Union[int, UUID]):
    return sa.select(table).where(table.id == entity_id)


def update(table: sa.Table, **kwargs):
    """Base query builder for update rows. Don't use it without filters!"""
    return sa.update(table).values(updated=datetime.now(UTC), **kwargs).returning(table)


def update_by_id(table: sa.Table, entity_id: Union[int, UUID], **kwargs):
    return update(table, **kwargs).where(table.id == entity_id)


def delete(table: sa.Table):
    """Base query builder for delete rows. Don't use it without filters!"""
    return sa.delete(table).returning(table)


def delete_by_id(table: sa.Table, entity_id: Union[int, UUID]):
    return delete(table).where(table.id == entity_id)
