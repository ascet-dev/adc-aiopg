from typing import Optional, Union, Mapping, List, TypeVar, Generic, Type
from uuid import UUID
from weakref import WeakValueDictionary
from asyncpg import Pool
from sqlalchemy import MetaData

from adc_aiopg.types import Pagination, Paginated, Base
from adc_aiopg.errors import RowNotFoundError
from adc_aiopg.query import create, get_by_id, search, update_by_id, update, count, delete_by_id, delete
from .db_repository import PGPoolManager

T = TypeVar('T', bound=Base)


class PGDataAccessObject(PGPoolManager, Generic[T]):
    model: Type[T] | None = None
    metadata: MetaData | None = None
    table_name: str | None = None
    table_model: Type[T] | None = None
    _bind_cache: WeakValueDictionary = WeakValueDictionary()

    def __init_subclass__(cls, table_name: str = None, metadata: MetaData = None, **kwargs):
        super().__init_subclass__(**kwargs)
        if table_name:
            cls.table_name = table_name
        if metadata:
            cls.metadata = metadata

    @classmethod
    def from_model(
        cls,
        model: Type[T],
        table_name: str | None = None,
        metadata: MetaData | None = None,
    ) -> 'Type[PGDataAccessObject[T]]':
        """Create a DAO class from a model."""
        table_name = table_name or getattr(model, '__tablename__', model.__name__)
        return type(f'{model.__name__}DAO', (cls,), {'model': model},
                    table_name=table_name, metadata=metadata)

    @classmethod
    def bind(cls):
        if cls.table_model:
            if cls.metadata and hasattr(cls.table_model, '__table__') \
                    and cls.table_model.__table__.metadata is not cls.metadata:
                raise ValueError(
                    f"{cls.__name__} is already bound to a different MetaData. "
                    "Create a new DAO class for each PostgresAccessLayer."
                )
            return
        if not cls.table_name:
            cls.table_name = getattr(cls.model, '__tablename__', cls.model.__name__)
        if hasattr(cls.model, '__table__') and (not cls.metadata or cls.model.__table__.metadata is cls.metadata):
            cls.table_model = cls.model
        else:
            cache_key = (cls.metadata, cls.table_name, cls.model)
            cached = PGDataAccessObject._bind_cache.get(cache_key)
            if cached:
                cls.table_model = cached
            else:
                cls.table_model = type(
                    f'SQLModel{cls.model.__name__}',
                    (cls.model,),
                    {
                        '__tablename__': cls.table_name,
                        'metadata': cls.metadata,
                    },
                    table=True,
                )
                PGDataAccessObject._bind_cache[cache_key] = cls.table_model

    def __init__(self, db_pool: Pool, entity_versions: Type[T] | None = None):
        if not self.table_model:
            self.bind()
        self.model = self.table_model
        self.entity_versions = entity_versions
        super().__init__(db_pool)

    @property
    def has_version(self) -> bool:
        return self.entity_versions is not None

    def _get_filter_bool_expression(self, filter_name, filter_value):
        if filter_name in self.model.__dict__:
            return getattr(self.model, filter_name).__eq__(filter_value)

        split_by_underscore = filter_name.split('_')
        sign = split_by_underscore.pop()
        col_name = '_'.join(split_by_underscore)
        col = getattr(self.model, col_name)

        if sign in {'lt', 'le', 'gt', 'ge', 'ne'}:
            return getattr(col, f'__{sign}__')(filter_value)
        elif sign == 'in':
            return col.in_(filter_value)
        elif sign == 'notin':
            return ~col.in_(filter_value)
        elif sign == 'is':
            return col.is_(filter_value)
        elif sign == 'isnot':
            return col.is_not(filter_value)
        elif sign == 'like':
            return col.like(filter_value)
        elif sign == 'ilike':
            return col.ilike(filter_value)

        raise ValueError(f'Unknown filter name ({filter_name})')

    def _apply_filters(self, query, **filters):
        for filter_name, filter_value in filters.items():
            query = query.where(self._get_filter_bool_expression(filter_name, filter_value))

        return query

    async def count(self, **filters) -> int:
        query = count(self.model)
        query = self._apply_filters(query, **filters)
        return await self.fetchval(query)

    async def create(self, *args, **kwargs) -> T:
        payload = [*args] if args else [kwargs]
        row = await self.fetchrow(create(self.model, payload))
        return self.model(**row)

    async def create_many(self, payload: List[Mapping]) -> List[T]:
        rows = await self.fetch(create(self.model, payload)) if payload else []
        return [self.model(**row) for row in rows]

    async def search(
        self,
        order_by: Union[List, str] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        **filters,
    ) -> List[T]:
        query = search(self.model, order_by=order_by, limit=limit, offset=offset)
        query = self._apply_filters(query, **filters)
        rows = await self.fetch(query)
        return [self.model(**row) for row in rows]

    async def get_by_id(self, entity_id: Union[int, UUID]) -> T:
        row = await self.fetchrow(get_by_id(table=self.model, entity_id=entity_id))
        if not row:
            raise RowNotFoundError()
        return self.model(**row)

    async def get_or_create(self, **kwargs) -> T:
        existing_rows = await self.search(**kwargs)
        if len(existing_rows) == 1:
            return existing_rows[0]
        elif len(existing_rows) > 1:
            raise ValueError('Ambiguous value for %s' % kwargs)

        return await self.create(**kwargs)

    async def update_by_id(self, entity_id: Union[int, UUID], **payload) -> T:
        update_query = update_by_id(table=self.model, entity_id=entity_id, **payload)
        row = await self.fetchrow(update_query)
        if not row:
            raise RowNotFoundError('No row has been updated')
        return self.model(**row)

    async def update(self, payload: Mapping, **filters) -> List[T]:
        update_query = update(self.model, **payload)
        update_query = self._apply_filters(update_query, **filters)

        return await self.fetch(update_query)

    async def delete_by_id(self, entity_id: Union[int, UUID]) -> T:
        row = await self.fetchrow(delete_by_id(table=self.model, entity_id=entity_id))
        if not row:
            raise RowNotFoundError('No row has been deleted')
        return self.model(**row)

    async def delete(self, **filters) -> List[T]:
        delete_query = delete(self.model)
        delete_query = self._apply_filters(delete_query, **filters)
        rows = await self.fetch(delete_query)
        return [self.model(**row) for row in rows]

    async def archive_by_id(self, entity_id: Union[int, UUID]) -> T:
        return await self.update_by_id(entity_id=entity_id, archived=True)

    async def archive(self, **filters) -> List[T]:
        return await self.update({'archived': True}, **filters)

    async def paginated_search(
        self,
        order_by: Union[List, str] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        **filters,
    ) -> Paginated[T]:
        rows = await self.search(order_by, limit, offset, **filters)
        total = await self.count(**filters)

        return Paginated[self.model](
            items=rows,
            pagination=Pagination(
                limit=limit,
                offset=offset,
                total=total,
            ),
        )
