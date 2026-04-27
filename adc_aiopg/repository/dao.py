from typing import Type

from asyncpg import Pool
from sqlalchemy import MetaData

from .entity_db_repository import PGDataAccessObject, PGPoolManager


class TableDescriptor:
    def __init__(self, dao_class: Type[PGDataAccessObject]):
        self.dao_class = dao_class

    def __get__(self, obj, owner) -> PGDataAccessObject:
        if not isinstance(obj, PostgresAccessLayer):
            raise ValueError("TableDescriptor can only be used with PostgresAccessLayer instances")

        if self.dao_class not in obj.daos:
            obj.daos[self.dao_class] = self.dao_class(db_pool=obj.pool)
        return obj.daos[self.dao_class]


class PostgresAccessLayer:
    meta: MetaData

    def __init_subclass__(cls, metadata: MetaData = None, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.meta = metadata
        for key, value in vars(cls).items():
            if isinstance(value, TableDescriptor):
                # Isolate: each DAL gets its own DAO class copy
                dao_copy = type(value.dao_class.__name__, (value.dao_class,), {'table_model': None})
                value.dao_class = dao_copy
                if not dao_copy.table_name:
                    dao_copy.table_name = key
                dao_copy.metadata = metadata
                dao_copy.bind()

    def __init__(self, pool: Pool):
        self.pool = pool
        self.daos = {}
        self.pm = PGPoolManager(pool)
