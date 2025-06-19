from logging import getLogger
from typing import Type, TypeVar, Generic

from asyncpg import Pool
from sqlalchemy import MetaData
from sqlmodel import SQLModel

from .entity_db_repository import PGDataAccessObject, PGPoolManager

logger = getLogger(__name__)

T = TypeVar('T', bound=SQLModel)


class DAOMeta(type):
    def __new__(mcs, name, bases, namespace, metadata: MetaData = None):
        namespace['meta'] = metadata
        for key, value in namespace.items():
            if isinstance(value, TableDescriptor):
                model_table = type(
                    f'SQLModel{value.model.__name__}',
                    (value.model,),
                    {
                        '__tablename__': value.table_name or key,
                        'metadata': namespace['meta'],
                    },
                    table=True,
                )

                value.set_table_model(model_table)

        return super().__new__(mcs, name, bases, namespace)


class TableDescriptor(Generic[T]):
    def __init__(self, model: Type[T], table_name: str | None = None):
        self.model = model
        self.table_name = table_name
        self.table_model = None
        self.dao = None

    def set_table_model(self, table_model: Type[T]) -> None:
        self.table_model = table_model

    def __get__(self, obj, owner) -> PGDataAccessObject[T]:
        if not isinstance(obj, PostgresAccessLayer):
            raise ValueError("ModelDescriptor can only be used with DBDAO instances")
        if not self.table_model:
            raise NotImplementedError("Use set_table_model first")

        if self.model not in obj.daos:
            obj.daos[self.model] = PGDataAccessObject(self.table_model, db_pool=obj.pool)
        return obj.daos[self.model]


class PostgresAccessLayer(metaclass=DAOMeta):
    meta: MetaData

    def __init__(self, pool: Pool):
        self.pool = pool
        self.daos = {}
        self.pm = PGPoolManager(pool)
