import re
from enum import Enum
from typing import Type, TypeVar

from sqlalchemy import Enum as SQLAlchemyEnum, Column
from sqlmodel import Field

T = TypeVar("T", bound=Enum)


def sqla_enum(enum_cls: Type[T]) -> Field:
    name = re.sub(r'(?<!^)(?=[A-Z])', '_', enum_cls.__name__).lower()
    meta_ = getattr(enum_cls, '__meta__', None)
    schema = getattr(meta_, 'schema', None)

    return Field(sa_column=Column(
        SQLAlchemyEnum(
            enum_cls,
            name=name,
            schema=schema,
            create_type=False,
        )
    ))
