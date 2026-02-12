import re
from enum import Enum
from typing import Type

from sqlalchemy import Enum as SQLAlchemyEnum, Column
from sqlmodel import Field


    
def sqla_enum(enum_cls: Type[Enum], **field_kwargs) -> Field:
    name = re.sub(r'(?<!^)(?=[A-Z])', '_', enum_cls.__name__).lower()
    meta_ = getattr(enum_cls, '__meta__', None)
    schema = getattr(meta_, 'schema', None)
    
    return Field(sa_column=Column(
        SQLAlchemyEnum(
            enum_cls,
            name=name,
            schema=schema,
            create_type=True,
        ),
        **field_kwargs,
    ))