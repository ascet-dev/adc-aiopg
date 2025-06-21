import typing as t
from typing import List

from pydantic import Field
from pydantic import create_model
from sqlmodel import SQLModel

T = t.TypeVar('T', bound='Base')


class Base(SQLModel, t.Generic[T]):
    @classmethod
    def partial(cls: t.Type[T]) -> t.Type[T]:
        fields = {k: (v.annotation, v) for k, v in cls.model_fields.items()}
        for field in fields:
            fields[field][1].default = None
        return create_model(f'Partial{cls.__name__}', __base__=cls, **fields)

    @classmethod
    def only(cls: t.Type[T], *fields: str) -> t.Type[T]:
        fields = {k: (v.annotation, v) for k, v in cls.model_fields.items() if k in fields}
        name = f'{cls.__name__}Only_' + '_'.join(fields)
        return create_model(name, __base__=Base, **fields)

    @classmethod
    def exclude(cls: t.Type[T], *excluded: str) -> t.Type[T]:
        fields = {k: (v.annotation, v) for k, v in cls.model_fields.items() if k not in excluded}
        name = f'{cls.__name__}Exclude_' + '_'.join(excluded)
        return create_model(name, __base__=Base, **fields)

    class Config:
        use_enum_values = True
        arbitrary_types_allowed = True
        from_attributes = True


class Pagination(Base):
    total: int
    limit: t.Optional[int] = Field(default=0)
    offset: t.Optional[int] = Field(default=0)


class Paginated[T](Base):
    items: List[T]
    pagination: Pagination
