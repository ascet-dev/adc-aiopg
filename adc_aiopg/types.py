import typing as t
from typing import List, Generic

from pydantic import Field, create_model, BaseModel
from pydantic.fields import FieldInfo
from sqlmodel import SQLModel

T = t.TypeVar('T', bound='Base')

FieldDefinition = t.Tuple[t.Any, FieldInfo]


def _copy_fields(
        fields: t.Mapping[str, FieldInfo],
        optional: bool = False,
) -> t.Dict[str, FieldDefinition]:
    """Build create_model() field definitions from existing fields.

    Every field is rebuilt as a fresh FieldInfo, so a derived model shares
    nothing mutable with the model it was derived from: changing the copies
    cannot corrupt the source model's `model_fields`.

    With `optional=True` the annotation becomes `Optional[...]` and the default
    becomes `None`, so a partial schema also accepts an explicit `null`.
    """
    definitions: t.Dict[str, FieldDefinition] = {}
    for name, field in fields.items():
        annotation = field.annotation
        overrides: t.Dict[str, t.Any] = {}
        if optional:
            annotation = t.Optional[annotation]
            # `default_factory` is dropped: pydantic forbids it together with a default
            overrides = {'default': None, 'default_factory': None}
        # merge_field_infos() copies the FieldInfo and applies the overrides to both
        # the attributes and pydantic's "explicitly set" bookkeeping, keeping them
        # consistent; assigning to a copied FieldInfo would leave the latter stale.
        clone = FieldInfo.merge_field_infos(field, annotation=annotation, **overrides)
        definitions[name] = (annotation, clone)
    return definitions


class Base(SQLModel):
    @classmethod
    def partial(cls: t.Type[T]) -> t.Type[T]:
        """Derive a DTO where every field is optional and nullable.

        The source model is left untouched. Like `only()`/`exclude()`, the result is
        built on `Base`: it is a plain (non-table) schema, it is *not* a subclass of
        `cls`, and it does not inherit `cls`'s model_config or validators. The
        `Type[T]` return type is a typing convenience for field autocompletion, not
        a promise that the result is usable where `cls` is expected.
        """
        definitions = _copy_fields(cls.model_fields, optional=True)
        return create_model(f'Partial{cls.__name__}', __base__=Base, **definitions)

    @classmethod
    def only(cls: t.Type[T], *fields: str) -> t.Type[T]:
        """Derive a DTO with only the given fields.

        Raises ValueError on names `cls` does not define — a typo there would
        silently drop the field from the schema. See `partial()` for what the
        derived model does and does not inherit.
        """
        unknown = [name for name in fields if name not in cls.model_fields]
        if unknown:
            raise ValueError(f'{cls.__name__} has no field(s): {", ".join(sorted(unknown))}')
        definitions = _copy_fields({k: v for k, v in cls.model_fields.items() if k in fields})
        name = f'{cls.__name__}Only_' + '_'.join(definitions)
        return create_model(name, __base__=Base, **definitions)

    @classmethod
    def exclude(cls: t.Type[T], *excluded: str) -> t.Type[T]:
        """Derive a DTO without the given fields.

        Names `cls` does not define are ignored, so one exclusion set can be reused
        across models with slightly different columns. See `partial()` for what the
        derived model does and does not inherit.
        """
        definitions = _copy_fields({k: v for k, v in cls.model_fields.items() if k not in excluded})
        name = f'{cls.__name__}Exclude_' + '_'.join(excluded)
        return create_model(name, __base__=Base, **definitions)

    class Config:
        use_enum_values = True
        arbitrary_types_allowed = True
        from_attributes = True


class Pagination(Base):
    total: int
    limit: t.Optional[int] = Field(default=0)
    offset: t.Optional[int] = Field(default=0)


B = t.TypeVar('B', bound=Base)


class Paginated(BaseModel, Generic[B]):
    items: List[B]
    pagination: Pagination
