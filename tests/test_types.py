"""Unit tests for the derived-schema helpers on Base (partial/only/exclude)."""
import enum

import pytest
from pydantic import ValidationError, field_validator
from sqlmodel import Field

from adc_aiopg.enum import sqla_enum
from adc_aiopg.types import Base


class Model(Base):
    a: str
    b: int = 0
    c: str | None = None


# ---------------------------------------------------------------------------
# partial()
# ---------------------------------------------------------------------------

def test_partial_does_not_mutate_source_model():
    class M(Base):
        a: str
        b: int

    assert M.model_fields['a'].is_required()

    P = M.partial()

    # the source model is untouched, even after the core schema is rebuilt
    assert M.model_fields['a'].is_required()
    M.model_rebuild(force=True)
    with pytest.raises(ValidationError):
        M()

    # the partial schema really is optional and accepts an explicit null
    assert not P.model_fields['a'].is_required()
    assert P().a is None
    assert P(a=None).a is None


def test_partial_does_not_share_field_objects():
    class M(Base):
        a: str = Field(default='x', max_length=5)

    P = M.partial()
    source, derived = M.model_fields['a'], P.model_fields['a']
    assert derived is not source
    assert derived.metadata is not source.metadata
    # pydantic's "explicitly set" bookkeeping is copied and kept consistent with
    # the attributes, so nothing can resurrect the source default
    assert derived._attributes_set['default'] is None
    assert source._attributes_set['default'] == 'x'


def test_derived_schema_is_order_independent():
    class M(Base):
        a: str
        b: int

    M.partial()
    assert M.only('a').model_fields['a'].is_required()
    assert M.exclude('b').model_fields['a'].is_required()


def test_partial_keeps_values_and_constraints():
    class M(Base):
        a: str = Field(max_length=3, description='hello')

    P = M.partial()
    assert P.model_fields['a'].description == 'hello'
    assert P(a='ab').a == 'ab'
    with pytest.raises(ValidationError):
        P(a='abcd')


def test_partial_overrides_default_factory():
    class M(Base):
        a: list = Field(default_factory=list)

    P = M.partial()
    assert P().a is None


def test_partial_is_a_plain_schema():
    class Color(str, enum.Enum):
        red = 'red'

    class Table(Base, table=True):
        __tablename__ = 'test_partial_table'
        id: int | None = Field(default=None, primary_key=True)
        name: str
        color: Color = sqla_enum(Color, nullable=False)

    P = Table.partial()

    # a DTO, not another mapping of the same table
    assert '__table__' not in P.__dict__
    assert not issubclass(P, Table)
    assert P().name is None
    assert Table.model_fields['name'].is_required()


# ---------------------------------------------------------------------------
# only() / exclude()
# ---------------------------------------------------------------------------

def test_only_keeps_requested_fields():
    S = Model.only('a', 'b')
    assert set(S.model_fields) == {'a', 'b'}
    assert S.model_fields['a'].is_required()
    assert S(a='x').b == 0


def test_exclude_drops_requested_fields():
    S = Model.exclude('c')
    assert set(S.model_fields) == {'a', 'b'}
    assert S.model_fields['a'].is_required()


def test_only_rejects_unknown_field():
    with pytest.raises(ValueError, match='no field'):
        Model.only('a', 'nmae')


def test_exclude_ignores_unknown_field():
    """Deliberate: one exclusion set is reused across models with different columns."""
    S = Model.exclude('c', 'zzz')
    assert set(S.model_fields) == {'a', 'b'}


def test_only_does_not_mutate_source_model():
    class M(Base):
        a: str

    S = M.only('a')
    S.model_fields['a'].default = None
    assert M.model_fields['a'].is_required()
    M.model_rebuild(force=True)
    with pytest.raises(ValidationError):
        M()


def test_derived_schemas_do_not_inherit_config_or_validators():
    """Documented behaviour: derived schemas are plain DTOs built on Base."""

    class M(Base):
        model_config = {'extra': 'forbid'}
        a: str

        @field_validator('a')
        @classmethod
        def upper(cls, v):
            return v.upper()

    assert M(a='x').a == 'X'
    assert M.only('a')(a='x').a == 'x'
    assert M.only('a').model_config.get('extra') is None
