"""Unit tests for PGDataAccessObject.from_model() helper, __init_subclass__, and bind()."""
import pytest
from sqlalchemy import MetaData
from sqlmodel import Field

from adc_aiopg.repository.entity_db_repository import PGDataAccessObject
from adc_aiopg.types import Base


class SimpleModel(Base):
    id: int | None = Field(default=None, primary_key=True)
    name: str


class TableModel(Base, table=True):
    __tablename__ = "table_model_test"
    id: int | None = Field(default=None, primary_key=True)
    name: str


# ---------------------------------------------------------------------------
# PGDataAccessObject.from_model() helper
# ---------------------------------------------------------------------------

class TestFromModel:
    def test_creates_subclass(self):
        cls = PGDataAccessObject.from_model(SimpleModel, "simple_items")
        assert issubclass(cls, PGDataAccessObject)

    def test_sets_model(self):
        cls = PGDataAccessObject.from_model(SimpleModel, "simple_items")
        assert cls.model is SimpleModel

    def test_sets_table_name(self):
        cls = PGDataAccessObject.from_model(SimpleModel, "simple_items")
        assert cls.table_name == "simple_items"

    def test_table_name_fallback_to_model_name(self):
        cls = PGDataAccessObject.from_model(SimpleModel)
        assert cls.table_name == "simplemodel"

    def test_table_name_fallback_to_tablename_attr(self):
        cls = PGDataAccessObject.from_model(TableModel)
        assert cls.table_name == "table_model_test"

    def test_class_name(self):
        cls = PGDataAccessObject.from_model(SimpleModel)
        assert cls.__name__ == "SimpleModelDAO"


# ---------------------------------------------------------------------------
# __init_subclass__
# ---------------------------------------------------------------------------

class TestInitSubclass:
    def test_table_name_kwarg(self):
        class MyDAO(PGDataAccessObject, table_name="my_table"):
            model = SimpleModel

        assert MyDAO.table_name == "my_table"

    def test_metadata_kwarg(self):
        meta = MetaData()

        class MyDAO(PGDataAccessObject, metadata=meta):
            model = SimpleModel

        assert MyDAO.metadata is meta

    def test_no_kwargs(self):
        class MyDAO(PGDataAccessObject):
            model = SimpleModel

        assert MyDAO.table_name is None

    def test_both_kwargs(self):
        meta = MetaData()

        class MyDAO(PGDataAccessObject, table_name="t", metadata=meta):
            model = SimpleModel

        assert MyDAO.table_name == "t"
        assert MyDAO.metadata is meta


# ---------------------------------------------------------------------------
# bind()
# ---------------------------------------------------------------------------

class TestBind:
    def test_creates_table_model_from_plain_model(self):
        meta = MetaData()

        class MyDAO(PGDataAccessObject, table_name="bind_test", metadata=meta):
            model = SimpleModel

        MyDAO.bind()

        assert MyDAO.table_model is not None
        assert hasattr(MyDAO.table_model, "__table__")
        assert MyDAO.table_model.__tablename__ == "bind_test"

    def test_idempotent(self):
        meta = MetaData()

        class MyDAO(PGDataAccessObject, table_name="idem_test", metadata=meta):
            model = SimpleModel

        MyDAO.bind()
        first = MyDAO.table_model

        MyDAO.bind()
        assert MyDAO.table_model is first

    def test_table_name_fallback(self):
        meta = MetaData()

        class MyDAO(PGDataAccessObject, metadata=meta):
            model = SimpleModel

        MyDAO.bind()
        assert MyDAO.table_name == "simplemodel"

    def test_uses_existing_table_same_metadata(self):
        """Model already table=True with same metadata — use directly."""
        assert hasattr(TableModel, "__table__")
        target_meta = TableModel.__table__.metadata

        class MyDAO(PGDataAccessObject, metadata=target_meta):
            model = TableModel

        MyDAO.bind()
        assert MyDAO.table_model is TableModel

    def test_wraps_existing_table_different_metadata(self):
        """Model already table=True but different metadata — create wrapper."""
        different_meta = MetaData()

        class MyDAO(PGDataAccessObject, table_name="wrapped", metadata=different_meta):
            model = TableModel

        MyDAO.bind()
        assert MyDAO.table_model is not TableModel
        assert hasattr(MyDAO.table_model, "__table__")

    def test_uses_existing_table_no_target_metadata(self):
        """Model already table=True, no target metadata — use directly."""

        class MyDAO(PGDataAccessObject):
            model = TableModel

        MyDAO.bind()
        assert MyDAO.table_model is TableModel

    def test_registers_in_metadata(self):
        meta = MetaData()

        class MyDAO(PGDataAccessObject, table_name="reg_test", metadata=meta):
            model = SimpleModel

        MyDAO.bind()
        assert "reg_test" in meta.tables
