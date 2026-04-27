"""Unit tests for TableDescriptor and PostgresAccessLayer."""
from unittest.mock import MagicMock

import pytest
from sqlalchemy import MetaData
from sqlmodel import Field

from adc_aiopg.repository.dao import TableDescriptor, PostgresAccessLayer
from adc_aiopg.repository.entity_db_repository import PGDataAccessObject
from adc_aiopg.types import Base


class Animal(Base):
    id: int | None = Field(default=None, primary_key=True)
    name: str


# ---------------------------------------------------------------------------
# PostgresAccessLayer.__init_subclass__
# ---------------------------------------------------------------------------

class TestPostgresAccessLayer:
    def test_stores_metadata(self):
        meta = MetaData()

        class DAL(PostgresAccessLayer, metadata=meta):
            pass

        assert DAL.meta is meta

    def test_propagates_metadata_to_dao(self):
        meta = MetaData()
        AnimalDAO = PGDataAccessObject.from_model(Animal, "animals_meta_test")

        class DAL(PostgresAccessLayer, metadata=meta):
            animals = TableDescriptor(AnimalDAO)

        dao_copy = vars(DAL)['animals'].dao_class
        assert dao_copy.metadata is meta

    def test_calls_bind(self):
        meta = MetaData()
        AnimalDAO = PGDataAccessObject.from_model(Animal, "animals_bind_test")

        class DAL(PostgresAccessLayer, metadata=meta):
            animals = TableDescriptor(AnimalDAO)

        dao_copy = vars(DAL)['animals'].dao_class
        assert dao_copy.table_model is not None

    def test_does_not_mutate_original_dao(self):
        meta = MetaData()
        AnimalDAO = PGDataAccessObject.from_model(Animal, "animals_isolate_test")

        class DAL(PostgresAccessLayer, metadata=meta):
            animals = TableDescriptor(AnimalDAO)

        assert AnimalDAO.metadata is None
        assert AnimalDAO.table_model is None

    def test_fallback_table_name_to_attr_name(self):
        meta = MetaData()

        class MyDAO(PGDataAccessObject):
            model = Animal

        class DAL(PostgresAccessLayer, metadata=meta):
            creatures = TableDescriptor(MyDAO)

        dao_copy = vars(DAL)['creatures'].dao_class
        assert dao_copy.table_name == "creatures"

    def test_does_not_override_explicit_table_name(self):
        meta = MetaData()
        AnimalDAO = PGDataAccessObject.from_model(Animal, "zoo_animals")

        class DAL(PostgresAccessLayer, metadata=meta):
            animals = TableDescriptor(AnimalDAO)

        dao_copy = vars(DAL)['animals'].dao_class
        assert dao_copy.table_name == "zoo_animals"

    def test_registers_tables_in_metadata(self):
        meta = MetaData()

        class DAL(PostgresAccessLayer, metadata=meta):
            beasts = TableDescriptor(PGDataAccessObject.from_model(Animal, "beasts_table"))

        assert "beasts_table" in meta.tables

    def test_same_dao_in_two_dals_isolated(self):
        """Same DAO class used in two DALs with different metadata stays isolated."""
        meta1 = MetaData()
        meta2 = MetaData()
        AnimalDAO = PGDataAccessObject.from_model(Animal, "animals_shared")

        class DAL1(PostgresAccessLayer, metadata=meta1):
            animals = TableDescriptor(AnimalDAO)

        class DAL2(PostgresAccessLayer, metadata=meta2):
            animals = TableDescriptor(AnimalDAO)

        copy1 = vars(DAL1)['animals'].dao_class
        copy2 = vars(DAL2)['animals'].dao_class
        assert copy1 is not copy2
        assert copy1.metadata is meta1
        assert copy2.metadata is meta2


# ---------------------------------------------------------------------------
# TableDescriptor
# ---------------------------------------------------------------------------

class TestTableDescriptor:
    def test_get_raises_on_non_dal_instance(self):
        AnimalDAO = PGDataAccessObject.from_model(Animal, "td_test")
        td = TableDescriptor(AnimalDAO)

        with pytest.raises(ValueError, match="PostgresAccessLayer"):
            td.__get__("not a DAL", type("Fake", (), {}))

    def test_caches_dao_instance(self):
        """Second access returns the same DAO instance."""
        meta = MetaData()
        AnimalDAO = PGDataAccessObject.from_model(Animal, "cache_test")

        class DAL(PostgresAccessLayer, metadata=meta):
            animals = TableDescriptor(AnimalDAO)

        pool = MagicMock()
        dal = DAL(pool)

        first = dal.animals
        second = dal.animals
        assert first is second

    def test_custom_dao_class(self):
        meta = MetaData()

        class CustomAnimalDAO(PGDataAccessObject, table_name="custom_anim"):
            model = Animal

        class DAL(PostgresAccessLayer, metadata=meta):
            animals = TableDescriptor(CustomAnimalDAO)

        pool = MagicMock()
        dal = DAL(pool)

        assert isinstance(dal.animals, CustomAnimalDAO)

    def test_pm_available(self):
        meta = MetaData()

        class DAL(PostgresAccessLayer, metadata=meta):
            pass

        pool = MagicMock()
        dal = DAL(pool)
        assert dal.pm is not None
