"""Tests for MetaData registration, version tables, and Alembic integration."""
import pytest
from sqlalchemy import MetaData, inspect, create_engine
from sqlmodel import Field

from adc_aiopg.repository.dao import TableDescriptor, PostgresAccessLayer
from adc_aiopg.repository.entity_db_repository import PGDataAccessObject
from adc_aiopg.types import Base
from adc_aiopg.version_table import (
    declare_version_table,
    get_create_version_trigger_sql,
    get_delete_version_trigger_sql,
)


class Widget(Base):
    id: int | None = Field(default=None, primary_key=True)
    label: str


# ---------------------------------------------------------------------------
# MetaData registration
# ---------------------------------------------------------------------------

class TestMetadataRegistration:
    def test_dao_helper_registers_table(self):
        meta = MetaData()
        WidgetDAO = PGDataAccessObject.from_model(Widget, "widgets_reg")
        WidgetDAO.metadata = meta
        WidgetDAO.bind()

        assert "widgets_reg" in meta.tables

    def test_dal_registers_all_tables(self):
        meta = MetaData()
        W1 = PGDataAccessObject.from_model(Widget, "w1_reg")
        W2 = PGDataAccessObject.from_model(Widget, "w2_reg")

        class DAL(PostgresAccessLayer, metadata=meta):
            w1 = TableDescriptor(W1)
            w2 = TableDescriptor(W2)

        assert "w1_reg" in meta.tables
        assert "w2_reg" in meta.tables

    def test_custom_dao_registers_table(self):
        meta = MetaData()

        class CustomWidgetDAO(PGDataAccessObject, table_name="custom_widgets"):
            model = Widget

        class DAL(PostgresAccessLayer, metadata=meta):
            widgets = TableDescriptor(CustomWidgetDAO)

        assert "custom_widgets" in meta.tables

    def test_dal_meta_attr(self):
        meta = MetaData()

        class DAL(PostgresAccessLayer, metadata=meta):
            pass

        assert DAL.meta is meta


# ---------------------------------------------------------------------------
# Version tables
# ---------------------------------------------------------------------------

class TestVersionTable:
    def test_declare_version_table(self):
        meta = MetaData()

        class Versioned(Base, table=True):
            __tablename__ = "versioned_test"
            metadata = meta
            id: int | None = Field(default=None, primary_key=True)
            name: str

        VersionModel = declare_version_table(Versioned)
        assert VersionModel.__name__ == "VersionedLog"
        assert "public.versioned_test_log" in meta.tables

    def test_version_table_columns_nullable(self):
        meta = MetaData()

        class Strict(Base, table=True):
            __tablename__ = "strict_test"
            metadata = meta
            id: int | None = Field(default=None, primary_key=True)
            name: str

        VersionModel = declare_version_table(Strict)
        log_table = meta.tables["public.strict_test_log"]

        for col in log_table.columns:
            assert col.nullable is True


# ---------------------------------------------------------------------------
# Trigger SQL generation
# ---------------------------------------------------------------------------

class TestTriggerSQL:
    def test_create_trigger_sql(self):
        sql = get_create_version_trigger_sql(
            schema="public",
            table_name="orders",
            version_table_name="orders_log",
        )
        assert "CREATE OR REPLACE FUNCTION" in sql
        assert "CREATE TRIGGER" in sql
        assert "orders" in sql
        assert "orders_log" in sql

    def test_delete_trigger_sql(self):
        sql = get_delete_version_trigger_sql(
            schema="public",
            table_name="orders",
        )
        assert "DROP TRIGGER" in sql
        assert "DROP FUNCTION" in sql


# ---------------------------------------------------------------------------
# Integration: tables actually created in DB
# ---------------------------------------------------------------------------

class TestTablesInDB:
    def test_tables_exist(self, pg_sync_url):
        engine = create_engine(pg_sync_url)
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        engine.dispose()

        assert "items" in tables
        assert "products" in tables
