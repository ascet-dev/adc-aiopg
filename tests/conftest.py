from datetime import datetime

import pytest
import pytest_asyncio
from asyncpg import Pool
from sqlalchemy import MetaData, create_engine
from sqlmodel import Field

from adc_aiopg.connection import create_db_pool
from adc_aiopg.repository.dao import TableDescriptor, PostgresAccessLayer
from adc_aiopg.repository.entity_db_repository import PGDataAccessObject
from adc_aiopg.types import Base
from testcontainers.postgres import PostgresContainer


# ---------------------------------------------------------------------------
# Test models
# ---------------------------------------------------------------------------

class Item(Base):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    email: str | None = None
    age: int | None = None
    active: bool | None = None
    archived: bool | None = None
    updated: datetime | None = None


class Product(Base):
    id: int | None = Field(default=None, primary_key=True)
    title: str
    price: int = 0


# ---------------------------------------------------------------------------
# Custom DAO for testing
# ---------------------------------------------------------------------------

class ItemsDAO(PGDataAccessObject[Item], table_name="items"):
    model = Item

    async def find_by_name(self, name: str):
        results = await self.search(name=name)
        return results[0] if results else None


# ---------------------------------------------------------------------------
# Docker PostgreSQL
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def pg_container():
    with PostgresContainer("postgres:16") as pg:
        yield pg


@pytest.fixture(scope="session")
def pg_url(pg_container):
    url = pg_container.get_connection_url()
    return url.replace("+psycopg2", "")


@pytest.fixture(scope="session")
def pg_sync_url(pg_container):
    return pg_container.get_connection_url()


# ---------------------------------------------------------------------------
# Metadata & DAL
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def metadata():
    return MetaData()


@pytest.fixture(scope="session")
def dal_class(metadata):
    """Create PostgresAccessLayer subclass with test DAOs."""
    ItemDAO = PGDataAccessObject.from_model(Item, "items")
    ProductDAO = PGDataAccessObject.from_model(Product, "products")

    class TestDAL(PostgresAccessLayer, metadata=metadata):
        items = TableDescriptor(ItemDAO)
        products = TableDescriptor(ProductDAO)

    return TestDAL


@pytest.fixture(scope="session")
def custom_dal_class(metadata, dal_class):
    """DAL with custom DAO class — same MetaData, bind() handles extend_existing."""
    class CustomDAL(PostgresAccessLayer, metadata=metadata):
        items = TableDescriptor(ItemsDAO)

    return CustomDAL


# ---------------------------------------------------------------------------
# Create tables
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def create_tables(pg_sync_url, dal_class, custom_dal_class, metadata):
    engine = create_engine(pg_sync_url)
    metadata.create_all(engine)
    engine.dispose()


# ---------------------------------------------------------------------------
# Pool
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session")
async def pool(pg_url, create_tables) -> Pool:
    p = await create_db_pool(pg_url)
    yield p
    await p.close()


# ---------------------------------------------------------------------------
# DAL instances
# ---------------------------------------------------------------------------

@pytest.fixture
def dal(dal_class, pool):
    return dal_class(pool)


@pytest.fixture
def custom_dal(custom_dal_class, pool):
    return custom_dal_class(pool)


# ---------------------------------------------------------------------------
# Cleanup between tests
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(autouse=True)
async def cleanup(pool):
    yield
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE items, products RESTART IDENTITY CASCADE")
