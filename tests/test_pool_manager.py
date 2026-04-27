"""Integration tests for PGPoolManager."""
import pytest
import sqlalchemy as sa

from adc_aiopg.repository.db_repository import PGPoolManager


@pytest.fixture
def pm(pool):
    return PGPoolManager(pool)


class TestFetch:
    async def test_fetch_returns_list_of_dicts(self, pm, pool):
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO items (name) VALUES ('a'), ('b')")

        rows = await pm.fetch(sa.text("SELECT * FROM items ORDER BY id"))
        assert isinstance(rows, list)
        assert len(rows) == 2
        assert rows[0]["name"] == "a"

    async def test_fetch_empty(self, pm):
        rows = await pm.fetch(sa.text("SELECT * FROM items"))
        assert rows == []


class TestFetchrow:
    async def test_fetchrow_returns_dict(self, pm, pool):
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO items (name) VALUES ('one')")

        row = await pm.fetchrow(sa.text("SELECT * FROM items LIMIT 1"))
        assert isinstance(row, dict)
        assert row["name"] == "one"

    async def test_fetchrow_returns_none(self, pm):
        row = await pm.fetchrow(sa.text("SELECT * FROM items WHERE id = -1"))
        assert row is None


class TestFetchval:
    async def test_fetchval_returns_scalar(self, pm, pool):
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO items (name) VALUES ('x'), ('y')")

        val = await pm.fetchval(sa.text("SELECT count(*) FROM items"))
        assert val == 2


class TestConnection:
    async def test_connection_context_manager(self, pm):
        async with pm.connection() as conn:
            result = await conn.fetchval("SELECT 1")
            assert result == 1


class TestTransaction:
    async def test_transaction_commits(self, pm, pool):
        async with pm.transaction() as conn:
            await conn.execute("INSERT INTO items (name) VALUES ('committed')")

        async with pool.acquire() as conn:
            val = await conn.fetchval("SELECT count(*) FROM items WHERE name = 'committed'")
        assert val == 1

    async def test_transaction_rollbacks_on_error(self, pm, pool):
        with pytest.raises(RuntimeError):
            async with pm.transaction() as conn:
                await conn.execute("INSERT INTO items (name) VALUES ('rollback')")
                raise RuntimeError("boom")

        async with pool.acquire() as conn:
            val = await conn.fetchval("SELECT count(*) FROM items WHERE name = 'rollback'")
        assert val == 0
