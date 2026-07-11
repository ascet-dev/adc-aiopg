import asyncio

import asyncpg
import pytest

from adc_aiopg.connection import create_db_pool


async def _count(pool, table):
    """Count rows from a connection outside any transaction context."""
    async with pool.acquire() as con:
        return await con.fetchval(f'SELECT count(*) FROM {table}')


async def test_commit_spans_multiple_daos(dal, pool):
    async with dal.transaction():
        item = await dal.items.create(name='i1')
        await dal.products.create(title='p1', price=1)

    assert item.id is not None
    assert await _count(pool, 'items') == 1
    assert await _count(pool, 'products') == 1


async def test_rollback_spans_multiple_daos(dal, pool):
    with pytest.raises(RuntimeError):
        async with dal.transaction():
            await dal.items.create(name='i1')
            await dal.products.create(title='p1', price=1)
            raise RuntimeError('boom')

    assert await _count(pool, 'items') == 0
    assert await _count(pool, 'products') == 0


async def test_custom_dao_methods_join_the_transaction(custom_dal, pool):
    """Custom DAO methods going through self.fetch/fetchrow also participate."""
    with pytest.raises(RuntimeError):
        async with custom_dal.transaction():
            await custom_dal.items.create(name='i1')
            found = await custom_dal.items.find_by_name('i1')
            assert found is not None
            raise RuntimeError('boom')

    assert await _count(pool, 'items') == 0


async def test_dao_calls_actually_join_the_transaction(dal, pool):
    """Uncommitted rows are visible through the DAL but not from outside."""
    async with dal.transaction():
        await dal.items.create(name='i1')
        assert await dal.items.count() == 1
        # A separate connection must not see the uncommitted row: proves the
        # DAO calls above ran inside the transaction, not in autocommit.
        assert await _count(pool, 'items') == 0

    assert await _count(pool, 'items') == 1


async def test_single_acquire_inside_transaction(dal, pool, monkeypatch):
    acquires = []
    orig_acquire = asyncpg.pool.Pool.acquire

    def spy(self, *args, **kwargs):
        acquires.append(self)
        return orig_acquire(self, *args, **kwargs)

    monkeypatch.setattr(asyncpg.pool.Pool, 'acquire', spy)

    async with dal.transaction():
        await dal.items.create(name='i1')
        await dal.products.create(title='p1', price=1)
        await dal.items.count()
    assert len(acquires) == 1

    acquires.clear()
    await dal.items.create(name='i2')
    await dal.products.create(title='p2', price=2)
    assert len(acquires) == 2


async def test_outside_context_behavior_unchanged(dal, pool):
    """Without transaction(): autocommit, every row is immediately visible."""
    await dal.items.create(name='i1')
    assert await _count(pool, 'items') == 1

    with pytest.raises(RuntimeError):
        await dal.items.create(name='i2')
        raise RuntimeError('boom')
    assert await _count(pool, 'items') == 2


async def test_nested_transaction_is_a_savepoint(dal, pool):
    async with dal.transaction():
        await dal.items.create(name='outer')

        with pytest.raises(RuntimeError):
            async with dal.transaction():
                await dal.items.create(name='inner')
                raise RuntimeError('boom')

        # Inner block rolled back to the savepoint, outer row survived.
        names = [i.name for i in await dal.items.search()]
        assert names == ['outer']

    assert await _count(pool, 'items') == 1


async def test_nested_transaction_commits_with_outer(dal, pool):
    async with dal.transaction():
        async with dal.transaction():
            await dal.items.create(name='inner')
        # Savepoint released but the outer transaction is still open.
        assert await _count(pool, 'items') == 0

    assert await _count(pool, 'items') == 1


async def test_concurrent_tasks_are_isolated(dal, pool):
    """Two tasks with their own transactions: separate connections, one
    rollback does not affect the other commit (ContextVar isolation)."""
    pids = {}
    started = asyncio.Event()
    release = asyncio.Event()

    async def worker(name, fail):
        async with dal.transaction() as con:
            pids[name] = await con.fetchval('SELECT pg_backend_pid()')
            await dal.items.create(name=name)
            started.set()
            await release.wait()
            if fail:
                raise RuntimeError('boom')

    async def coordinator():
        await started.wait()
        release.set()

    results = await asyncio.gather(
        worker('committed', False),
        worker('rolled_back', True),
        coordinator(),
        return_exceptions=True,
    )

    assert any(isinstance(r, RuntimeError) for r in results)
    assert pids['committed'] != pids['rolled_back']
    async with pool.acquire() as con:
        names = [r['name'] for r in await con.fetch('SELECT name FROM items')]
    assert names == ['committed']


async def test_two_pools_do_not_share_transaction(dal_class, dal, pool, pg_url):
    pool2 = await create_db_pool(pg_url)
    dal2 = dal_class(pool2)
    try:
        async with dal.transaction():
            await dal.items.create(name='pool1')
            # dal2 has its own pool: its query must not join dal's
            # transaction, so it cannot see the uncommitted row.
            assert await dal2.items.count() == 0

        # Transactions on both pools at once stay independent: the inner
        # (pool2) one commits even though the outer (pool1) one rolls back.
        with pytest.raises(RuntimeError):
            async with dal.transaction():
                await dal.items.create(name='pool1-rolled-back')
                async with dal2.transaction():
                    await dal2.products.create(title='pool2-committed', price=1)
                raise RuntimeError('boom')

        assert await _count(pool, 'items') == 1
        assert await _count(pool, 'products') == 1
    finally:
        await pool2.close()


async def test_gather_inside_transaction_is_rejected(dal_class, pg_url):
    """asyncpg forbids concurrent use of one connection; the documented
    behavior is an InterfaceError, not silent serialization."""
    pool = await create_db_pool(pg_url)
    dal = dal_class(pool)
    try:
        with pytest.raises(asyncpg.InterfaceError):
            async with dal.transaction():
                await asyncio.gather(
                    dal.items.create(name='a'),
                    dal.items.create(name='b'),
                )
    finally:
        pool.terminate()
