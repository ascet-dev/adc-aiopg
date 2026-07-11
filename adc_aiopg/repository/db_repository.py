from contextlib import asynccontextmanager
from contextvars import ContextVar

from asyncpg import Connection, Pool

from adc_aiopg.query import compile_query

# Maps pool -> connection that holds the currently active transaction() block
# in this asyncio context. Keyed by pool so that several access layers with
# different pools in one process never leak a transaction into each other.
# The default is None (not {}): a mutable default would be shared across all
# contexts. The dict itself is never mutated in place — set()/reset() always
# install a fresh copy — which keeps concurrent tasks isolated.
_current_tx: ContextVar[dict | None] = ContextVar('adc_aiopg_current_tx', default=None)


class PGPoolManager:
    def __init__(self, db_pool: Pool):
        self.db_pool = db_pool

    @asynccontextmanager
    async def connection(self):
        current = _current_tx.get()
        if current is not None and self.db_pool in current:
            # Inside a transaction() block for this pool: reuse its connection
            # so the query joins the transaction.
            yield current[self.db_pool]
            return
        # Acquire a connection from the pool
        async with self.db_pool.acquire() as conn:
            yield conn

    @asynccontextmanager
    async def transaction(self) -> Connection:
        """Run the block on a single connection inside a single transaction.

        While the block is active, every query issued through any
        PGPoolManager that shares this pool — including all DAO methods —
        reuses the same connection and therefore joins the transaction.
        Commit on normal exit, rollback on exception. Outside the block
        behavior is unchanged: every call acquires its own connection.

        Nested transaction() calls on the same pool reuse the outer
        connection and open a savepoint, so a failed inner block rolls back
        to the savepoint without aborting the outer transaction.
        Backward-compatibility note: before 1.1.0 a nested call acquired a
        separate connection with an independent transaction.

        The transaction connection must not be used concurrently:

        * asyncio.gather() over DAO calls inside the block is not supported —
          asyncpg raises InterfaceError ("another operation is in progress").
          Issue the calls sequentially.
        * Tasks spawned inside the block (asyncio.create_task) inherit the
          transaction context; they must complete before the block exits,
          otherwise they may use a connection that was already released.
        """
        current = _current_tx.get()
        if current is not None and self.db_pool in current:
            con = current[self.db_pool]
            async with con.transaction():  # nested block -> savepoint
                yield con
            return

        async with self.db_pool.acquire() as con:
            async with con.transaction():
                token = _current_tx.set({**(current or {}), self.db_pool: con})
                try:
                    yield con
                finally:
                    _current_tx.reset(token)

    async def fetch(self, query):
        compiled_query, compiled_params = compile_query(query)
        async with self.connection() as con:
            records = await con.fetch(compiled_query, *compiled_params)
            return [dict(record) for record in records]

    async def fetchrow(self, query):
        compiled_query, compiled_params = compile_query(query)
        async with self.connection() as con:
            record = await con.fetchrow(compiled_query, *compiled_params)
            return dict(record) if record else None

    async def fetchval(self, query):
        compiled_query, compiled_params = compile_query(query)
        async with self.connection() as con:
            return await con.fetchval(compiled_query, *compiled_params)
