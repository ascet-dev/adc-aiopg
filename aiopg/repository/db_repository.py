from contextlib import asynccontextmanager
from asyncpg import Connection, Pool
from aiopg.query import compile_query


class PGPoolManager:
    def __init__(self, db_pool: Pool):
        self.db_pool = db_pool

    @asynccontextmanager
    async def connection(self):
        # Acquire a connection from the pool
        async with self.db_pool.acquire() as conn:
            yield conn

    @asynccontextmanager
    async def transaction(self):
        async with self.connection() as con:
            async with con.transaction():
                yield con

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
