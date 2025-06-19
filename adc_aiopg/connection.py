from datetime import datetime, timedelta, timezone
from logging import getLogger

from asyncpg import Connection, Pool, create_pool
from ujson import dumps, loads

logger = getLogger(__name__)

TIMESTAMP_2000Y = datetime(2000, 1, 1, tzinfo=timezone.utc)
DATETIME_2000Y = datetime(2000, 1, 1, tzinfo=timezone.utc)


def create_db_pool(dsn, **kwargs) -> Pool:
    return create_pool(dsn=dsn, init=_init_connection, **kwargs)


async def _init_connection(con: Connection):
    def _encoder(value):
        return b'\x01' + dumps(value).encode('utf-8')

    def _decoder(value):
        return loads(value[1:].decode('utf-8'))

    await con.set_type_codec(
        'jsonb',
        encoder=_encoder,
        decoder=_decoder,
        schema='pg_catalog',
        format='binary',
    )

    await con.set_type_codec(
        'timestamp',
        encoder=lambda x: int.to_bytes(
            int((x.timestamp() - TIMESTAMP_2000Y.timestamp()) * 1000000), 
            byteorder='big', 
            length=8,
            signed=True,
        ),
        decoder=lambda x: datetime.fromtimestamp(
            TIMESTAMP_2000Y.timestamp() + int.from_bytes(x, byteorder='big', signed=True) / 1000000,
            tz=timezone.utc
        ),
        schema='pg_catalog',
        format='binary',
    )

    await con.set_type_codec(
        'timestamptz',
        encoder=lambda x: int.to_bytes(
            int((x.timestamp() - TIMESTAMP_2000Y.timestamp()) * 1000000), 
            byteorder='big', 
            length=8,
            signed=True,
        ),
        decoder=lambda x: datetime.fromtimestamp(
            TIMESTAMP_2000Y.timestamp() + int.from_bytes(x, byteorder='big', signed=True) / 1000000,
            tz=timezone.utc
        ),
        schema='pg_catalog',
        format='binary',
    )


