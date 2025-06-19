import copy
from typing import Type, TypeVar

from sqlalchemy import Table, Column
from sqlmodel import SQLModel

T = TypeVar("T", bound=SQLModel)

CREATE_VERSION_TRIGGER_KEY = 'CREATE_VERSION_TRIGGER_KEY'
DELETE_VERSION_TRIGGER_KEY = 'DELETE_VERSION_TRIGGER_KEY'


def _col_copy(col: Column) -> Column:
    col = col.copy()
    col.unique = False
    col.default = col.server_default = None
    col.autoincrement = False
    col.nullable = True
    col._user_defined_nullable = col.nullable
    col.primary_key = False
    return col


def declare_version_table(model_cls: Type[T]) -> Type[SQLModel]:
    orig_table = model_cls.__table__
    version_table_name = f"{orig_table.name}_log"
    schema = orig_table.schema or "public"

    version_columns = [_col_copy(col) for col in orig_table.columns]

    version_table = Table(
        version_table_name,
        orig_table.metadata,
        *version_columns,
        schema=schema,
        info={
            CREATE_VERSION_TRIGGER_KEY: get_create_version_trigger_sql(
                schema=schema,
                table_name=orig_table.name,
                version_table_name=version_table_name
            ),
            DELETE_VERSION_TRIGGER_KEY: get_delete_version_trigger_sql(
                schema=schema,
                table_name=orig_table.name
            ),
        }
    )

    class VersionModel(SQLModel):
        __table__ = version_table

    VersionModel.__name__ = f"{model_cls.__name__}Log"
    return VersionModel


def generate_trigger_name(table_name: str) -> tuple:
    trigger_name = f'tg_{table_name}_versions'
    function_name = f'process_{trigger_name}'
    return trigger_name, function_name


def get_create_version_trigger_sql(schema: str, table_name: str, version_table_name: str) -> str:
    return f"""
    CREATE OR REPLACE FUNCTION {schema}.version_{table_name}_row()
    RETURNS TRIGGER AS $$
    DECLARE
        col_list TEXT;
        col_list_values TEXT;
    BEGIN
        SELECT
            string_agg(quote_ident(column_name), ','),
            string_agg(format('$1.%s', column_name), ', ')
        INTO col_list, col_list_values
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name = '{table_name}';

        EXECUTE format(
            'INSERT INTO {schema}.{version_table_name} (%s) VALUES (%s)',
            col_list,
            col_list_values
        ) USING OLD;

        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DROP TRIGGER IF EXISTS version_trigger_{table_name} ON {schema}.{table_name};
    CREATE TRIGGER version_trigger_{table_name}
    AFTER UPDATE OR DELETE ON {schema}.{table_name}
    FOR EACH ROW EXECUTE FUNCTION {schema}.version_{table_name}_row();
    """


def get_delete_version_trigger_sql(schema: str, table_name: str) -> str:
    return f"""
    DROP TRIGGER IF EXISTS version_trigger_{table_name} ON {schema}.{table_name};
    DROP FUNCTION IF EXISTS {schema}.version_{table_name}_row();
    """
