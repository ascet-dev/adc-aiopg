from logging.config import fileConfig

from alembic import context
from alembic.operations import ops
from alembic.autogenerate import rewriter
from alembic.script import ScriptDirectory
from sqlalchemy import engine_from_config, pool, MetaData

from .version_table import CREATE_VERSION_TRIGGER_KEY, DELETE_VERSION_TRIGGER_KEY

config = context.config

writer_rename_migration = rewriter.Rewriter()
writer_add_version_trigger = rewriter.Rewriter()
writer_del_version_trigger = rewriter.Rewriter()

writer_chain = writer_rename_migration.chain(
    writer_add_version_trigger.chain(
        writer_del_version_trigger
    ))


@writer_rename_migration.rewrites(ops.MigrationScript)
def rename_migration_script(migration_context, revision, migration_script):
    # extract current head revision
    head_revision = ScriptDirectory.from_config(migration_context.config).get_current_head()
    if head_revision is None:
        # edge case with first migration
        new_rev_id = 1
    else:
        # default branch with incrementation
        last_rev_id = int(head_revision.lstrip('0'))
        new_rev_id = last_rev_id + 1
    # fill zeros up to 4 digits: 1 -> 0001
    migration_script.rev_id = '{0:04}'.format(new_rev_id)
    return migration_script


@writer_add_version_trigger.rewrites(ops.CreateTableOp)
def add_version_trigger(migration_context, revision, create_table_op):
    if create_table_op.info.get(CREATE_VERSION_TRIGGER_KEY):
        create_trigger_op = ops.ExecuteSQLOp(create_table_op.info[CREATE_VERSION_TRIGGER_KEY])
        return [create_table_op, create_trigger_op]
    return create_table_op


@writer_del_version_trigger.rewrites(ops.DropTableOp)
def del_version_trigger(migration_context, revision, drop_table_op):
    if drop_table_op.info.get(DELETE_VERSION_TRIGGER_KEY):
        del_trigger_op = ops.ExecuteSQLOp(drop_table_op.info[DELETE_VERSION_TRIGGER_KEY])
        return [drop_table_op, del_trigger_op]
    return drop_table_op


def run_migrations_offline(target_metadata, **configure_kwargs):
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        process_revision_directives=writer_chain,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        **configure_kwargs,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online(target_metadata, **configure_kwargs):
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            process_revision_directives=writer_chain,
            include_schemas=True,
            **configure_kwargs,
        )

        with context.begin_transaction():
            context.run_migrations()


def run_alembic(sqlalchemy_url: str, target_metadata: MetaData, configure_kwargs: dict = None):
    # this is the Alembic Config object, which provides
    # access to the values within the .ini file in use.

    # Interpret the config file for Python logging.
    # This line sets up loggers basically.
    configure_kwargs = configure_kwargs or {}
    fileConfig(config.config_file_name)

    config.set_main_option('sqlalchemy.url', sqlalchemy_url)

    if context.is_offline_mode():
        run_migrations_offline(target_metadata, **configure_kwargs)
    else:
        run_migrations_online(target_metadata, **configure_kwargs)
