from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine

from udp.settings import Settings

if context.config.config_file_name is not None:
    fileConfig(context.config.config_file_name)

url = Settings().database_url.replace("postgresql://", "postgresql+psycopg://", 1)  # type: ignore[call-arg]

with create_engine(url).connect() as connection:
    context.configure(connection=connection)
    with context.begin_transaction():
        context.run_migrations()
