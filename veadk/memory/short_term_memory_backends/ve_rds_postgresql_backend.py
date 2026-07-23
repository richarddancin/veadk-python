# Copyright (c) 2025 Beijing Volcano Engine Technology Co., Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import concurrent.futures
import re
import secrets
import string
from functools import cached_property
from typing import Any
from urllib.parse import quote_plus

from google.adk.sessions import BaseSessionService, DatabaseSessionService
from pydantic import Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from typing_extensions import override

import veadk.config  # noqa E401
from veadk.configs.database_configs import VeRdsPostgresqlConfig
from veadk.integrations.ve_rds_postgresql.ve_rds_postgresql import VeRdsPostgresqlClient
from veadk.memory.short_term_memory_backends.base_backend import (
    BaseShortTermMemoryBackend,
)
from veadk.utils.adk_compat import should_use_async_db_drivers
from veadk.utils.logger import get_logger

logger = get_logger(__name__)

# A conservative, injection-safe schema identifier: letters, digits, underscore,
# not starting with a digit. It is interpolated into DDL, so it must be validated.
_SCHEMA_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_schema(schema: str) -> None:
    if not _SCHEMA_RE.match(schema):
        raise ValueError(
            f"Invalid PostgreSQL schema name '{schema}'. Allowed: letters, "
            "digits, underscore; must not start with a digit."
        )


def _with_search_path(db_kwargs: dict, schema: str) -> dict:
    """Return db_kwargs with the connection's search_path pinned to `schema` via
    the asyncpg startup parameter (reliable; unlike a per-statement ``SET`` in a
    connect listener, which asyncpg does not persist). Existing connect_args are
    preserved."""
    kwargs = dict(db_kwargs)
    connect_args = dict(kwargs.get("connect_args", {}))
    server_settings = dict(connect_args.get("server_settings", {}))
    server_settings["search_path"] = schema
    connect_args["server_settings"] = server_settings
    kwargs["connect_args"] = connect_args
    return kwargs


async def _acreate_schema(db_url: str, schema: str) -> None:
    # Use a throwaway engine so the real session-service engine is never bound to
    # this temporary event loop. CREATE SCHEMA is schema-qualified, so it works
    # regardless of search_path.
    engine = create_async_engine(db_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
    finally:
        await engine.dispose()


def _ensure_schema(db_url: str, schema: str) -> None:
    """Create `schema` if absent, before ADK's lazy ``create_all`` runs, so the
    session tables are created inside it. Safe to call from sync or async
    context."""
    try:
        asyncio.get_running_loop()
        running = True
    except RuntimeError:
        running = False
    if running:
        # A loop is already running here; run the coroutine on its own loop in a
        # worker thread so we don't touch the caller's loop.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(
                lambda: asyncio.run(_acreate_schema(db_url, schema))
            ).result()
    else:
        asyncio.run(_acreate_schema(db_url, schema))


def _generate_password(length: int = 16) -> str:
    """Generate a secure random password for database user."""
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


class VeRdsPostgresqlSTMBackend(BaseShortTermMemoryBackend):
    """Short term memory backend for Volcengine RDS PostgreSQL.

    This backend can work in two modes:
    1. Direct connection: Uses the host, user, password, database directly
    2. RDS API mode: Uses Volcengine RDS API to discover connection details
       from an instance ID, and optionally creates the database and user.
    """
    ve_rds_postgresql_config: VeRdsPostgresqlConfig = Field(
        default_factory=VeRdsPostgresqlConfig
    )
    db_kwargs: dict = Field(default_factory=dict)

    def model_post_init(self, context: Any) -> None:
        config = self.ve_rds_postgresql_config

        if config.uses_direct_connection:
            # Direct connection mode
            host = config.host
            port = config.port
            user = config.user
            password = config.password
            database = config.database
            logger.info("Using direct connection mode for Ve RDS PostgreSQL.")
        elif config.uses_rds_api:
            # RDS API mode: Get connection details from RDS instance
            logger.info("Using RDS API mode for Ve RDS PostgreSQL.")
            client = VeRdsPostgresqlClient(
                region=config.region,
                ak=config.access_key,
                sk=config.secret_key,
            )

            # Get connection info from the instance
            conn_info = client.get_connection_info(config.instance_id)
            host = conn_info["host"]
            port = conn_info["port"] or 5432

            if not host:
                raise ValueError(
                    f"Could not get host for RDS instance {config.instance_id}"
                )

            if conn_info["instance_status"] != "Running":
                logger.warning(
                    f"RDS instance {config.instance_id} is in status: {conn_info['instance_status']}"
                )

            # Try to ensure database and user exist
            db_name = config.db_name
            db_user = config.db_user
            db_password = config.db_password or _generate_password()

            try:
                # Check if database exists
                dbs = client.describe_databases(config.instance_id)
                db_exists = any(
                    db.get("DBName") == db_name
                    for db in dbs.get("Result", {}).get("Databases", [])
                )

                if not db_exists:
                    logger.info(f"Creating database {db_name} on RDS instance.")
                    client.create_database(
                        instance_id=config.instance_id,
                        db_name=db_name,
                        character_set_name="UTF8",
                    )

                # Check if user exists
                accounts = client.describe_accounts(config.instance_id)
                user_exists = any(
                    account.get("AccountName") == db_user
                    for account in accounts.get("Result", {}).get("Accounts", [])
                )

                if not user_exists:
                    logger.info(f"Creating user {db_user} on RDS instance.")
                    client.create_account(
                        instance_id=config.instance_id,
                        account_name=db_user,
                        account_password=db_password,
                        account_desc="VeADK STM user",
                    )

                # Grant privileges
                client.grant_account_privilege(
                    instance_id=config.instance_id,
                    account_name=db_user,
                    db_name=db_name,
                    account_privilege="ReadWrite",
                )

                logger.info(
                    f"Successfully configured RDS PostgreSQL database {db_name} for VeADK."
                )
            except Exception as e:
                logger.warning(
                    f"Could not auto-configure RDS PostgreSQL database/user: {e}. "
                    "Using existing credentials if available."
                )

            user = db_user
            password = db_password
            database = db_name
        else:
            raise ValueError(
                "VeRdsPostgresqlConfig must either have direct connection details "
                "(host, user, password, database) or RDS API details "
                "(instance_id, access_key, secret_key)."
            )

        # Build the DB URL
        encoded_username = quote_plus(user)
        encoded_password = quote_plus(password)
        if should_use_async_db_drivers():
            self._db_url = f"postgresql+asyncpg://{encoded_username}:{encoded_password}@{host}:{port}/{database}"
        else:
            self._db_url = f"postgresql://{encoded_username}:{encoded_password}@{host}:{port}/{database}"

    @cached_property
    @override
    def session_service(self) -> BaseSessionService:
        schema = self.ve_rds_postgresql_config.schema
        if not schema:
            return DatabaseSessionService(db_url=self._db_url, **self.db_kwargs)

        _validate_schema(schema)
        # 1) make sure the schema exists, then 2) pin every connection to it.
        _ensure_schema(self._db_url, schema)
        db_kwargs = _with_search_path(self.db_kwargs, schema)
        logger.info(f"Short-term memory isolated in PostgreSQL schema '{schema}'.")
        return DatabaseSessionService(db_url=self._db_url, **db_kwargs)
