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

import secrets
import string
from functools import cached_property
from typing import Any
from urllib.parse import quote_plus

from google.adk.sessions import BaseSessionService, DatabaseSessionService
from pydantic import Field
from typing_extensions import override

import veadk.config  # noqa E401
from veadk.configs.database_configs import VeRdsMysqlConfig
from veadk.integrations.ve_rds_mysql.ve_rds_mysql import VeRdsMysqlClient
from veadk.memory.short_term_memory_backends.base_backend import (
    BaseShortTermMemoryBackend,
)
from veadk.utils.adk_compat import should_use_async_db_drivers
from veadk.utils.logger import get_logger

logger = get_logger(__name__)


def _generate_password(length: int = 16) -> str:
    """Generate a secure random password for database user."""
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


class VeRdsMysqlSTMBackend(BaseShortTermMemoryBackend):
    """Short term memory backend for Volcengine RDS MySQL.

    This backend can work in two modes:
    1. Direct connection: Uses the host, user, password, database directly
    2. RDS API mode: Uses Volcengine RDS API to discover connection details
       from an instance ID, and optionally creates the database and user.
    """
    ve_rds_mysql_config: VeRdsMysqlConfig = Field(default_factory=VeRdsMysqlConfig)
    db_kwargs: dict = Field(default_factory=dict)

    def model_post_init(self, context: Any) -> None:
        config = self.ve_rds_mysql_config

        if config.uses_direct_connection:
            # Direct connection mode
            host = config.host
            port = config.port
            user = config.user
            password = config.password
            database = config.database
            charset = config.charset
            logger.info("Using direct connection mode for Ve RDS MySQL.")
        elif config.uses_rds_api:
            # RDS API mode: Get connection details from RDS instance
            logger.info("Using RDS API mode for Ve RDS MySQL.")
            client = VeRdsMysqlClient(
                region=config.region,
                ak=config.access_key,
                sk=config.secret_key,
            )

            # Get connection info from the instance
            conn_info = client.get_connection_info(config.instance_id)
            host = conn_info["host"]
            port = conn_info["port"] or 3306

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
                        character_set_name=config.charset,
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
                    f"Successfully configured RDS MySQL database {db_name} for VeADK."
                )
            except Exception as e:
                logger.warning(
                    f"Could not auto-configure RDS MySQL database/user: {e}. "
                    "Using existing credentials if available."
                )

            user = db_user
            password = db_password
            database = db_name
            charset = config.charset
        else:
            raise ValueError(
                "VeRdsMysqlConfig must either have direct connection details "
                "(host, user, password, database) or RDS API details "
                "(instance_id, access_key, secret_key)."
            )

        # Build the DB URL
        encoded_username = quote_plus(user)
        encoded_password = quote_plus(password)
        if should_use_async_db_drivers():
            self._db_url = f"mysql+aiomysql://{encoded_username}:{encoded_password}@{host}:{port}/{database}?charset={charset}"
        else:
            self._db_url = f"mysql+pymysql://{encoded_username}:{encoded_password}@{host}:{port}/{database}?charset={charset}"

    @cached_property
    @override
    def session_service(self) -> BaseSessionService:
        return DatabaseSessionService(db_url=self._db_url, **self.db_kwargs)
