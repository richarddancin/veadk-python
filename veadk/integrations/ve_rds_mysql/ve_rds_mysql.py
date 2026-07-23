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

import json
import os
import secrets
import string

from veadk.utils.volcengine_sign import ve_request


class VeRdsMysqlException(Exception):
    def __init__(self, code, request_id, message=None):
        self.code = code
        self.request_id = request_id
        self.message = "{}, code:{}, request_id:{}".format(
            message, self.code, self.request_id
        )

    def __str__(self):
        return self.message


class VeRdsMysqlClient:

    def __init__(
        self,
        region="cn-beijing",
        ak="",
        sk="",
        sts_token="",
    ):
        self.region = region
        self.ak = ak or os.getenv("VOLCENGINE_ACCESS_KEY", "")
        self.sk = sk or os.getenv("VOLCENGINE_SECRET_KEY", "")
        self.sts_token = sts_token

        provider = os.getenv("CLOUD_PROVIDER", "").lower()
        if provider == "byteplus":
            self.host = "open.byteplusapi.com"
        else:
            self.host = "open.volcengineapi.com"

    def _request(self, action: str, request_body: dict):
        header = {}
        if self.sts_token:
            header["X-Security-Token"] = self.sts_token

        response = ve_request(
            request_body=request_body,
            action=action,
            ak=self.ak,
            sk=self.sk,
            service="rds_mysql",
            version="2022-01-01",
            region=self.region,
            host=self.host,
            header=header,
        )

        if "ResponseMetadata" in response and "Error" in response["ResponseMetadata"]:
            error = response["ResponseMetadata"]["Error"]
            raise VeRdsMysqlException(
                error.get("Code", "Unknown"),
                response["ResponseMetadata"].get("RequestId", "Unknown"),
                error.get("Message", "Unknown error"),
            )

        return response

    def describe_db_instances(
        self,
        region=None,
        instance_ids=None,
        instance_name=None,
        instance_status=None,
        db_engine_version=None,
        vpc_id=None,
        zone_id=None,
        tags=None,
        page_number=1,
        page_size=100,
    ):
        """List RDS MySQL instances.

        Args:
            region: Region to query (defaults to client region)
            instance_ids: List of instance IDs to filter
            instance_name: Filter by instance name
            instance_status: Filter by instance status
            db_engine_version: Filter by DB engine version
            vpc_id: Filter by VPC ID
            zone_id: Filter by zone ID
            tags: Filter by tags
            page_number: Page number for pagination
            page_size: Page size for pagination

        Returns:
            Dict with DB instances information
        """
        body = {
            "PageNumber": page_number,
            "PageSize": page_size,
        }
        if region:
            body["Region"] = region
        if instance_ids:
            body["InstanceIds"] = instance_ids
        if instance_name:
            body["InstanceName"] = instance_name
        if instance_status:
            body["InstanceStatus"] = instance_status
        if db_engine_version:
            body["DBEngineVersion"] = db_engine_version
        if vpc_id:
            body["VpcId"] = vpc_id
        if zone_id:
            body["ZoneId"] = zone_id
        if tags:
            body["Tags"] = tags

        return self._request("DescribeDBInstances", body)

    def describe_db_instance_detail(self, instance_id):
        """Get detailed information about a specific RDS MySQL instance.

        Args:
            instance_id: The ID of the DB instance

        Returns:
            Dict with detailed instance information including connection endpoints
        """
        body = {
            "InstanceId": instance_id,
        }
        return self._request("DescribeDBInstanceDetail", body)

    def describe_databases(self, instance_id, db_name=None):
        """List databases on a specific RDS MySQL instance.

        Args:
            instance_id: The ID of the DB instance
            db_name: Optional database name to filter

        Returns:
            Dict with database information
        """
        body = {
            "InstanceId": instance_id,
        }
        if db_name:
            body["DBName"] = db_name

        return self._request("DescribeDatabases", body)

    def describe_accounts(self, instance_id, account_name=None):
        """List accounts on a specific RDS MySQL instance.

        Args:
            instance_id: The ID of the DB instance
            account_name: Optional account name to filter

        Returns:
            Dict with account information
        """
        body = {
            "InstanceId": instance_id,
        }
        if account_name:
            body["AccountName"] = account_name

        return self._request("DescribeAccounts", body)

    def create_database(self, instance_id, db_name, character_set_name="utf8mb4"):
        """Create a database on a specific RDS MySQL instance.

        Args:
            instance_id: The ID of the DB instance
            db_name: Name of the database to create
            character_set_name: Character set (default: utf8mb4)

        Returns:
            API response
        """
        body = {
            "InstanceId": instance_id,
            "DBName": db_name,
            "CharacterSetName": character_set_name,
        }
        return self._request("CreateDatabase", body)

    def create_account(
        self,
        instance_id,
        account_name,
        account_password,
        account_type="Normal",
        account_desc="",
    ):
        """Create an account on a specific RDS MySQL instance.

        Args:
            instance_id: The ID of the DB instance
            account_name: Name of the account to create
            account_password: Password for the account
            account_type: Account type (Normal or Super)
            account_desc: Description of the account

        Returns:
            API response
        """
        body = {
            "InstanceId": instance_id,
            "AccountName": account_name,
            "AccountPassword": account_password,
            "AccountType": account_type,
        }
        if account_desc:
            body["AccountDesc"] = account_desc

        return self._request("CreateAccount", body)

    def grant_account_privilege(
        self,
        instance_id,
        account_name,
        db_name,
        account_privilege="ReadWrite",
    ):
        """Grant privileges to an account on a specific database.

        Args:
            instance_id: The ID of the DB instance
            account_name: Name of the account
            db_name: Name of the database
            account_privilege: Privilege type (ReadOnly, ReadWrite, DDLOnly, DMLOnly, DBOwner)

        Returns:
            API response
        """
        body = {
            "InstanceId": instance_id,
            "AccountName": account_name,
            "DBName": db_name,
            "AccountPrivilege": account_privilege,
        }
        return self._request("GrantAccountPrivilege", body)

    def get_connection_info(self, instance_id):
        """Get connection information for an RDS MySQL instance.

        Args:
            instance_id: The ID of the DB instance

        Returns:
            Dict with host, port, and other connection details
        """
        detail = self.describe_db_instance_detail(instance_id)
        result = detail.get("Result", {})

        endpoints = result.get("Endpoints", {}).get("Address", [])
        private_endpoint = None
        public_endpoint = None

        for ep in endpoints:
            if ep.get("NetworkType") == "Private":
                private_endpoint = ep
            elif ep.get("NetworkType") == "Public":
                public_endpoint = ep

        endpoint = private_endpoint or public_endpoint

        return {
            "instance_id": instance_id,
            "host": endpoint.get("Domain") if endpoint else None,
            "port": endpoint.get("Port") if endpoint else 3306,
            "private_endpoint": private_endpoint,
            "public_endpoint": public_endpoint,
            "instance_status": result.get("InstanceStatus"),
            "engine": result.get("DBEngine"),
            "engine_version": result.get("DBEngineVersion"),
            "vpc_id": result.get("VpcId"),
        }

    @staticmethod
    def _generate_password(length=16):
        """Generate a secure random password."""
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*()"
        password = "".join(secrets.choice(alphabet) for _ in range(length))
        password = (
            secrets.choice(string.ascii_uppercase)
            + secrets.choice(string.ascii_lowercase)
            + secrets.choice(string.digits)
            + password[3:]
        )
        return password

    def auto_setup_database(
        self, instance_id, db_name=None, db_user=None, db_password=None
    ):
        """Automatically set up a database and user with proper permissions.

        This will:
        1. Check if the database already exists, create it if not
        2. Check if the user already exists, create if not
        3. Grant proper permissions to the user

        Args:
            instance_id: RDS instance ID
            db_name: Database name (default: veadk_stm)
            db_user: Database user (default: veadk)
            db_password: Database password (auto-generated if not provided)

        Returns:
            Dict with connection info
        """
        db_name = db_name or "veadk_stm"
        db_user = db_user or "veadk"
        db_password = db_password or self._generate_password()

        try:
            databases = self.describe_databases(instance_id)
            db_list = databases.get("Result", {}).get("Databases", [])
            db_exists = any(db.get("DBName") == db_name for db in db_list)

            if not db_exists:
                self.create_database(instance_id, db_name)
        except Exception:
            pass

        try:
            accounts = self.describe_accounts(instance_id)
            account_list = accounts.get("Result", {}).get("Accounts", [])
            user_exists = any(acc.get("AccountName") == db_user for acc in account_list)

            if not user_exists:
                self.create_account(instance_id, db_user, db_password)
        except Exception:
            pass

        try:
            self.grant_account_privilege(instance_id, db_user, db_name, "ReadWrite")
        except Exception:
            pass

        conn_info = self.get_connection_info(instance_id)
        conn_info.update(
            {
                "db_name": db_name,
                "db_user": db_user,
                "db_password": db_password,
            }
        )

        return conn_info
