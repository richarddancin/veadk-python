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

import os

from veadk.utils.volcengine_sign import ve_request


class VeVikingDBException(Exception):
    def __init__(self, code, request_id, message=None):
        self.code = code
        self.request_id = request_id
        self.message = "{}, code:{}, request_id:{}".format(
            message, self.code, self.request_id
        )

    def __str__(self):
        return self.message


class VeVikingDBClient:

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
            service="vikingdb",
            version="2025-06-09",
            region=self.region,
            host=self.host,
            header=header,
        )

        if "ResponseMetadata" in response and "Error" in response["ResponseMetadata"]:
            error = response["ResponseMetadata"]["Error"]
            raise VeVikingDBException(
                error.get("Code", "Unknown"),
                response["ResponseMetadata"].get("RequestId", "Unknown"),
                error.get("Message", "Unknown error"),
            )

        return response

    def list_collections(
        self,
        project_name="default",
        collection_name=None,
        page_number=1,
        page_size=100,
    ):
        """List VikingDB collections.

        Args:
            project_name: Project name to query (default: "default")
            collection_name: Optional collection name to filter
            page_number: Page number for pagination
            page_size: Page size for pagination

        Returns:
            Dict with collections information
        """
        body = {
            "ProjectName": project_name,
            "PageNumber": page_number,
            "PageSize": page_size,
        }
        if collection_name:
            body["CollectionName"] = collection_name

        return self._request("ListCollections", body)

    def get_collection(
        self,
        collection_name,
        project_name="default",
    ):
        """Get detailed information about a specific VikingDB collection.

        Args:
            collection_name: The name of the collection
            project_name: Project name (default: "default")

        Returns:
            Dict with detailed collection information
        """
        body = {
            "ProjectName": project_name,
            "CollectionName": collection_name,
        }
        return self._request("GetCollection", body)
