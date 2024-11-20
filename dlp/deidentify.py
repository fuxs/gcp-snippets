# Copyright 2024 Michael Bungenstock
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
#
from typing import Optional, List
from google import auth
import base64
import google.cloud.dlp


class DLPservice:
    def __init__(
        self, key_name: str, wrapped_key: str, surrogate_type: Optional[str] = None
    ):
        if surrogate_type is None:
            surrogate_type = "🔒DLP🔒"
        self.key_name = key_name
        self.unwrapped_key = base64.b64decode(wrapped_key)
        self.surrogate_type = surrogate_type

        credentials, project_id = auth.default()
        self._dlp = google.cloud.dlp_v2.DlpServiceClient(credentials=credentials)
        self.parent = f"projects/{project_id}/locations/global"

    def deidentify_encrypted(
        self, input_str: str, info_types: Optional[List[str]] = None
    ):
        if info_types is None:
            info_types = ["EMAIL_ADDRESS", "PERSON_NAME"]
        info_types = [{"name": info_type} for info_type in info_types]

        deidentify_config = {
            # "transformation_error_handling": {"leave_untransformed": {}},
            # "transformation_error_handling": {"throw_error":{}},
            "info_type_transformations": {"transformations": [self.transformation()]},
        }
        matching_type = google.cloud.dlp_v2.MatchingType.MATCHING_TYPE_PARTIAL_MATCH
        inspect_config = {
            "info_types": info_types,
            "rule_set": [
                {
                    "info_types": [{"name": "PERSON_NAME"}],
                    "rules": [
                        {
                            "exclusion_rule": {
                                "exclude_info_types": {
                                    "info_types": [{"name": "EMAIL_ADDRESS"}]
                                },
                                "matching_type": matching_type,
                            }
                        }
                    ],
                }
            ],
            "include_quote": True,
        }
        request = {
            "parent": self.parent,
            "deidentify_config": deidentify_config,
            "inspect_config": inspect_config,
            "item": {"value": input_str},
        }
        response = self._dlp.deidentify_content(request=request)
        # for summary in response.overview.transformation_summaries:
        #    print(summary.results)
        return response.item.value

    def reidentify_encrypted(self, input_str: str):
        reidentify_config = {
            # "transformation_error_handling": {"leave_untransformed": {}},
            "info_type_transformations": {"transformations": [self.transformation()]},
        }
        inspect_config = {
            "custom_info_types": [
                {"info_type": {"name": self.surrogate_type}, "surrogate_type": {}},
            ]
        }
        request = {
            "parent": self.parent,
            "reidentify_config": reidentify_config,
            "inspect_config": inspect_config,
            "item": {"value": input_str},
        }
        response = self._dlp.reidentify_content(request=request)
        return response.item.value

    def transformation(self):
        return {
            "primitive_transformation": {
                "crypto_deterministic_config": {
                    "crypto_key": {
                        "kms_wrapped": {
                            "wrapped_key": self.unwrapped_key,
                            "crypto_key_name": self.key_name,
                        }
                    },
                    "surrogate_info_type": {"name": self.surrogate_type},
                }
            },
        }
