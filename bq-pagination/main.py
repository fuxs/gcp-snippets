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
import requests


# this is just a helper function to call the service
def get(base: str, path: str, params: dict[str, any]):
    resp = requests.get(f"{base}/{path}", params=params)
    if resp.status_code != 200:
        raise Exception(f"status: {resp.status_code} message: {resp.text}")
    return resp.json()


def main():
    # step 1: execute the query and wait for the result
    job_info = get(base="http://127.0.0.1:8000", path="query", params={"wait": True})
    next_page_token = None
    while True:
        params = {
            # request pages with 1000 rows max
            "max_results": 1000,
        }
        if next_page_token is not None:
            # there is a next page
            params["page_token"] = next_page_token
        result = get(
            base="http://127.0.0.1:8000",
            path=f"rows/{job_info['table_id']}",
            params=params,
        )
        # print the rows
        for row in result["rows"]:
            print(row)
        # get the next page token
        next_page_token = result.get("next_page_token", None)
        if next_page_token is None:
            # no more pages, exit
            break


if __name__ == "__main__":
    main()
