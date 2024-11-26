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
from fastapi import FastAPI
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from typing import Optional

app = FastAPI()
app.client = bigquery.Client()


@app.get("/query")
def execute_query(wait: bool = False):
    """Get the next rows in JSON for the passed table id.

    Parameters:

    wait (bool): if True, wait for the result of the query, otherwise return immediately

    Returns:

    dict[str,str]: contains the state and the job_id
    """
    query = """
    SELECT name, SUM(number) as total_people
    FROM `bigquery-public-data.usa_names.usa_1910_2013`
    GROUP BY name
    ORDER BY total_people DESC
    """
    job = app.client.query(query)
    if wait:
        job.result()
    result = {"state": job.state, "job_id": job.job_id}
    if job.state == "DONE":
        result["table_id"] = str(job.destination)
    return result

@app.get("/job/{job_id}")
def get_query_state(job_id: str):
    """Get the state of the passed job

    Parameters:

    job_id (str): the job id of the

    Returns:

    dict[str,str]: contains the state and the job_id
    """
    try:
        job = app.client.get_job(job_id)
        if job.state == "DONE":
            return {"state": "DONE", "table_id": str(job.destination)}
    except NotFound as _:
        return {"state": "UNKNOWN_ID", "error": f"Unknown job id {job_id}"}
    return {"state": job.state}


@app.get("/rows/{table_id}")
def get_rows(
    table_id: str,
    max_results: int = 10,
    page_token: Optional[str] = None,
):
    """Get the next rows in JSON for the passed table id.

    Parameters:

    table_id (str): the table id in form of `project_id.dataset_id.table_id``

    max_results (int): the maximum number of returned rows

    next_page_token (Optional[str]): the page token returned from the previous
    call of this function

    Returns:

    dict[str,any]: contains the rows and a next_page_token if available
    """
    rows_iter = app.client.list_rows(
        table=table_id, page_token=page_token, max_results=max_results
    )
    rows = [dict(row) for row in rows_iter]
    result = {
        "rows": rows,
    }
    if rows_iter.next_page_token is not None:
        result["next_page_token"] = rows_iter.next_page_token
    return result
