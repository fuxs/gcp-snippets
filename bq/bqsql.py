# -*- coding: utf-8 -*-
# Copyright 2023 Michael Bungenstock
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
from google.cloud import dataform_v1beta1 as dataform
import uuid


def list_sql(
    project: str, region: str, client=dataform.DataformClient()
) -> dict[str, str]:
    """List all assets in a repository.

    Parameters
    ----------
    project: str
        The Google Cloud project id
    region: str
        The Google Cloud region
    client: DataformClient
        The Dataform Python client object
    """
    # get the full names of all SQL resources
    request = dataform.ListRepositoriesRequest(
        parent=f"projects/{project}/locations/{region}",
        filter='labels.single-file-asset-type="sql"',
    )
    page_result = client.list_repositories(request=request)
    # map the disply names to the full names
    sqls = dict()
    for page in page_result:
        sqls.setdefault(page.display_name, []).append(page.name)
    return sqls


def get_full_names(
    display_name: str, project: str, region: str, client=dataform.DataformClient()
) -> str:
    """Returns the full name for a saved SQL script in the provided repository.

    Parameters
    ----------
    display_name: str
        The name of the SQL script as used in the BQ UI
    project: str
        The Google Cloud project id
    region: str
        The Google Cloud region
    client: DataformClient
        The Dataform Python client object
    """
    sqls = list_sql(project=project, region=region, client=client)
    # use a dsiplay name to get the file
    return sqls.get(display_name, None)


def load_sql(full_name: str, client=dataform.DataformClient()) -> str:
    """Loads the SQL script from the provided repository.

    Parameters
    ----------
    full_name: str
        The full name of the SQL asset
    client: DataformClient
        The Dataform Python client object
    """
    request = dataform.ReadRepositoryFileRequest(name=full_name, path="content.sql")
    response = client.read_repository_file(request=request)
    # response.contents is a binary array
    return response.contents.decode()


def save_sql(full_name: str, sql: str, client=dataform.DataformClient()):
    """Saves a new version of the SQL script in the provided repository.

    Parameters
    ----------
    sql: str
        The new SQL code to be saved
    full_name: str
        The full name of the SQL asset
    client: DataformClient
        The Dataform Python client object
    """
    # Please provide your own metadata,
    # it is for information only!
    commit_metadata = dataform.CommitMetadata(commit_message="Written in Python")
    commit_metadata.author.name = "Your Name"
    commit_metadata.author.email_address = "user@mail.com"
    fo = dataform.CommitRepositoryChangesRequest.FileOperation()
    fo.write_file.contents = sql.encode()
    request = dataform.CommitRepositoryChangesRequest(
        name=full_name,
        commit_metadata=commit_metadata,
        file_operations={"content.sql": fo},
    )
    client.commit_repository_changes(request=request)


def create_sql(
    sql: str,
    display_name: str,
    project: str,
    region: str,
    client=dataform.DataformClient(),
):
    """Create a new SQL script in the provided repository.

    Parameters
    ----------
    sql: str
        The SQL code to be saved
    display_name: str
        The name of the SQL script as used in the BQ UI
    project: str
        The Google Cloud project id
    region: str
        The Google Cloud region
    client: DataformClient
        The Dataform Python client object
    """
    repository = dataform.Repository(
        display_name=display_name,
        set_authenticated_user_admin=True,
        labels={"single-file-asset-type": "sql"},
    )
    request = dataform.CreateRepositoryRequest(
        parent=f"projects/{project}/locations/{region}",
        repository_id=str(uuid.uuid4()),
        repository=repository,
    )
    response = client.create_repository(request=request)
    save_sql(full_name=response.name, sql=sql, client=client)


def delete_sql(
    full_name: str,
    client=dataform.DataformClient(),
):
    """Deletes the SQL script with the given name in the provided repository.

    Parameters
    ----------
    full_name: str
        The full name of the SQL asset
    client: DataformClient
        The Dataform Python client object
    """
    request = dataform.DeleteRepositoryRequest(name=full_name, force=True)
    client.delete_repository(request=request)
