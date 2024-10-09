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
from google import auth
from google.cloud import bigquery
from google.cloud import dataform_v1beta1 as dataform
from bqsql import get_full_names, list_sql, create_sql, load_sql, save_sql, delete_sql
import argparse
import os


def resolve_display_name(display_name: str, project: str, region: str, client):
    """This function gets the full name for a display name.

    One display name (the name you can see in the BQ UI) can be used by several
    assets. If a display name returns more than one asset, the user has to
    choose one.
    """
    names = get_full_names(
        display_name=display_name,
        project=project,
        region=region,
        client=client,
    )
    if names is None:
        print(f"{display_name} not found in projects/{project}/locations/{region}")
        exit(1)
    length = len(names)
    if length == 1:
        return names[0]
    print("Multiple assets available.")
    for i, name in enumerate(names):
        print(f"[{i}]: {name}")
    while True:
        s = input(f"Select one [0-{length-1}]: ")
        if s.isnumeric():
            s = int(s)
            if 0 <= s < length:
                return names[s]


def main():
    # we are using ADC
    credentials, project_id = auth.default()
    client = dataform.DataformClient(credentials=credentials)
    parser = argparse.ArgumentParser("bq_saved_sql")
    parser.add_argument(
        "cmd",
        help="The command: list (default),load, execute ,create, save, delete",
        nargs="?",
        default="list",
    )
    parser.add_argument(
        "--project",
        help="The Google Cloud project id",
        default=os.environ.get("PROJECT", project_id),
    )
    parser.add_argument(
        "--region",
        help="The Google Cloud region (default: us-central1)",
        default=os.environ.get("REGION", "us-central1"),
    )
    parser.add_argument("--name", help="Display name of the stored SQL file")

    parser.add_argument("--sql", help="The SQL code for create and update commands")

    args = parser.parse_args()
    if not args.region:
        print("Region not provided. Either use --region or set REGION.")
        exit(parser.print_usage())

    if args.cmd == "list":
        assets = list_sql(project=args.project, region=args.region, client=client)
        for n, v in assets.items():
            print(f"{n} = {v}")
    elif args.cmd == "load":
        if not args.name:
            print("Please provide --name")
            exit(parser.print_usage())
        full_name = resolve_display_name(
            display_name=args.name,
            project=args.project,
            region=args.region,
            client=client,
        )
        sql = load_sql(
            full_name=full_name,
            client=client,
        )
        print(sql)
    elif args.cmd == "execute":
        if not args.name:
            print("Please provide --name")
            exit(parser.print_usage())
        full_name = resolve_display_name(
            display_name=args.name,
            project=args.project,
            region=args.region,
            client=client,
        )
        sql = load_sql(
            full_name=full_name,
            client=client,
        )
        # now execute the SQL with the BQ client
        bqclient = bigquery.Client()
        result = bqclient.query(sql)
        for row in result:
            print(row)
    elif args.cmd == "create":
        if args.name is None:
            print("Please provide --name")
            exit(parser.print_usage())
        if args.sql is None:
            print("Please provide --sql")
            exit(parser.print_usage())
        create_sql(
            sql=args.sql,
            display_name=args.name,
            project=args.project,
            region=args.region,
            client=client,
        )
    elif args.cmd == "save":
        if args.name is None:
            print("Please provide --name")
            exit(parser.print_usage())
        if args.sql is None:
            print("Please provide --sql")
            exit(parser.print_usage())
        full_name = resolve_display_name(
            display_name=args.name,
            project=args.project,
            region=args.region,
            client=client,
        )
        save_sql(
            sql=args.sql,
            full_name=full_name,
            client=client,
        )
    elif args.cmd == "delete":
        if args.name is None:
            print("Please provide --name")
            exit(parser.print_usage())
        full_name = resolve_display_name(
            display_name=args.name,
            project=args.project,
            region=args.region,
            client=client,
        )
        delete_sql(full_name=full_name, client=client)
    else:
        exit(parser.print_help())


if __name__ == "__main__":
    main()
