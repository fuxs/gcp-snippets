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
import logging
import json
import apache_beam as beam
from apache_beam import io
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
)
from apache_beam.utils import shared


class EnrichOrderFn(beam.DoFn):

    def __init__(self, uri):
        import re

        self.handle = shared.Shared()
        matches = re.match("gs://(.*?)/(.*)", uri)
        if matches:
            self.bucket, self.blob = matches.groups()
        else:
            raise Exception(f"invalid URI {uri}")

    def setup(self):
        self.users_lookup = self.handle.acquire(self.load_users)

    def load_users(self):
        from google.cloud.storage import Client

        class WeakRefDict(dict):
            pass

        client = Client()
        bucket = client.bucket(self.bucket)
        blob = bucket.blob(self.blob)
        result = {}
        with blob.open() as f:
            for line in f.readlines():
                obj = json.loads(line)
                kv = (obj["user_id"], (obj["first_name"], obj["last_name"]))
                result[kv[0]] = kv[1]
        return WeakRefDict(result)

    def process(self, element):
        result = self.users_lookup.get(element["user_id"], None)
        if result is None:
            yield {**element, "user_name": "unknown"}
        else:
            yield {**element, "user_name": result}


class CheckData(beam.DoFn):
    from apache_beam.pvalue import TaggedOutput

    def __init__(self, row_key):
        self.row_key = row_key

    def process(self, element):
        if element.get(self.row_key, None) is None:
            yield self.TaggedOutput("err", ("missing key", element))
        else:
            yield self.TaggedOutput("ok", element)


def run_pipeline(known_args, pipeline_args):
    subscription = known_args.subscription
    uri = known_args.uri
    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    with Pipeline(options=options) as p:
        outputs = (
            p
            | "Pull" >> io.ReadFromPubSub(subscription=subscription)
            | "To Python" >> beam.Map(json.loads)
            | "Check" >> beam.ParDo(CheckData("user_id")).with_outputs("ok", "err")
        )
        (
            outputs.ok
            | "Enrich" >> beam.ParDo(EnrichOrderFn(uri))
            | "Result" >> beam.Map(logging.info)
        )
        (
            outputs.err
            | "Error Message" >> beam.Map(lambda e: f"error '{e[0]}' for {e[1]}")
            | "Error Logging" >> beam.Map(logging.info)
        )


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--subscription",
        dest="subscription",
        required=True,
        help='Pub/Sub subscription ("projects/PROJECT/subscriptions/SUBSCRIPTION")',
    )
    parser.add_argument(
        "--uri",
        dest="uri",
        required=True,
        help="File with side input, e.g. gs://my_bucket/side_input.ndjson",
    )
    run_pipeline(*parser.parse_known_args())
