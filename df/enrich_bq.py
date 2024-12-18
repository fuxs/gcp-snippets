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
from apache_beam.transforms.enrichment import Enrichment
from apache_beam.transforms.enrichment_handlers.bigquery import (
    BigQueryEnrichmentHandler,
)


class ToRow(beam.DoFn):
    from apache_beam import Row

    def process(self, element):
        yield self.Row(**element)


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
    table = known_args.table
    project = table.split(".")[0]

    enrichment = Enrichment(
        BigQueryEnrichmentHandler(
            project=project,
            table_name=table,
            row_restriction_template="user_id = '{}'",
            fields=["user_id"],
        )
    )
    if known_args.redis_host is not None:
        adr = known_args.redis_host.split(":")
        host = adr[0]
        port = int(adr[1]) if len(adr) == 2 else 6379
        enrichment = enrichment.with_redis_cache(host, port)
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
            | "To Row" >> beam.ParDo(ToRow())
            | "BigQuery Enrichment" >> enrichment
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
        "--table",
        dest="table",
        required=True,
        help='BigQuery table ("PROJECT.DATASET.TABLE")',
    )
    parser.add_argument(
        "--redis_host",
        dest="redis_host",
        required=False,
        help='Redis host with port (e.g. "10.89.1.3:6379")',
    )
    run_pipeline(*parser.parse_known_args())
