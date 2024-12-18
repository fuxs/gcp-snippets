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
from apache_beam import io, pvalue
from apache_beam.pipeline import Pipeline
from apache_beam.transforms import window
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
)
from apache_beam.transforms.trigger import (
    AfterCount,
    AccumulationMode,
    Repeatedly,
)


class Log(beam.DoFn):

    def process(self, element):
        logging.info(f"Impulse for side-input {element}")
        yield element


class Enrich(beam.DoFn):

    def process(self, element, users_lookup):
        result = users_lookup.get(element["user_id"], None)
        if result is None:
            yield {**element, "user_name": "unknown"}
        else:
            yield {**element, "user_name": result}


def to_key_value(obj):
    return (obj["user_id"], (obj["first_name"], obj["last_name"]))


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
    event_subscription = known_args.event_subscription
    uri = known_args.uri
    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    with Pipeline(options=options) as p:
        sideinput_pipeline = (
            p
            | "Event pull" >> io.ReadFromPubSub(subscription=event_subscription)
            | "Event window"
            >> beam.WindowInto(
                beam.window.FixedWindows(3600),
                trigger=Repeatedly(AfterCount(1)),
                accumulation_mode=AccumulationMode.DISCARDING,
            )
            | "Event logging" >> beam.ParDo(Log())
            | "File name" >> beam.Map(lambda _: uri)
            | "Read file" >> io.ReadAllFromText()
            | "JSON to user" >> beam.Map(json.loads)
            | "KeyValue" >> beam.Map(to_key_value)
        )
        outputs = (
            p
            | "Pull" >> io.ReadFromPubSub(subscription=subscription)
            | "To Python" >> beam.Map(json.loads)
            | "Check" >> beam.ParDo(CheckData("user_id")).with_outputs("ok", "err")
        )
        (
            outputs.ok
            | "Window" >> beam.WindowInto(window.FixedWindows(20))
            | "Enrich" >> beam.ParDo(Enrich(), pvalue.AsDict(sideinput_pipeline))
            | "Result" >> beam.ParDo(logging.info)
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
        "--event_subscription",
        dest="event_subscription",
        required=True,
        help="Pub/Sub subscription for Cloud Storage events "
        + '("projects/PROJECT/subscriptions/SUBSCRIPTION")',
    )
    parser.add_argument(
        "--uri",
        dest="uri",
        required=True,
        help="File with side input, e.g. gs://my_bucket/my_file.ndjson",
    )
    run_pipeline(*parser.parse_known_args())
