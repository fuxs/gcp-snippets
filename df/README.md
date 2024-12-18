# Enrichment of unbounded streams with Dataflow

These code snippets demonstrate the enrichment of unbounded streams with data
from external sources. All examples use the following Pub/Sub subscription as
main input.

## Prerequisites

### Pub/Sub

For this example we need a Pub/Sub topic with a related pull subscription:

```bash
gcloud pubsub topics create df-sample-events
gcloud pubsub subscriptions create df-sub --topic=df-sample-events
```

### Cloud Storage

Dataflow requires a Google Cloud Storage location for temporary files. Please
replace `BUCKET_NAME` with your individual bucket name:

```bash
gcloud storage buckets create gs://BUCKET_NAME --location=us-central1
```

## Enrichment with REST API

We will use the service [genderize.io](https://genderize.io/) to determine the
gender by the first name. You can find the code in the file
[enrich_rest.py](enrich_rest.py).

Please replace `PROJECT_ID` with your project id and `BUCKET_NAME` with your
bucket name before you call the following command:

```bash
python enrich_rest.py \
    --subscription="projects/PROJECT_ID/subscriptions/df-sub" \
    --runner="DataflowRunner" \
    --region="us-central1" \
    --project=PROJECT_ID \
    --temp_location="gs://BUCKET_NAME/temp/" \
    --max_num_workers=1
```

Now you can send a JSON message with a `first_name` attribute like:

```bash
gcloud pubsub topics publish df-sample-events --message='{
    "first_name": "Michael",
    "last_name": "Meyer"
}'
```

Open the Dataflow UI, select the job, open the logs at the bottom and select
WORKER LOGS. Switch Severity to Info and you should see a log entry like:

```json
{'first_name': 'Michael', 'last_name': 'Meyer', 'gender': 'male', 'probability': 1.0}
```

Stop the job to avoid any interferences with the following examples and
unnecessary costs.

### Optional: use Redis cache

We can use Redis to cache the results. Google Cloud provides a managed Redis
with
[Memorystore](https://console.cloud.google.com/memorystore/redis/instances).

Create an instance and use the [connection
information](https://console.cloud.google.com/memorystore/redis/locations/us-central1/instances/dfcache/details/connections)
in the `--redis_host` parameter. Replace `IP` with the ip-address of your Redis
instance an `PORT` with the port:

```bash
python enrich_rest.py \
    --subscription="projects/PROJECT_ID/subscriptions/df-sub" \
    --redis_host="IP:PORT" \
    --runner="DataflowRunner" \
    --region="us-central1" \
    --project=PROJECT_ID \
    --temp_location="gs://BUCKET_NAME/temp/" \
    --max_num_workers=1
```

Stop the job to avoid any interferences with the following examples and
unnecessary costs.

## Enrich with BiqQuery

We can use other databases like BigTable and data warehouses like BigQuery to
enrich messages in our Dataflow pipelines. The file [enrich_bq.py](enrich_bq.py)
takes again a Pub/Sub message and enriches it with data from a BigQuery table.
First, we have to create the table with the additional information.

```sql
# Create a new dataset
CREATE SCHEMA IF NOT EXISTS dataflow_ds OPTIONS (location = 'us-central1');

# Create the table
CREATE TABLE IF NOT EXISTS dataflow_ds.user_data (
  user_id STRING NOT NULL OPTIONS (description = 'This is our key'),
  first_name STRING,
  last_name STRING
);

# Add some values
INSERT INTO dataflow_ds.user_data VALUES
('abc','Maria','Jackson'),
('123','Thomas','Schmidt');
```

Please replace `PROJECT_ID` with your project id and `BUCKET_NAME` with your
bucket name before you call the following command:

```bash
python enrich_bq.py \
    --subscription="projects/PROJECT_ID/subscriptions/df-sub" \
    --table="PROJECT_ID.dataflow_ds.user_data" \
    --runner="DataflowRunner" \
    --region="us-central1" \
    --project=PROJECT_ID \
    --temp_location="gs://BUCKET_NAME/temp/" \
    --max_num_workers=1
```

Now you can send a JSON message with a `user_id` attribute like:

```bash
gcloud pubsub topics publish df-sample-events --message='{
    "user_id": "123",
    "event": "email_open"
}'
```

You should find now a row like the following in your Dataflow worker logs:

```python
Row(user_id='123', event='email_open', first_name='Thomas', last_name='Schmidt')
```

Stop the job to avoid any interferences with the following examples and
unnecessary costs.

## Enrich with shared object

We can use a shared Python object like a dictionary to enrich the streamed data.
In this example we use a Cloud Storage object as data source.

### Cloud Storage data

We want to provide a *Newline Delimited JSON* file for the side input data in a
separate bucket. The second bucket is necessary to get notifications for updates
on new files in later examples. Please replace `DATA_BUCKET_NAME` with your
individual bucket name:

```bash
gcloud storage buckets create gs://DATA_BUCKET_NAME --location=us-central1
```

Copy the file [side_input.ndjson](side_input.ndjson) to your new bucket. :

```bash
gcloud storage cp side_input.ndjson gs://DATA_BUCKET_NAME
```

## One-shot data loading

The example [enrich_oneshot.py](enrich_oneshot.py) loads the NDJSON data to a
dictionary and enriches messages with the attribute `user_id`.

Please replace `PROJECT_ID` with your project id and `BUCKET_NAME` as well as
`DATA_BUCKET_NAME` with your buckets name before you call the following command:

```bash
python enrich_oneshot.py \
    --subscription="projects/PROJECT_ID/subscriptions/df-sub" \
    --uri="gs://DATA_BUCKET_NAME/side_input.ndjson" \
    --runner="DataflowRunner" \
    --region="us-central1" \
    --project=PROJECT_ID \
    --temp_location="gs://BUCKET_NAME/temp/" \
    --max_num_workers=1
```

Now you can send a JSON message with a `user_id` attribute like:

```bash
gcloud pubsub topics publish df-sample-events --message='{
    "user_id": "123",
    "event": "email_open"
}'
```

## Periodical data loading

The example [enrich_periodically.py](enrich_periodically.py) loads the NDJSON
data to a dictionary and enriches messages with the attribute `user_id`. It
updates the data every 60 seconds, this is one way to deal with changed lookup
data.

Please replace `PROJECT_ID` with your project id and `BUCKET_NAME` as well as
`DATA_BUCKET_NAME` with your buckets name before you call the following command:

```bash
python enrich_periodically.py \
    --subscription="projects/PROJECT_ID/subscriptions/df-sub" \
    --uri="gs://DATA_BUCKET_NAME/side_input.ndjson" \
    --runner="DataflowRunner" \
    --region="us-central1" \
    --project=PROJECT_ID \
    --temp_location="gs://BUCKET_NAME/temp/" \
    --max_num_workers=1
```

## Update side input with constant frequency

The file [enrich_sideinput.py](enrich_sideinput.py) uses the `PeriodicImpulse` source to
trigger the pipeline for the side input with the given frequency. Please replace
`DATA_BUCKET_NAME`,`BUCKET_NAME` and `PROJECT_ID` with your values.

```bash
python enrich_sideinput.py \
    --subscription="projects/PROJECT_ID/subscriptions/df-sub" \
    --uri="gs://DATA_BUCKET_NAME/side_input.ndjson" \
    --runner="DataflowRunner" \
    --region="us-central1" \
    --project=PROJECT_ID \
    --temp_location="gs://BUCKET_NAME/temp/" \
    --staging_location="gs://BUCKET_NAME/staging/"
```

## Update side input on object change

Activate notifications for object creation in our data bucket. Please replace
`DATA_BUCKET_NAME` and `PROJECT_ID` with your values:

```bash
gcloud storage buckets notifications create gs://DATA_BUCKET_NAME \
    --topic="projects/PROJECT_ID/topics/cloud-storage-notification" \
    --event-types=OBJECT_FINALIZE
```

Create a subscription for the new topic:

```bash
gcloud pubsub subscriptions create csn-sub --topic=cloud-storage-notification
```

Execute the the [enrich_event.py](enrich_event.py):

```bash
python enrich_event.py \
    --subscription="projects/PROJECT_ID/subscriptions/df-sub" \
    --uri="gs://DATA_BUCKET_NAME/side_input.ndjson" \
    --runner="DataflowRunner" \
    --region="us-central1" \
    --project=PROJECT_ID \
    --temp_location="gs://BUCKET_NAME/temp/" \
    --staging_location="gs://BUCKET_NAME/staging/"
```

Copy the file again, before you submit a message!

```bash
gcloud storage cp side_input.ndjson gs://DATA_BUCKET_NAME
```

This is necessary, otherwise the processing will be blocked. Now you can send a
JSON message with a `user_id` attribute like:

```bash
gcloud pubsub topics publish df-sample-events --message='{
    "user_id": "123",
    "event": "email_open"
}'
```
