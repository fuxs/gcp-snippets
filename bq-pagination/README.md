# BigQuery Pagination

BigQuery supports pagination with a pagination ID. This is an example for a
simple web service build with FastAPI.

## Prerequisites

Install the required Python packages:

```bash
pip install -r requirements.txt
```

## Example

Start the example application:

```bash
fastapi dev app.py
```

The example client calls the service and requests all pages in increments of 1000:

```bash
python main.py
```
