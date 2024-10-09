# BigQuery Saved Queries
This example provides code for managing saved SQL queries in BigQuery. The file
`bqsql.py` contains all functions to list, create, execute, load, update and
delete saved queries.

The file `main.py`is just an example for the usage of the provided functions.

## Prerequisites
Install the required Python packages:

```bash
pip install -r requirements.txt
```

## Optional
Use the environment variables `PROJECT` and `REGION` to skip the command line
arguments:

```bash
export REGION=europe-west3
export PROJECT=your-project
```

Then you can use

```bash
python main.py list
```

instead of

```bash
python main.py --project your-project --region europe-west3 list
```

## List SQLs
List the available SQL assets:

```bash
python main.py
```

or

```bash
python main.py list
```

## Load SQL
Load the SQL and print it out:

```bash
python main.py load --name="name of asset"
```

## Execute SQL
Executes the stored SQL and prints the result.

```bash
python main.py execute --name="name of asset"
```

## Create SQL
Creates an new saved SQL query in BigQuery:

```bash
python main.py create --name="name of asset" --sql="SELECT * FROM `project.dataset.table`"
```

## Save SQL
Save a new version of an exisiting saved SQL query:

```bash
python main.py save --name="name of asset" --sql="SELECT * FROM `project.dataset.table`"
```

## Delete SQL
Delete a saved SQL query:

```bash
python main.py delete --name="name of asset"
```