# Java UDF in Spark SQL

This is an example for a user defined function (UDF) written in Java and called
from Spark SQL.

## Build the JAR

We use [Gradle](https://gradle.org/) to build this example.

```shell
./gradlew jar
```

Copy the built JAR file and the required third party libraries to a Cloud Storage bucket:

```shell
gsutil cp ./app/build/libs/app.jar \
  gs://mybucket/spark/app/app.jar

curl -L https://repo1.maven.org/maven2/io/github/mysto/ff3/1.2.0/ff3-1.2.0.jar | \
  gsutil cp - gs://mybucket/spark/libs/ff3-1.2.0.jar
```

Submit the code to Dataproc Serverless with the following command
([documentation](https://cloud.google.com/sdk/gcloud/reference/dataproc/batches/submit)).
Please consider the single `--`, it separates the arguments you want to pass to
your code.
```shell
gcloud dataproc batches submit spark \
  --batch=mybatch001 \
  --jars=gs://mybucket/spark/app/app.jar,gs://mybucket/spark/libs/ff3-1.2.0.jar \
  --class=udfexample.App \
  --region=us-central1 \
  --version=2.2 \
  --properties spark.dataproc.appContext.enabled=true \
  -- \
  --src=myproject.demos.customer \
  --dst=myproject.demos.customer_new \
  --query="SELECT CONCAT(encrypt(first_name),' - ',  last_name) AS name FROM source"
```

Please replace the arguments `--src`, `--dst` and  `--query` with your values.
In this example, the source table `myproject.demos.customer` contains at least
two columns `first_name` and `last_name`. `encrypt` is the name of the UDF.
