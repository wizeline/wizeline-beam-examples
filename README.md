# wizeline-beam-examples

#### To install dependencies

```mvn clean install -X    ```

#### To compile + deploy (DirectRunner)

mvn compile -X exec:java \                                                                                    
-Dexec.mainClass=com.wizeline.examples.LosAngelesPgSqlToBigQuery \
-Dexec.args="--runner=DirectRunner \
--project=apache-beam-poc-4a7e4215 \
--region=us-central1 \
--gcpTempLocation=gs://gcp-beam-hassan-test/temp/ \
--username=postgres \
--password=password \
--hostname=127.0.0.1 \
--port=5432 \
--serviceAccount=gcp-beam-poc-hassan@apache-beam-poc-4a7e4215.iam.gserviceaccount.com \
--outputTable=apache-beam-poc-4a7e4215:google_beam_poc.merged_orders \
--bigQueryLoadingTemporaryDirectory=gs://gcp-beam-hassan-test/temp/ \
--defaultWorkerLogLevel=DEBUG"

