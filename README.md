# wizeline-beam-examples

#### To install dependencies

```mvn clean install -X    ```

#### To compile + deploy (DirectRunner)
```
mvn compile -X exec:java \                                                                                    
-Dexec.mainClass=com.wizeline.examples.LosAngelesPgSqlToBigQuery \
-Dexec.args="--runner=DirectRunner \
--project=<<your_project_id>> \
--region=us-central1 \
--gcpTempLocation=gs://<<your_biucket>>/temp/ \
--username=postgres \
--password=password \
--hostname=127.0.0.1 \
--port=5432 \
--serviceAccount=<<your_service_account>> \
--outputTable=<<project_id>>:<<dataset>>.<<table>> \
--bigQueryLoadingTemporaryDirectory=gs://<<your_bucket>>/temp/ \
--defaultWorkerLogLevel=DEBUG"
```

