package com.wizeline.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import io.debezium.connector.mysql.MySqlConnector;
import org.apache.beam.io.debezium.DebeziumIO;
import org.apache.beam.io.debezium.SourceRecordJson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class SampleCdcMySqlToBigQuery {
    private static final Logger LOG = LoggerFactory.getLogger(SampleCdcMySqlToBigQuery.class);

    public static void main(String[] args) {
        CdcPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CdcPipelineOptions.class);
        run(options);
    }

    private static PipelineResult run(CdcPipelineOptions options) {
        // Create the pipeline with options
        Pipeline pipeline = Pipeline.create(options);

        // Step 1: Configure Debezium connector
        pipeline
            .apply(
                "Read from DebeziumIO",
                DebeziumIO.<String>read()
                    .withConnectorConfiguration(
                        DebeziumIO.ConnectorConfiguration.create()
                            .withUsername(options.getUsername())
                            .withPassword(options.getPassword())
                            .withHostName(options.getHostname())
                            .withPort(options.getPort())
                            .withConnectorClass(MySqlConnector.class)
                            .withConnectionProperty("database.server.name", "dbserver1")
                            .withConnectionProperty("database.include.list", "inventory")
                            .withConnectionProperty("include.schema.changes", "false")
                            .withConnectionProperty("table.include.list", "inventory.customers")
                            .withConnectionProperty("connect.keep.alive", "false")
                            .withConnectionProperty("connect.keep.alive.interval.ms", "200")
                    )
                    .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
                    .withCoder(StringUtf8Coder.of())
            )

            //Step 2: Get record from CDC Json event and create a TableRow per event
            .apply("Json to TableRow", ParDo.of(new DoFn<String, TableRow>() {
                @DoFn.ProcessElement
                public void processElement(ProcessContext c) throws Exception {
                    String json = c.element();
                    LOG.debug("PIPELINE STEP2: {}", json);

                    TableRow tr = new TableRow();
                    tr.set("json", json);
                    c.output(tr);
                }
            }))

            // Step 3: Append TableRow to a given BigQuery table: outputTable argument.
            .apply(
                "Write to BigQuery",
                BigQueryIO.writeTableRows()
                    .withoutValidation()
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                    .withSchema(new TableSchema().setFields(Arrays.asList(
                        new TableFieldSchema().setName("json").setType("STRING")
                    )))
                    .to(options.getOutputTable()));

        return pipeline.run();
    }
}
