package com.wizeline.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.protobuf.FieldType;
import io.debezium.connector.postgresql.PostgresConnector;
import org.apache.beam.io.debezium.DebeziumIO;
import org.apache.beam.io.debezium.SourceRecordJson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import java.text.SimpleDateFormat;
import java.util.*;

public class LosAngelesPgSqlToBigQuery {
    private static final Logger LOG = LoggerFactory.getLogger(LosAngelesPgSqlToBigQuery.class);

    private static final TableSchema TABLE_SCHEMA = new TableSchema()
        .setFields(
            Arrays.asList(
                new TableFieldSchema().setName("order_number").setType("INTEGER"),
                new TableFieldSchema().setName("order_datetime_utc").setType("DATETIME"),
                new TableFieldSchema().setName("purchaser").setType("INTEGER"),
                new TableFieldSchema().setName("quantity").setType("INTEGER"),
                new TableFieldSchema().setName("product_id").setType("INTEGER"),
                new TableFieldSchema().setName("source").setType("STRING"),
                new TableFieldSchema().setName("billing_zipcode").setType("STRING")
            ));

    public static void main(String[] args) {
        CdcPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CdcPipelineOptions.class);
        run(options);
    }

    /**
     * Runs the pipeline with the supplied options.
     *
     * @return The result of the pipeline execution.
     */
    private static PipelineResult run(CdcPipelineOptions options) {
        Pipeline pipeline = Pipeline.create();

        // Step 1: Configure Debezium connector
        pipeline
            .apply(
                "Read from DebeziumIO",
                DebeziumIO.<String>read()
                    .withConnectorConfiguration(
                        DebeziumIO.ConnectorConfiguration.create()
                            .withUsername(options.getUsername())
                            .withPassword(options.getPassword())
                            .withConnectorClass(PostgresConnector.class)
                            .withHostName(options.getHostname())
                            .withPort(options.getPort())
                            .withConnectionProperty("database.dbname", "postgres")
                            .withConnectionProperty("database.server.name", "server2")
                            .withConnectionProperty("schema.include.list", "inventory")
                            .withConnectionProperty("database.include.list", "inventory")
                            .withConnectionProperty("table.include.list", "inventory.online_orders")
                    .withConnectionProperty("include.schema.changes", "false")
                    .withConnectionProperty("poll.interval.ms", "3000")
                    ).withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper()).withCoder(StringUtf8Coder.of())
                )

            //Step 2: Transform Debezium event (Json) into TableRow, single field: json.
            .apply("Json to TableRow", ParDo.of(new DoFn<String, TableRow>()   {
                @DoFn.ProcessElement
                public void processElement(ProcessContext c) throws Exception {
                    LOG.debug("PIPELINE STEP2: {}", c.element());
                    DocumentContext context = JsonPath.parse(c.element());

                    TableRow tr = new TableRow();
                    tr.set("order_number", context.read("$.after.fields.order_number", Integer.class));
                    tr.set("purchaser", context.read("$.after.fields.purchaser", Integer.class));
                    tr.set("quantity", context.read("$.after.fields.quantity", Integer.class));
                    tr.set("product_id", context.read("$.after.fields.product_id", Integer.class));
                    String datetime = context.read("$.after.fields.order_local_datetime_pst");

                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                    dateFormat.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
                    Date order_date = dateFormat.parse(datetime);
                    // To UTC
                    TimeZone timeZone = TimeZone.getTimeZone("America/Los_Angeles");
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(order_date);
                    calendar.setTimeZone(timeZone);
                    calendar.add(Calendar.HOUR, 8); // GMT-8
                    SimpleDateFormat formatterWithTimeZone = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    formatterWithTimeZone.setTimeZone(TimeZone.getTimeZone("UTC"));

                    tr.set("order_datetime_utc", formatterWithTimeZone.format(calendar.getTime()));
                    tr.set("source", "los_angeles");

                    String[] parts = context.read("$.after.fields.billing_address").toString().split(" ");
                    tr.set("billing_zipcode", parts[parts.length - 1]);

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
                    .withSchema(TABLE_SCHEMA)
                    .to(options.getOutputTable()));

        return pipeline.run();
    }
}

