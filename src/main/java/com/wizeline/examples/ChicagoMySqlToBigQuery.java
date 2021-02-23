package com.wizeline.examples;

import com.google.api.services.bigquery.model.TableRow;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Pipeline that replicates online orders from MySql using Debezium to a BigQuery table.
 */
public class ChicagoMySqlToBigQuery {
    private static final Logger LOG = LoggerFactory.getLogger(ChicagoMySqlToBigQuery.class);

    public static void main(String[] args) {

        // Parse the user options passed from the command-line
        CdcPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(CdcPipelineOptions.class);

        run(options);
    }

    /**
     * Runs the pipeline with the supplied options.
     *
     * @param options The execution parameters to the pipeline.
     * @return The result of the pipeline execution.
     */
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
                            .withConnectionProperty("table.include.list", "inventory.online_orders")
                            .withConnectionProperty("include.schema.changes", "false")
                            .withConnectionProperty("database.allowPublicKeyRetrieval", "true")
                            .withConnectionProperty("connect.keep.alive", "false")
                            .withConnectionProperty("connect.keep.alive.interval.ms", "200")
                    )
                    .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
                    .withCoder(StringUtf8Coder.of())
            ).setCoder(StringUtf8Coder.of())

            //Step 2: Get record from CDC Json event, transform localtime to UTC, create a TableRow per event
            .apply("Json to TableRow", ParDo.of(new DoFn<String, TableRow>() {
                @DoFn.ProcessElement
                public void processElement(ProcessContext c) throws Exception {
                    LOG.debug("PIPELINE STEP2: {}", c.element());
                    DocumentContext context = JsonPath.parse(c.element());

                    TableRow tr = new TableRow();
                    tr.set("order_number", context.read("$.after.fields.order_number", Integer.class));
                    tr.set("purchaser", context.read("$.after.fields.purchaser", Integer.class));
                    tr.set("quantity", context.read("$.after.fields.quantity", Integer.class));
                    tr.set("product_id", context.read("$.after.fields.product_id", Integer.class));
                    // Get date
                    int epochDay = context.read("$.after.fields.order_date", Integer.class);
                    long epochMillis = TimeUnit.DAYS.toMillis(epochDay);
                    Date order_date = new Date(epochMillis);
                    // Get time
                    long microsecs = context.read("$.after.fields.order_time_cst", Long.class);
                    long millis = TimeUnit.MILLISECONDS.convert(microsecs, TimeUnit.MICROSECONDS);
                    Date order_time = new Date(millis);
                    Calendar calendar_time = Calendar.getInstance();
                    calendar_time.setTime(order_time);
                    // To UTC
                    TimeZone timeZone = TimeZone.getTimeZone("America/Chicago");
                    Calendar calendar_date = Calendar.getInstance();
                    calendar_date.setTime(order_date);
                    calendar_date.add(Calendar.HOUR, calendar_time.get(Calendar.HOUR));
                    calendar_date.add(Calendar.MINUTE, calendar_time.get(Calendar.MINUTE));
                    calendar_date.add(Calendar.SECOND, calendar_time.get(Calendar.SECOND));
                    calendar_date.add(Calendar.HOUR, 6); // GMT-6
                    calendar_date.setTimeZone(timeZone);
                    SimpleDateFormat formatterWithTimeZone = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    formatterWithTimeZone.setTimeZone(TimeZone.getTimeZone("UTC"));

                    tr.set("order_datetime_utc", formatterWithTimeZone.format(calendar_date.getTime()));
                    tr.set("source", "chicago");

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
                    .withSchema(CdcPipelineOptions.TABLE_SCHEMA)
                    .to(options.getOutputTable()));

        // Execute the pipeline and return the result.
        return pipeline.run();
    }
}
