package com.wizeline.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import java.util.Arrays;

/**
 * Options supported by DebeziumIO pipelines.
 */
public interface CdcPipelineOptions extends DataflowPipelineOptions {

    public static final TableSchema TABLE_SCHEMA = new TableSchema()
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

    @Description(
        "The JDBC connection Hostname string.")
    ValueProvider<String> getHostname();

    void setHostname(ValueProvider<String> hostname);

    @Description(
        "The JDBC connection Port string.")
    ValueProvider<String> getPort();

    void setPort(ValueProvider<String> port);

    @Description("JDBC connection user name. ")
    ValueProvider<String> getUsername();

    void setUsername(ValueProvider<String> username);

    @Description("JDBC connection password. ")
    ValueProvider<String> getPassword();

    void setPassword(ValueProvider<String> password);

    @Description("Output topic to write to")
    ValueProvider<String> getOutputTable();

    void setOutputTable(ValueProvider<String> value);

    @Validation.Required
    @Description("Temporary directory for BigQuery loading process")
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);
}