package org.example;

import com.google.api.Logging;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hcatalog.HCatToRow;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);
        Map<String, String> configProperties = new HashMap<String, String>();
        configProperties.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        configProperties.put("hive.metastore.uris","thrift://172.30.192.27:9083");
        System.out.println(configProperties);
        pipeline
                .apply(HCatalogIO.read()
                                .withConfigProperties(configProperties)
                                .withDatabase("default")
                                .withTable("transactions"));
//                 .apply("print", ParDo.of(new DoFn<HCatRecord, String>() {
//                     @ProcessElement
//                     public void processElement(@Element HCatRecord record, OutputReceiver<String> out) {
//                         System.out.println("Printing data");
//                         out.output(record.toString());
//                     }
//                 }));


        System.out.println("Completed read");

//                .apply("ConvertHCatRecordToString", ParDo.of(new DoFn<HCatRecord, String>() {
//                    @ProcessElement
//                    public void processElement(ProcessContext c) {
//                        c.output(c.element().toString());
//                    }
//                }))
//                // Print the data to the console
//                .apply(ParDo.of(new DoFn<String, Void>() {
//                    @ProcessElement
//                    public void processElement(ProcessContext c) {
//                        System.out.println(c.element());
//                    }
//                }));
//        // Create a list of strings and convert it to a PCollection
//        PCollection<String> inputCollection = pipeline.apply("create data", Create.of(
//                "Element 1",
//                "Element 2",
//                "Element 3"));
//
//        // Apply a ParDo transform to print the elements
//        inputCollection.apply("Print Data", ParDo.of(new PrintDataFn()));

        // Run the pipeline
        pipeline.run();
    }

    public static class PrintDataFn extends DoFn<String, Void> {
        @ProcessElement
        public void processElement(@Element String data, OutputReceiver<Void> out) {
            System.out.println("Data: " + data);
        }
    }
}