package com.example;

import com.google.api.client.util.Preconditions;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1beta1.AvroProto;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto.TableReference;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;

public class Main {
    private static class SimpleRowReader {

        private final DatumReader<GenericRecord> datumReader;

        // Decoder object will be reused to avoid re-allocation and too much garbage collection.
        private BinaryDecoder decoder = null;

        // GenericRecord object will be reused.
        private GenericRecord row = null;

        public SimpleRowReader(Schema schema) {
            Preconditions.checkNotNull(schema);
            datumReader = new GenericDatumReader<>(schema);
        }

        /**
         * Sample method for processing AVRO rows which only validates decoding.
         *
         * @param avroRows object returned from the ReadRowsResponse.
         */
        public void processRows(AvroProto.AvroRows avroRows) throws IOException {
            decoder = DecoderFactory.get()
                    .binaryDecoder(avroRows.getSerializedBinaryRows().toByteArray(), decoder);

            while (!decoder.isEnd()) {
                // Reusing object row
                row = datumReader.read(row, decoder);
                System.out.println(row.toString());
            }
        }
    }

    public static void main(String[] args) throws IOException {
        try (BigQueryStorageClient client = BigQueryStorageClient.create()) {
            String projectId = args[0];
            String datasetId = args[1];
            String tableId = args[2];
            TableReference ref = TableReference
                    .newBuilder()
                    .setProjectId(projectId)
                    .setDatasetId(datasetId)
                    .setTableId(tableId)
                    .build();
            String parent = "projects/" + projectId;
            Storage.ReadSession session = client.createReadSession(ref, parent, 0);
            SimpleRowReader reader = new SimpleRowReader(
                    new Schema.Parser().parse(session.getAvroSchema().getSchema()));
            Storage.StreamPosition pos = Storage.StreamPosition
                    .newBuilder().setStream(session.getStreams(0)).build();
            Storage.ReadRowsRequest req = Storage.ReadRowsRequest
                    .newBuilder().setReadPosition(pos).build();
            ServerStream<Storage.ReadRowsResponse> stream = client.readRowsCallable().call(req);
            for (Storage.ReadRowsResponse res : stream) {
                reader.processRows(res.getAvroRows());
            }
        }
    }
}
