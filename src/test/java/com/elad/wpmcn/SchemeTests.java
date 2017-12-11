package com.elad.wpmcn;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by eladw on 11/29/17.
 */
public class SchemeTests {

    /**
     * MyPair v1 has 2 fields: right and left
     * Write stream using generic record.
     * read stream using: a.generic record  b.specificReader
     * @throws IOException
     */
    @Test
    public void createStreamUsingGenericRecordV1ReadUsingGenericAndSpecific() throws IOException {

        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/schemes/MyPairVer1.avsc"));// Deserialize it.
        System.out.println("Use GenericRecord and GenericDatumWriter");
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", new String("left"));
        datum.put("right", new String("right"));
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        final ByteArrayOutputStream byteArrayStream = AvroUtils.serializeCreateByteStreamUsingDatumWriter(schema, datum, writer);

        System.out.println("write stream");

        System.out.println("Use GenericRecord and GenericDatumReader to deserialize");
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        GenericRecord genericRecord = AvroUtils.deserializeByteStreamUsingDatumReader(byteArrayStream, schema, reader);
        System.out.println("right:" + genericRecord.get("right") + ",left:" + genericRecord.get("left"));
        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) genericRecord.get("right").toString()));
        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) genericRecord.get("left").toString()));


        System.out.println("Use MyPair and SpecificDatumReader to deserialize");
        SpecificDatumReader<com.elad.wpmcn.MyPair> reader2 = new SpecificDatumReader<>(schema);
        com.elad.wpmcn.MyPair specificRecord = AvroUtils.deserializeByteStreamUsingDatumReader(byteArrayStream, schema, reader2);
        System.out.println("right:" + specificRecord.getRight() + ",left:" + specificRecord.getLeft());
        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) specificRecord.getRight().toString()));
        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) specificRecord.getLeft().toString()));

    }

    /**
     * MyPair v1 has 2 fields: right and left
     * Write stream using specific writer.
     * read stream using: a.generic record  b.specificReader
     * @throws IOException
     */
    @Test
    public void createStreamUsingSpecificRecordV1ReadUsingGenericAndSpecific() throws IOException {

        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/schemes/MyPairVer1.avsc"));// Deserialize it.
        System.out.println("Use Specific object and DatumWriter");
        com.elad.wpmcn.MyPair datum = new com.elad.wpmcn.MyPair("left", "right");
        SpecificDatumWriter<com.elad.wpmcn.MyPair> writer = new SpecificDatumWriter<>(schema);
        final ByteArrayOutputStream byteArrayStream = AvroUtils.serializeCreateByteStreamUsingDatumWriter(schema, datum, writer);

        System.out.println("write stream");

        System.out.println("Use GenericRecord and GenericDatumReader to deserialize");
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        GenericRecord genericRecord = AvroUtils.deserializeByteStreamUsingDatumReader(byteArrayStream, schema, reader);
        System.out.println("right:" + genericRecord.get("right") + ",left:" + genericRecord.get("left"));
        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) genericRecord.get("right").toString()));
        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) genericRecord.get("left").toString()));


        System.out.println("Use MyPair and SpecificDatumReader to deserialize");
        SpecificDatumReader<com.elad.wpmcn.MyPair> reader2 = new SpecificDatumReader<>(schema);
        com.elad.wpmcn.MyPair specificRecord = AvroUtils.deserializeByteStreamUsingDatumReader(byteArrayStream, schema, reader2);
        System.out.println("right:" + specificRecord.getRight() + ",left:" + specificRecord.getLeft());
        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) specificRecord.getRight().toString()));
        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) specificRecord.getLeft().toString()));

    }

//    /**
//     * Create a file using a genericRecord
//     * Read it using: a.genericRecord b.specific
//     * @throws IOException
//     */
//    @Test
//    public void createFileUsingGenericRecordV1ReadUsingGenericAndSpecific() throws IOException {
//
//        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/schemes/MyPairVer1.avsc"));// Deserialize it.
//        System.out.println("Use GenericRecord and GenericDatumWriter");
//        GenericRecord datum = new GenericData.Record(schema);
//        datum.put("left", new String("left"));
//        datum.put("right", new String("right"));
//        DatumWriter<GenericRecord> writer = new <GenericRecord>(schema);
//        final ByteArrayOutputStream byteArrayStream = AvroUtils.serializeCreateByteStreamUsingDatumWriter(schema, datum, writer);
//
//        System.out.println("write stream");
//
//        System.out.println("Use GenericRecord and GenericDatumReader to deserialize");
//        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
//        GenericRecord genericRecord = AvroUtils.deserializeByteStreamUsingDatumReader(byteArrayStream, schema, reader);
//        System.out.println("right:" + genericRecord.get("right") + ",left:" + genericRecord.get("left"));
//        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) genericRecord.get("right").toString()));
//        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) genericRecord.get("left").toString()));
//
//
//        System.out.println("Use MyPair and SpecificDatumReader to deserialize");
//        SpecificDatumReader<com.elad.wpmcn.MyPair> reader2 = new SpecificDatumReader<>(schema);
//        com.elad.wpmcn.MyPair specificRecord = AvroUtils.deserializeByteStreamUsingDatumReader(byteArrayStream, schema, reader2);
//        System.out.println("right:" + specificRecord.getRight() + ",left:" + specificRecord.getLeft());
//        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) specificRecord.getRight().toString()));
//        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) specificRecord.getLeft().toString()));
//
//    }

}