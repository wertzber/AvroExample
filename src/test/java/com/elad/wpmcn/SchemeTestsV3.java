package com.elad.wpmcn;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import static com.elad.wpmcn.MyPair.SCHEMA$;

/**
 * Created by eladw on 11/29/17.
 */
public class SchemeTestsV3 {


    /**
     * Create a file using a genericRecord
     * @throws IOException
     */
    @Test
    public void createFileV3() throws IOException {

        System.out.println("write to file");
        com.elad.wpmcn.MyPair myPair = new com.elad.wpmcn.MyPair("left","right","true", com.elad.wpmcn.GirlName.YUVAL);
        AvroUtils.createFile(myPair, "/Users/eladw/git-dp/AvroExample/src/main/resources/output/ex3", "MyPairOutput-V3.bin");
        System.out.println("write stream");
    }


    /**
     * MyPair v1 has 2 fields: right and left
     * Write stream using generic record.
     * read stream using: a.generic record  b.specificReader
     * @throws IOException
     */
    @Test
    public void createStreamUsingGenericRecordReadUsingGenericAndSpecific() throws IOException {

        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/schemes/MyPairVer3.avsc"));// Deserialize it.
        System.out.println("Use GenericRecord and GenericDatumWriter");
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", new String("left"));
        datum.put("right", new String("right"));
        datum.put("isValid", new String("true"));
        datum.put("nameOfGirl", com.elad.wpmcn.GirlName.YUVAL);

        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        final ByteArrayOutputStream byteArrayStream = AvroUtils.serializeCreateByteStreamUsingDatumWriter(datum, writer);

        System.out.println("write stream");

        System.out.println("Use GenericRecord and GenericDatumReader to deserialize");
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        GenericRecord genericRecord = AvroUtils.deserializeByteStreamUsingDatumReader(byteArrayStream, reader);
        System.out.println("right:" + genericRecord.get("right") + ",left:" + genericRecord.get("left"));
        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) genericRecord.get("right").toString()));
        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) genericRecord.get("left").toString()));
        Assert.assertTrue("failed to get isValid",  "true".equalsIgnoreCase(genericRecord.get("isValid").toString()));
        Assert.assertTrue("failed to get nameOfGirl",  "YUVAL".equalsIgnoreCase(genericRecord.get("nameOfGirl").toString()));


        System.out.println("Use MyPair and SpecificDatumReader to deserialize");
        SpecificDatumReader<com.elad.wpmcn.MyPair> reader3 = new SpecificDatumReader<>(schema);
        com.elad.wpmcn.MyPair specificRecord = AvroUtils.deserializeByteStreamUsingDatumReader(byteArrayStream, reader3);
        System.out.println("right:" + specificRecord.getRight() + ",left:" + specificRecord.getLeft());
        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) specificRecord.getRight().toString()));
        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) specificRecord.getLeft().toString()));
        Assert.assertTrue("failed to get isValid ", "true".equalsIgnoreCase(specificRecord.getIsValid().toString()));
        Assert.assertTrue("failed to get nameOfGirl",  "YUVAL".equalsIgnoreCase(specificRecord.getNameOfGirl().toString()));

    }

    /**
     * MyPair v1 has 2 fields: right and left
     * Write stream using specific writer.
     * read stream using: a.generic record  b.specificReader
     * @throws IOException
     */
    @Test
    public void createStreamUsingSpecificRecordReadUsingGenericAndSpecific() throws IOException {

        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/schemes/MyPairVer3.avsc"));// Deserialize it.
        System.out.println("Use Specific object and DatumWriter");
        com.elad.wpmcn.MyPair datum = new com.elad.wpmcn.MyPair("left", "right", "true", com.elad.wpmcn.GirlName.YUVAL);
        SpecificDatumWriter<com.elad.wpmcn.MyPair> writer = new SpecificDatumWriter<>(schema);
        final ByteArrayOutputStream byteArrayStream = AvroUtils.serializeCreateByteStreamUsingDatumWriter(datum, writer);

        System.out.println("write stream");

        System.out.println("Use GenericRecord and GenericDatumReader to deserialize");
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        GenericRecord genericRecord = AvroUtils.deserializeByteStreamUsingDatumReader(byteArrayStream, reader);
        System.out.println("right:" + genericRecord.get("right") + ",left:" + genericRecord.get("left"));
        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) genericRecord.get("right").toString()));
        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) genericRecord.get("left").toString()));
        Assert.assertTrue("failed to get isValid",  "true".equalsIgnoreCase(genericRecord.get("isValid").toString()));
        Assert.assertTrue("failed to get nameOfGirl",  "YUVAL".equalsIgnoreCase(genericRecord.get("nameOfGirl").toString()));


        System.out.println("Use MyPair and SpecificDatumReader to deserialize");
        SpecificDatumReader<com.elad.wpmcn.MyPair> reader3 = new SpecificDatumReader<>(schema);
        com.elad.wpmcn.MyPair specificRecord = AvroUtils.deserializeByteStreamUsingDatumReader(byteArrayStream, reader3);
        System.out.println("right:" + specificRecord.getRight() + ",left:" + specificRecord.getLeft());
        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) specificRecord.getRight().toString()));
        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) specificRecord.getLeft().toString()));
        Assert.assertTrue("failed to get isValid ", "true".equalsIgnoreCase(specificRecord.getIsValid().toString()));
        Assert.assertTrue("failed to get nameOfGirl",  "YUVAL".equalsIgnoreCase(genericRecord.get("nameOfGirl").toString()));

    }




}