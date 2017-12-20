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
public class SchemeTestsV1 {

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
        final ByteArrayOutputStream byteArrayStream = AvroUtils.serializeCreateByteStreamUsingDatumWriter(datum, writer);

        System.out.println("write stream");

        System.out.println("Use GenericRecord and GenericDatumReader to deserialize");
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        GenericRecord genericRecord = AvroUtils.deserializeByteStreamUsingDatumReader(byteArrayStream, reader);
        System.out.println("right:" + genericRecord.get("right") + ",left:" + genericRecord.get("left"));
        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) genericRecord.get("right").toString()));
        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) genericRecord.get("left").toString()));
        Assert.assertNull("failed to get isValid",  genericRecord.get("isValid"));


        System.out.println("Use MyPair and SpecificDatumReader to deserialize");
        SpecificDatumReader<com.elad.wpmcn.MyPair> reader2 = new SpecificDatumReader<>(schema);
        com.elad.wpmcn.MyPair specificRecord = AvroUtils.deserializeByteStreamUsingDatumReader(byteArrayStream, reader2);
        System.out.println("right:" + specificRecord.getRight() + ",left:" + specificRecord.getLeft());
        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) specificRecord.getRight().toString()));
        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) specificRecord.getLeft().toString()));
        Assert.assertNull("failed to get isValid null", specificRecord.getIsValid());

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
        com.elad.wpmcn.MyPair datum = new com.elad.wpmcn.MyPair("left", "right", "true");
        SpecificDatumWriter<com.elad.wpmcn.MyPair> writer = new SpecificDatumWriter<>(schema);
        final ByteArrayOutputStream byteArrayStream = AvroUtils.serializeCreateByteStreamUsingDatumWriter(datum, writer);

        System.out.println("write stream");

        System.out.println("Use GenericRecord and GenericDatumReader to deserialize");
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        GenericRecord genericRecord = AvroUtils.deserializeByteStreamUsingDatumReader(byteArrayStream, reader);
        System.out.println("right:" + genericRecord.get("right") + ",left:" + genericRecord.get("left"));
        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) genericRecord.get("right").toString()));
        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) genericRecord.get("left").toString()));
        Assert.assertNull("failed to get isValid",  genericRecord.get("isValid"));


        System.out.println("Use MyPair and SpecificDatumReader to deserialize");
        SpecificDatumReader<com.elad.wpmcn.MyPair> reader2 = new SpecificDatumReader<>(schema);
        com.elad.wpmcn.MyPair specificRecord = AvroUtils.deserializeByteStreamUsingDatumReader(byteArrayStream, reader2);
        System.out.println("right:" + specificRecord.getRight() + ",left:" + specificRecord.getLeft());
        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) specificRecord.getRight().toString()));
        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) specificRecord.getLeft().toString()));
        Assert.assertNull("failed to get isValid null", specificRecord.getIsValid());

    }

    /**
     * Create a file using a genericRecord
     * @throws IOException
     */
    @Test
    public void createFileV2() throws IOException {

        System.out.println("write to file");
        com.elad.wpmcn.MyPair myPair = new com.elad.wpmcn.MyPair("left","right","true");
        AvroUtils.createFile(myPair, "/Users/eladw/git-dp/AvroExample/src/main/resources/output/ex2", "MyPairOutput-V2.bin");
        System.out.println("write stream");
    }


    @Test
    public void readFileUsingSpecificDatumReader() throws IOException {
        System.out.println("Use GenericRecord and GenericDatumReader to deserialize");

        Schema oldCchema = new Schema.Parser().parse(AvroUtils.class.getResourceAsStream("/schemes/MyPairVer1.avsc"));// Deserialize it.
        System.out.println("Old scheme: " + oldCchema);
        FileReader<com.elad.wpmcn.MyPair> reader = DataFileReader.openReader(new File("/Users/eladw/git-dp/AvroExample/src/main/resources/output/ex1/MyPairOutput-V1.bin"),
                new SpecificDatumReader<>(oldCchema, SCHEMA$));

        com.elad.wpmcn.MyPair result = reader.next();
        System.out.printf("Read MyPair from file: \n Left: %s, Right: %s\n, scheme: %s\n", result.left, result.right, result.getSchema());

        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) result.getRight().toString()));
        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) result.getLeft().toString()));
        Assert.assertNull("failed to get isValid null", result.getIsValid());

    }

    @Test
    public void readFileUsingGenericDatumReader() throws IOException {
        System.out.println("Use GenericRecord and GenericDatumReader to deserialize");

        Schema oldCchema = new Schema.Parser().parse(AvroUtils.class.getResourceAsStream("/schemes/MyPairVer1.avsc"));// Deserialize it.
        System.out.println("Old scheme: " + oldCchema);
        FileReader<GenericRecord> reader = DataFileReader.openReader(new File("/Users/eladw/git-dp/AvroExample/src/main/resources/output/ex1/MyPairOutput-V1.bin"),
                new GenericDatumReader<>(oldCchema, SCHEMA$));

        GenericRecord result = reader.next();
        System.out.printf("Read MyPair from file: \n Left: %s, Right: %s\n, scheme: %s\n", result.get("left"), result.get("right"), result.getSchema());

        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) result.get("right").toString()));
        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) result.get("left").toString()));
        Assert.assertNull("failed to get isValid null", result.get("isValid"));
        Assert.assertNull("failed to get isValid null", result.get("krkrkrkrk"));

    }


}