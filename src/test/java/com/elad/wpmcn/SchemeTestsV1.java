package com.elad.wpmcn;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static com.elad.wpmcn.MyPair.SCHEMA$;

/**
 * Created by eladw on 11/29/17.
 */
public class SchemeTestsV1 {

    @Test
    public void readFileUsingSpecificDatumReaderV1() throws IOException {
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
    public void readFileUsingGenericDatumReaderV1() throws IOException {
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