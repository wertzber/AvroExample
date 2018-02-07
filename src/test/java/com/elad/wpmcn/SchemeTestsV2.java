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

import com.elad.wpmcn.MyPair;

import static com.elad.wpmcn.MyPair.SCHEMA$;

/**
 * Created by eladw on 11/29/17.
 */
public class SchemeTestsV2 {


    /**
     * Create a file using a genericRecord
     * @throws IOException
     */
    //@Test
    public void createFileV2() throws IOException {

        System.out.println("write to file");
        MyPair myPair = MyPair.newBuilder() //Good example
                .setLeft("left")
                .setRight("right")
                .setIsValid("true")
                .build();
        AvroUtils.createFile(myPair, "/Users/eladw/git-dp/AvroExample/src/main/resources/output/ex2", "MyPairOutput-V2.bin");
        System.out.println("write stream");
    }


    @Test
    public void readFileUsingSpecificDatumReader() throws IOException {
        System.out.println("Use GenericRecord and GenericDatumReader to deserialize");

        Schema oldCchema = new Schema.Parser().parse(AvroUtils.class.getResourceAsStream("/schemes/MyPairVer2.avsc"));// Deserialize it.
        System.out.println("Old scheme: " + oldCchema);
        FileReader<MyPair> reader = DataFileReader.openReader(new File("/Users/eladw/git-dp/AvroExample/src/main/resources/output/ex2/MyPairOutput-V2.bin"),
                new SpecificDatumReader<>(oldCchema, SCHEMA$));

        MyPair result = reader.next();
        System.out.printf("Read MyPair from file: \n Left: %s, Right: %s\n, scheme: %s\n", result.left, result.right, result.getSchema());

        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) result.getRight().toString()));
        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) result.getLeft().toString()));
        Assert.assertTrue("failed to get isValid ", "true".equals(result.getIsValid().toString()));

    }

    @Test
    public void readFileUsingGenericDatumReader() throws IOException {
        System.out.println("Use GenericRecord and GenericDatumReader to deserialize");

        Schema oldCchema = new Schema.Parser().parse(AvroUtils.class.getResourceAsStream("/schemes/MyPairVer2.avsc"));// Deserialize it.
        System.out.println("Old scheme: " + oldCchema);
        FileReader<GenericRecord> reader = DataFileReader.openReader(new File("/Users/eladw/git-dp/AvroExample/src/main/resources/output/ex2/MyPairOutput-V2.bin"),
                new GenericDatumReader<>(oldCchema, SCHEMA$));

        GenericRecord result = reader.next();
        System.out.printf("Read MyPair from file: \n Left: %s, Right: %s\n, scheme: %s\n", result.get("left"), result.get("right"), result.getSchema());

        Assert.assertTrue("failed to get right", "right".equalsIgnoreCase((String) result.get("right").toString()));
        Assert.assertTrue("failed to get left", "left".equalsIgnoreCase((String) result.get("left").toString()));
        Assert.assertTrue("failed to get isValid", "true".equals(result.get("isValid").toString()));
        Assert.assertNull("failed to get isValid null", result.get("krkrkrkrk"));

    }


}