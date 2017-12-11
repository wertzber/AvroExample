package com.elad.wpmcn;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.elad.wpmcn.MyPair.SCHEMA$;

/**
 * Created by eladw on 11/29/17.
 */
public class AvroUtils {

    /**
     * Serialize and create stream using genericDatumReader
     * @param schema
     * @param datum
     * @throws IOException
     */
    public  static <T> ByteArrayOutputStream serializeCreateByteStreamUsingDatumWriter(Schema schema,
                                    T datum, DatumWriter<T> writer) throws IOException {
        // Serialize it.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try{
            writer.write((T) datum, encoder);
            encoder.flush();
            System.out.println("input: " + datum + ", after Serialization: " + out);
            return out;
        } catch (Exception e){
            out.close();
            return  null;
        }
    }

    public static <R> R deserializeByteStreamUsingDatumReader(ByteArrayOutputStream out, Schema schema, DatumReader<R> reader) throws IOException {

        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        try{
            return reader.read(null, decoder);

        } catch (Exception e){
            out.close();
            return  null;
        }
    }

    public static void createFile(){

    }



//
//
//    public static void serializeToFileUsingSpecificDatumWriter(com.elad.wpmcn.MyPair datum, String fileName) throws IOException {
//        // Create a datum to serialize.
//        //datum.left = new Utf8("dog");
//        //datum.right = new Utf8("cat");
//        //datum.nameOfGirl = com.elad.wpmcn.GirlName.YUVAL;
//        Path path = Paths.get("/Users/eladw/git-dp/AvroExample/src/main/resources/files", fileName);
//        Path tmpFile = Files.createFile(path);
//        // Serialize it.
//        DataFileWriter<com.elad.wpmcn.MyPair> writer = new DataFileWriter<com.elad.wpmcn.MyPair>(new SpecificDatumWriter<com.elad.wpmcn.MyPair>(com.elad.wpmcn.MyPair.class));
//        writer.create(SCHEMA$, tmpFile.toFile());
//        writer.append(datum);
//        writer.close();
//        System.out.println("Serialization: " + tmpFile);
//    }






}
