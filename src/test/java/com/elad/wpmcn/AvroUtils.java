package com.elad.wpmcn;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
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
     * @param datum
     * @param writer

     * @throws IOException
     */
    public  static <T> ByteArrayOutputStream serializeCreateByteStreamUsingDatumWriter(T datum, DatumWriter<T> writer) throws IOException {
        // Serialize it.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try{
            writer.write((T) datum, encoder);
            encoder.flush();
            System.out.println("input: " + datum + ", after Serialization: " + out);
            return out;
        } catch (Exception e){
            System.out.println("Error serialize " + e);
            e.printStackTrace();
            out.close();
            return  null;
        }
    }

    public static <R> R deserializeByteStreamUsingDatumReader(ByteArrayOutputStream out, DatumReader<R> reader) throws IOException {

        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        try{
            return reader.read(null, decoder);
        } catch (Exception e){
            System.out.println("Error deserialize " + e);
            e.printStackTrace();
            out.close();
            return  null;
        }
    }

    /**
     * Create a bin file
     * @param datum
     * @param directory
     * @param fileName
     * @throws IOException
     */
    public static void createFile(com.elad.wpmcn.MyPair datum,String directory, String fileName) throws IOException {
        Path path = Paths.get(directory, fileName) ;
        Path tmpFile = null;
        if(!Files.isReadable(path)){
            tmpFile = Files.createFile(path);
            System.out.println("Create a file " + path.getFileName());
        } else {
            tmpFile = path;
        }
        // Serialize it.
        DataFileWriter<com.elad.wpmcn.MyPair> writer = new DataFileWriter<com.elad.wpmcn.MyPair>
                (new SpecificDatumWriter<com.elad.wpmcn.MyPair>(com.elad.wpmcn.MyPair.class));
        writer.create(SCHEMA$, tmpFile.toFile()); //use the built in scheme
        writer.append(datum);
        writer.close();
        System.out.println("Serialization: " + tmpFile);
    }



//    public static com.elad.wpmcn.MyPair readBinAvroFileGenericReader(File refFileToRead, String schemeFile) throws IOException {
//        FileReader<com.elad.wpmcn.MyPair> reader = null;
//        try{
//            Schema oldCchema = new Schema.Parser().parse(AvroUtils.class.getResourceAsStream(schemeFile));// Deserialize it.
//            System.out.println("Old scheme: "+ oldCchema);
//            reader = DataFileReader.openReader(refFileToRead, new GenericDatumReader<>(oldCchema, SCHEMA$));
//            com.elad.wpmcn.MyPair result = reader.next();
//            System.out.printf("Left: %s, Right: %s\n, scheme: %s\n", result.left, result.right, result.getSchema());
//            return  result;
//
//        } finally {
//            if(reader!=null) reader.close();
//        }
//
//    }




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
