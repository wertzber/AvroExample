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

import com.elad.wpmcn.MyPair;
import static com.elad.wpmcn.MyPair.SCHEMA$;
import static java.io.FileDescriptor.out;

/**
 * A simple Avro example that serializes and deserialize a pair of strings.
 *
 * @author <a href="bmcneill@intelius.com">W.P. McNeill</a>
 */
public class AvroExample {

//   private void readBinAvroFileSpecific(File refFileToRead, String schemeFile) throws IOException {
//
//      Schema oldCchema = new Schema.Parser().parse(getClass().getResourceAsStream(schemeFile));// Deserialize it.
//      System.out.println("Old scheme: "+ oldCchema);
//      FileReader<MyPair> reader = DataFileReader.openReader(refFileToRead, new SpecificDatumReader<>(oldCchema, SCHEMA$ ));
//      while (reader.hasNext()) {
//         MyPair result = reader.next();
//         System.out.printf("girl: %s, Left: %s, Right: %s, isValid: %s\n, scheme: %s\n", result.nameOfGirl.toString(), result.left, result.right,
//                 result.getIsValid(), result.getSchema());
//      }
//      reader.close();
//   }
//
//
//   private void readBinAvroFileGeneric(File refFileToRead, String schemeFile) throws IOException {
//
//      Schema oldCchema = new Schema.Parser().parse(new File(schemeFile));// Deserialize it.
//
//      FileReader<GenericRecord> reader = DataFileReader.openReader(refFileToRead, new GenericDatumReader<>(SCHEMA$, oldCchema ));
//      while (reader.hasNext()) {
//         final GenericRecord result = reader.next();
//         System.out.printf("Left: %s, Right: %s, stam: %s, isValid: %s\n, scheme: %s\n", result.get("left"), result.get("right"), result.get("gg"),
//                 result.get("isValid"), result.getSchema());
//      }
//      reader.close();
//   }
//
//
//   static public void main(String[] args) throws IOException {
//      AvroExample example = new AvroExample();
//      example.readBinAvroFileSpecific(new File("/Users/eladw/git-dp/AvroExample/src/main/resources/files/myPairAvroExample-enum2.avro"),
//             "/schemes/MyPairVer3.avsc");
////     example.readBinAvroFileGeneric(new File("/Users/eladw/git-dp/AvroExample/src/main/resources/files/myPairAvroExample-2vars.avro"),
////              "/Users/eladw/git-dp/AvroExample/src/main/resources/schemes/MyPairVer3.avsc");
////      System.out.println("Generic");
////     example.serializeGeneric();
////      System.out.println("Specific");
//
////create a file
////      example.serializeSpecific();
//   }
}
