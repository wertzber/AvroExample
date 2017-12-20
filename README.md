Avro Example
============
This is a basic example of how to use Avro in Java. It serializes and deserializes a pair of strings.

exercise 1
==========
1. create an avro scheme: MyPairVer1
   change 

{
    "namesapce": "com.elad",
    "type": "record",
    "name": "com.elad.wpmcn.MyPair",
    "doc": "A pair of strings",
    "fields": [
        {"name": "left", "type": "string"},
        {"name": "right", "type": "string"}
    ]
}

2. write a stream as GenericRecord
  
   read stream with GenericReader
   read stream with specificDatumReader
      
3. write a stream as SpecificDatumWriter
     
   read stream with GenericReader
   read stream with specificDatumReader

you can see the output is the same.

4. write a bin file for later test: MyPairOutput-V1.bin

5. read the file using: SpecificDatumReader

note 1:
when reading the bin file:
        "right".equalsIgnoreCase((String) myPair.getRight())); - this cause an exception:

java.lang.ClassCastException: org.apache.avro.util.Utf8 cannot be cast to java.lang.String

In order to solve it you should use the toString(), meaning myPair.getRight().toString()
same issue also happen when using a generic record and result.get("left").

Ex2 bad 
======
1. clean and compile
2. now adding a new variable to scheme - without any default.
{
    "namesapce": "com.elad",
    "type": "record",
    "name": "com.elad.wpmcn.MyPair",
    "doc": "A pair of strings",
    "fields": [
        {"name": "left", "type": "string"},
        {"name": "right", "type": "string"},
        {"name": "isValid", "type": "string"}
    ]
}

3. when reading file v1 we have issues...
issue happen with generic and also in specific. 

 FileReader<GenericRecord> reader = DataFileReader.openReader(new File("/Users/eladw/git-dp/AvroExample/src/main/resources/output/ex1/MyPairOutput-V1.bin"),
                new GenericDatumReader<>(oldCchema, SCHEMA$));
                
  error: Found com.elad.wpmcn.MyPair, expecting com.elad.wpmcn.MyPair
  


4. replace the reader SCHEMA$ with oldCchema - and test will pass.\
but in real case in production, we'll need to use the current SCHEMA$ when building an  object
