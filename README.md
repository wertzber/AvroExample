Avro Example
============
This is a basic example of how to use Avro in Java. 
It serializes and deserialize a pair object.

exercise 1
==========


1. create an avro scheme: MyPairVer1
it has two fields(type String)  

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

run maven clean and compile - in order to create an object MyPair.

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
