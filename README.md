Avro Example
============
This is a basic example of how to use Avro in Java. It serializes and deserialize a pair
 of strings.

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

compile the project.


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

ex2 - add new field
===================
1. using MyPairVer2.avsc we now have additional field: isValid

{
    "namesapce": "com.elad",
    "type": "record",
    "name": "com.elad.wpmcn.MyPair",
    "doc": "A pair of strings",
    "fields": [
        {"name": "left", "type": "string"},
        {"name": "right", "type": "string"},
        {"name": "isValid", "type": ["null", "string"],"default": null}
    ]
}

2. compile the project using the new scheme
run all tests - all pass.
(I've update the constructor to have additional "true" var)

3. run same tests - all should pass

when running v1 tests for read file:
 - also as generic try to read isValid - and you recv null.
   same behaviour will happen when calling variable which does not exists.
   
   Assert.assertNull("failed to get isValid null", result.get("isValid"));
   Assert.assertNull("failed to get isValid null", result.get("krkrkrkrk"));
