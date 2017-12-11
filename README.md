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

2. write a stream as GenericRecord(ex1-file-generic)
  
   read stream with GenericReader
   read stream with mypair
      

