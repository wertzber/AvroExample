Avro Example
============
This is a basic example of how to use Avro in Java. It serializes and deserializes a pair of strings.


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
  


4. replace the reader SCHEMA$ with oldCchema - and test will pass.
but in real case in production, we'll need to use the current SCHEMA$ when building an  object
