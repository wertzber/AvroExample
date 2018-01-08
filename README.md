Avro Example
============
This is a basic example of how to use Avro in Java. It serializes and deserialize a pair
 of strings.


ex 4 - update fields
===================

1. using MyPairVer3.avsc we now have additional field: nameOfGirl (enum) 
  
  {
      "namesapce": "com.elad",
      "type": "record",
      "name": "com.elad.wpmcn.MyPair",
      "doc": "A pair of strings",
      "fields": [
          {"name": "left", "type": "string"},
          {"name": "right", "type": "string"},
          {"name": "isValid", "type": ["null", "string"],"default": null},
          {"name": "nameOfGirl", "type": {
                          "type": "enum",
                          "name": "GirlName",
                          "doc": "`YUVAL`: good name",
                          "symbols": ["YUVAL", "NOA", "BABY"]
                        }
          }
      ]
  }
  
  2. compile the project using the new scheme
  run all tests - all pass.
  (I've update the constructor to have additional "true" var)

  3.  when trying to use null as default for boolean you'll get runtime error (not discovered during compilation):
  
org.apache.avro.AvroTypeException: Non-null default value for null type: "null"

	at org.apache.avro.io.parsing.ResolvingGrammarGenerator.encode(ResolvingGrammarGenerator.java:413)
	at org.apache.avro.io.parsing.ResolvingGrammarGenerator.encode(ResolvingGrammarGenerator.java:365)
	at org.apache.avro.io.parsing.ResolvingGrammarGenerator.getBinary(ResolvingGrammarGenerator.java:307)
	at org.apache.avro.io.parsing.ResolvingGrammarGenerator.resolveRecords(ResolvingGrammarGenerator.java:285)
	at org.apache.avro.io.parsing.ResolvingGrammarGenerator.generate(ResolvingGrammarGenerator.java:118)
	at org.apache.avro.io.parsing.ResolvingGrammarGenerator.generate(ResolvingGrammarGenerator.java:50)
	at org.apache.avro.io.ResolvingDecoder.resolve(ResolvingDecoder.java:85)
	at org.apache.avro.io.ResolvingDecoder.<init>(ResolvingDecoder.java:49)
	at org.apache.avro.io.DecoderFactory.resolvingDecoder(DecoderFactory.java:307)
	at org.apache.avro.generic.GenericDatumReader.getResolver(GenericDatumReader.java:128)
	at org.apache.avro.generic.GenericDatumReader.read(GenericDatumReader.java:143)
	at org.apache.avro.file.DataFileStream.next(DataFileStream.java:233)
