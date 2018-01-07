Avro Example
============
This is a basic example of how to use Avro in Java. It serializes and deserialize a pair
 of strings.


ex 3 - enum
===========

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

  3.  