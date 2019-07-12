# Avro Example

Trying to get Apache Avro to work with Kotlin for a personal project. If this is working at some point, you could also use it as a refrence :) .

At this point, this project is not what I intend it to be. 

**The file writers and readers are not working.**

## How to use this example

In order to generate the Java classes from the Avro files, run the following command:

```cmd
mvn spotless:schema
```

The classes will be generated in the folder:

```
.../target/generated-sources/avro/nl/joerihofman/avroexample
```
