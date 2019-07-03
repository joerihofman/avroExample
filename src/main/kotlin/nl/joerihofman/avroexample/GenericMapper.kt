package nl.joerihofman.avroexample

import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificRecordBase
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.IOException
import java.nio.charset.Charset

object GenericMapper {

    private val genericWriter = GenericDatumWriter<Any>()
    private val fileWriter = DataFileWriter(genericWriter)
    private val genericReader = GenericDatumReader<Any>()

    private val logger = LoggerFactory.getLogger(GenericMapper::class.java)

    fun recordToByteArray(record: SpecificRecordBase): ByteArray {
        val stream = ByteArrayOutputStream()

        record.schema.logicalType

        return try {
            val jsonEncoder = EncoderFactory.get().binaryEncoder(stream, null)
            genericWriter.write(record, jsonEncoder)
            jsonEncoder.flush()

            stream.toByteArray()
        } catch (e: IOException) {
            logger.error("Unable to map Avro record to byte array", e)
            ByteArray(0)
        } catch (e: NullPointerException) {
            logger.error("Unable to map Avro record to byte array", e)
            ByteArray(0)
        }
    }

    fun byteArrayToRecord(data: ByteArray): Any? {
        return try {
            val decoder = DecoderFactory.get().binaryDecoder(data, null)

            genericReader.read(null, decoder)
        } catch (e: IOException) {
            logger.error("Unable to map byte array to Avro record", e)

            null
        }
    }

    fun recordToJson(record: SpecificRecordBase): String {
        val stream = ByteArrayOutputStream()

        try {
            val jsonEncoder = EncoderFactory.get().jsonEncoder(record.schema, stream)
            genericWriter.write(record, jsonEncoder)
            jsonEncoder.flush()
        } catch (e: IOException) {
            logger.error("Unable to map Avro record to json", e)
        }

        return stream.toString(Charset.defaultCharset())
    }

    fun jsonToRecord(data: String): Any? {
        return try {
            val decoder = DecoderFactory.get().jsonDecoder(null, data)

            genericReader.read(null, decoder)
        } catch (e: IOException) {
            logger.error("Unable to map json to Avro record", e)

            null
        }
    }

    fun recordToFile(person: Person): File? {
        return try {
            val outputFile = File.createTempFile("personexample", ".avro")

            fileWriter.create(Person.`SCHEMA$`, outputFile)
            fileWriter.close()

            outputFile
        } catch (e: IOException) {
            logger.error("Unable to map Avro record to file", e)

            null
        }
    }

    fun fileToRecord(file: File): Any? {
        return try {
            var person: Any? = null

            val fileReader = DataFileReader(file, genericReader)

            while (fileReader.hasNext()) {
                person = fileReader.next()
            }

            person
        } catch (e: IOException) {
            logger.error("Unable to map file to Avro record", e)

            null
        }
    }

    fun recordToFileTest(person: Person)  {
        try {
            val outputFile = File.createTempFile("personexample", ".avro")

            fileWriter.create(person.schema, outputFile)
            fileWriter.close()

            println("\nOUTPUTFILE ${outputFile.absolutePath}\n")

            val fileReader = DataFileReader(outputFile, genericReader)
            fileReader.sync(fileReader.previousSync())

            println("HAS NEXT ${fileReader.hasNext()}")

            while (fileReader.hasNext()) {
                println(fileReader.next())
            }

        } catch (e: IOException) {
            logger.error("Unable to map Avro record to file", e)
        }
    }
}