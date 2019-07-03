package nl.joerihofman.avroexample

import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.IOException
import java.nio.charset.Charset

object SpecificMapper {

    private val personWriter = SpecificDatumWriter(Person::class.java)
    private val fileWriter = DataFileWriter(personWriter)
    private val personReader = SpecificDatumReader(Person::class.java)

    private val logger = LoggerFactory.getLogger(SpecificMapper::class.java)

    fun personToByteArray(person: Person): ByteArray {
        val stream = ByteArrayOutputStream()

        return try {
            val jsonEncoder = EncoderFactory.get().binaryEncoder(stream, null)
            personWriter.write(person, jsonEncoder)
            jsonEncoder.flush()

            stream.toByteArray()
        } catch (e: IOException) {
            logger.error("Unable to map Avro record to byte array", e)

            ByteArray(0)
        }
    }

    fun byteArrayToPerson(data: ByteArray): Person? {
        return try {
            val decoder = DecoderFactory.get().binaryDecoder(data, null)

            personReader.read(null, decoder)
        } catch (e: IOException) {
            logger.error("Unable to map byte array to Avro record", e)

            null
        }
    }

    fun personToJson(person: Person): String {
        val stream = ByteArrayOutputStream()

        try {
            val jsonEncoder = EncoderFactory.get().jsonEncoder(Person.`SCHEMA$`, stream)
            personWriter.write(person, jsonEncoder)
            jsonEncoder.flush()
        } catch (e: IOException) {
            logger.error("Unable to map Avro record to json", e)
        }

        return stream.toString(Charset.defaultCharset())
    }

    fun jsonToPerson(data: String): Person? {
        return try {
            val decoder = DecoderFactory.get().jsonDecoder(Person.`SCHEMA$`, data)

            personReader.read(null, decoder)
        } catch (e: IOException) {
            logger.error("Unable to map json to Avro record", e)

            null
        }
    }

    fun personToFile(person: Person): File? {
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

    fun fileToPerson(file: File): Person? {
        return try {
            var person: Person? = null

            val fileReader = DataFileReader(file, personReader)

            while (fileReader.hasNext()) {
                person = fileReader.next()
            }

            person
        } catch (e: IOException) {
            logger.error("Unable to map file to Avro record", e)

            null
        }
    }

    fun personToFileTest(person: Person)  {
        try {
            val outputFile = File.createTempFile("personexample", ".avro")

            fileWriter.create(person.schema, outputFile)
            fileWriter.close()

            println("\nOUTPUTFILE ${outputFile.absolutePath}\n")

            val fileReader = DataFileReader(outputFile, personReader)
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