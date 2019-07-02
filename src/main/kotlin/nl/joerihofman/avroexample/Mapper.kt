package nl.joerihofman.avroexample

import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.IOException

object Mapper {

    private val personWriter = SpecificDatumWriter(Person::class.java)
    private val personReader = SpecificDatumReader(Person::class.java)

    private val logger = LoggerFactory.getLogger(Mapper::class.java)


    fun jsonToAvroByteArray(person: Person): ByteArray {
        var data = ByteArray(0)
        val stream = ByteArrayOutputStream()
        try {
            val jsonEncoder = EncoderFactory.get().jsonEncoder(Person.`SCHEMA$`, stream)
            personWriter.write(person, jsonEncoder)
            jsonEncoder.flush()
            data = stream.toByteArray()
        } catch (e: IOException) {
            logger.error("Unable to map Avro record to byte array", e)
        }
        return data
    }

    fun avroToJson(data: ByteArray): Person? {
        return try {
            val decoder = DecoderFactory.get().jsonDecoder(Person.`SCHEMA$`, String(data))
            personReader.read(null, decoder)
        } catch (e: IOException) {
            logger.error("Unable to map byte array to avro record", e)
            null
        }
    }
}