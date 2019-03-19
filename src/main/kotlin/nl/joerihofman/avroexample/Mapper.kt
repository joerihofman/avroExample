package nl.joerihofman.avroexample

import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.IOException

class Mapper {
    companion object {
        val writer = SpecificDatumWriter(AvroExampleRecord::class.java)
        val reader = SpecificDatumReader(AvroExampleRecord::class.java)
        val stream = ByteArrayOutputStream()
    }

    val logger = LoggerFactory.getLogger(Mapper::class.java)

    fun jsonToAvro(avroSchema: AvroExampleRecord): ByteArray {
        var data = ByteArray(0)
        try {
            val jsonEncoder = EncoderFactory.get().jsonEncoder(AvroExampleRecord.`SCHEMA$`, stream)
            writer.write(avroSchema, jsonEncoder)
            jsonEncoder.flush()
            data = stream.toByteArray()
        } catch (e: IOException) {
            logger.error("Unable to map", e)
        }
        return data
    }

    fun avroToJson(data: ByteArray): AvroExampleRecord? {
        try {
            val decoder = DecoderFactory.get().jsonDecoder(AvroExampleRecord.`SCHEMA$`, String(data))
            return reader.read(null, decoder)
        } catch (e: IOException) {
            logger.error("Unable to map", e)
            return null
        }
    }
}