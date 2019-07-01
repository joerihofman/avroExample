package nl.joerihofman.avroexample

import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("main")

fun main() {
    val exampleRecord = AvroExampleRecord()
    exampleRecord.setIntField(22)
    exampleRecord.setStringField("abc")
    exampleRecord.setStringFieldTwo("def")

    val avro = Mapper().jsonToAvro(exampleRecord)
    logger.info("AVRO : ${String(avro)}")

    val json = Mapper().avroToJson(avro)
    logger.info("JSON : $json")
}
