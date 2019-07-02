package nl.joerihofman.avroexample

import org.slf4j.LoggerFactory
import java.time.LocalDate

private val logger = LoggerFactory.getLogger("main")

fun main() {
    val person = Person.newBuilder()
        .setFirstName("Jon")
        .setLastName("Doe")
        .setAddress("Streetname 2, 1000 AA, Amsterdam")
        .setDateOfBirth(LocalDate.now())
        .setGender(Gender.UNSPECIFIED)
        .setJob(
            Job.newBuilder()
                .setCompany("Acme")
                .setTitle("Software Engineer")
                .setSalary(3500)
                .build()
        )
        .build()

    val avro = Mapper.jsonToAvro(person)
    logger.info("AVRO : ${String(avro)}")

    val json = Mapper.avroToJson(avro)
    logger.info("JSON : $json")
}
