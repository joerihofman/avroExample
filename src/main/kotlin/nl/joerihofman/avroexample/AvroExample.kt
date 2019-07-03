package nl.joerihofman.avroexample

import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.util.*

private val logger = LoggerFactory.getLogger("main")

fun main() {
    val completePerson = getCompletePerson()
    val incompletePerson = getIncompletePerson()

    val avroCompletePerson = SpecificMapper.personToByteArray(completePerson)
    logger.info("AVRO COMPLETE : ${String(avroCompletePerson)}")
    logger.info("AVRO LIST OF BYTES : ${avroCompletePerson.asList()}")

    val avroIncompletePerson = SpecificMapper.personToByteArray(incompletePerson)
    logger.info("AVRO INCOMPLETE : ${String(avroIncompletePerson)}")

    val jsonCompletePerson = SpecificMapper.byteArrayToPerson(avroCompletePerson)
    logger.info("JSON COMPLETE : $jsonCompletePerson")

    val jsonIncompletePerson = SpecificMapper.byteArrayToPerson(avroIncompletePerson)
    logger.info("JSON INCOMPLETE : $jsonIncompletePerson")
}

fun getCompletePerson(): Person = Person.newBuilder()
    .setId(UUID.randomUUID().toString())
    .setFirstName("Jon")
    .setLastName("Doe")
    .setAddress("Streetname 2, 1000 AA, Amsterdam")
    .setDateOfBirth(LocalDate.now())
    .setGender(Gender.UNSPECIFIED)
    .setFavoriteBand("ABC")
    .setJob(
        Job.newBuilder()
            .setCompany("Acme")
            .setTitle("Software Engineer")
            .setSalary(3500)
            .build()
    )
    .build()

fun getIncompletePerson(): Person = Person.newBuilder()
    .setId(UUID.randomUUID().toString())
    .setFirstName("Jon")
    .setLastName("Doe")
    .setAddress("Streetname 2, 1000 AA, Amsterdam")
    .setDateOfBirth(LocalDate.now())
    .setGender(Gender.UNSPECIFIED)
    .setJob(null)
    .build()
