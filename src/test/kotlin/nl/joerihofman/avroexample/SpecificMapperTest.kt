package nl.joerihofman.avroexample

import org.apache.avro.AvroMissingFieldException
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDate
import java.util.*
import kotlin.test.assertFailsWith

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
object SpecificMapperTest {

    private lateinit var mapper: SpecificMapper

    @BeforeAll
    fun setUp() {
        mapper = SpecificMapper
    }

    @Test
    fun missingFieldExample() {
        assertFailsWith(AvroMissingFieldException::class) {
            getInvalidPersonFirstNameMissing()
        }
    }

    @Test
    fun byteArrayExample() {
        val person = getCompletePerson()

        val byteArray = mapper.personToByteArray(person).also { println("Person to byte array : ${String(it)}")}

        mapper.byteArrayToPerson(byteArray).also { println("Person to byte array : $it") }
    }

    @Test
    fun byteArrayExampleWithoutJob() {
        val person = getPersonWithoutJob()

        val byteArray = mapper.personToByteArray(person).also { println("Person to byte array without job : ${String(it)}") }

        mapper.byteArrayToPerson(byteArray).also { println("Person to byte array without job : $it") }
    }

    @Test
    fun byteArrayExampleWithoutJobAndBand() {
        val person = getPersonWithoutJobAndBand()

        val byteArray = mapper.personToByteArray(person).also { println("Person to byte array without job and band : ${String(it)}") }

        mapper.byteArrayToPerson(byteArray).also { println("Person to byte array without job and band : $it") }
    }

    @Test
    fun jsonExample() {
        val person = getCompletePerson()

        val json = mapper.personToJson(person).also { println("Person to json : $it") }

        mapper.jsonToPerson(json).also { println("Person to json : $it") }
    }

    @Test
    fun fileExample() {
        val person = getCompletePerson()

        mapper.personToFileForTest(person)
    }

    private fun getCompletePerson(): Person {
        val person = Person.newBuilder()

        person.id = UUID.randomUUID().toString()
        person.firstName = "Jon"
        person.lastName = "Doe"
        person.address = "Streetname 2, 1000 AA, Amsterdam"
        person.dateOfBirth  = LocalDate.now()
        person.gender = Gender.UNSPECIFIED
        person.favoriteBand = "ABC"

        val job = Job.newBuilder()
        job.company = "Acme"
        job.title = "Software Engineer"
        job.salary = 3500
        person.job = job.build()

        return person.build()
    }

    private fun getPersonWithoutJob(): Person {
        val person = Person.newBuilder()

        person.id = UUID.randomUUID().toString()
        person.firstName = "Jon"
        person.lastName = "Doe"
        person.address = "Streetname 2, 1000 AA, Amsterdam"
        person.dateOfBirth  = LocalDate.now()
        person.gender = Gender.UNSPECIFIED
        person.favoriteBand = "ABC"

        person.job = null

        return person.build()
    }

    private fun getPersonWithoutJobAndBand(): Person {
        val person = Person.newBuilder()

        person.id = UUID.randomUUID().toString()
        person.firstName = "Jon"
        person.lastName = "Doe"
        person.address = "Streetname 2, 1000 AA, Amsterdam"
        person.dateOfBirth  = LocalDate.now()
        person.gender = Gender.UNSPECIFIED

        person.job = null
        person.favoriteBand = null

        return person.build()
    }

    private fun getInvalidPersonFirstNameMissing(): Person {
        val person = Person.newBuilder()

        person.id = UUID.randomUUID().toString()
        person.lastName = "Doe"
        person.address = "Streetname 2, 1000 AA, Amsterdam"
        person.dateOfBirth  = LocalDate.now()
        person.gender = Gender.UNSPECIFIED
        person.favoriteBand = "ABC"
        person.job = null

        return person.build()
    }
}