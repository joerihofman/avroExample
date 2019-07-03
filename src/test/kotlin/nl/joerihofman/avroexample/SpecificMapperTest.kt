package nl.joerihofman.avroexample

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDate
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
object SpecificMapperTest {

    private lateinit var mapper: SpecificMapper

    @BeforeAll
    fun setUp() {
        mapper = SpecificMapper
    }

    @Test
    fun byteArrayExample() {
        val person = getCompletePerson()

        val byteArray = mapper.personToByteArray(person)
        println(String(byteArray))

        val personReturned = mapper.byteArrayToPerson(byteArray)
        println(personReturned)
    }

    @Test
    fun jsonExample() {
        val person = getCompletePerson()

        val json = mapper.personToJson(person)
        println(json)

        val personReturned = mapper.jsonToPerson(json)
        println(personReturned)
    }

    @Test
    fun fileExample() {
        val person = getCompletePerson()

        mapper.personToFileTest(person)
    }

    fun getCompletePerson(): Person {
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

    fun getPersonWithoutJob(): Person {
        val person = Person.newBuilder()

        person.id = UUID.randomUUID().toString()
        person.firstName = "Jon"
        person.lastName = "Doe"
        person.address = "Streetname 2, 1000 AA, Amsterdam"
        person.dateOfBirth  = LocalDate.now()
        person.gender = Gender.UNSPECIFIED
        person.favoriteBand = "ABC"

        return person.build()
    }

    fun getPersonWithoutJobAndBand(): Person {
        val person = Person.newBuilder()

        person.id = UUID.randomUUID().toString()
        person.firstName = "Jon"
        person.lastName = "Doe"
        person.address = "Streetname 2, 1000 AA, Amsterdam"
        person.dateOfBirth  = LocalDate.now()
        person.gender = Gender.UNSPECIFIED

        return person.build()
    }

    fun getInvalidPersonFirstNameMissing(): Person {
        val person = Person.newBuilder()

        person.id = UUID.randomUUID().toString()
        person.lastName = "Doe"
        person.address = "Streetname 2, 1000 AA, Amsterdam"
        person.dateOfBirth  = LocalDate.now()
        person.gender = Gender.UNSPECIFIED
        person.favoriteBand = "ABC"

        return person.build()
    }
}