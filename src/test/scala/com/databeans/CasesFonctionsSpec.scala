package com.databeans

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class AddressHistory
(id: Long, first_name: String, last_name: String, address: String, moved_in: LocalDate, moved_out: LocalDate, current: Boolean)

case class AddressUpdates(id: Long, first_name: String, last_name: String, address: String, moved_in: LocalDate)


class CasesFonctionsSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Sample App")
    .getOrCreate()

  import spark.implicits._

  val pattern = DateTimeFormatter.ofPattern("dd-MM-yyyy")

  val addressHistory = Seq(
    AddressHistory(1, "Boazizi", "Sayf", "Soussa", LocalDate.parse("01-06-2020", pattern), null, true),
    AddressHistory(2, "abidi", "jacer", "amesterdam", LocalDate.parse("01-02-2020", pattern), null, true),
    AddressHistory(3, "banneni", "oussema", "paris", LocalDate.parse("05-06-2019", pattern), null, true),
    AddressHistory(4, "dalhoumi", "houssem", "kasserine", LocalDate.parse("01-12-1992", pattern), null, true),
    AddressHistory(2, "abidi", "jacer", "kasserine", LocalDate.parse("01-02-1992", pattern), LocalDate.parse("01-02-2020", pattern), false)
  ).toDF()
  val addressUpdate = Seq(
    AddressUpdates(1, "Boazizi", "Sayf", "Kasserine", LocalDate.parse("01-09-2020", pattern)),
    AddressUpdates(2, "abidi", "jacer", "amesterdam", LocalDate.parse("01-01-2020", pattern)),
    AddressUpdates(3, "banneni", "oussema", "kasserine", LocalDate.parse("01-08-1992", pattern)),
    AddressUpdates(4, "dalhoumi", "houssem", "kasserine", LocalDate.parse("01-08-2002", pattern)),
    AddressUpdates(5, "abidi", "ghassen", "tatawin", LocalDate.parse("01-08-2020", pattern))


  ).toDF()



  val addressUpdate1 = Seq(AddressUpdates(1, "Boazizi", "Sayf", "Kasserine", LocalDate.parse("01-09-2020", pattern))).toDF()

  "AddressHistoryBuilder" should "update the history when given a diffrent address and a diffrent time update " in {
    Given("the address history and the update")
    val addressHistoryInput = addressHistory
    val addressUpdateInput = addressUpdate1
    When("AddressHistoryUpdate is Invoked")
    val dataframeUpdater1 = CasesFonctions.addressHistoryUpdate(addressHistoryInput, addressUpdateInput)
    Then("the address history should be updated")
    val expectedResult1 = Seq(
      AddressHistory(1, "Boazizi", "Sayf", "Soussa", LocalDate.parse("01-06-2020", pattern), LocalDate.parse("01-09-2020", pattern), false),
      AddressHistory(1, "Boazizi", "Sayf", "Kasserine", LocalDate.parse("01-09-2020", pattern), null, true),
      AddressHistory(2, "abidi", "jacer", "amesterdam", LocalDate.parse("01-02-2020", pattern), null, true),
      AddressHistory(3, "banneni", "oussema", "paris", LocalDate.parse("05-06-2019", pattern), null, true),
      AddressHistory(4, "dalhoumi", "houssem", "kasserine", LocalDate.parse("01-12-1992", pattern), null, true),
      AddressHistory(2, "abidi", "jacer", "kasserine", LocalDate.parse("01-02-1992", pattern), LocalDate.parse("01-02-2020", pattern), false)
    ).toDF()

    expectedResult1.collect() should contain theSameElementsAs (dataframeUpdater1.collect())


  }

  val addressUpdate2 = Seq(AddressUpdates(2, "abidi", "jacer", "amesterdam", LocalDate.parse("01-01-2020", pattern))).toDF()

  "AddressHistoryBuilder" should "update the history when given a same address and a diffrent time update " in {
    Given("the address history and the update")
    val addressHistoryInput = addressHistory
    val addressUpdateInput = addressUpdate2

    When("AddressHistoryUpdate is Invoked")
    val dataframeUpdater2 = CasesFonctions.addressHistoryUpdate(addressHistoryInput, addressUpdateInput)

    Then("the address history should be updated")
    val expectedResult2 = Seq(
      AddressHistory(1, "Boazizi", "Sayf", "Soussa", LocalDate.parse("01-06-2020", pattern), null, true),
      AddressHistory(2, "abidi", "jacer", "amesterdam", LocalDate.parse("01-01-2020", pattern), null, true),
      AddressHistory(3, "banneni", "oussema", "paris", LocalDate.parse("05-06-2019", pattern), null, true),
      AddressHistory(4, "dalhoumi", "houssem", "kasserine", LocalDate.parse("01-12-1992", pattern), null, true),
      AddressHistory(2, "abidi", "jacer", "kasserine", LocalDate.parse("01-02-1992", pattern), LocalDate.parse("01-02-2020", pattern), false)
    ).toDF()

    expectedResult2.collect() should contain theSameElementsAs (dataframeUpdater2.collect())
  }


  val addressUpdate3 = Seq(AddressUpdates(3, "banneni", "oussema", "kasserine", LocalDate.parse("01-08-1992", pattern))).toDF()

  "AddressHistoryBuilder" should "update the history when given a diffrent address and a diffrent time2  update " in {
    Given("the address history and the update")
    val addressHistoryInput = addressHistory
    val addressUpdateInput = addressUpdate3

    When("AddressHistoryUpdate is Invoked")
    val dataframeUpdater3 = CasesFonctions.addressHistoryUpdate(addressHistoryInput, addressUpdateInput)

    Then("the address history should be updated")
    val expectedResult3 =Seq(
      AddressHistory(1, "Boazizi", "Sayf", "Soussa", LocalDate.parse("01-06-2020", pattern), null, true),
      AddressHistory(2, "abidi", "jacer", "amesterdam", LocalDate.parse("01-02-2020", pattern), null, true),
      AddressHistory(3, "banneni", "oussema", "paris", LocalDate.parse("05-06-2019", pattern), null, true),
      AddressHistory(3, "banneni", "oussema", "kasserine", LocalDate.parse("01-08-1992", pattern), LocalDate.parse("05-06-2019", pattern), false),
      AddressHistory(4, "dalhoumi", "houssem", "kasserine", LocalDate.parse("01-12-1992", pattern), null, true),
      AddressHistory(2, "abidi", "jacer", "kasserine", LocalDate.parse("01-02-1992", pattern), LocalDate.parse("01-02-2020", pattern), false)
    ).toDF()

    expectedResult3.collect() should contain theSameElementsAs (dataframeUpdater3.collect())
  }


  val addressUpdate4 = Seq( AddressUpdates(4, "dalhoumi", "houssem", "kasserine", LocalDate.parse("01-08-2002", pattern))).toDF()
  "AddressHistoryBuilder" should "update the history when given a same address and a diffrent time2  update " in {
    Given("the address history and the update")
    val addressHistoryInput = addressHistory
    val addressUpdateInput = addressUpdate4

    When("AddressHistoryUpdate is Invoked")
    val dataframeUpdater4 = CasesFonctions.addressHistoryUpdate(addressHistoryInput, addressUpdateInput)

    Then("the address history should be updated")
    val expectedResult4 = Seq(
      AddressHistory(1, "Boazizi", "Sayf", "Soussa", LocalDate.parse("01-06-2020", pattern), null, true),
      AddressHistory(2, "abidi", "jacer", "amesterdam", LocalDate.parse("01-02-2020", pattern), null, true),
      AddressHistory(3, "banneni", "oussema", "paris", LocalDate.parse("05-06-2019", pattern), null, true),
      AddressHistory(4, "dalhoumi", "houssem", "kasserine", LocalDate.parse("01-12-1992", pattern), null, true),
      AddressHistory(2, "abidi", "jacer", "kasserine", LocalDate.parse("01-02-1992", pattern), LocalDate.parse("01-02-2020", pattern), false)
    ).toDF()
    expectedResult4.collect() should contain theSameElementsAs (dataframeUpdater4.collect())

  }

  val addressUpdate5 = Seq(AddressUpdates(5, "abidi", "ghassen", "tatawin", LocalDate.parse("01-08-2020", pattern))).toDF()
  "AddressHistoryBuilder" should "update the history when given a new person " in {
    Given("the address history and the update")
    val addressHistoryInput = addressHistory
    val addressUpdateInput = addressUpdate5

    When("AddressHistoryUpdate is Invoked")
    val dataframeUpdater5 = CasesFonctions.addressHistoryUpdate(addressHistoryInput, addressUpdateInput)

    Then("the address history should be updated")
     val expectedResult5 = Seq(
       AddressHistory(1, "Boazizi", "Sayf", "Soussa", LocalDate.parse("01-06-2020", pattern), null, true),
       AddressHistory(2, "abidi", "jacer", "amesterdam", LocalDate.parse("01-02-2020", pattern), null, true),
       AddressHistory(3, "banneni", "oussema", "paris", LocalDate.parse("05-06-2019", pattern), null, true),
       AddressHistory(4, "dalhoumi", "houssem", "kasserine", LocalDate.parse("01-12-1992", pattern), null, true),
       AddressHistory(5, "abidi", "ghassen", "tatawin", LocalDate.parse("01-08-2020", pattern), null,true),
       AddressHistory(2, "abidi", "jacer", "kasserine", LocalDate.parse("01-02-1992", pattern), LocalDate.parse("01-02-2020", pattern), false)
     ).toDF()

    expectedResult5.collect() should contain theSameElementsAs (dataframeUpdater5.collect())

  }

}
