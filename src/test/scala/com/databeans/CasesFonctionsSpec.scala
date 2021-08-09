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

  val addressHistory = Seq(AddressHistory(1, "Boazizi", "Sayf", "Soussa", LocalDate.parse("01-06-2020", pattern), null, true),
    AddressHistory(2, "abidi", "jacer", "amesterdam", LocalDate.parse("01-02-2020", pattern), null, true)
  ).toDF()
  val addressUpdate = Seq(AddressUpdates(1, "Boazizi", "Sayf", "Kasserine", LocalDate.parse("01-09-2020", pattern))).toDF()
  val expectedResult = Seq(AddressHistory(1, "Boazizi", "Sayf", "Soussa", LocalDate.parse("01-06-2020", pattern), LocalDate.parse("01-09-2020", pattern), false),
                           AddressHistory(2, "abidi", "jacer", "amesterdam", LocalDate.parse("01-02-2020", pattern), null, true),
                           AddressHistory(1, "Boazizi", "Sayf", "Kasserine", LocalDate.parse("01-09-2020", pattern), null, true)
  ).toDF()

  "AddressHistoryBuilder" should "update the address history when given an update" in {
    Given("the address history and the update")
    val addressHistoryInput = addressHistory
    val addressUpdateInput = addressUpdate
    When("AddressHistoryUpdate is Invoked")
    val dataframeUpdater = CasesFonctions.addressHistoryUpdate(addressHistoryInput, addressUpdateInput)
    Then("the address history should be updated")
    expectedResult.collect() should contain theSameElementsAs(dataframeUpdater.collect())


  }

}
