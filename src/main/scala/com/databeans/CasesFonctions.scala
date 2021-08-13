package com.databeans

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CasesFonctions {


  def addressHistoryUpdate (addressHistoryInput: DataFrame, addressUpdateInput: DataFrame): DataFrame = {
    val renamedDataframe = addressUpdateInput.withColumnRenamed("address","new_address")
      .withColumnRenamed("moved_in","new_moved_in").select("id","new_address","new_moved_in")
    val shouldBeUpdated = addressHistoryInput.where(col("current") === true)
    val joinedDataframe = shouldBeUpdated.join(renamedDataframe, "id")

    val diffrentAddressDiffrentTime = joinedDataframe.where(col("address" ) =!= col("new_address"))
      .where(col("new_moved_in") > col("moved_in"))
    val firstRow = diffrentAddressDiffrentTime
      .withColumn("moved_out",col("new_moved_in"))
      .withColumn("current",lit(false))
      .drop("new_address","new_moved_in")
    val secondRow = diffrentAddressDiffrentTime
      .withColumn("address",col("new_address"))
      .withColumn("moved_in",col("new_moved_in"))
      .drop("new_address","new_moved_in")
    val oldRow = joinedDataframe
      .drop("new_address", "new_moved_in")

    val diffrentAddressDiffrentTime2 = joinedDataframe.where(col("new_moved_in") < col("moved_in"))
      .where(col("new_address") =!= col("address"))
    val firstRow3 = diffrentAddressDiffrentTime2
      .withColumn("address", col("new_address"))
      .withColumn("moved_out", col("moved_in"))
      .withColumn("moved_in", col("new_moved_in"))
      .withColumn("current", lit(false))
      .drop("new_address", "new_moved_in")
    val secondRow3 = diffrentAddressDiffrentTime2
      .drop("new_address", "new_moved_in")

    val sameAddressDiffrentTime = joinedDataframe.where(col("new_moved_in" ) < col("moved_in"))
      .where(col("address") === col("new_address"))
    val firstRow1 = sameAddressDiffrentTime
      .withColumn("moved_in", col("new_moved_in"))
      .drop("new_address","new_moved_in")

    val sameAddressDiffrentTime2 = joinedDataframe.where(col("new_moved_in" ) > col("moved_in"))
      .where(col("address") === col("new_address"))
    val secondRow4 = sameAddressDiffrentTime2
      .drop("new_address", "new_moved_in")


    val newPerson = addressUpdateInput.join(shouldBeUpdated, shouldBeUpdated("id") === renamedDataframe("id") , "left_anti")
    val newPersonAdd = newPerson.withColumn("moved_out", lit(null))
      .withColumn("current" , lit(true))


    val updatedDataframe = firstRow.union(secondRow).union(firstRow1).union(addressHistoryInput).except(oldRow).union(firstRow3).union(newPersonAdd)
    val finalDataframe = updatedDataframe.union(secondRow3).union(secondRow4)

    finalDataframe

  }

}
