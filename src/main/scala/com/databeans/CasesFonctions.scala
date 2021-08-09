package com.databeans

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CasesFonctions {


  def addressHistoryUpdate (addressHistoryInput: DataFrame, addressUpdateInput: DataFrame): DataFrame = {
    val renamedDataframe = addressUpdateInput.withColumnRenamed("address","new_address")
      .withColumnRenamed("moved_in","new_moved_in").select("id","new_address","new_moved_in")
    val joinedDataframe = addressHistoryInput.join(renamedDataframe, "id")
    val firstRow = joinedDataframe
      .withColumn("moved_out",col("new_moved_in"))
      .withColumn("current",lit(false))
      .drop("new_address","new_moved_in")
    val secondRow = joinedDataframe
      .withColumn("address",col("new_address"))
      .withColumn("moved_in",col("new_moved_in"))
      .drop("new_address","new_moved_in")
    val oldRow = joinedDataframe
      .drop("new_address", "new_moved_in")

    val updatedDataframe = firstRow.union(secondRow).union(addressHistoryInput).except(oldRow)
    updatedDataframe
  }

}
