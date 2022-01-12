// Databricks notebook source
// MAGIC %md # Spark Streaming on Retail data 

// COMMAND ----------

// MAGIC %md ## 1. Read the data schema 

// COMMAND ----------

// To read the csv files ((with header))
val df_retail = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
  .csv("FileStore/tables/retail-data/*.csv")
val retailSchema = df_retail.schema

// COMMAND ----------

// MAGIC %md ## 2. Read the streaming data

// COMMAND ----------

val streaming_retail = spark
                  .readStream.schema(retailSchema)
                  .format("csv")
                  .option("maxFilesPerTrigger", 20)
                  .csv("/FileStore/tables/retail-data")

// COMMAND ----------

// MAGIC %md ### Please discard the coming hidden cells as these were for an approach about aggregation

// COMMAND ----------

// MAGIC %md ## 3. Q3)b) Aggregate by the total value and total stocks
// MAGIC ### 3.1. Approach (1) ((DISCARDED)): for each batch regardless customerID
// MAGIC #### 3.1.1. Make the aggregate dataframe

// COMMAND ----------

// Q3) b) approach 1): Aggregates for the batch as while (regardless which customer; now I hate customers -- including me)
import org.apache.spark.sql.functions._

val stocksANDvalues = streaming_retail.select(expr("sum(Quantity*UnitPrice)").alias("total value"), 
                                              sum("Quantity").alias("total stock"))

// COMMAND ----------

// MAGIC %md #### 3.1.2. Write stream of (3.1.) output

// COMMAND ----------

// Q3) b) To query stocks and values [Regardless customer]
val stocksANDvaluesQuery = stocksANDvalues.writeStream.queryName("stocks_values")
  .format("memory").outputMode("complete")
  .start()

// COMMAND ----------

// MAGIC %md #### 3.1.3. Display the stream with 5 secs sleep (to slow down the change of data or sth)

// COMMAND ----------

// Loop to display; 
for( i <- 1 to 15 ) {
    spark.sql("SELECT * FROM stocks_values").show()
    Thread.sleep(5000) // this is in milliseconds
}

// COMMAND ----------

// MAGIC %md ## 3. Q3)b) Aggregate by the total value and total stocks
// MAGIC ### 3.2. For each customerID
// MAGIC #### 3.2.1. Make the aggregate dataframe

// COMMAND ----------

// Q3) b) Approach 2) Aggregates by customer
import org.apache.spark.sql.functions._

// Here we add a column of Sale value, then groupBy customerID and lastly aggregate based on the quantity and sale value
val customerAgg = streaming_retail.withColumn("Sale value", expr("Quantity*UnitPrice"))
                                    .groupBy("CustomerID")
                                    .agg(sum("Quantity").alias("total stock"), sum("Sale value").alias("total value"))

// COMMAND ----------

// MAGIC %md #### 3.2.2. Write stream of (3.2.) output

// COMMAND ----------

// Q3) b) To query stocks and values [with customer]
val CustomerAggQuery = customerAgg.writeStream.queryName("customer_agg")
  .format("memory").outputMode("complete")
  .start()

// COMMAND ----------

// MAGIC %md #### 3.2.3. Display the stream with 5 secs sleep (to slow down the change of data or sth)

// COMMAND ----------

/* 
Loop to display (Customer Aggregates)
 */
for( i <- 1 to 15 ) {
    spark.sql("SELECT * FROM customer_agg").show()
    Thread.sleep(5000) // this is in milliseconds
}

// COMMAND ----------

// MAGIC %md ## 4. Q3)c) Aggregate by the total value, total records/rows imported and trigger time
// MAGIC #### 4.1. Make the aggregate dataframe ==most likely solution==

// COMMAND ----------

// Q3) c) For the time, records and sale value -- my lord, I want to quit 

import org.apache.spark.sql.functions._
val timeANDrecords = streaming_retail.withColumn("Trigger_time", current_timestamp())
                                    .withColumn("Sale value", expr("Quantity*UnitPrice"))
                                    .groupBy("Trigger_time")
                                    .agg(count(lit(1)).alias("Records_Imported"), sum("Sale value").alias("Sale_value"))

// For the records imported we may use:: count(quantity) but it differs in small numbers where lit retrieves more numbers by 20 columns

// COMMAND ----------

// MAGIC %md #### 4.1. (Another solution) for the aggregation ==less likely solution==
// MAGIC The only difference is that here we aggregate by the count of any arbitrary column as a counter for it's rows; however it's always less than that from the literat function by 20 rows
// MAGIC #### Please only uncomment the next cell if the previous code is wrong [that depends on literal function] as it'll override the previous dataframe

// COMMAND ----------

// import org.apache.spark.sql.functions._
// val timeANDrecords = streaming_retail.withColumn("Trigger_time", current_timestamp())
//                                     .withColumn("Sale value", expr("Quantity*UnitPrice"))
//                                     .groupBy("Trigger_time")
//                                     .agg(count("Quantity").alias("Records_Imported"), sum("Sale value").alias("Sale_value"))

// COMMAND ----------

// MAGIC %md #### 4.2. Write stream of (4) output

// COMMAND ----------

// To query time trigger and others
val timeANDrecordsQuery = timeANDrecords.writeStream.queryName("time_records_sales")
  .format("memory").outputMode("complete")
  .start()

// COMMAND ----------

// MAGIC %md #### 4.3. Display the stream with 5 secs sleep (to slow down the change of data and be able to track the changes)

// COMMAND ----------

// Loop to display; 
for( i <- 1 to 15 ) {
    spark.sql("SELECT * FROM time_records_sales").show(false)
    Thread.sleep(5000) // this is in milliseconds
}

// COMMAND ----------

// MAGIC %md ## 5. Q3)d) Lineplot for 2 timelines (Records imported, total sale value) w.r.t the trigger time

// COMMAND ----------

// MAGIC %sql select Sales_value, Records_Imported, date_format(Time_trigger, "MMM-dd HH:mm") from time_records_sales;

// COMMAND ----------

display(timeANDrecords)

// COMMAND ----------


