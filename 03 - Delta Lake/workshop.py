# Databricks notebook source
# MAGIC %run ../Includes/Classroom-Setup-03.1

# COMMAND ----------

# MAGIC %sql
# MAGIC --create schema
# MAGIC CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_default_location;

# COMMAND ----------

# MAGIC %sql
# MAGIC --use above created schema
# MAGIC USE ${da.schema_name}_default_location;
# MAGIC 
# MAGIC --create temporary view
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
# MAGIC   path = '${da.paths.datasets}/flights/departuredelays.csv',
# MAGIC   header = "true",
# MAGIC   mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
# MAGIC );
# MAGIC CREATE OR REPLACE TABLE flight_delays_external_table LOCATION '${da.paths.working_dir}/external_table' AS
# MAGIC   SELECT * FROM temp_delays;
# MAGIC 
# MAGIC SELECT * FROM flight_delays_external_table; 

# COMMAND ----------

# MAGIC %sql
# MAGIC --use above created schema
# MAGIC USE ${da.schema_name}_default_location;
# MAGIC 
# MAGIC --use above create temp view
# MAGIC CREATE OR REPLACE TABLE flight_delays_managed_table AS
# MAGIC   SELECT * FROM temp_delays;
# MAGIC 
# MAGIC SELECT * FROM flight_delays_managed_table; 

# COMMAND ----------

# MAGIC %sql
# MAGIC --create view
# MAGIC CREATE OR REPLACE VIEW flight_delays_vw AS
# MAGIC   SELECT * from flight_delays_managed_table; 

# COMMAND ----------

# MAGIC %sql 
# MAGIC --query files directly from file
# MAGIC SELECT * FROM csv.`${da.paths.datasets}/flights/departuredelays.csv`

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-02.2

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ${da.schema_name}_default_location;
# MAGIC CREATE TABLE IF NOT EXISTS sales_csv
# MAGIC   (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   delimiter = "|"
# MAGIC )
# MAGIC LOCATION "${DA.paths.sales_csv}"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE sales_managed_table AS
# MAGIC   SELECT * from sales_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_managed_table;

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-02.4

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*), count(user_id), count(user_first_touch_timestamp), count(email), count(updated)
# MAGIC FROM users_dirty

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users_dirty limit 2;

# COMMAND ----------

# MAGIC %sql 
# MAGIC USE ${da.schema_name}_default_location;
# MAGIC CREATE OR REPLACE TABLE users_dirty_managed AS
# MAGIC   SELECT * from ${da.schema_name}.users_dirty

# COMMAND ----------


from pyspark.sql.functions import col
usersDF = spark.read.table("users_dirty_managed") 

usersDF.selectExpr("count_if(email IS NULL)")
usersDF.where(col("email").isNull()).count()

# COMMAND ----------

usersDF.distinct().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW deduped_users AS 
# MAGIC SELECT user_id, user_first_touch_timestamp, max(email) AS email, max(updated) AS updated
# MAGIC FROM users_dirty_managed
# MAGIC WHERE user_id IS NOT NULL
# MAGIC GROUP BY user_id, user_first_touch_timestamp;
# MAGIC 
# MAGIC SELECT count(*) FROM deduped_users

# COMMAND ----------

# MAGIC %sql
# MAGIC --create bronze table from users_dirty_managed table by deduplicating values
# MAGIC CREATE OR REPLACE TABLE users_bronze AS
# MAGIC   SELECT * from deduped_users

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --check if email is associated with at most one `user_id`
# MAGIC SELECT max(user_id_count) <= 1 at_most_one_id FROM (
# MAGIC   SELECT email, count(user_id) AS user_id_count
# MAGIC   FROM deduped_users
# MAGIC   WHERE email IS NOT NULL
# MAGIC   GROUP BY email)

# COMMAND ----------

from pyspark.sql.functions import max, count
dedupedDF = (usersDF
    .where(col("user_id").isNotNull())
    .groupBy("user_id", "user_first_touch_timestamp")
    .agg(max("email").alias("email"), 
         max("updated").alias("updated"))
    )

dedupedDF.count()



# COMMAND ----------

display(dedupedDF
    .where(col("email").isNotNull())
    .groupby("email")
    .agg(count("user_id").alias("user_id_count"))
    .select((max("user_id_count") <= 1).alias("at_most_one_id")))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- create silver table
# MAGIC CREATE OR REPLACE TABLE users_silver AS
# MAGIC   SELECT *, 
# MAGIC   date_format(first_touch, "MMM d, yyyy") AS first_touch_date,
# MAGIC   date_format(first_touch, "HH:mm:ss") AS first_touch_time,
# MAGIC   regexp_extract(email, "(?<=@).+", 0) AS email_domain
# MAGIC FROM (
# MAGIC   SELECT *,
# MAGIC     CAST(user_first_touch_timestamp / 1e6 AS timestamp) AS first_touch 
# MAGIC   FROM deduped_users
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE students
# MAGIC   (id INT, name STRING, value DOUBLE);
# MAGIC   
# MAGIC INSERT INTO students VALUES (1, "Yve", 1.0);
# MAGIC INSERT INTO students VALUES (2, "Omar", 2.5);
# MAGIC INSERT INTO students VALUES (3, "Elia", 3.3);
# MAGIC 
# MAGIC INSERT INTO students
# MAGIC VALUES 
# MAGIC   (4, "Ted", 4.7),
# MAGIC   (5, "Tiffany", 5.5),
# MAGIC   (6, "Vini", 6.3);
# MAGIC   
# MAGIC UPDATE students 
# MAGIC SET value = value + 1
# MAGIC WHERE name LIKE "T%";
# MAGIC 
# MAGIC DELETE FROM students 
# MAGIC WHERE value > 6;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
# MAGIC   (2, "Omar", 15.2, "update"),
# MAGIC   (3, "", null, "delete"),
# MAGIC   (7, "Blue", 7.7, "insert"),
# MAGIC   (11, "Diya", 8.8, "update");
# MAGIC   
# MAGIC MERGE INTO students b
# MAGIC USING updates u
# MAGIC ON b.id=u.id
# MAGIC WHEN MATCHED AND u.type = "update"
# MAGIC   THEN UPDATE SET *
# MAGIC WHEN MATCHED AND u.type = "delete"
# MAGIC   THEN DELETE
# MAGIC WHEN NOT MATCHED AND u.type = "insert"
# MAGIC   THEN INSERT *;

# COMMAND ----------

display(dbutils.fs.ls(f"{DA.schema_name}/students"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED students

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-03.2 

# COMMAND ----------

display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

# COMMAND ----------


