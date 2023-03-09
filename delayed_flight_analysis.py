# Databricks notebook source
# MAGIC %run ../Includes/Classroom-Setup-03.1

# COMMAND ----------

# MAGIC 
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


