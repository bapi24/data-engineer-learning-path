-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Clean up
-- MAGIC ### Drop the schema.

-- COMMAND ----------

DROP SCHEMA ${da.schema_name}_default_location CASCADE;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC DA.cleanup()
