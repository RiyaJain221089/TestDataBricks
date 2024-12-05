# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-2b2de306-5587-4e60-aa3f-34ae6c344907
# MAGIC %run ../Includes/Classroom-Setup-05.2.4L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Explorando os resultados de um pipeline DLT
# MAGIC
# MAGIC Execute a seguinte célula para enumerar a saída do seu local de armazenamento:

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-f8f98f46-f3d3-41e5-b86a-fc236813e67e
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC O **system** diretório captura eventos associados ao pipeline.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

DA.print_job_config()

# COMMAND ----------

# DBTITLE 0,--i18n-b8afa35d-667e-40b8-915c-d754d5bdb5ee
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Esses logs de eventos são armazenados como uma tabela Delta. 
# MAGIC
# MAGIC Vamos consultar a tabela.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${DA.paths.storage_location}/system/events`

# COMMAND ----------

# DBTITLE 0,--i18n-adc883c4-30ec-4391-8d7f-9554f77a0feb
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Vamos visualizar o conteúdo do diretório *tables*.

# COMMAND ----------

# Note: this will error if it is not being run as part of the job configured in DE 5.2.1L
files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-72eb29d2-cde0-4488-954b-a0ed47ead8eb
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Vamos consultar a tabela ouro.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${DA.schema_name}.daily_patient_avg

# COMMAND ----------

DA.cleanup()
