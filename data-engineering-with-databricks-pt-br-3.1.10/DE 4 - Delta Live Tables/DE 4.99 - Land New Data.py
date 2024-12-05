# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-cee8cef7-2341-42cd-beef-9a60958d8e70
# MAGIC %md
# MAGIC # Obter novos dados
# MAGIC
# MAGIC Este notebook é fornecido com o único propósito de acionar manualmente novos lotes de dados a serem processados por um pipeline do Delta Live Tables já configurado.
# MAGIC
# MAGIC Observe que a lógica fornecida é idêntica à do primeiro notebook interativo, mas não redefine os diretórios de origem ou de destino do pipeline.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.99

# COMMAND ----------

# DBTITLE 0,--i18n-884cce97-7746-4b9e-abe8-1fdbd38124bf
# MAGIC %md
# MAGIC Cada vez que esta célula é executada, um novo lote de arquivos de dados será carregado no diretório de origem usado nestas lições.

# COMMAND ----------

DA.dlt_data_factory.load()

# COMMAND ----------

# DBTITLE 0,--i18n-f9e15f42-07c9-4522-9c3f-7e0c975ad41d
# MAGIC %md
# MAGIC
# MAGIC # Obter todos os dados restantes
# MAGIC Como alternativa, você pode remover o comentário e executar a célula a seguir para carregar todos os lotes de dados restantes.

# COMMAND ----------

# TODO
# This should run for a little over 5 minutes. To abort
# early, click the Stop Execution button above
# DA.dlt_data_factory.load(continuous=True, delay_seconds=10)

# COMMAND ----------

# ANSWER
# When testing, we don't want any artificial delay
DA.dlt_data_factory.load(continuous=True, delay_seconds=0)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
