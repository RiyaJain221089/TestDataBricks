# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-fea707eb-314a-41a8-8da5-fdac27ebe622
# MAGIC %md
# MAGIC # Explorando os logs de eventos do pipeline
# MAGIC
# MAGIC O DLT usa os logs de eventos para armazenar várias informações importantes usadas para gerenciar, relatar e entender o que acontece durante a execução do pipeline.
# MAGIC
# MAGIC Abaixo fornecemos uma série de queries úteis para explorar o log de eventos e obter mais informações sobre os pipelines do DLT.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.4

# COMMAND ----------

# DBTITLE 0,--i18n-db58d66a-73bf-412a-ae17-b00f98338f56
# MAGIC %md
# MAGIC ## Consultar o log de eventos
# MAGIC O log de eventos é gerenciado como uma tabela do Delta Lake com alguns dos campos mais importantes armazenados como dados JSON aninhados.
# MAGIC
# MAGIC A query abaixo mostra como é simples ler essa tabela e criar um DataFrame e uma view temporária para queries interativas.

# COMMAND ----------

event_log_path = f"{DA.paths.storage_location}/system/events"

event_log = spark.read.format('delta').load(event_log_path)
event_log.createOrReplaceTempView("event_log_raw")

display(event_log)

# COMMAND ----------

# DBTITLE 0,--i18n-b5f6dcac-b958-4809-9942-d45e475b6fb7
# MAGIC %md
# MAGIC ## Definir o ID da última atualização
# MAGIC
# MAGIC Em muitos casos, você pode querer saber qual foi a atualização mais recente (ou as últimas n atualizações) do pipeline.
# MAGIC
# MAGIC Podemos capturar facilmente o ID da atualização mais recente com uma query SQL.

# COMMAND ----------

latest_update_id = spark.sql("""
    SELECT origin.update_id
    FROM event_log_raw
    WHERE event_type = 'create_update'
    ORDER BY timestamp DESC LIMIT 1""").first().update_id

print(f"Latest Update ID: {latest_update_id}")

# Push back into the spark config so that we can use it in a later query.
spark.conf.set('latest_update.id', latest_update_id)

# COMMAND ----------

# DBTITLE 0,--i18n-de7c7817-fcfd-4994-beb0-704099bd5c30
# MAGIC %md
# MAGIC ## Executar registro em log de auditoria
# MAGIC
# MAGIC Os eventos relacionados à execução de pipelines e edição de configurações são capturados como **`user_action`**.
# MAGIC
# MAGIC O seu nome deve ser o único **`user_name`** no pipeline que você configurou durante esta lição.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT timestamp, details:user_action:action, details:user_action:user_name
# MAGIC FROM event_log_raw 
# MAGIC WHERE event_type = 'user_action'

# COMMAND ----------

# DBTITLE 0,--i18n-887a16ce-e1a5-4d27-bacb-7e6c84cbaf37
# MAGIC %md
# MAGIC ## Examinar linhagem
# MAGIC
# MAGIC O DLT fornece informações integradas de linhagem sobre o fluxo de dados na tabela.
# MAGIC
# MAGIC Embora a query abaixo indique apenas os predecessores diretos de cada tabela, essas informações podem ser facilmente combinadas para rastrear dados em qualquer tabela até o momento de entrada no lakehouse.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT details:flow_definition.output_dataset, details:flow_definition.input_datasets 
# MAGIC FROM event_log_raw 
# MAGIC WHERE event_type = 'flow_definition' AND 
# MAGIC       origin.update_id = '${latest_update.id}'

# COMMAND ----------

# DBTITLE 0,--i18n-1b1c0687-163f-4684-a570-3cf4cc32c272
# MAGIC %md
# MAGIC ## Examinar as métricas de qualidade de dados
# MAGIC
# MAGIC Por fim, as métricas de qualidade de dados podem ser extremamente úteis para gerar percepções de longo e curto prazo sobre seus dados.
# MAGIC
# MAGIC No passo abaixo, capturamos as métricas de cada restrição ao longo de toda a vida útil da tabela.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT row_expectations.dataset as dataset,
# MAGIC        row_expectations.name as expectation,
# MAGIC        SUM(row_expectations.passed_records) as passing_records,
# MAGIC        SUM(row_expectations.failed_records) as failing_records
# MAGIC FROM
# MAGIC   (SELECT explode(
# MAGIC             from_json(details :flow_progress :data_quality :expectations,
# MAGIC                       "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>")
# MAGIC           ) row_expectations
# MAGIC    FROM event_log_raw
# MAGIC    WHERE event_type = 'flow_progress' AND 
# MAGIC          origin.update_id = '${latest_update.id}'
# MAGIC   )
# MAGIC GROUP BY row_expectations.dataset, row_expectations.name

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
