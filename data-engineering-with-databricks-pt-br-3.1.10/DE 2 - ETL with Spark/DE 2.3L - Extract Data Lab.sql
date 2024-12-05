-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-037a5204-a995-4945-b6ed-7207b636818c
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC # Laboratório de extração de dados
-- MAGIC
-- MAGIC Neste laboratório, você extrairá dados brutos de arquivos JSON.
-- MAGIC
-- MAGIC ## Objetivos de aprendizado
-- MAGIC Ao final deste laboratório, você será capaz de:
-- MAGIC - Registrar uma tabela externa para extrair dados de arquivos JSON

-- COMMAND ----------

-- DBTITLE 0,--i18n-3b401203-34ae-4de5-a706-0dbbd5c987b7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Executar a configuração
-- MAGIC
-- MAGIC Execute a célula a seguir para configurar variáveis e datasets para esta lição.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.3L

-- COMMAND ----------

-- DBTITLE 0,--i18n-9adc06e2-7298-4306-a062-7ff70adb8032
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Visão geral dos dados
-- MAGIC
-- MAGIC Trabalharemos com uma amostra de dados brutos do Kafka gravados em arquivos JSON. 
-- MAGIC
-- MAGIC Cada arquivo contém todos os registros consumidos durante 5- segundo intervalo, armazenado com o esquema Kafka completo como um múltiplo- gravar arquivo JSON. 
-- MAGIC
-- MAGIC O esquema da tabela:
-- MAGIC
-- MAGIC | campo  | tipo | descrição |
-- MAGIC | ------ | ---- | ----------- |
-- MAGIC | key    | BINARY | O**`user_id`** campo é usado como chave; este é um campo alfanumérico exclusivo que corresponde às informações da sessão/cookie |
-- MAGIC | offset | LONG | Este é um valor único, aumentando monotonicamente para cada partição |
-- MAGIC | partition | INTEGER | Nossa implementação atual do Kafka usa apenas 2 partições (0 e 1) |
-- MAGIC | timestamp | LONG    | Este carimbo de data/hora é registrado em milissegundos desde a época e representa o momento em que o produtor anexa um registro a uma partição |
-- MAGIC | topic | STRING | Embora o serviço Kafka hospede vários tópicos, apenas os registros do**`clickstream`** o tópico está incluído aqui |
-- MAGIC | value | BINARY | Esta é a carga completa de dados (a ser discutida mais tarde), enviada como JSON |

-- COMMAND ----------

-- DBTITLE 0,--i18n-8cca978a-3034-4339-8e6e-6c48d389cce7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC  
-- MAGIC ## Extrair eventos brutos de arquivos JSON
-- MAGIC Para carregar corretamente os dados no Delta, primeiro precisamos extrair os dados JSON usando o esquema correto.
-- MAGIC
-- MAGIC Crie uma tabela externa a partir de arquivos JSON localizados no caminho de arquivo fornecido abaixo. Nomeie essa tabela **`events_json`** e declare o esquema acima.

-- COMMAND ----------

-- TODO
-- <FILL_IN> "${DA.paths.kafka_events}" 

-- COMMAND ----------

-- ANSWER
-- Create the table using CTAS
CREATE OR REPLACE TABLE events_json
AS
SELECT
  CAST(key AS BINARY) AS key,
  CAST(offset AS BIGINT) AS offset,
  CAST(partition AS INT) AS partition,
  CAST(timestamp AS BIGINT) AS timestamp,
  CAST(topic AS STRING) AS topic,
  CAST(value AS BINARY) AS value
FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-33231985-3ff1-4f44-8098-b7b862117689
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC **OBSERVAÇÃO**: Usaremos Python para executar verificações ocasionais durante o laboratório. A célula a seguir retornará um erro com uma mensagem informando o que precisa ser alterado caso você não tenha seguido as instruções. A execução da célula não gerará nenhuma saída se você tiver concluído este passo.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_json"), "Table named `events_json` does not exist"
-- MAGIC assert spark.table("events_json").columns == ['key', 'offset', 'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_json").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC
-- MAGIC total = spark.table("events_json").count()
-- MAGIC assert total == 2252, f"Expected 2252 records, found {total}"

-- COMMAND ----------

-- DBTITLE 0,--i18n-6919d58a-89e4-4c02-812c-98a15bb6f239
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC  
-- MAGIC Execute a célula a seguir para excluir as tabelas e arquivos associados a esta lição.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
-- MAGIC
