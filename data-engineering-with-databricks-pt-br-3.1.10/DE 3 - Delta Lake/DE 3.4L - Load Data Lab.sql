-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-02080b66-d11c-47fb-a096-38ce02af4dbb
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC # Laboratório de carregamento de dados
-- MAGIC
-- MAGIC Neste laboratório, você carregará dados em tabelas Delta novas e existentes.
-- MAGIC
-- MAGIC ## Objetivos de aprendizado
-- MAGIC Ao final deste laboratório, você será capaz de:
-- MAGIC - Criar uma tabela Delta vazia com um esquema fornecido
-- MAGIC - Inserir registros de uma tabela existente em uma tabela Delta
-- MAGIC - Usar uma instrução CTAS para criar uma tabela Delta a partir de arquivos

-- COMMAND ----------

-- DBTITLE 0,--i18n-50357195-09c5-4ab4-9d60-ca7fd44feecc
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Executar a configuração
-- MAGIC
-- MAGIC Execute a célula a seguir para configurar variáveis ​​e conjuntos de dados para esta lição.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.6L

-- COMMAND ----------

-- DBTITLE 0,--i18n-5b20c9b2-6658-4536-b79b-171d984b3b1e
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Visão geral dos dados
-- MAGIC
-- MAGIC Trabalharemos com uma amostra de dados brutos do Kafka escritos como arquivos JSON. 
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

-- DBTITLE 0,--i18n-5140f012-898a-43ac-bed9-b7e01a916505
-- MAGIC %md
-- MAGIC
-- MAGIC ## Definir o esquema para uma tabela Delta vazia
-- MAGIC Crie uma tabela Delta gerenciada vazia chamada **`events_raw`** usando o mesmo esquema.

-- COMMAND ----------

-- TODO
-- <FILL_IN>

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TABLE events_raw
  (key BINARY, offset BIGINT, partition INT, timestamp BIGINT, topic STRING, value BINARY);

-- COMMAND ----------

-- DBTITLE 0,--i18n-70f4dbf1-f4cb-4ec6-925a-a939cbc71bd5
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Execute a célula abaixo para confirmar se a tabela foi criada corretamente.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC suite = DA.tests.new("Define Schema")
-- MAGIC expected_table = lambda: spark.table("events_raw")
-- MAGIC suite.test_not_none(lambda: expected_table(), "Created the table \"events_raw\"")
-- MAGIC suite.test_equals(lambda: expected_table().count(), 0, "The table should have 0 records")
-- MAGIC
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "key", "BinaryType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "offset", "LongType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "partition", "IntegerType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "timestamp", "LongType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "topic", "StringType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "value", "BinaryType")
-- MAGIC
-- MAGIC suite.display_results()
-- MAGIC assert suite

-- COMMAND ----------

-- DBTITLE 0,--i18n-7b4e55f2-737f-4996-a51b-4f18c2fc6eb7
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Inserir eventos brutos na tabela Delta
-- MAGIC
-- MAGIC Assim que os dados extraídos e a tabela Delta estiverem prontos, insira os registros JSON da tabela **`events_json`** na nova tabela Delta **`events_raw`**.

-- COMMAND ----------

-- TODO
-- <FILL_IN>

-- COMMAND ----------

-- ANSWER
INSERT INTO events_raw
SELECT * FROM events_json

-- COMMAND ----------

-- DBTITLE 0,--i18n-65ffce96-d821-4792-b545-5725814003a0
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Revise manualmente o conteúdo da tabela para conferir se os dados foram gravados conforme o esperado.

-- COMMAND ----------

-- TODO
-- <FILL_IN>

-- COMMAND ----------

-- ANSWER
SELECT * FROM events_raw

-- COMMAND ----------

-- DBTITLE 0,--i18n-1cbff40d-e916-487c-958f-2eb7807c5aeb
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Execute a célula abaixo para confirmar que os dados foram carregados corretamente.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.functions as F
-- MAGIC
-- MAGIC suite = DA.tests.new("Validate events_raw")
-- MAGIC expected_table = lambda: spark.table("events_raw")
-- MAGIC suite.test_not_none(lambda: expected_table(), "Created the table \"events_raw\"")
-- MAGIC suite.test_equals(lambda: expected_table().count(), 2252, "The table should have 2252 records")
-- MAGIC
-- MAGIC first_five = lambda: [r["timestamp"] for r in expected_table().orderBy(F.col("timestamp").asc()).limit(5).collect()]
-- MAGIC suite.test_sequence(first_five, [1593879303631, 1593879304224, 1593879305465, 1593879305482, 1593879305746], True, "First 5 values are correct")
-- MAGIC
-- MAGIC last_five = lambda: [r["timestamp"] for r in expected_table().orderBy(F.col("timestamp").desc()).limit(5).collect()]
-- MAGIC suite.test_sequence(last_five, [1593881096290, 1593881095799, 1593881093452, 1593881093394, 1593881092076], True, "Last 5 values are correct")
-- MAGIC
-- MAGIC suite.display_results()
-- MAGIC assert suite.passed

-- COMMAND ----------

-- DBTITLE 0,--i18n-b3c62fea-b75d-41d6-8214-660f9cfa3acd
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Criar uma tabela Delta a partir dos resultados da query
-- MAGIC
-- MAGIC Além dos novos dados de eventos, também carregaremos uma pequena tabela de pesquisa com detalhes de produtos que usaremos posteriormente no curso.
-- MAGIC Use uma instrução CTAS para criar uma tabela Delta gerenciada chamada **`item_lookup`** que extrai dados do diretório Parquet fornecido abaixo. 

-- COMMAND ----------

-- TODO
-- <FILL_IN> ${da.paths.datasets}/ecommerce/raw/item-lookup

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TABLE item_lookup 
AS SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/item-lookup`

-- COMMAND ----------

-- DBTITLE 0,--i18n-5a971532-0003-4665-9064-26196cd31e89
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Execute a célula abaixo para confirmar que a tabela de pesquisa foi carregada corretamente.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC suite = DA.tests.new("Validate item_lookup")
-- MAGIC expected_table = lambda: spark.table("item_lookup")
-- MAGIC suite.test_not_none(lambda: expected_table(), "Created the table \"item_lookup\"")
-- MAGIC
-- MAGIC actual_values = lambda: [r["item_id"] for r in expected_table().collect()]
-- MAGIC expected_values = ['M_PREM_Q','M_STAN_F','M_PREM_F','M_PREM_T','M_PREM_K','P_DOWN_S','M_STAN_Q','M_STAN_K','M_STAN_T','P_FOAM_S','P_FOAM_K','P_DOWN_K']
-- MAGIC suite.test_sequence(actual_values, expected_values, False, "Contains the 12 expected item IDs")
-- MAGIC
-- MAGIC suite.display_results()
-- MAGIC assert suite.passed

-- COMMAND ----------

-- DBTITLE 0,--i18n-4db73493-3920-44e2-a19b-f335aa650f76
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC  
-- MAGIC Execute a célula a seguir para excluir as tabelas e arquivos associados a esta lição.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
