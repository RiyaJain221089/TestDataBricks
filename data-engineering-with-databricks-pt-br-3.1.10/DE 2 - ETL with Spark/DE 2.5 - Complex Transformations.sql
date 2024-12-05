-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-daae326c-e59e-429b-b135-5662566b6c34
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Transformações complexas
-- MAGIC
-- MAGIC Consultar dados tabulares armazenados no data lakehouse com o Spark SQL é fácil, eficiente e rápido.
-- MAGIC
-- MAGIC A complexidade desse processo aumenta à medida que a estrutura de dados se torna menos regular, quando muitas tabelas precisam ser usadas em uma única query ou quando o formato dos dados precisa ser alterado drasticamente. Este notebook apresenta uma série de funções presentes no Spark SQL para ajudar os engenheiros a realizar até mesmo as transformações mais complicadas.
-- MAGIC
-- MAGIC ## Objetivos de aprendizado
-- MAGIC Ao final desta lição, você deverá ser capaz de:
-- MAGIC - Usar a sintaxe **`.`** e **`:`** para consultar dados aninhados
-- MAGIC - Extrair strings JSON e convertê-las em estruturas
-- MAGIC - Nivelar e descompactar matrizes e estruturas
-- MAGIC - Combinar datasets usando junções
-- MAGIC - Remodelar dados usando tabelas dinâmicas

-- COMMAND ----------

-- DBTITLE 0,--i18n-b01af8a2-da4a-4c8f-896e-790a60dc8d0c
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Executar a configuração
-- MAGIC
-- MAGIC O script de configuração criará os dados e declarará os valores necessários para a execução do restante deste notebook.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.5

-- COMMAND ----------

-- DBTITLE 0,--i18n-a6be8b8a-1c1f-40dd-a71c-8e91ae079b5c
-- MAGIC %md
-- MAGIC
-- MAGIC ## Visão geral dos dados
-- MAGIC
-- MAGIC A tabela **`events_raw`** foi registrada com base nos dados que representam uma carga do Kafka. Na maioria dos casos, os dados do Kafka são valores JSON com codificação binária. 
-- MAGIC
-- MAGIC Vamos converter **`key`** e **`value`** em strings para visualizar esses valores em um formato legível por humanos.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_strings AS 
SELECT string(key), string(value) FROM events_raw;

SELECT * FROM events_strings

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC events_stringsDF = (spark
-- MAGIC     .table("events_raw")
-- MAGIC     .select(col("key").cast("string"), 
-- MAGIC             col("value").cast("string"))
-- MAGIC     )
-- MAGIC display(events_stringsDF)

-- COMMAND ----------

-- DBTITLE 0,--i18n-67712d1a-cae1-41dc-8f7b-cc97e933128e
-- MAGIC %md
-- MAGIC ## Manipular tipos complexos

-- COMMAND ----------

-- DBTITLE 0,--i18n-c6a0cd9e-3bdc-463a-879a-5551fa9a8449
-- MAGIC %md
-- MAGIC
-- MAGIC ### Trabalhar com dados aninhados
-- MAGIC
-- MAGIC A célula de código abaixo consulta as strings convertidas para visualizar um objeto JSON de exemplo sem campos nulos (precisaremos desse resultado na próxima seção).
-- MAGIC
-- MAGIC **OBSERVAÇÃO:** O Spark SQL tem funcionalidade integrada para interagir diretamente com dados aninhados armazenados como strings JSON ou tipos de estrutura.
-- MAGIC - Use a sintaxe **`:`** em queries para acessar subcampos em strings JSON
-- MAGIC - Use a sintaxe **`.`** em queries para acessar subcampos em tipos de estrutura

-- COMMAND ----------

SELECT * FROM events_strings WHERE value:event_name = "finalize" ORDER BY key LIMIT 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(events_stringsDF
-- MAGIC     .where("value:event_name = 'finalize'")
-- MAGIC     .orderBy("key")
-- MAGIC     .limit(1)
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 0,--i18n-914b04cd-a1c1-4a91-aea3-ecd87714ea7d
-- MAGIC %md
-- MAGIC Vamos usar o exemplo de string JSON acima para derivar o esquema e, em seguida, extrair toda a coluna JSON e converter os dados em tipos de estrutura.
-- MAGIC - **`schema_of_json()`** retorna o esquema derivado de uma string JSON de exemplo.
-- MAGIC - **`from_json()`** extrai uma coluna contendo uma string JSON e converte-a em um tipo de estrutura usando o esquema especificado.
-- MAGIC
-- MAGIC Depois de descompactar a string JSON como um tipo de estrutura, vamos descompactar e nivelar todos os campos de estrutura em colunas.
-- MAGIC A descompactação de - **`*`** pode ser usada para nivelar estruturas; **`col_name.*`** extrai os subcampos de **`col_name`** em suas próprias colunas.

-- COMMAND ----------

SELECT schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}') AS schema

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_events AS SELECT json.* FROM (
SELECT from_json(value, 'STRUCT<device: STRING, ecommerce: STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name: STRING, event_previous_timestamp: BIGINT, event_timestamp: BIGINT, geo: STRUCT<city: STRING, state: STRING>, items: ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source: STRING, user_first_touch_timestamp: BIGINT, user_id: STRING>') AS json 
FROM events_strings);

SELECT * FROM parsed_events

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import from_json, schema_of_json
-- MAGIC
-- MAGIC json_string = """
-- MAGIC {"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1047.6,"total_item_quantity":2,"unique_items":2},"event_name":"finalize","event_previous_timestamp":1593879787820475,"event_timestamp":1593879948830076,"geo":{"city":"Huntington Park","state":"CA"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_Q","item_name":"Standard Queen Mattress","item_revenue_in_usd":940.5,"price_in_usd":1045.0,"quantity":1},{"coupon":"NEWBED10","item_id":"P_DOWN_S","item_name":"Standard Down Pillow","item_revenue_in_usd":107.10000000000001,"price_in_usd":119.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593583891412316,"user_id":"UA000000106459577"}
-- MAGIC """
-- MAGIC parsed_eventsDF = (events_stringsDF
-- MAGIC     .select(from_json("value", schema_of_json(json_string)).alias("json"))
-- MAGIC     .select("json.*")
-- MAGIC )
-- MAGIC
-- MAGIC display(parsed_eventsDF)

-- COMMAND ----------

-- DBTITLE 0,--i18n-5ca54e9c-dcb7-4177-99ab-77377ce8d899
-- MAGIC %md
-- MAGIC ### Manipular matrizes
-- MAGIC
-- MAGIC O Spark SQL tem diversas funções para manipular dados de matrizes, incluindo:
-- MAGIC - **`explode()`** separa os elementos de uma matriz em múltiplas linhas, o que cria uma nova linha para cada elemento.
-- MAGIC - **`size()`** fornece uma contagem do número de elementos em uma matriz para cada linha.
-- MAGIC
-- MAGIC O código abaixo explode o campo **`items`** (uma matriz de estruturas) em múltiplas linhas e mostra eventos contendo matrizes com três ou mais itens.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW exploded_events AS
SELECT *, explode(items) AS item
FROM parsed_events;

SELECT * FROM exploded_events WHERE size(items) > 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import explode, size
-- MAGIC
-- MAGIC exploded_eventsDF = (parsed_eventsDF
-- MAGIC     .withColumn("item", explode("items"))
-- MAGIC )
-- MAGIC
-- MAGIC display(exploded_eventsDF.where(size("items") > 2))

-- COMMAND ----------

DESCRIBE exploded_events

-- COMMAND ----------

-- DBTITLE 0,--i18n-0810444d-1ce9-4cb7-9ba9-f4596e84d895
-- MAGIC %md
-- MAGIC O código abaixo combina transformações de matriz para criar uma tabela que mostra a coleção exclusiva de ações e os itens no carrinho do usuário.
-- MAGIC - **`collect_set()`** coleta valores exclusivos para um campo, incluindo campos dentro de matrizes.
-- MAGIC - **`flatten()`** combina várias matrizes em uma única matriz.
-- MAGIC - **`array_distinct()`** remove elementos duplicados de uma matriz.

-- COMMAND ----------

SELECT user_id,
  collect_set(event_name) AS event_history,
  array_distinct(flatten(collect_set(items.item_id))) AS cart_history
FROM exploded_events
GROUP BY user_id

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import array_distinct, collect_set, flatten
-- MAGIC
-- MAGIC display(exploded_eventsDF
-- MAGIC     .groupby("user_id")
-- MAGIC     .agg(collect_set("event_name").alias("event_history"),
-- MAGIC             array_distinct(flatten(collect_set("items.item_id"))).alias("cart_history"))
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 0,--i18n-8744b315-393b-4f8b-a8c1-3d6f9efa93b0
-- MAGIC %md
-- MAGIC  
-- MAGIC ## Combinar e remodelar dados

-- COMMAND ----------

-- DBTITLE 0,--i18n-15407508-ba1c-4aef-bd40-1c8eb244ed83
-- MAGIC %md
-- MAGIC  
-- MAGIC ### Unir tabelas
-- MAGIC
-- MAGIC O Spark SQL dá suporte a operações **`JOIN`** padrão (inner, outer, left, right, anti, cross, semi).  
-- MAGIC Neste passo, vamos unir o dataset de eventos explodidos a uma tabela de pesquisa para obter o nome do item impresso padrão.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW item_purchases AS

SELECT * 
FROM (SELECT *, explode(items) AS item FROM sales) a
INNER JOIN item_lookup b
ON a.item.item_id = b.item_id;

SELECT * FROM item_purchases

-- COMMAND ----------

-- MAGIC %python
-- MAGIC exploded_salesDF = (spark
-- MAGIC     .table("sales")
-- MAGIC     .withColumn("item", explode("items"))
-- MAGIC )
-- MAGIC
-- MAGIC itemsDF = spark.table("item_lookup")
-- MAGIC
-- MAGIC item_purchasesDF = (exploded_salesDF
-- MAGIC     .join(itemsDF, exploded_salesDF.item.item_id == itemsDF.item_id)
-- MAGIC )
-- MAGIC
-- MAGIC display(item_purchasesDF)

-- COMMAND ----------

-- DBTITLE 0,--i18n-6c1f0e6f-c4f0-4b86-bf02-783160ea00f7
-- MAGIC %md
-- MAGIC ### Tabelas dinâmicas
-- MAGIC
-- MAGIC Podemos usar **`PIVOT`** para visualizar dados de diferentes perspectivas, girando valores exclusivos em uma coluna dinâmica especificada em várias colunas com base em uma função agregada.
-- MAGIC -A cláusula **`PIVOT`** segue o nome da tabela ou a subquery especificada em uma cláusula **`FROM`**, que é a entrada para a tabela dinâmica.
-- MAGIC - Os valores exclusivos na coluna dinâmica são agrupados e agregados usando a expressão de agregação fornecida, criando uma coluna separada para cada valor exclusivo na tabela dinâmica resultante.
-- MAGIC
-- MAGIC A célula de código a seguir usa **`PIVOT`** para nivelar as informações de compra do item contidas em vários campos derivados do dataset **`sales`**. O formato nivelado dos dados pode ser útil para criar painéis, mas também para aplicar algoritmos de machine learning para inferência ou previsão.

-- COMMAND ----------

SELECT *
FROM item_purchases
PIVOT (
  sum(item.quantity) FOR item_id IN (
    'P_FOAM_K',
    'M_STAN_Q',
    'P_FOAM_S',
    'M_PREM_Q',
    'M_STAN_F',
    'M_STAN_T',
    'M_PREM_K',
    'M_PREM_F',
    'M_STAN_K',
    'M_PREM_T',
    'P_DOWN_S',
    'P_DOWN_K')
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC transactionsDF = (item_purchasesDF
-- MAGIC     .groupBy("order_id", 
-- MAGIC         "email",
-- MAGIC         "transaction_timestamp", 
-- MAGIC         "total_item_quantity", 
-- MAGIC         "purchase_revenue_in_usd", 
-- MAGIC         "unique_items",
-- MAGIC         "items",
-- MAGIC         "item",
-- MAGIC         "name",
-- MAGIC         "price")
-- MAGIC     .pivot("item_id")
-- MAGIC     .sum("item.quantity")
-- MAGIC )
-- MAGIC display(transactionsDF)

-- COMMAND ----------

-- DBTITLE 0,--i18n-b89c0c3e-2352-4a82-973d-7e655276bede
-- MAGIC %md
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
