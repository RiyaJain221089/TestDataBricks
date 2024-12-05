-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-af5bea55-ebfc-4d31-a91f-5d6cae2bc270
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Laboratório de remodelagem de dados
-- MAGIC
-- MAGIC Neste laboratório, você criará uma tabela **`clickpaths`** que agrega o número de vezes que cada usuário realizou uma determinada ação em **`events`** e unirá essas informações em uma view simplificada de **`transactions`** para criar um registro das ações e compras finais de cada usuário.
-- MAGIC
-- MAGIC A tabela **`clickpaths`** deve conter todos os campos de **`transactions`**, bem como uma contagem de cada **`event_name`** de **`events`** em sua própria coluna. Essa tabela deve conter uma única linha para cada usuário que concluiu uma compra.
-- MAGIC
-- MAGIC ## Objetivos de aprendizado
-- MAGIC Ao final deste laboratório, você será capaz de:
-- MAGIC - Dinamizar e unir tabelas para criar caminhos de clique para cada usuário

-- COMMAND ----------

-- DBTITLE 0,--i18n-5258fa9b-065e-466d-9983-89f0be627186
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Executar a configuração
-- MAGIC
-- MAGIC O script de configuração criará os dados e declarará os valores necessários para a execução do restante deste notebook.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.6L

-- COMMAND ----------

-- DBTITLE 0,--i18n-082bfa19-8e4e-49f7-bd5d-cd833c471109
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Usaremos Python para executar verificações ocasionalmente durante o laboratório. As funções auxiliares abaixo retornarão um erro com uma mensagem informando o que precisa ser alterado caso você não tenha seguido as instruções. Nenhuma saída será gerada se você tiver concluído este passo.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, num_rows, column_names):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert set(spark.table(table_name).columns) == set(column_names), "Please name the columns as shown in the schema above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- DBTITLE 0,--i18n-2799ea4f-4a8e-4ad4-8dc1-a8c1f807c6d7
-- MAGIC %md
-- MAGIC
-- MAGIC ## Dinamizar eventos para obter contagens de eventos para cada usuário
-- MAGIC
-- MAGIC Vamos começar dinamizando a tabela **`events`** para obter contagens para cada **`event_name`**.
-- MAGIC
-- MAGIC Queremos agregar o número de vezes que cada usuário realizou um evento específico, especificado na coluna **`event_name`**. Para fazer isso, agrupe por **`user_id`** e dinamize por **`event_name`** para fornecer uma contagem de cada tipo de evento em uma coluna separada, resultando no esquema abaixo. Observe que **`user_id`** é renomeado para **`user`** no esquema de destino.
-- MAGIC
-- MAGIC | campo | tipo | 
-- MAGIC | --- | --- | 
-- MAGIC | user | STRING |
-- MAGIC | cart | BIGINT |
-- MAGIC | pillows | BIGINT |
-- MAGIC | login | BIGINT |
-- MAGIC | main | BIGINT |
-- MAGIC | careers | BIGINT |
-- MAGIC | guest | BIGINT |
-- MAGIC | faq | BIGINT |
-- MAGIC | down | BIGINT |
-- MAGIC | warranty | BIGINT |
-- MAGIC | finalize | BIGINT |
-- MAGIC | register | BIGINT |
-- MAGIC | shipping_info | BIGINT |
-- MAGIC | checkout | BIGINT |
-- MAGIC | mattresses | BIGINT |
-- MAGIC | add_item | BIGINT |
-- MAGIC | press | BIGINT |
-- MAGIC | email_coupon | BIGINT |
-- MAGIC | cc_info | BIGINT |
-- MAGIC | foam | BIGINT |
-- MAGIC | reviews | BIGINT |
-- MAGIC | original | BIGINT |
-- MAGIC | delivery | BIGINT |
-- MAGIC | premium | BIGINT |
-- MAGIC
-- MAGIC Uma lista dos nomes dos eventos é fornecida nas células TODO abaixo.

-- COMMAND ----------

-- DBTITLE 0,--i18n-0d995af9-e6f3-47b0-8b78-44bda953fa37
-- MAGIC %md
-- MAGIC
-- MAGIC ### Resolver com SQL

-- COMMAND ----------

-- TODO
-- CREATE OR REPLACE TEMP VIEW events_pivot
-- <FILL_IN>
-- ("cart", "pillows", "login", "main", "careers", "guest", "faq", "down", "warranty", "finalize", 
-- "register", "shipping_info", "checkout", "mattresses", "add_item", "press", "email_coupon", 
-- "cc_info", "foam", "reviews", "original", "delivery", "premium")

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TEMP VIEW events_pivot AS
SELECT * FROM (
  SELECT user_id user, event_name 
  FROM events
) PIVOT ( count(*) FOR event_name IN (
    "cart", "pillows", "login", "main", "careers", "guest", "faq", "down", "warranty", "finalize", 
    "register", "shipping_info", "checkout", "mattresses", "add_item", "press", "email_coupon", 
    "cc_info", "foam", "reviews", "original", "delivery", "premium" ))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # SOURCE_ONLY
-- MAGIC # for testing only; include checks after each language solution
-- MAGIC check_table_results("events_pivot", 204586, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium'])

-- COMMAND ----------

-- DBTITLE 0,--i18n-afd696e4-049d-47e1-b266-60c7310b169a
-- MAGIC %md
-- MAGIC
-- MAGIC ### Resolver com Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # TODO
-- MAGIC # (spark.read
-- MAGIC #     <FILL_IN>
-- MAGIC #     .createOrReplaceTempView("events_pivot"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # ANSWER
-- MAGIC (spark.read.table("events")
-- MAGIC     .groupBy("user_id")
-- MAGIC     .pivot("event_name")
-- MAGIC     .count()
-- MAGIC     .withColumnRenamed("user_id", "user")
-- MAGIC     .createOrReplaceTempView("events_pivot"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-2fe0f24b-2364-40a3-9656-c124d6515c4d
-- MAGIC %md
-- MAGIC
-- MAGIC ### Verifique seu trabalho
-- MAGIC Execute a célula abaixo para confirmar se a view foi criada corretamente.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("events_pivot", 204586, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium'])

-- COMMAND ----------

-- DBTITLE 0,--i18n-eaac4506-501a-436a-b2f3-3788c689e841
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Unir contagens de eventos e transações para todos os usuários
-- MAGIC
-- MAGIC A seguir, una **`events_pivot`** a **`transactions`** para criar a tabela **`clickpaths`**. Essa tabela deve ter as mesmas colunas de nome de evento da tabela **`events_pivot`** criada acima, seguidas pelas colunas da tabela **`transactions`**, conforme mostrado abaixo.
-- MAGIC
-- MAGIC | campo | tipo | 
-- MAGIC | --- | --- | 
-- MAGIC | user | STRING |
-- MAGIC | cart | BIGINT |
-- MAGIC | ... | ... |
-- MAGIC | user_id | STRING |
-- MAGIC | order_id | BIGINT |
-- MAGIC | transaction_timestamp | BIGINT |
-- MAGIC | total_item_quantity | BIGINT |
-- MAGIC | purchase_revenue_in_usd | DOUBLE |
-- MAGIC | unique_items | BIGINT |
-- MAGIC | P_FOAM_K | BIGINT |
-- MAGIC | M_STAN_Q | BIGINT |
-- MAGIC | P_FOAM_S | BIGINT |
-- MAGIC | M_PREM_Q | BIGINT |
-- MAGIC | M_STAN_F | BIGINT |
-- MAGIC | M_STAN_T | BIGINT |
-- MAGIC | M_PREM_K | BIGINT |
-- MAGIC | M_PREM_F | BIGINT |
-- MAGIC | M_STAN_K | BIGINT |
-- MAGIC | M_PREM_T | BIGINT |
-- MAGIC | P_DOWN_S | BIGINT |
-- MAGIC | P_DOWN_K | BIGINT |

-- COMMAND ----------

-- DBTITLE 0,--i18n-03571117-301e-4c35-849a-784621656a83
-- MAGIC %md
-- MAGIC
-- MAGIC ### Resolva com SQL

-- COMMAND ----------

-- TODO
-- CREATE OR REPLACE TEMP VIEW clickpaths AS
-- <FILL_IN>

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TEMP VIEW clickpaths AS
SELECT * 
FROM events_pivot a
JOIN transactions b 
  ON a.user = b.user_id

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # SOURCE_ONLY
-- MAGIC check_table_results("clickpaths", 9085, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium', 'user_id', 'order_id', 'transaction_timestamp', 'total_item_quantity', 'purchase_revenue_in_usd', 'unique_items', 'P_FOAM_K', 'M_STAN_Q', 'P_FOAM_S', 'M_PREM_Q', 'M_STAN_F', 'M_STAN_T', 'M_PREM_K', 'M_PREM_F', 'M_STAN_K', 'M_PREM_T', 'P_DOWN_S', 'P_DOWN_K'])

-- COMMAND ----------

-- DBTITLE 0,--i18n-e0ad84c7-93f0-4448-9846-4b56ba71acf8
-- MAGIC %md
-- MAGIC
-- MAGIC ### Resolva com Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # TODO
-- MAGIC # (spark.read
-- MAGIC #     <FILL_IN>
-- MAGIC #     .createOrReplaceTempView("clickpaths"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # ANSWER
-- MAGIC from pyspark.sql.functions import col
-- MAGIC (spark.read.table("events_pivot")
-- MAGIC     .join(spark.table("transactions"), col("events_pivot.user") == col("transactions.user_id"), "inner")
-- MAGIC     .createOrReplaceTempView("clickpaths"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-ac19c8e1-0ab9-4558-a0eb-a6c954e84167
-- MAGIC %md
-- MAGIC
-- MAGIC ### Verifique seu trabalho
-- MAGIC Execute a célula abaixo para confirmar se a visualização foi criada corretamente.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("clickpaths", 9085, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium', 'user_id', 'order_id', 'transaction_timestamp', 'total_item_quantity', 'purchase_revenue_in_usd', 'unique_items', 'P_FOAM_K', 'M_STAN_Q', 'P_FOAM_S', 'M_PREM_Q', 'M_STAN_F', 'M_STAN_T', 'M_PREM_K', 'M_PREM_F', 'M_STAN_K', 'M_PREM_T', 'P_DOWN_S', 'P_DOWN_K'])

-- COMMAND ----------

-- DBTITLE 0,--i18n-f352b51d-72ce-48d9-9944-a8f4c0a2a5ce
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
