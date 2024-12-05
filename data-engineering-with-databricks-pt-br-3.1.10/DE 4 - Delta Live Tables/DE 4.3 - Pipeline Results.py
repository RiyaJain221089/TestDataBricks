# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-2d116d36-8ed8-44aa-8cfa-156e16f88492
# MAGIC %md
# MAGIC # Explorando os resultados de um pipeline do DLT
# MAGIC
# MAGIC
# MAGIC
# MAGIC Embora o DLT consiga abstrair boa parte da complexidade associada à execução de ETL de produção no Databricks, muitas pessoas podem querer saber o que realmente acontece nos bastidores.
# MAGIC
# MAGIC Neste notebook, evitaremos nos aprofundar demais em questões laterais, mas exploraremos como os dados e os metadados são persistidos pelo DLT.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.3

# COMMAND ----------

# DBTITLE 0,--i18n-ee147dd8-867c-44a7-a4d6-964a5178e8ff
# MAGIC %md
# MAGIC ## Consultando tabelas no banco de dados de destino
# MAGIC
# MAGIC Se o banco de dados de destino for especificado durante a configuração do pipeline do DLT, as tabelas deverão estar disponíveis para os usuários em todo o ambiente do Databricks.
# MAGIC
# MAGIC Execute a célula abaixo para ver as tabelas registradas no banco de dados utilizado nesta demonstração.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ${DA.schema_name};
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

# DBTITLE 0,--i18n-d5368f5c-7a7d-41e6-b6a2-7a6af2d95c15
# MAGIC %md
# MAGIC Observe que a view que definimos no pipeline está ausente da lista de tabelas.
# MAGIC
# MAGIC Resultados da query da tabela **`orders_bronze`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_bronze

# COMMAND ----------

# DBTITLE 0,--i18n-a6a09270-4f8e-4f17-95ab-5e82220a83ed
# MAGIC %md
# MAGIC
# MAGIC Lembre-se de que **`orders_bronze`** foi definido como uma streaming live table no DLT, mas os resultados aqui são estáticos.
# MAGIC
# MAGIC Como o DLT usa o Delta Lake para armazenar todas as tabelas, cada vez que uma query é executada, o retorno é sempre a versão mais recente da tabela. Mas as queries fora do DLT retornarão resultados de snapshot das tabelas do DLT, independentemente de sua definição.

# COMMAND ----------

# DBTITLE 0,--i18n-9439da5b-7ab5-4b31-a66d-ea50040f2501
# MAGIC %md
# MAGIC ## Examinar os resultados de `APPLY CHANGES INTO`
# MAGIC
# MAGIC Lembre-se de que a tabela **customers_silver** foi implementada com alterações de um feed de CDC aplicado como SCD de Tipo 1.
# MAGIC
# MAGIC Vamos consultar esta tabela no passo abaixo.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_silver

# COMMAND ----------

# DBTITLE 0,--i18n-20b2f8b4-9a16-4a8a-b63f-c974e9ab167f
# MAGIC %md
# MAGIC
# MAGIC A tabela **`customers_silver`** representa corretamente o estado ativo atual da nossa tabela de Tipo 1 com as alterações aplicadas. No entanto, a tabela **customers_silver** é na verdade implementada como uma view em uma tabela oculta chamada **__apply_changes_storage_customers_silver**, que inclui estes campos adicionais: **__Timestamp**, **__DeleteVersion** e **__UpsertVersion**.
# MAGIC
# MAGIC Podemos ver esse resultado quando executamos **`DESCRIBE EXTENDED`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED customers_silver

# COMMAND ----------

# DBTITLE 0,--i18n-6c70c0ce-abdd-4dab-99fb-661324056120
# MAGIC %md
# MAGIC Se consultarmos a tabela oculta, veremos os três campos. No entanto, nenhum usuário deve precisar interagir diretamente com essa tabela, que só é utilizada pelo DLT para garantir a aplicação das atualizações na ordem certa a fim de materializar os resultados corretamente.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM __apply_changes_storage_customers_silver

# COMMAND ----------

# DBTITLE 0,--i18n-6c64b04a-77be-4a2b-9d2b-dd7978b213f7
# MAGIC %md
# MAGIC ## Examinando arquivos de dados
# MAGIC
# MAGIC Execute a célula a seguir para ver os arquivos no **local de armazenamento** configurado.

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-54bf6537-bf01-4963-9b18-f16c4b2f7692
# MAGIC %md
# MAGIC Os diretórios **autoloader** e **checkpoint** contêm dados usados para gerenciar o processamento incremental de dados com Structured Streaming
# MAGIC
# MAGIC O diretório **system** captura eventos associados ao pipeline.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-a459d740-2091-40e0-8b47-d67ecdb2fd8e
# MAGIC %md
# MAGIC Os logs de eventos são armazenados como uma tabela Delta. Vamos consultar a tabela.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM delta.`{DA.paths.storage_location}/system/events`"))

# COMMAND ----------

# DBTITLE 0,--i18n-61fd77b8-9bd6-4440-a37a-f45169fbf4c0
# MAGIC %md
# MAGIC Vamos explorar as métricas mais a fundo no notebook a seguir.
# MAGIC
# MAGIC Vamos visualizar o conteúdo do diretório **tables**.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-a36ca049-9586-4551-8988-c1b8ec1da349
# MAGIC %md
# MAGIC Cada um desses diretórios contém uma tabela Delta Lake gerenciada pelo DLT.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
