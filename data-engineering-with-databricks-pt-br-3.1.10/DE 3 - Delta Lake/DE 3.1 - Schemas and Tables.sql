-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-4c4121ee-13df-479f-be62-d59452a5f261
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Esquemas e tabelas no Databricks
-- MAGIC Nesta demonstração, você criará e explorará esquemas e tabelas.
-- MAGIC
-- MAGIC ## Objetivos de aprendizado
-- MAGIC Ao final desta lição, você deverá ser capaz de:
-- MAGIC * Usar o Spark SQL DDL para definir esquemas e tabelas
-- MAGIC * Descrever como a palavra-chave **`LOCATION`** afeta o diretório de armazenamento default
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC **Recursos**
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Esquemas e tabelas - Documentação do Databricks</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">Tabelas gerenciadas e não gerenciadas</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">Criar uma tabela com a UI</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">Criar uma tabela local</a>
-- MAGIC * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">Salvar em tabelas persistentes</a>

-- COMMAND ----------

-- DBTITLE 0,--i18n-acb0c723-a2bf-4d00-b6cb-6e9aef114985
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Configuração da lição
-- MAGIC O script a seguir limpa as execuções anteriores desta demonstração e configura algumas variáveis do Hive que serão usadas nas consultas SQL.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.1

-- COMMAND ----------

-- DBTITLE 0,--i18n-cc3d2766-764e-44bb-a04b-b03ae9530b6d
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## Esquemas
-- MAGIC Vamos começar criando um esquema (banco de dados).

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_default_location;

-- COMMAND ----------

-- DBTITLE 0,--i18n-427db4b9-fa6c-47aa-ae70-b95087298362
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC  
-- MAGIC Observe que o primeiro esquema (banco de dados) usa o local default em **`dbfs:/user/hive/warehouse/`** e que o diretório do esquema é o nome do esquema com a extensão **`.db`**

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED ${da.schema_name}_default_location;

-- COMMAND ----------

-- DBTITLE 0,--i18n-a0fda220-4a73-419b-969f-664dd4b80024
-- MAGIC %md
-- MAGIC ## Tabelas gerenciadas
-- MAGIC
-- MAGIC Vamos criar uma tabela **gerenciada** (não especificando um caminho para o local).
-- MAGIC
-- MAGIC Criaremos a tabela no esquema (banco de dados) que criamos acima.
-- MAGIC
-- MAGIC Observe que o esquema da tabela deve ser definido porque não há dados dos quais as colunas e os tipos de dados da tabela podem ser inferidos

-- COMMAND ----------

USE ${da.schema_name}_default_location; 

CREATE OR REPLACE TABLE managed_table (width INT, length INT, height INT);
INSERT INTO managed_table 
VALUES (3, 2, 1);
SELECT * FROM managed_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-5c422056-45b4-419d-b4a6-2c3252e82575
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Podemos conferir a descrição estendida da tabela para encontrar o local (você precisará rolar para baixo nos resultados).

-- COMMAND ----------

DESCRIBE EXTENDED managed_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-bdc6475c-1c77-46a5-9ea1-04d5a538c225
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Por default, as tabelas **gerenciadas** em um esquema sem local especificado serão criadas no diretório **`dbfs:/user/hive/warehouse/<schema_name>.db/`**.
-- MAGIC
-- MAGIC Podemos ver que, como esperado, os dados e metadados da tabela estão armazenados nesse local.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_location = spark.sql(f"DESCRIBE DETAIL managed_table").first().location
-- MAGIC print(tbl_location)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 0,--i18n-507a84a5-f60f-4923-8f48-475ee3270dbd
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Descarte a tabela.

-- COMMAND ----------

DROP TABLE managed_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-0b390bf4-3e3b-4d1a-bcb8-296fa1a7edb8
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Observe que o diretório da tabela e seus arquivos de log e dados foram excluídos. Apenas o diretório do esquema (banco de dados) permanece.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC schema_default_location = spark.sql(f"DESCRIBE SCHEMA {DA.schema_name}_default_location").collect()[3].database_description_value
-- MAGIC print(schema_default_location)
-- MAGIC dbutils.fs.ls(schema_default_location)

-- COMMAND ----------

-- DBTITLE 0,--i18n-0e4046c8-2c3a-4bab-a14a-516cc0f41eda
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## Tabelas externas
-- MAGIC A seguir, criaremos uma tabela **externa** (não gerenciada) de dados de amostra. 
-- MAGIC
-- MAGIC Os dados que usaremos estão no formato CSV. Queremos criar uma tabela Delta com um **`LOCATION`** fornecido no diretório de nossa escolha.

-- COMMAND ----------

USE ${da.schema_name}_default_location;

CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${da.paths.datasets}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);
CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
  SELECT * FROM temp_delays;

SELECT * FROM external_table; 

-- COMMAND ----------

-- DBTITLE 0,--i18n-6b5d7597-1fc1-4747-b5bb-07f67d806c2b
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Observe a localização dos dados da tabela no diretório de trabalho desta lição.

-- COMMAND ----------

DESCRIBE EXTENDED external_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-72f7bef4-570b-4c20-9261-b763b66b6942
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Agora descartamos a tabela.

-- COMMAND ----------

DROP TABLE external_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-f71374ea-db51-4a2c-8920-9f8a000850df
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC A definição da tabela não existe mais no metastore, mas os dados subjacentes permanecem intactos.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_path = f"{DA.paths.working_dir}/external_table"
-- MAGIC files = dbutils.fs.ls(tbl_path)
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 0,--i18n-7defc948-a8e4-4019-9633-0886d653b7c6
-- MAGIC %md
-- MAGIC
-- MAGIC ## Limpar
-- MAGIC Descarte o esquema.

-- COMMAND ----------

DROP SCHEMA ${da.schema_name}_default_location CASCADE;

-- COMMAND ----------

-- DBTITLE 0,--i18n-bb4a8ae9-450b-479f-9e16-a76f1131bd1a
-- MAGIC %md
-- MAGIC
-- MAGIC Execute a célula a seguir para excluir as tabelas e arquivos associados a esta lição.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
