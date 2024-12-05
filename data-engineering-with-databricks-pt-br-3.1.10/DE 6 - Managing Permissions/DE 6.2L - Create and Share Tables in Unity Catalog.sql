-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-246366e0-139b-4ee6-8231-af9ffc8b9f20
-- MAGIC %md
-- MAGIC # Criar e compartilhar tabelas no Unity Catalog
-- MAGIC
-- MAGIC Neste notebook você aprenderá como:
-- MAGIC * Criar esquemas e tabelas
-- MAGIC * Controlar o acesso a esquemas e tabelas
-- MAGIC *Explorar concessões em vários objetos no Unity Catalog

-- COMMAND ----------

-- DBTITLE 0,--i18n-675fb98e-c798-4468-9231-710a39216650
-- MAGIC %md
-- MAGIC ## Configuração
-- MAGIC
-- MAGIC Execute as células a seguir para realizar a configuração. 
-- MAGIC
-- MAGIC Para evitar conflitos em um ambiente de treinamento compartilhado, isso gerará um nome de catálogo exclusivo para seu uso. 
-- MAGIC
-- MAGIC Em seu próprio ambiente, você é livre para escolher os nomes dos catálogos, mas tome cuidado para não afetar outros usuários e sistemas neste ambiente.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-06.2

-- COMMAND ----------

-- DBTITLE 0,--i18n-1f8f7a5b-7c09-4333-9057-f9e25f635f94
-- MAGIC %md
-- MAGIC ## Namespace de três níveis do Unity Catalog
-- MAGIC
-- MAGIC A maioria dos desenvolvedores de SQL está familiarizada com o uso de um namespace de dois níveis para lidar com tabelas em um esquema sem ambiguidades, como descrito a seguir:
-- MAGIC
-- MAGIC     SELECT * FROM schema.table;
-- MAGIC
-- MAGIC O Unity Catalog introduz o conceito de um *catálogo* que reside acima do esquema na hierarquia de objetos. Os metastores podem hospedar qualquer número de catálogos, que, por sua vez, podem hospedar qualquer número de esquemas. Para lidar com esse nível adicional, as referências completas de tabela no Unity Catalog usam namespaces de três níveis. A instrução a seguir exemplifica isso:
-- MAGIC
-- MAGIC     SELECT * FROM catalog.schema.table;
-- MAGIC     
-- MAGIC Os desenvolvedores de SQL provavelmente também estão familiarizados com a instrução **`USE`** para selecionar um esquema default, a fim de evitar a necessidade de sempre especificar um esquema ao fazer referência a tabelas. O Unity Catalog amplifica esse recurso com a instrução **`USE CATALOG`**, que também seleciona um catálogo default.
-- MAGIC
-- MAGIC Para simplificar sua experiência, criamos o catálogo e o definimos como default, como você pode ver no comando a seguir.

-- COMMAND ----------

SELECT current_catalog(), current_database()

-- COMMAND ----------

-- DBTITLE 0,--i18n-7608a78c-6f96-4f0b-a9fb-b303c76ad899
-- MAGIC %md
-- MAGIC ## Criar e usar um novo esquema
-- MAGIC
-- MAGIC Vamos criar um esquema exclusivamente para nosso uso neste exercício e, em seguida, defini-lo como default para podermos fazer referência a tabelas apenas pelo nome.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS my_own_schema;
USE my_own_schema;

SELECT current_database()

-- COMMAND ----------

-- DBTITLE 0,--i18n-013687c8-6a3f-4c8b-81c1-0afb0429914f
-- MAGIC %md
-- MAGIC ## Criar a arquitetura do Delta
-- MAGIC
-- MAGIC Vamos criar e preencher uma coleção simples de esquemas e tabelas de acordo com a arquitetura do Delta:
-- MAGIC * Um esquema prata contendo dados de frequência cardíaca de pacientes lidos de um dispositivo médico
-- MAGIC * Uma tabela de esquema ouro que calcula diariamente a média dos dados de frequência cardíaca por paciente
-- MAGIC
-- MAGIC Por enquanto, não haverá tabela bronze neste exemplo simples.
-- MAGIC
-- MAGIC Observe que precisamos apenas especificar o nome da tabela abaixo, pois definimos um catálogo e um esquema default acima.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS patient_silver;

CREATE OR REPLACE TABLE patient_silver.heartrate (
  device_id  INT,
  mrn        STRING,
  name       STRING,
  time       TIMESTAMP,
  heartrate  DOUBLE
);

INSERT INTO patient_silver.heartrate VALUES
  (23,'40580129','Nicholas Spears','2020-02-01T00:01:58.000+0000',54.0122153343),
  (17,'52804177','Lynn Russell','2020-02-01T00:02:55.000+0000',92.5136468131),
  (37,'65300842','Samuel Hughes','2020-02-01T00:08:58.000+0000',52.1354807863),
  (23,'40580129','Nicholas Spears','2020-02-01T00:16:51.000+0000',54.6477014191),
  (17,'52804177','Lynn Russell','2020-02-01T00:18:08.000+0000',95.033344842),
  (37,'65300842','Samuel Hughes','2020-02-01T00:23:58.000+0000',57.3391541312),
  (23,'40580129','Nicholas Spears','2020-02-01T00:31:58.000+0000',56.6165053697),
  (17,'52804177','Lynn Russell','2020-02-01T00:32:56.000+0000',94.8134313932),
  (37,'65300842','Samuel Hughes','2020-02-01T00:38:54.000+0000',56.2469995332),
  (23,'40580129','Nicholas Spears','2020-02-01T00:46:57.000+0000',54.8372685558)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS patient_gold;

CREATE OR REPLACE TABLE patient_gold.heartrate_stats AS (
  SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
  FROM patient_silver.heartrate
  GROUP BY mrn, name, DATE_TRUNC("DD", time)
);
  
SELECT * FROM patient_gold.heartrate_stats;  

-- COMMAND ----------

-- DBTITLE 0,--i18n-3961e85e-2bba-460a-9e00-12e37a07cb87
-- MAGIC %md
-- MAGIC ## Conceder acesso ao esquema ouro [optional]
-- MAGIC
-- MAGIC Agora vamos permitir que os usuários do grupo **account users** leiam o esquema **gold**.
-- MAGIC
-- MAGIC Execute esta seção removendo o comentário das células de código e executando-as em sequência. 
-- MAGIC Você também verá uma solicitação para executar algumas queries. 
-- MAGIC
-- MAGIC Para fazer isso:
-- MAGIC 1. Abra uma tab separada do navegador e carregue seu Databricks workspace.
-- MAGIC 1. Alterne para o Databricks SQL clicando no alternador de aplicativos e selecionando SQL.
-- MAGIC 1. Crie um SQL warehouse seguindo as instruções em *Criar um SQL Warehouse no Unity Catalog*.
-- MAGIC 1. Prepare-se para inserir queries no ambiente conforme as instruções abaixo.

-- COMMAND ----------

-- DBTITLE 0,--i18n-e4ec17fc-7dfa-4d3f-a82f-02eca61e6e53
-- MAGIC %md
-- MAGIC Vamos conceder o privilégio **SELECT** na tabela **gold**.

-- COMMAND ----------

-- GRANT SELECT ON TABLE patient_gold.heartrate_stats to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-d20d5b94-b672-48cf-865a-7c9aa0629955
-- MAGIC %md
-- MAGIC ### Consultar a tabela como usuário
-- MAGIC
-- MAGIC Com a concessão **SELECT** implementada, tente consultar a tabela no ambiente do Databricks SQL.
-- MAGIC
-- MAGIC Execute a célula a seguir para gerar uma instrução de query que lê a tabela **gold**. Copie e cole a saída em uma nova query no ambiente SQL e execute-a.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"SELECT * FROM {DA.catalog_name}.patient_gold.heartrate_stats")

-- COMMAND ----------

-- DBTITLE 0,--i18n-3309187d-5a52-4b51-9198-7fc54ea9ca84
-- MAGIC %md
-- MAGIC Esse comando funciona em nosso caso, pois somos os proprietários da view. No entanto, ele não funcionará para outros membros do grupo **account users** porque o privilégio **SELECT** somente na tabela é insuficiente. O privilégio **USAGE** também é necessário nos elementos contendo os dados. Vamos corrigir isso agora executando o seguinte.

-- COMMAND ----------

-- GRANT USAGE ON CATALOG ${DA.catalog_name} TO `account users`;
-- GRANT USAGE ON SCHEMA patient_gold TO `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-a6fcc71b-ceb3-4067-acb2-85504ad4d6ea
-- MAGIC %md
-- MAGIC
-- MAGIC Repita a query no ambiente do Databricks SQL e, com essas duas concessões em vigor, a operação deverá ser bem-sucedida.

-- COMMAND ----------

-- DBTITLE 0,--i18n-5dd6b7c1-6fed-4d09-b808-0615a10b2502
-- MAGIC %md
-- MAGIC
-- MAGIC ## Explorar concessões
-- MAGIC
-- MAGIC Vamos explorar as concessões em alguns dos objetos na hierarquia do Unity Catalog, começando com a tabela **gold**.

-- COMMAND ----------

-- SHOW GRANT ON TABLE ${DA.catalog_name}.patient_gold.heartrate_stats

-- COMMAND ----------

-- DBTITLE 0,--i18n-e7b6ddda-b6ef-4558-a5b6-c9f97bb7db80
-- MAGIC %md
-- MAGIC No momento, há apenas a concessão **SELECT** que configuramos anteriormente. Agora vamos verificar as concessões em **silver**.

-- COMMAND ----------

SHOW TABLES IN ${DA.catalog_name}.patient_silver;

-- COMMAND ----------

-- SHOW GRANT ON TABLE ${DA.catalog_name}.patient_silver.heartrate

-- COMMAND ----------

-- DBTITLE 0,--i18n-1e9d3c88-af92-4755-a01c-6e0aefdd8c9d
-- MAGIC %md
-- MAGIC No momento, não há concessões nessa tabela, e somente o proprietário pode acessá-la.
-- MAGIC
-- MAGIC Agora vamos examinar o esquema que o contém.

-- COMMAND ----------

-- SHOW GRANT ON SCHEMA ${DA.catalog_name}.patient_silver

-- COMMAND ----------

-- DBTITLE 0,--i18n-17125954-7b25-40dc-ab84-5a159198e9fc
-- MAGIC %md
-- MAGIC No momento, não há concessões nesse esquema. 
-- MAGIC
-- MAGIC Agora vamos examinar o catálogo.

-- COMMAND ----------

-- SHOW GRANT ON CATALOG `${DA.catalog_name}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-7453efa0-62d0-4af2-901f-c222dd9b2d07
-- MAGIC %md
-- MAGIC Atualmente vemos a concessão de** USAGE** que configuramos anteriormente.

-- COMMAND ----------

-- DBTITLE 0,--i18n-b4f88042-e7cb-4a1f-b853-cb328356778c
-- MAGIC %md
-- MAGIC ## Limpar
-- MAGIC Execute a célula a seguir para remover o esquema que criamos neste exemplo.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
