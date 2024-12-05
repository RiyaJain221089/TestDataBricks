-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-5ec757b3-50cf-43ac-a74d-6902d3e18983
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # UDFs SQL e fluxo de controle
-- MAGIC
-- MAGIC
-- MAGIC ## Objetivos de aprendizado
-- MAGIC Ao final desta lição, você deverá ser capaz de:
-- MAGIC * Definir e registrar UDFs SQL
-- MAGIC * Descrever o modelo de segurança usado para compartilhar UDFs SQL
-- MAGIC * Usar instruções **`CASE`**/**`WHEN`** em código SQL
-- MAGIC * Usar instruções **`CASE`**/**`WHEN`** em UDFs SQL para personalizar o fluxo de controle

-- COMMAND ----------

-- DBTITLE 0,--i18n-fd5d37b8-b720-4a88-a2cf-9b3c43f697eb
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Executar a configuração
-- MAGIC Execute a célula a seguir para configurar o ambiente.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.7A

-- COMMAND ----------

-- DBTITLE 0,--i18n-e8fa445b-db52-43c4-a649-9904526c6a04
-- MAGIC %md
-- MAGIC
-- MAGIC ## Funções definidas pelo usuário
-- MAGIC
-- MAGIC As funções definidas pelo usuário (UDFs) no Spark SQL permitem registrar lógica SQL personalizada como funções em um banco de dados que podem ser reutilizadas em qualquer execução de SQL no Databricks. Essas funções são registradas nativamente em SQL e mantêm todas as otimizações do Spark ao aplicar lógica personalizada a grandes datasets.
-- MAGIC
-- MAGIC No mínimo, a criação de uma UDF SQL requer um nome de função, parâmetros opcionais, o tipo a ser retornado e uma lógica personalizada.
-- MAGIC
-- MAGIC A função simples chamada **`sale_announcement`**, mostrada abaixo, utiliza **`item_name`** e **`item_price`** como parâmetros. Essa função retorna uma string que anuncia a venda de um item por 80% do preço original.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION sale_announcement(item_name STRING, item_price INT)
RETURNS STRING
RETURN concat("The ", item_name, " is on sale for $", round(item_price * 0.8, 0));

SELECT *, sale_announcement(name, price) AS message FROM item_lookup

-- COMMAND ----------

-- DBTITLE 0,--i18n-5a5dfa8f-f9e7-4b5f-b229-30bed4497009
-- MAGIC %md
-- MAGIC
-- MAGIC Observe que essa função é aplicada a todos os valores da coluna de forma paralela dentro do mecanismo de processamento do Spark. As UDFs SQL são uma forma eficiente de definir lógica personalizada otimizada para execução no Databricks.

-- COMMAND ----------

-- DBTITLE 0,--i18n-f9735833-a4f3-4966-8739-eb351025dc28
-- MAGIC %md
-- MAGIC
-- MAGIC ## Escopo e permissões de UDFs SQL
-- MAGIC As funções SQL definidas pelo usuário:
-- MAGIC - Persistem entre ambientes de execução (que podem incluir notebooks, queries DBSQL e jobs).
-- MAGIC - Existem como objetos no metastore e são governadas pelas mesmas ACLs de tabela usadas por bancos de dados, tabelas ou views.
-- MAGIC - Para **criar** uma UDF SQL, você precisa definir **`USE CATALOG`** no catálogo e **`USE SCHEMA`** e **`CREATE FUNCTION`** no esquema.
-- MAGIC - Para **usar** uma UDF SQL, você precisa definir **`USE CATALOG`** no catálogo, **`USE SCHEMA`** no esquema e **`EXECUTE`** na função.
-- MAGIC
-- MAGIC Podemos usar **`DESCRIBE FUNCTION`** para verificar onde uma função foi registrada e obter informações básicas sobre as entradas esperadas e o retorno dado (é possível obter ainda mais informações com **`DESCRIBE FUNCTION EXTENDED`**).

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED sale_announcement

-- COMMAND ----------

-- DBTITLE 0,--i18n-091c02b4-07b5-4b2c-8e1e-8cb561eed5a3
-- MAGIC %md
-- MAGIC
-- MAGIC Observe que o campo **`Body`** na parte inferior da descrição da função mostra a lógica SQL usada na própria função.

-- COMMAND ----------

-- DBTITLE 0,--i18n-bf549dbc-edb7-465f-a310-0f5c04cfbe0a
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Funções simples de fluxo de controle
-- MAGIC
-- MAGIC Combinar UDFs SQL e fluxo de controle usando cláusulas **`CASE`**/**`WHEN`** otimizada a execução de fluxos de controle em cargas de trabalho SQL. A construção sintática de SQL padrão **`CASE`**/**`WHEN`** permite avaliar múltiplas instruções condicionais com resultados alternativos baseados no conteúdo da tabela.
-- MAGIC
-- MAGIC No passo a seguir, demonstramos o agrupamento dessa lógica de fluxo de controle em uma função que poderá ser reutilizada em qualquer execução de SQL. 

-- COMMAND ----------

CREATE OR REPLACE FUNCTION item_preference(name STRING, price INT)
RETURNS STRING
RETURN CASE 
  WHEN name = "Standard Queen Mattress" THEN "This is my default mattress"
  WHEN name = "Premium Queen Mattress" THEN "This is my favorite mattress"
  WHEN price > 100 THEN concat("I'd wait until the ", name, " is on sale for $", round(price * 0.8, 0))
  ELSE concat("I don't need a ", name)
END;

SELECT *, item_preference(name, price) FROM item_lookup

-- COMMAND ----------

-- DBTITLE 0,--i18n-14f5f9df-d17e-4e6a-90b0-d22bbc4e1e10
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Embora os exemplos fornecidos aqui sejam simples, esses mesmos princípios básicos podem ser usados para adicionar cálculos e lógica personalizados para execução nativa no Spark SQL. 
-- MAGIC
-- MAGIC Especialmente no caso de empresas migrando usuários de sistemas com muitos procedimentos definidos ou fórmulas personalizadas, as UDFs SQL podem permitir que alguns usuários definam a lógica complexa necessária para relatórios comuns e queries analíticas.

-- COMMAND ----------

-- DBTITLE 0,--i18n-451ef10d-9e38-4b71-ad69-9c2ed74601b5
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
