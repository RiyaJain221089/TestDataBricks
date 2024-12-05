-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-2ad42144-605b-486f-ad65-ca24b47b1924
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC # Limpando dados
-- MAGIC
-- MAGIC À medida que inspecionamos e limpamos os dados, precisaremos construir várias expressões de coluna e queries para expressar transformações a serem aplicadas no dataset.
-- MAGIC
-- MAGIC As expressões de coluna são construídas a partir de colunas, operadores e funções integradas existentes. Elas podem ser usadas em instruções **`SELECT`** para expressar transformações que criam novas colunas.
-- MAGIC
-- MAGIC Há vários comandos de consulta SQL padrão (por exemplo, **`DISTINCT`**, **`WHERE`**, **`GROUP BY`** etc.) disponíveis no Spark SQL para expressar transformações.
-- MAGIC
-- MAGIC Neste notebook, revisaremos alguns conceitos que podem diferir de outros sistemas que você conhece, além de destacar algumas funções úteis para operações comuns.
-- MAGIC
-- MAGIC Prestaremos atenção especial aos comportamentos em torno de valores **`NULL`**, bem como à formatação de strings e campos de data e hora.
-- MAGIC
-- MAGIC ## Objetivos de aprendizado
-- MAGIC Ao final desta lição, você deverá ser capaz de:
-- MAGIC - Resumir datasets e descrever comportamentos null
-- MAGIC - Recuperar e remover duplicatas
-- MAGIC - Validar datasets para verificar contagens esperadas, valores ausentes e registros duplicados
-- MAGIC - Aplicar transformações comuns para limpar e transformar dados

-- COMMAND ----------

-- DBTITLE 0,--i18n-2a604768-1aac-40e2-8396-1e15de60cc96
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Executar a configuração
-- MAGIC
-- MAGIC O script de configuração criará os dados e declarará os valores necessários para a execução do restante deste notebook.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.4

-- COMMAND ----------

-- DBTITLE 0,--i18n-31202e20-c326-4fa0-8892-ab9308b4b6f0
-- MAGIC %md
-- MAGIC ## Visão geral dos dados
-- MAGIC
-- MAGIC Trabalharemos com registros de novos usuários da tabela **`users_dirty`**, que tem o seguinte esquema:
-- MAGIC
-- MAGIC | campo | tipo | descrição |
-- MAGIC |---|---|---|
-- MAGIC | user_id | string | identificador exclusivo |
-- MAGIC | user_first_touch_timestamp | long | hora em que o registro do usuário foi criado em microssegundos começando pela época |
-- MAGIC | email | string | endereço de email mais recente fornecido pelo usuário para concluir uma ação |
-- MAGIC | updated | timestamp | hora em que este registro foi atualizado pela última vez |
-- MAGIC
-- MAGIC Vamos começar contando os valores em cada campo dos dados.

-- COMMAND ----------

SELECT count(*), count(user_id), count(user_first_touch_timestamp), count(email), count(updated)
FROM users_dirty

-- COMMAND ----------

-- DBTITLE 0,--i18n-c414c24e-3b72-474b-810d-c3df32032c26
-- MAGIC %md
-- MAGIC
-- MAGIC ## Inspecionar dados ausentes
-- MAGIC
-- MAGIC As contagens acima indicam que há alguns valores nulos em todos os campos.
-- MAGIC
-- MAGIC **OBSERVAÇÃO:** Valores nulos se comportam incorretamente em algumas funções matemáticas, incluindo **`count()`**.
-- MAGIC
-- MAGIC A função - **`count(col)`** ignora valores **`NULL`** ao contar colunas ou expressões específicas.
-- MAGIC - **`count(*)`** é um caso especial que conta o número total de linhas (incluindo linhas que são apenas valores **`NULL`**).
-- MAGIC
-- MAGIC Podemos contar valores nulos em um campo filtrando os registros onde o campo é nulo usando:  
-- MAGIC **`count_if(col IS NULL)`** ou **`count(*)`** com um filtro em que **`col IS NULL`**. 
-- MAGIC
-- MAGIC Ambas as instruções abaixo contam corretamente os registros com emails ausentes.

-- COMMAND ----------

SELECT count_if(email IS NULL) FROM users_dirty;
SELECT count(*) FROM users_dirty WHERE email IS NULL;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from pyspark.sql.functions import col
-- MAGIC usersDF = spark.read.table("users_dirty")
-- MAGIC
-- MAGIC usersDF.selectExpr("count_if(email IS NULL)")
-- MAGIC usersDF.where(col("email").isNull()).count()

-- COMMAND ----------

-- DBTITLE 0,--i18n-ea1ca35c-6421-472b-b70b-4f36bdab6d79
-- MAGIC %md
-- MAGIC  
-- MAGIC ## Desduplicar linhas
-- MAGIC Podemos usar **`DISTINCT *`** para remover registros duplicados verdadeiros onde linhas inteiras contêm os mesmos valores.

-- COMMAND ----------

SELECT DISTINCT(*) FROM users_dirty

-- COMMAND ----------

-- MAGIC %python
-- MAGIC usersDF.distinct().display()

-- COMMAND ----------

-- DBTITLE 0,--i18n-5da6599b-756c-4d22-85cd-114ff02fc19d
-- MAGIC %md
-- MAGIC
-- MAGIC   
-- MAGIC ## Desduplicar linhas com base em colunas específicas
-- MAGIC
-- MAGIC O código abaixo usa **`GROUP BY`** para remover registros duplicados com base nos valores de coluna **`user_id`** e **`user_first_touch_timestamp`**. (Lembre-se de que esses campos são gerados quando um determinado usuário é encontrado pela primeira vez, formando assim tuplas exclusivas.)
-- MAGIC
-- MAGIC No passo a seguir, usamos a função agregada **`max`** como uma solução alternativa para:
-- MAGIC - Manter os valores das colunas **`email`** e **`updated`** no resultado do agrupamento
-- MAGIC - Capturar emails não nulos quando houver múltiplos registros presentes

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW deduped_users AS 
SELECT user_id, user_first_touch_timestamp, max(email) AS email, max(updated) AS updated
FROM users_dirty
WHERE user_id IS NOT NULL
GROUP BY user_id, user_first_touch_timestamp;

SELECT count(*) FROM deduped_users

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import max
-- MAGIC dedupedDF = (usersDF
-- MAGIC     .where(col("user_id").isNotNull())
-- MAGIC     .groupBy("user_id", "user_first_touch_timestamp")
-- MAGIC     .agg(max("email").alias("email"), 
-- MAGIC          max("updated").alias("updated"))
-- MAGIC     )
-- MAGIC
-- MAGIC dedupedDF.count()

-- COMMAND ----------

-- DBTITLE 0,--i18n-5e2c98db-ea2d-44dc-b2ae-680dfd85c74b
-- MAGIC %md
-- MAGIC
-- MAGIC Vamos confirmar se temos a contagem esperada de registros restantes após a desduplicação com base em valores **`user_id`** e **`user_first_touch_timestamp`** distintos.

-- COMMAND ----------

SELECT COUNT(DISTINCT(user_id, user_first_touch_timestamp))
FROM users_dirty
WHERE user_id IS NOT NULL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (usersDF
-- MAGIC     .dropDuplicates(["user_id", "user_first_touch_timestamp"])
-- MAGIC     .filter(col("user_id").isNotNull())
-- MAGIC     .count())

-- COMMAND ----------

-- DBTITLE 0,--i18n-776b4ee7-9f29-4a19-89da-1872a1f8cafa
-- MAGIC %md
-- MAGIC
-- MAGIC ## Validar datasets
-- MAGIC Com base na revisão manual acima, confirmamos visualmente que temos as contagens esperadas.
-- MAGIC  
-- MAGIC Também podemos fazer validações programáticas usando filtros simples e cláusulas **`WHERE`**.
-- MAGIC
-- MAGIC Valide que o **`user_id`** em cada linha é único.

-- COMMAND ----------

SELECT max(row_count) <= 1 no_duplicate_ids FROM (
  SELECT user_id, count(*) AS row_count
  FROM deduped_users
  GROUP BY user_id)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import count
-- MAGIC
-- MAGIC display(dedupedDF
-- MAGIC     .groupBy("user_id")
-- MAGIC     .agg(count("*").alias("row_count"))
-- MAGIC     .select((max("row_count") <= 1).alias("no_duplicate_ids")))

-- COMMAND ----------

-- DBTITLE 0,--i18n-d405e7cd-9add-44e3-976a-e56b8cdf9d83
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Confirme que cada email está associado a no máximo um **`user_id`**.

-- COMMAND ----------

SELECT max(user_id_count) <= 1 at_most_one_id FROM (
  SELECT email, count(user_id) AS user_id_count
  FROM deduped_users
  WHERE email IS NOT NULL
  GROUP BY email)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(dedupedDF
-- MAGIC     .where(col("email").isNotNull())
-- MAGIC     .groupby("email")
-- MAGIC     .agg(count("user_id").alias("user_id_count"))
-- MAGIC     .select((max("user_id_count") <= 1).alias("at_most_one_id")))

-- COMMAND ----------

-- DBTITLE 0,--i18n-8630c04d-0752-404f-bfd1-bb96f7b06ffa
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## Formato de data e regex
-- MAGIC Agora que removemos os campos nulos e eliminamos as duplicatas, podemos extrair mais valor dos dados.
-- MAGIC
-- MAGIC O código abaixo:
-- MAGIC - Dimensiona e converte corretamente **`user_first_touch_timestamp`** em um carimbo de data/hora válido
-- MAGIC - Extrai a data do calendário e a hora do relógio para o carimbo de data/hora em um formato legível por humanos
-- MAGIC - Usa **`regexp_extract`** para extrair os domínios da coluna de email usando regex

-- COMMAND ----------

SELECT *, 
  date_format(first_touch, "MMM d, yyyy") AS first_touch_date,
  date_format(first_touch, "HH:mm:ss") AS first_touch_time,
  regexp_extract(email, "(?<=@).+", 0) AS email_domain
FROM (
  SELECT *,
    CAST(user_first_touch_timestamp / 1e6 AS timestamp) AS first_touch 
  FROM deduped_users
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import date_format, regexp_extract
-- MAGIC
-- MAGIC display(dedupedDF
-- MAGIC     .withColumn("first_touch", (col("user_first_touch_timestamp") / 1e6).cast("timestamp"))
-- MAGIC     .withColumn("first_touch_date", date_format("first_touch", "MMM d, yyyy"))
-- MAGIC     .withColumn("first_touch_time", date_format("first_touch", "HH:mm:ss"))
-- MAGIC     .withColumn("email_domain", regexp_extract("email", "(?<=@).+", 0))
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 0,--i18n-c9e02918-f105-4c12-b553-3897fa7387cc
-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Execute a célula a seguir para excluir as tabelas e arquivos associados a esta lição.

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
