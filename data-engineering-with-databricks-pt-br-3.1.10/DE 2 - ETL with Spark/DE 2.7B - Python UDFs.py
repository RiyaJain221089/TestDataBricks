# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-7c0e5ecf-c2e4-4a89-a418-76faa15ce226
# MAGIC %md
# MAGIC
# MAGIC # Funções Python definidas pelo usuário
# MAGIC
# MAGIC ##### Objetivos
# MAGIC 1. Definir uma função
# MAGIC 1. Criar e aplicar uma UDF
# MAGIC 1. Criar e registrar uma UDF com sintaxe de decoradores do Python
# MAGIC 1. Criar e aplicar uma UDF Pandas (vetorizada)
# MAGIC
# MAGIC ##### Métodos
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.udf.html" target="_blank">Decorador de UDF Python</a>: **`@udf`**
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.pandas_udf.html" target="_blank">Decorador de UDF Pandas</a>: **`@pandas_udf`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.7B

# COMMAND ----------

# DBTITLE 0,--i18n-1e94c419-dd84-4f8d-917a-019b15fc6700
# MAGIC %md
# MAGIC
# MAGIC ### Função definida pelo usuário (UDF)
# MAGIC Uma função personalizada de transformação de coluna tem estas características:
# MAGIC
# MAGIC - A função não pode ser otimizada pelo otimizador Catalyst
# MAGIC - A função é serializada e enviada aos executores
# MAGIC - Os dados de linha são desserializados do formato binário nativo do Spark e passados para a UDF; os resultados são serializados de volta no formato nativo do Spark
# MAGIC - Em UDFs Python, há uma sobrecarga adicional de comunicação interprocessual entre o executor e um interpretador Python em execução em cada nó de worker

# COMMAND ----------

# DBTITLE 0,--i18n-4d1eb639-23fb-42fa-9b62-c407a0ccde2d
# MAGIC %md
# MAGIC
# MAGIC Nesta demonstração, usaremos os dados de vendas.

# COMMAND ----------

sales_df = spark.table("sales")
display(sales_df)

# COMMAND ----------

# DBTITLE 0,--i18n-05043672-b02a-4194-ba44-75d544f6af07
# MAGIC %md
# MAGIC
# MAGIC ### Definir uma função
# MAGIC
# MAGIC Defina uma função (no driver) para obter a primeira letra de uma string do campo **`email`**.

# COMMAND ----------

def first_letter_function(email):
    return email[0]

first_letter_function("annagray@kaufman.com")

# COMMAND ----------

# DBTITLE 0,--i18n-17f25aa9-c20f-41da-bac5-95ebb413dcd4
# MAGIC %md
# MAGIC
# MAGIC ### Criar e aplicar uma UDF
# MAGIC Registre a função como uma UDF. Essa ação serializa a função e a envia aos executores para permitir a transformação dos registros do DataFrame.

# COMMAND ----------

first_letter_udf = udf(first_letter_function)

# COMMAND ----------

# DBTITLE 0,--i18n-75abb6ee-291b-412f-919d-be646cf1a580
# MAGIC %md
# MAGIC
# MAGIC Aplique a UDF na coluna **`email`**.

# COMMAND ----------

from pyspark.sql.functions import col

display(sales_df.select(first_letter_udf(col("email"))))

# COMMAND ----------

# DBTITLE 0,--i18n-26f93012-a994-4b6a-985e-01720dbecc25
# MAGIC %md
# MAGIC
# MAGIC ### Usar a sintaxe do decorador (somente Python)
# MAGIC
# MAGIC Alternativamente, você pode definir e registrar uma UDF usando a <a href="https://realpython.com/primer-on-python-decorators/" target="_blank">sintaxe do decorador do Python</a>. O parâmetro do decorador **`@udf`** é o tipo de dado de coluna que a função retorna.
# MAGIC
# MAGIC Você não poderá mais chamar a função local Python (ou seja, **`first_letter_udf("annagray@kaufman.com")`** não funcionará).
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> Este exemplo também usa <a href="https://docs.python.org/3/library/typing.html" target="_blank">dicas de tipo de Python</a>, que foram introduzidas no Python 3.5. As dicas de tipo não são necessárias para este exemplo, mas servem como um guia para ajudar os desenvolvedores a usar a função corretamente. Elas são usadas neste exemplo para enfatizar que a UDF processa um registro de cada vez, utilizando um único argumento **`str`** e retornando um valor **`str`**.

# COMMAND ----------

# Our input/output is a string
@udf("string")
def first_letter_udf(email: str) -> str:
    return email[0]

# COMMAND ----------

# DBTITLE 0,--i18n-4d628fe1-2d94-4d86-888d-7b9df4107dba
# MAGIC %md
# MAGIC
# MAGIC O decorador de UDF é usado neste passo.

# COMMAND ----------

from pyspark.sql.functions import col

sales_df = spark.table("sales")
display(sales_df.select(first_letter_udf(col("email"))))

# COMMAND ----------

# DBTITLE 0,--i18n-3ae354c0-0b10-4e8c-8cf6-da68e8fba9f2
# MAGIC %md
# MAGIC
# MAGIC ### UDFs vetorizadas/Pandas
# MAGIC
# MAGIC A disponibilidade em Python aumenta a eficiência das UDFs Pandas. As UDFs Pandas utilizam o Apache Arrow para acelerar a computação.
# MAGIC
# MAGIC * <a href="https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html" target="_blank">Postagem no blog</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html?highlight=arrow" target="_blank">Documentação</a>
# MAGIC
# MAGIC <img src="https://databricks.com/wp-content/uploads/2017/10/image1-4.png" alt="Benchmark" width ="500" height="1500">
# MAGIC
# MAGIC As funções definidas pelo usuário são executadas usando: 
# MAGIC * O <a href="https://arrow.apache.org/" target="_blank">Apache Arrow</a>, um formato de dados colunares na memória usado no Spark para transferir dados com eficiência entre processos JVM e Python com custo quase zero de (des)serialização
# MAGIC * Pandas dentro da função para trabalhar com instâncias e APIs do Pandas
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> A partir do Spark 3.0, você deve **sempre** definir a UDF Pandas usando dicas de tipo de Python.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

# We have a string input/output
@pandas_udf("string")
def vectorized_udf(email: pd.Series) -> pd.Series:
    return email.str[0]

# Alternatively
# def vectorized_udf(email: pd.Series) -> pd.Series:
#     return email.str[0]
# vectorized_udf = pandas_udf(vectorized_udf, "string")

# COMMAND ----------

display(sales_df.select(vectorized_udf(col("email"))))

# COMMAND ----------

# DBTITLE 0,--i18n-9a2fb1b1-8060-4e50-a759-f30dc73ce1a1
# MAGIC %md
# MAGIC
# MAGIC Podemos registrar essas UDFs Pandas no namespace SQL.

# COMMAND ----------

spark.udf.register("sql_vectorized_udf", vectorized_udf)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the Pandas UDF from SQL
# MAGIC SELECT sql_vectorized_udf(email) AS firstLetter FROM sales

# COMMAND ----------

# DBTITLE 0,--i18n-5e506b8d-a488-4373-af9a-9ebb14834b1b
# MAGIC %md
# MAGIC
# MAGIC ### Limpar sala de aula
# MAGIC

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
# MAGIC
