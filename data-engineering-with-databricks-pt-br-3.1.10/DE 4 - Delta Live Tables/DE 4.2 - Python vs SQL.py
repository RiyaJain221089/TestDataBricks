# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-6ee8963d-1bec-43df-9729-b4da38e8ad0d
# MAGIC %md
# MAGIC # Delta Live Tables: Python versus SQL
# MAGIC
# MAGIC Nesta lição, revisaremos as principais diferenças entre as implementações Python e SQL do Delta Live Tables
# MAGIC
# MAGIC Ao final desta lição você será capaz de: 
# MAGIC
# MAGIC * Identificar as principais diferenças entre as implementações Python e SQL do Delta Live Tables

# COMMAND ----------

# DBTITLE 0,--i18n-48342971-ca6a-4774-88e1-951b8189bec4
# MAGIC %md
# MAGIC
# MAGIC # Python x SQL
# MAGIC | Python | SQL | Notas |
# MAGIC |--------|--------|--------|
# MAGIC | API do Python | API de SQL proprietária |  |
# MAGIC | Sem verificação de sintaxe | Tem verificações de sintaxe| No Python, uma célula de notebook do DLT executada isoladamente gera um erro, enquanto que o SQL faz uma verificação e informa se o comando é sintaticamente válido. Em ambos os casos, células individuais do notebook não devem ser executadas em pipelines do DLT. |
# MAGIC | Observações sobre importações | Nenhuma | O módulo dlt deve ser importado explicitamente para suas bibliotecas de notebook do Python. Isso não é necessário no SQL. |
# MAGIC | Tabelas como DataFrames | Tabelas como resultados de query | A API DataFrame do Python permite múltiplas transformações de um dataset agrupando várias chamadas de API. Comparando ao SQL, é necessário salvar essas transformações em tabelas temporárias à medida que são processadas. |
# MAGIC |@dlt.table()  | Instrução SELECT | No SQL, a lógica central da query, com as transformações a fazer nos dados, está contida na instrução SELECT. No Python, as transformações de dados são especificadas quando você configura as opções de @dlt.table().  |
# MAGIC | @dlt.table(comment = "Python comment",table_properties = {"quality": "silver"}) | COMMENT "SQL comment"       TBLPROPERTIES ("quality" = "silver") | É assim que você adiciona comentários e propriedades de tabela no Python, em comparação com o SQL |
