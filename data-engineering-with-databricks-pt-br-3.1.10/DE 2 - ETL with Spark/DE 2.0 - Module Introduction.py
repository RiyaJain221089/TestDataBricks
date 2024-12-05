# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-2fbad123-4065-4749-a925-d24f111ab27c
# MAGIC %md
# MAGIC ## Transformar dados com o Spark
# MAGIC Este módulo faz parte do roteiro de aprendizagem para engenheiros de dados da Databricks Academy e pode ser feito em SQL ou Python.
# MAGIC
# MAGIC #### Extraindo dados
# MAGIC Estes notebooks demonstram conceitos do Spark SQL relevantes para usuários de SQL e PySpark.  
# MAGIC
# MAGIC [DE 2.1 - Consultando arquivos diretamente]($./DE 2.1 - Consultando arquivos diretamente)  
# MAGIC [DE 2.2 - Fornecendo opções para fontes externas]($./DE 2.2 - Fornecendo opções para fontes externas)  
# MAGIC [DE 2.3L - Laboratório de extração de dados]($./DE 2.3L - Laboratório de extração de dados)
# MAGIC
# MAGIC #### Transformando dados
# MAGIC Estes notebooks incluem queries Spark SQL ao lado de código PySpark do DataFrame para demonstrar os mesmos conceitos em ambas as linguagens.
# MAGIC
# MAGIC [DE 2.4 - Limpando dados]($./DE 2.4 - Limpando dados)  
# MAGIC [DE 2.5 - Transformações complexas]($./DE 2.5 - Transformações complexas)  
# MAGIC [DE 2.6L - Laboratório de remodelagem de dados]($./DE 2.6L - Laboratório de remodelagem de dados)
# MAGIC
# MAGIC #### Funções adicionais
# MAGIC
# MAGIC [DE 2.7A - UDFs SQL]($./DE 2.7A - UDFs SQL)  
# MAGIC [DE 2.7B - UDFs Python]($./DE 2.7B - UDFs Python)  
# MAGIC [DE 2.99 - Funções de ordem superior – OPCIONAIS]($./DE 2.99 - Funções de ordem superior – OPCIONAIS)  
# MAGIC
# MAGIC ### Pré-requisitos
# MAGIC Pré-requisitos para as duas versões deste curso (Spark SQL e PySpark):
# MAGIC * Familiaridade elementar com conceitos básicos de nuvem (máquinas virtuais, armazenamento de objetos, gerenciamento de identidades)
# MAGIC * Capacidade de executar tarefas básicas de desenvolvimento de código usando o workspace de ciência de dados e engenharia do Databricks (criar clusters, executar código em notebooks, realizar operações básicas em notebooks, importar repos do git etc.)
# MAGIC * Familiaridade intermediária com conceitos básicos de SQL (select, filter, groupby, join etc)
# MAGIC
# MAGIC Pré-requisitos adicionais para a versão PySpark deste curso:
# MAGIC * Experiência inicial em programação com Python (sintaxe, condições, loops, funções)
# MAGIC * Experiência inicial em programação com a API DataFrame do Spark:
# MAGIC * Configurar o DataFrameReader e o DataFrameWriter para ler e gravar dados
# MAGIC * Expressar transformações de query usando métodos do DataFrame e expressões de coluna
# MAGIC * Navegar na documentação do Spark para identificar funções integradas para várias transformações e tipos de dados
# MAGIC
# MAGIC Os alunos podem fazer o curso Introdução à programação PySpark da Databricks Academy para aprender as habilidades prévias de programação com a API DataFrame do Spark. <br>
# MAGIC
# MAGIC
# MAGIC #### Considerações técnicas
# MAGIC * Este curso é executado no DBR 11.3.
# MAGIC *Este curso não pode ser ministrado no Databricks Community Edition.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
# MAGIC
