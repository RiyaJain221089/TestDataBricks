# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-8d3d2aed-1539-4db1-8e52-0aa71a4ecc9d
# MAGIC %md
# MAGIC ## Orquestração com Jobs Workflows do Databricks
# MAGIC Este módulo faz parte do Caminho de aprendizagem do engenheiro de dados da Databricks Academy.
# MAGIC
# MAGIC #### Lições
# MAGIC Palestra: Introdução ao Workflows <br>
# MAGIC Palestra: Criando e monitorando Workflow Jobs <br>
# MAGIC Demonstração: Criação e monitoramento Workflows Jobs <br>
# MAGIC DE 5.1 - Programando tarefas com a IU de jobs <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.1.1 - Orquestração de tarefas]($./DE 5.1 - Programando tarefas com a IU de jobs/DE 5.1.1 - Orquestração de tarefas) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.1.2 - Reset]($./DE 5.1 - Programando tarefas com a IU de jobs/DE 5.1.2 - Reset) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.1.3 - Job de DLT]($./DE 5.1 - Programando tarefas com a IU de jobs/DE 5.1.3 - Job de DLT) <br>
# MAGIC DE 5.2L - Laboratório de jobs <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.2.1L - Instruções do laboratório]($./DE 5.2L - Laboratório de jobs/DE 5.2.1L - Instruções do laboratório) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.2.2L - Trabalho em lote]($./DE 5.2L - Laboratório de jobs/DE 5.2.2L - Trabalho em lote) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.2.3L - Job de DLT]($./DE 5.2L - Laboratório de jobs/DE 5.2.3L - Job de DLT) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.2.4L - Job de resultados da query]($./DE 5.2L - Laboratório de jobs/DE 5.2.4L - Job de resultados da query) <br>
# MAGIC
# MAGIC #### Pré-requisitos
# MAGIC *Capacidade de executar tarefas básicas de desenvolvimento de código usando o espaço de trabalho Databricks Data Engineering & Data Science (criar clusters, executar código em notebooks, usar operações básicas de notebook, importar repositórios do git, etc.)
# MAGIC * Capacidade de configurar e executar pipelines de dados usando a UI do Delta Live Tables
# MAGIC * Experiência de iniciante em definição de pipelines do Delta Live Tables (DLT) usando PySpark
# MAGIC   * Ingerir e processar dados usando o Auto Loader e a sintaxe do PySpark
# MAGIC   * Processar feeds da captura de dados de alterações (CDC) com a sintaxe APPLY CHANGES INTO
# MAGIC * Revisar os logs de eventos e resultados do pipeline para solucionar problemas de sintaxe do DLT
# MAGIC *Experiência de produção trabalhando com data warehouses e data lakes
# MAGIC
# MAGIC
# MAGIC #### Considerações técnicas
# MAGIC *Este curso é executado em DBR 11.3.
# MAGIC *Este curso não pode ser ministrado no Databricks Community Edition.
