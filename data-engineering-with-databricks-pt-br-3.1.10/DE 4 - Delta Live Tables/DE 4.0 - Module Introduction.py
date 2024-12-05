# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-05f37e48-e8d1-4b0c-87e8-38cd4c42edc6
# MAGIC %md
# MAGIC ## Criando pipelines de dados com o Delta Live Tables
# MAGIC Este módulo faz parte do Caminho de aprendizagem do engenheiro de dados da Databricks Academy.
# MAGIC
# MAGIC #### IU do DLT
# MAGIC
# MAGIC Palestra: A arquitetura medallion <br>
# MAGIC Palestra: Introdução ao Delta Live Tables <br>
# MAGIC [DE 4.1 - Usando a IU do DLT]($./DE 4.1 - Passo a passo da IU do DLT) <br>
# MAGIC
# MAGIC #### Sintaxe DLT
# MAGIC DE 4.1.1 - Pipeline de pedidos: [SQL]($./DE 4.1A - Pipelines SQL/DE 4.1.1 - Pipeline de pedidos) ou [Python]($./DE 4.1B - Pipelines Python/DE 4.1.1 - Pipeline de pedidos)<br>
# MAGIC DE 4.1.2 - Pipeline de clientes: [SQL]($./DE 4.1A - Pipelines SQL/DE 4.1.2 - Pipeline de clientes) ou [Python]($./DE 4.1B - Pipelines Python/DE 4.1.2 - Pipeline de clientes) <br>
# MAGIC [DE 4.2 - Python versus SQL]($./DE 4.2 - Python versus SQL) <br>
# MAGIC
# MAGIC #### Resultados, monitoramento e solução de problemas do pipeline
# MAGIC [DE 4.3 - Resultados do pipeline]($./DE 4.3 - Resultados do pipeline) <br>
# MAGIC [DE 4.4 - Logs de eventos de pipeline]($./DE 4.4 - Logs de eventos de pipeline) <br>
# MAGIC DE 4.1.3 - Pipeline de status: [SQL]($./DE 4.1A - Pipelines SQL/DE 4.1.3 - Pipeline de status) ou [Python]($./DE 4.1B - Pipelines Python/DE 4.1.3 - Pipeline de status) <br>
# MAGIC [DE 4.99 - Obter novos dados]($./DE 4.99 - Obter novos dados) <br>
# MAGIC
# MAGIC #### Pré-requisitos
# MAGIC
# MAGIC *Familiaridade para iniciantes com conceitos de computação em nuvem (máquinas virtuais, armazenamento de objetos, etc.)
# MAGIC * Capacidade de executar tarefas básicas de desenvolvimento de código usando o workspace de ciência de dados e engenharia do Databricks (criar clusters, executar código em notebooks, realizar operações básicas em notebooks, importar repos do Git etc.)
# MAGIC * Experiência inicial de programação com o Delta Lake
# MAGIC * Usar a DLL do Delta Lake para criar tabelas, compactar arquivos, restaurar versões anteriores de tabelas e realizar a coleta de lixo em tabelas no Lakehouse
# MAGIC   * Usar CTAS para armazenar dados derivados de uma query em uma tabela do Delta Lake
# MAGIC   * Usar SQL para realizar atualizações completas e incrementais em tabelas existentes
# MAGIC * Experiência inicial de programação com Spark SQL ou PySpark
# MAGIC   *Extraia dados de vários formatos de arquivo e fontes de dados
# MAGIC   *Aplique uma série de transformações comuns para limpar dados
# MAGIC   *Remodele e manipule dados complexos usando recursos avançados- em funções
# MAGIC * Experiência de produção trabalhando com data warehouses e data lakes
# MAGIC
# MAGIC #### Considerações técnicas
# MAGIC
# MAGIC *Este curso é executado em DBR 11.3.
# MAGIC *Este curso não pode ser ministrado no Databricks Community Edition.
