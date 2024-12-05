# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-26e0e9c0-768d-4b35-9e7c-edf0bbca828b
# MAGIC %md
# MAGIC ## Gerenciando o acesso a dados para análise
# MAGIC Este módulo faz parte da trilha de aprendizagem de engenharia de dados da Databricks Academy.
# MAGIC
# MAGIC #### Lições
# MAGIC Aula: Introdução ao Unity Catalog<br>
# MAGIC DE 6.1 - [Criar e administrar dados com o UC]($./DE 6.1 - Criar e administrar dados com o UC)<br>
# MAGIC DE 6.2L - [Criar e compartilhar tabelas no Unity Catalog]($./DE 6.2L - Criar e compartilhar tabelas no Unity Catalog)<br>
# MAGIC DE 6.3L - [Criar views e limitar o acesso a tabelas]($./DE 6.3L - Criar views e limitar o acesso a tabelas)<br>
# MAGIC
# MAGIC
# MAGIC #### Administração com o Unity Catalog - OPCIONAL
# MAGIC DE 6.99 - Administração - OPCIONAL<br>
# MAGIC DE 6.99.2 - [Criar recursos de computação para acesso ao Unity Catalog]($./DE 6.99 - Administração - OPCIONAL/DE 6.99.2 - Criar recursos de computação para acesso ao Unity Catalog)<br>
# MAGIC DE 6.99.3 - [Atualizar uma tabela para o Unity Catalog]($./DE 6.99 - Administração - OPCIONAL/DE 6.99.3 - OPCIONAL - Atualizar uma tabela para o Unity Catalog)<br>
# MAGIC
# MAGIC
# MAGIC #### Pré-requisitos
# MAGIC * Conhecimento de nível iniciante da plataforma Databricks Lakehouse (conhecimento geral da estrutura e dos benefícios da plataforma Lakehouse)
# MAGIC * Conhecimento de nível iniciante de SQL (capacidade de entender e criar queries básicas)
# MAGIC
# MAGIC
# MAGIC #### Considerações técnicas
# MAGIC Este curso não pode ser feito no Databricks Community Edition e só pode ser acessado em clouds com suporte ao Unity Catalog. É necessário ter acesso de administração ao workspace e à conta para a execução completa de todos os exercícios. São demonstradas algumas tarefas opcionais que, além disso, exigem acesso básico ao ambiente na cloud.
