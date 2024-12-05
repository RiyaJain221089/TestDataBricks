# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-dccf35eb-f70e-4271-ad22-3a2f10837a13
# MAGIC %md
# MAGIC ## Gerenciar dados com o Delta Lake
# MAGIC Este módulo faz parte do Caminho de aprendizagem do engenheiro de dados da Databricks Academy.
# MAGIC
# MAGIC #### Lições
# MAGIC Palestra: O que é o Delta Lake <br>
# MAGIC [DE 3.1 - Esquemas e tabelas]($./DE 3.1 - Esquemas e tabelas) <br>
# MAGIC [DE 3.2 - Configurar tabelas Delta]($./DE 3.2 - Configurar tabelas Delta) <br>
# MAGIC [DE 3.3 - Carregar dados no Delta Lake]($./DE 3.3 - Carregar dados no Delta Lake) <br>
# MAGIC [DE 3.4 - Laboratório de carregamento de dados]($./DE 3.4L - Laboratório de carregamento de dados) <br>
# MAGIC [DE 3.5 - Versão e otimização de tabelas Delta]($./DE 3.5 - Versão e otimização de tabelas Delta) <br>
# MAGIC [DE 3.6 - Laboratório de manipulação de tabelas Delta]($./DE 3.6L - Laboratório de manipulação de tabelas Delta) <br>
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### Pré-requisitos
# MAGIC *Familiaridade para iniciantes com conceitos de computação em nuvem (máquinas virtuais, armazenamento de objetos, etc.)
# MAGIC *Capacidade de executar tarefas básicas de desenvolvimento de código usando o espaço de trabalho Databricks Data Engineering & Data Science (criar clusters, executar código em notebooks, usar operações básicas de notebook, importar repositórios do git, etc.)
# MAGIC * Experiência inicial de programação com Spark SQL
# MAGIC   * Extrair dados de vários formatos de arquivo e fontes de dados
# MAGIC   * Aplicar uma série de transformações comuns para limpar dados
# MAGIC   * Remodelar e manipular dados complexos usando funções integradas avançadas
# MAGIC
# MAGIC
# MAGIC #### Considerações técnicas
# MAGIC *Este curso é executado em DBR 11.3.
# MAGIC *Este curso não pode ser ministrado no Databricks Community Edition.
