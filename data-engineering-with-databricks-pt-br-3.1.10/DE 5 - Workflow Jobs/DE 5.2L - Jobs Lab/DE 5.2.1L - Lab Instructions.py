# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-7b85ffd8-b52c-4ea8-84db-b3668e13b402
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Laboratório: Orquestrando jobs com o Databricks
# MAGIC
# MAGIC Neste laboratório, você configurará um job multitarefa composto por:
# MAGIC * Um notebook que transfere um novo lote de dados para um diretório de armazenamento
# MAGIC * Um pipeline do Delta Live Tables que processa esses dados em várias tabelas
# MAGIC * Um notebook que consulta a tabela gold produzida pelo pipeline, bem como várias métricas geradas pelo DLT
# MAGIC
# MAGIC ## Objetivos de aprendizado
# MAGIC Ao final deste laboratório, você será capaz de:
# MAGIC * Programar um notebook como uma tarefa em um job do Databricks
# MAGIC * Programar um pipeline do DLT como uma tarefa em um job do Databricks
# MAGIC *Configurar dependências lineares entre tarefas usando a UI do Databricks Workflows

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-05.2.1L

# COMMAND ----------

# DBTITLE 0,--i18n-003b65fd-018a-43e5-8d1d-1ce2bee52fe3
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Obter dados iniciais
# MAGIC Preencha a zona de destino com alguns dados antes de continuar. 
# MAGIC
# MAGIC Você executará novamente esse comando para obter dados adicionais mais tarde.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# DBTITLE 0,--i18n-cc5b4584-59c4-49c9-a6d2-efcdadb98dbe
# MAGIC %md
# MAGIC
# MAGIC ## Gerar configuração de trabalho
# MAGIC
# MAGIC A configuração deste trabalho exigirá parâmetros exclusivos para um determinado usuário.
# MAGIC
# MAGIC Execute a célula abaixo para imprimir os valores que você usará para configurar seu pipeline nas etapas subsequentes.

# COMMAND ----------

DA.print_job_config()

# COMMAND ----------

# DBTITLE 0,--i18n-542fd1c6-0322-4c8f-8719-fe980f2a8013
# MAGIC %md
# MAGIC
# MAGIC ## Configurar trabalho com uma única tarefa de notebook
# MAGIC
# MAGIC Para começar, vamos programar o primeiro notebook.
# MAGIC
# MAGIC Passos:
# MAGIC 1. Clique no** Workflows** na barra lateral, clique no botão** Jobs** guia e clique no** Criar Job** botão.
# MAGIC 2. Configure o job e a tarefa conforme especificado abaixo. Você precisará dos valores fornecidos na saída da célula acima para esta etapa.
# MAGIC
# MAGIC | Configuração | Instruções |
# MAGIC |--|--|
# MAGIC | Nome da tarefa | Insira **Batch-Job** |
# MAGIC | Tipo | Escolha **Notebook** |
# MAGIC | Fonte | Escolha **Workspace** |
# MAGIC | Caminho | Use o navegador para especificar o **caminho do notebook de lotes** fornecido acima |
# MAGIC | Cluster | Selecione seu cluster no menu dropdown, em **Clusters all-purpose existentes** |
# MAGIC | Nome do job | No topo- esquerda da tela, digite o** Nome do Job** fornecido acima para adicionar um nome para o job (não para a tarefa) |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 3. Clique no** Criar** botão.
# MAGIC 4. Clique no azul** Run agora** botão no canto superior direito para iniciar o job.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Observação**: Ao selecionar o- cluster all purpose, você receberá um aviso sobre como isso será cobrado como all- purpose. Os jobs de produção devem sempre ser agendados em relação a novos clusters de jobs dimensionados adequadamente para a carga de trabalho, pois isso é cobrado a uma taxa muito mais baixa.

# COMMAND ----------

# DBTITLE 0,--i18n-4678fc9d-ab2f-4f8c-b4be-a67774b2afd4
# MAGIC %md
# MAGIC ## Gerar um pipeline
# MAGIC
# MAGIC Neste passo, adicionaremos um pipeline do DLT a ser executado após a conclusão bem-sucedida da tarefa configurada acima.
# MAGIC
# MAGIC Para focar em jobs e não em pipelines, usaremos o seguinte comando utilitário para criar um pipeline simples para nós.

# COMMAND ----------

DA.create_pipeline()

# COMMAND ----------

# DBTITLE 0,--i18n-8ab4bc2a-e08e-47ca-8507-9565342acfb6
# MAGIC %md
# MAGIC
# MAGIC ## Adicionar uma tarefa de pipeline
# MAGIC
# MAGIC Passos:
# MAGIC 1. Na página Detalhes do Job, clique no botão** Tarefas** aba.
# MAGIC 1. Clique no azul** + Adicionar tarefa** botão na parte inferior central da tela e selecione** Pipeline Delta Live Tables** no menu suspenso.
# MAGIC 1. Configure a tarefa:
# MAGIC
# MAGIC | Configuração | Instruções |
# MAGIC |--|--|
# MAGIC | Nome da tarefa | Insira **DLT** |
# MAGIC | Tipo | Escolher** Pipeline Delta Live Tables** |
# MAGIC | Pipeline | Escolha o pipeline DLT configurado acima |
# MAGIC | Depende de | Escolha **Batch-Job**, a tarefa que definimos anteriormente |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 4. Clique no azul** Criar tarefa** botão
# MAGIC     -Agora você deverá ver uma tela com 2 caixas e uma seta para baixo entre elas. 
# MAGIC     - A tarefa **`Batch-Job`** está no alto, conduzindo até a tarefa **`DLT`**. 

# COMMAND ----------

# DBTITLE 0,--i18n-1cdf590a-6da0-409d-b4e1-bd5a829f3a66
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Adicionar outra tarefa de notebook
# MAGIC
# MAGIC Foi fornecido um notebook adicional que consulta algumas métricas do DLT e a tabela ouro definida no pipeline do DLT. Adicionaremos essa tarefa ao final do job.
# MAGIC
# MAGIC Passos:
# MAGIC 1. Na página Detalhes do Job, clique no botão** Tarefas** aba.
# MAGIC 1. Clique no botão azul **+ Adicionar tarefa**, na parte inferior central da tela, e selecione **Notebook** no menu dropdown.
# MAGIC 1. Configure a tarefa:
# MAGIC
# MAGIC | Configuração | Instruções |
# MAGIC |--|--|
# MAGIC | Nome da tarefa | Insira **Query-Results** |
# MAGIC | Tipo | Escolha **Notebook** |
# MAGIC | Fonte | Escolha **Workspace** |
# MAGIC | Caminho | Use o navegador para especificar o **caminho do notebook de queries** fornecido acima |
# MAGIC | Cluster | Selecione seu cluster no menu suspenso, em** Clusters All Purpose existentes** |
# MAGIC | Depende de | Escolha **DLT**, que é a tarefa anterior que definimos |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 4. Clique no azul** Criar tarefa** botão
# MAGIC 5. Clique no botão azul **Executar agora**, no canto superior direito, para executar este job.
# MAGIC     - Na tab **Execuções**, você pode clicar no horário de início da execução na seção **Execuções ativas** e acompanhar visualmente o progresso da tarefa.
# MAGIC     - Depois que todas as tarefas forem bem-sucedidas, revise o conteúdo de cada uma delas para confirmar que tiveram o comportamento esperado.

# COMMAND ----------

# This function is provided for students who do not 
# want to work through the exercise of creating the job.
DA.create_job()

# COMMAND ----------

DA.validate_job_config()

# COMMAND ----------

# This function is provided to start the job and  
# block until it has completed, canceled or failed
DA.start_job()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
