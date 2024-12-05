# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-4603b7f5-e86f-44b2-a449-05d556e4769c
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Orquestração de jobs com o Databricks Workflows
# MAGIC
# MAGIC Novas atualizações na UI de jobs do Databricks adicionaram a capacidade de programar várias tarefas como parte de um job. Assim, os jobs do Databricks podem lidar com toda a orquestração da maioria das cargas de trabalho de produção.
# MAGIC
# MAGIC Para começar, vamos revisar as etapas da programação de uma tarefa de notebook como um job autônomo acionado e, em seguida, adicionar uma tarefa dependente usando um pipeline do DLT. 
# MAGIC
# MAGIC ## Objetivos de aprendizado
# MAGIC Ao final desta lição, você deverá ser capaz de:
# MAGIC * Programar uma tarefa de notebook em um job do fluxo de trabalho do Databricks
# MAGIC * Descrever opções de programação de jobs e as diferenças entre os tipos de cluster
# MAGIC * Revisar as execuções de jobs para acompanhar o progresso e ver os resultados
# MAGIC * Programar uma tarefa de pipeline do DLT em um Workflow Job do Databricks
# MAGIC * Configurar dependências lineares entre tarefas usando a UI do Databricks Workflows

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-05.1.1 

# COMMAND ----------

# DBTITLE 0,--i18n-fb1b72e2-458f-4930-b070-7d60a4d3b34f
# MAGIC %md
# MAGIC
# MAGIC ## Gerar a configuração do job
# MAGIC
# MAGIC A configuração deste job exige parâmetros exclusivos para um determinado usuário.
# MAGIC
# MAGIC Execute a célula abaixo para imprimir os valores que você usará para configurar seu pipeline nas etapas subsequentes.

# COMMAND ----------

DA.print_job_config_v1()

# COMMAND ----------

# DBTITLE 0,--i18n-b3634ee0-e06e-42ca-9e70-a9a02410f705
# MAGIC %md
# MAGIC
# MAGIC ## Configurar o job com uma única tarefa de notebook
# MAGIC
# MAGIC Ao usar a UI de jobs para orquestrar uma carga de trabalho com diversas tarefas, você sempre começa criando um job com uma única tarefa.
# MAGIC
# MAGIC Passos:
# MAGIC 1. Clique no botão **Workflows** na barra lateral, clique na tab **Jobs** e no botão **Criar job**.
# MAGIC 2. Configure o job e a tarefa conforme especificado abaixo. Você precisará dos valores fornecidos na saída da célula acima para esta etapa.
# MAGIC
# MAGIC | Configuração | Instruções |
# MAGIC |--|--|
# MAGIC | Nome da tarefa | Insira **Reset** |
# MAGIC | Tipo | Escolha **Notebook** |
# MAGIC | Fonte | Escolha **Workspace** |
# MAGIC | Caminho | Use o navegador para especificar o **caminho do notebook Reset** fornecido acima |
# MAGIC | Cluster | No menu dropdown, em **Clusters All Purpose existentes**, selecione seu cluster |
# MAGIC | Nome do job | No canto superior esquerdo da tela, digite o **nome do job** fornecido acima para nomear o job (não a tarefa) |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 3. Clique no** Criar** botão.
# MAGIC 4. Clique no botão azul **Executar agora** no canto superior direito para iniciar o job.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Observação**: Ao selecionar o cluster all-purpose, você receberá um aviso informando que isso será cobrado como um compute all-purpose. Os jobs de produção devem sempre ser programados em novos clusters de job com a dimensão apropriada à carga de trabalho, pois isso é cobrado a uma taxa muito mais baixa.

# COMMAND ----------

# ANSWER

# This function is provided for students who do not 
# want to work through the exercise of creating the job.
DA.create_job_v1()

# COMMAND ----------

DA.validate_job_v1_config()

# COMMAND ----------

# DBTITLE 0,--i18n-eb8d7811-b356-41d2-ae82-e3762add19f7
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Explorar as opções de programação
# MAGIC Passos:
# MAGIC 1. No lado direito da UI de jobs, localize a seção **Detalhes do job**.
# MAGIC 1. Na seção **Gatilho**, selecione o botão **Adicionar gatilho** para conhecer as opções de programação.
# MAGIC 1. Alterar o **Tipo de gatilho** de **Nenhum (Manual)** para **Programado** abrirá uma UI de programação cronológica.
# MAGIC    - Essa UI oferece diversas opções para configurar a programação cronológica dos jobs. As configurações definidas na UI também podem ser geradas na sintaxe cronológica, que você poderá editar se precisar usar uma configuração personalizada não disponível na UI.
# MAGIC 1. Neste momento, deixaremos o job definido com a programação **Manual**. Selecione **Cancelar** para retornar aos detalhes do job.

# COMMAND ----------

# DBTITLE 0,--i18n-eb585218-f5df-43f8-806f-c80d6783df16
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Revisar a execução
# MAGIC
# MAGIC Para revisar a execução do job:
# MAGIC 1. Na página de detalhes de jobs, selecione a tab **Execuções** no canto superior esquerdo da tela (você provavelmente está na tab **Tarefas**)
# MAGIC 1. Encontre o job.
# MAGIC     - **Jobs que estão em execução** podem ser encontrados na seção **Execuções ativas**. 
# MAGIC     - **Jobs que terminaram de ser executados** são colocados na seção **Execuções concluídas**
# MAGIC 1. Abra os detalhes de saída clicando no campo de carimbo de data/hora abaixo da coluna **Hora de início**
# MAGIC     - Se **o job ainda estiver em execução**, o estado ativo do notebook será um **Status** de **`Pending`** ou **`Running`**, como mostrado no painel lateral direito. 
# MAGIC     - Se **o job tiver sido concluído**, a execução completa do notebook terá um **Status** de **`Succeeded`** ou **`Failed`** mostrado no painel lateral direito
# MAGIC   
# MAGIC O notebook emprega o comando mágico **`%run`** para chamar um notebook adicional usando um caminho relativo. Observe que, embora o tópico não seja abordado neste curso, <a href="https://docs.databricks.com/repos.html#work-with-non-notebook-files-in-a-databricks-repo" target="_blank">uma nova funcionalidade adicionada ao Databricks Repos permite carregar módulos Python usando caminhos relativos</a>.
# MAGIC
# MAGIC O resultado real do notebook programado é redefinir o ambiente para o novo job e o pipeline.

# COMMAND ----------

# DBTITLE 0,--i18n-bc61c131-7d68-4633-afd7-609983e43e17
# MAGIC %md
# MAGIC ## Gerar um pipeline
# MAGIC
# MAGIC Neste passo, adicionaremos um pipeline do DLT a ser executado após a conclusão bem-sucedida da tarefa configurada no início desta lição.
# MAGIC
# MAGIC Para concentrar a operação em jobs, e não em pipelines, usaremos o seguinte comando utilitário para criar um pipeline simples.

# COMMAND ----------

DA.create_pipeline()

# COMMAND ----------

# DBTITLE 0,--i18n-19e4daea-c893-4871-8937-837970dc7c9b
# MAGIC %md
# MAGIC
# MAGIC ## Configurar uma tarefa de pipeline do DLT
# MAGIC
# MAGIC A seguir, precisamos adicionar uma tarefa para executar este pipeline.
# MAGIC
# MAGIC Passos:
# MAGIC 1. Na página Detalhes do job, clique na tab **Tarefas**.
# MAGIC 1. Clique no botão azul **+ Adicionar tarefa**, na parte inferior central da tela, e selecione **Pipeline do Delta Live Tables** no menu dropdown.
# MAGIC 1. Configure a tarefa conforme especificado abaixo.
# MAGIC
# MAGIC | Configuração | Instruções |
# MAGIC |--|--|
# MAGIC | Nome da tarefa | Insira **DLT** |
# MAGIC | Tipo | Escolha **Pipeline do Delta Live Tables** |
# MAGIC | Pipeline | Escolha o pipeline do DLT configurado acima |
# MAGIC | Depende de | Escolha **Reset**, que é a tarefa anterior que definimos |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 4. Clique no botão azul **Criar tarefa**
# MAGIC     - Agora você deverá ver uma tela com duas caixas e uma seta para baixo entre elas. 
# MAGIC     - A tarefa **`Reset`** está no alto, conduzindo até a tarefa **`DLT`**. 
# MAGIC     - Essa visualização representa as dependências entre as tarefas.
# MAGIC 5. Valide a configuração executando o comando abaixo.
# MAGIC     - Se forem relatados erros, repita o procedimento a seguir até que todos os erros tenham sido removidos.
# MAGIC       - Corrija os erros.
# MAGIC       - Clique no botão **Criar tarefa**.
# MAGIC       - Valide a configuração.

# COMMAND ----------

# ANSWER

# This function is provided for students who do not 
# want to work through the exercise of creating the job.
DA.create_job_v2()

# COMMAND ----------

DA.validate_job_v2_config()

# COMMAND ----------

# DBTITLE 0,--i18n-1c949168-e917-455d-8a54-2768592a16f1
# MAGIC %md
# MAGIC
# MAGIC ## Executar o job
# MAGIC Depois que o job tiver sido configurado corretamente, clique no botão azul **Executar agora** no canto superior direito para iniciá-lo.
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Observação**: Ao selecionar tudo- cluster de finalidade, você receberá um aviso sobre como isso será cobrado como todos- cálculo de propósito. Os trabalhos de produção devem sempre ser agendados em relação a novos clusters de trabalhos dimensionados adequadamente para a carga de trabalho, pois isso é cobrado a uma taxa muito mais baixa.
# MAGIC
# MAGIC **OBSERVAÇÃO**: Talvez seja necessário aguardar alguns minutos enquanto a infraestrutura do job e do pipeline é implantada.

# COMMAND ----------

# ANSWER

# This function is provided to start the pipeline and  
# block until it has completed, canceled or failed
DA.start_job()

# COMMAND ----------

# DBTITLE 0,--i18n-666a45d2-1a19-45ba-b771-b47456e6f7e4
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Revisar os resultados da execução multitarefa
# MAGIC
# MAGIC Para revisar os resultados da execução:
# MAGIC 1. Na página Detalhes do job, selecione a tab **Execuções** novamente e, em seguida, selecione a execução mais recente em **Execuções ativas** ou **Execuções concluídas**, a depender do status de conclusão do job.
# MAGIC     - As visualizações das tarefas serão atualizadas em tempo real para refletir as tarefas sendo executadas e mudarão de cor se ocorrerem falhas nas tarefas. 
# MAGIC 1. Clique em uma caixa de tarefa para renderizar o notebook programado na UI. 
# MAGIC     - Esse passo pode ser considerado apenas como uma camada adicional de orquestração além da UI anterior de jobs do Databricks, se isso ajudar.
# MAGIC     - Observe que, se você cargas de trabalho programando jobs com a CLI ou a API REST, <a href="https://docs.databricks.com/dev-tools/api/latest/jobs.html" target="_blank">a estrutura JSON usada para configurar e obter resultados dos jobs também passou por atualizações semelhantes de UI</a>.
# MAGIC
# MAGIC **OBSERVAÇÃO**: No momento, os pipelines do DLT programados como tarefas não geram resultados diretamente na GUI de execuções. Em vez disso, você retornará à GUI do pipeline do DLT programado.
