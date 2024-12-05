# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-67600b38-db2e-44e0-ba4a-1672ee796c77
# MAGIC %md
# MAGIC # Criar e gerenciar clusters interativos
# MAGIC
# MAGIC Um cluster do Databricks é um conjunto de configurações e recursos de computação nos quais você executa cargas de trabalho de engenharia de dados, ciência de dados e análise de dados, por exemplo, pipelines de ETL de produção, transmissões analíticas, análises ad hoc e machine learning. Você executa essas cargas de trabalho como um conjunto de comandos em um notebook ou como um job automatizado. 
# MAGIC
# MAGIC O Databricks diferencia clusters todo-propósito de clusters de jobs. 
# MAGIC - Você usa clusters todo-propósito para analisar dados de forma colaborativa em notebooks interativos.
# MAGIC - Você usa clusters de jobs para executar jobs automatizados rápidos e robustos.
# MAGIC
# MAGIC Esta demonstração abordará a criação e o gerenciamento de clusters todo-propósito do Databricks usando o workspace de ciência de dados e engenharia do Databricks. 
# MAGIC
# MAGIC ## Objetivos de aprendizado
# MAGIC Ao final desta lição, você deverá ser capaz de:
# MAGIC * Usar a IU de clusters para configurar e implantar um cluster
# MAGIC * Editar, encerrar, reiniciar e excluir clusters

# COMMAND ----------

# DBTITLE 0,--i18n-81a87f46-ce1b-482f-9ad8-62418db25665
# MAGIC %md
# MAGIC ## Criar um cluster
# MAGIC
# MAGIC Dependendo do workspace no qual está trabalhando, você pode ou não ter privilégios de criação de clusters. 
# MAGIC
# MAGIC As instruções nesta seção pressupõem que você **tem** privilégios de criação de clusters e precisa implantar um novo cluster para seguir as lições do curso.
# MAGIC
# MAGIC **OBSERVAÇÃO**: Pergunte a quem estiver instruindo ou administrando a plataforma se você deve criar um cluster ou se conectar a um cluster já criado. As políticas de cluster podem afetar suas opções de configuração de clusters. 
# MAGIC
# MAGIC Passos:
# MAGIC 1. Use a barra lateral esquerda para navegar até a página **Computação** clicando no ícone ![compute](https://files.training.databricks.com/images/clusters-icon.png)
# MAGIC 1. Clique no botão azul **Criar cluster**
# MAGIC 1. Use seu nome como **Nome do cluster** para encontrá-lo facilmente e para ajudar quem está instruindo a identificá-lo se você tiver algum problema
# MAGIC 1. Defina o **Modo cluster** como **Single Node** (modo obrigatório para executar este curso)
# MAGIC 1. Utilize a **versão do Databricks Runtime** recomendada para este curso
# MAGIC 1. Deixe as caixas de seleção marcadas de acordo com as configurações default em **Opções de piloto automático**
# MAGIC 1. Clique no azul** Criar cluster** botão
# MAGIC
# MAGIC **OBSERVAÇÃO:** A criação de um cluster pode levar vários minutos. Depois de concluir a criação do cluster, fique à vontade para continuar explorando a IU de criação de clusters.

# COMMAND ----------

# DBTITLE 0,--i18n-11f7b691-6ba9-49d5-b975-2924a44d05d1
# MAGIC %md
# MAGIC
# MAGIC ### <img src="https://files.training.databricks.com/images/icon_warn_24.png"> O cluster de nó único é obrigatório para este curso
# MAGIC **IMPORTANTE:** Este curso exige que você execute notebooks em um cluster de nó único. 
# MAGIC
# MAGIC Siga as instruções acima para criar um cluster cujo **Modo cluster** seja definido como **`Single Node`**.

# COMMAND ----------

# DBTITLE 0,--i18n-7323201d-6d28-4780-b2f7-47ab22eadb8f
# MAGIC %md
# MAGIC ## Gerenciar clusters
# MAGIC
# MAGIC Depois de criar o cluster, volte para a página **Computação** para visualizá-lo.
# MAGIC
# MAGIC Selecione um cluster para revisar sua configuração atual. 
# MAGIC
# MAGIC Clique no botão **Editar**. Observe que a maioria das configurações poderá ser modificada (se você tiver permissões suficientes). A alteração da maioria das configurações exigirá que os clusters em execução sejam reiniciados.
# MAGIC
# MAGIC **OBSERVAÇÃO**: Usaremos o cluster criado na próxima lição. Reiniciar, encerrar ou excluir o cluster pode atrasar o curso, pois você terá que esperar que novos recurso sejam criados.

# COMMAND ----------

# DBTITLE 0,--i18n-2fafe840-a86f-4bd7-9d60-7044610b8d5a
# MAGIC %md
# MAGIC ## Reiniciar, encerrar e excluir
# MAGIC
# MAGIC Observe que embora **Reiniciar**, **Encerrar** e **Excluir** tenham efeitos diferentes, todas essas ações começam com um evento de encerramento de cluster. (Os clusters também serão encerrados automaticamente por inatividade, supondo que essa configuração seja usada.)
# MAGIC
# MAGIC Quando um cluster é encerrado, todos os recursos de cloud em uso naquele momento são excluídos. Isso significa que:
# MAGIC * As VMs associadas e a memória operacional serão limpas
# MAGIC * O armazenamento de volume anexado será excluído
# MAGIC * As conexões de rede entre nós serão removidas
# MAGIC
# MAGIC Em resumo, todos os recursos anteriormente associados ao ambiente de computação serão completamente removidos. Isso significa que **qualquer resultado que precise ser persistido deve ser salvo em um local permanente**. Observe que você não perderá código nem arquivos de dados que salvar como for apropriado.
# MAGIC
# MAGIC O botão **Reiniciar** permite reiniciar manualmente o cluster. Isso poderá ser útil se precisarmos limpar completamente o cache do cluster ou quisermos reset completamente o ambiente de computação.
# MAGIC
# MAGIC O botão **Encerrar** permite interromper o cluster. A configuração do cluster é mantida, e podemos usar o botão **Reiniciar** para implantar um novo conjunto de recursos de cloud com a mesma configuração.
# MAGIC
# MAGIC O botão **Excluir** interromperá o cluster e removerá sua configuração.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
# MAGIC
