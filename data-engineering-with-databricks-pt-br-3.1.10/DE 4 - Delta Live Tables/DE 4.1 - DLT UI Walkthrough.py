# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-da41af42-59a3-42d8-af6d-4ab96146397c
# MAGIC %md
# MAGIC # Usando a IU do Delta Live Tables
# MAGIC
# MAGIC Esta demonstração explorará a UI do DLT. Ao final desta lição, você será capaz de: 
# MAGIC
# MAGIC * Implantar um pipeline do DLT
# MAGIC * Explorar o DAG resultante
# MAGIC * Executar uma atualização do pipeline

# COMMAND ----------

# DBTITLE 0,--i18n-d84e8f59-6cda-4c81-8547-132eb20b48b2
# MAGIC %md
# MAGIC ## Configuração da sala de aula
# MAGIC
# MAGIC Execute a célula a seguir para configurar o ambiente de trabalho para este curso.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.1

# COMMAND ----------

# DBTITLE 0,--i18n-ba2a4dfe-ca17-4070-b35a-37068ff9c51d
# MAGIC %md
# MAGIC
# MAGIC ## Gerar a configuração do pipeline
# MAGIC A configuração deste pipeline exigirá parâmetros exclusivos para um determinado usuário.
# MAGIC
# MAGIC Na célula de código abaixo, especifique qual linguagem usar removendo o comentário da linha apropriada.
# MAGIC
# MAGIC Em seguida, execute a célula para imprimir os valores que você usará para configurar o pipeline nas etapas subsequentes.

# COMMAND ----------

pipeline_language = "SQL"
#pipeline_language = "Python"

DA.print_pipeline_config(pipeline_language)

# COMMAND ----------

# DBTITLE 0,--i18n-bc4e7bc9-67e1-4393-a3c5-79a1f585cdc8
# MAGIC %md
# MAGIC Nesta lição, implantamos um pipeline com um único notebook, especificado como Notebook nº 1 na saída da célula acima. 
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"> **DICA:**  Será útil consultar os caminhos acima quando os Notebooks nº 2 e nº 3 forem adicionados ao pipeline em lições posteriores.

# COMMAND ----------

# DBTITLE 0,--i18n-9b609cc5-91c8-4213-b6f7-1c737a6e44a3
# MAGIC %md
# MAGIC ## Criar e configurar um pipeline
# MAGIC
# MAGIC Para começar, vamos criar um pipeline com um único notebook (Notebook nº 1).
# MAGIC
# MAGIC Passos:
# MAGIC 1. Clique no botão **Fluxos de trabalho** na barra lateral, clique na tab **Delta Live Tables** e em **Criar pipeline**. 
# MAGIC 2. Configure o pipeline conforme especificado abaixo. Você precisará dos valores fornecidos na saída da célula acima para esta etapa.
# MAGIC
# MAGIC | Configuração | Instruções |
# MAGIC |--|--|
# MAGIC | Nome do pipeline | Insira o **Nome do pipeline** fornecido acima |
# MAGIC | Edição do produto | Escolha **Avançado** |
# MAGIC | Modo pipeline | Escolha **Acionado** |
# MAGIC | Política de cluster | Escolha a **Política** fornecida acima |
# MAGIC | Bibliotecas de notebooks | Use o navegador para selecionar ou insira o **caminho do Notebook # 1** fornecido acima |
# MAGIC | Local de armazenamento | Insira o **Local de armazenamento** fornecido acima |
# MAGIC | Esquema de destino | Insira o nome do banco de dados de **Destino** fornecido acima |
# MAGIC | Modo cluster | Escolha **Tamanho fixo** para desativar o escalonamento automático do cluster |
# MAGIC | Workers | Insira **0** para usar um cluster single node |
# MAGIC | Aceleração do Photon | Marque esta caixa de seleção para ativar |
# MAGIC | Configuração | Clique em **Avançado** para visualizar configurações adicionais,<br>clique em **Adicionar configuração** para inserir a **Key** e o **Value** para a linha 1 na tabela abaixo,<br>clique em **Adicionar configuração** para inserir a **Key** e o **Value** para a linha 2 na tabela abaixo |
# MAGIC | Canal | Escolha **Atual** para usar a versão de runtime atual |
# MAGIC
# MAGIC | Configuração | Key                 | Value                                      |
# MAGIC | ------------- | ------------------- | ------------------------------------------ |
# MAGIC | #1            | **`spark.master`**  | **`local[*]`**                             |
# MAGIC | #2            | **`source`** | Insira a **fonte** fornecida acima |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 3. Clique no botão **Criar**.
# MAGIC 4. Verifique se o modo de pipeline está definido como **Desenvolvimento**.

# COMMAND ----------

# ANSWER

# This function is provided for students who do not 
# want to work through the exercise of creating the pipeline.
DA.create_pipeline(pipeline_language)

# COMMAND ----------

# DBTITLE 0,--i18n-d8e19679-0c2f-48cc-bc80-5f1243ff94c8
# MAGIC %md
# MAGIC #### Notas adicionais sobre a configuração do pipeline
# MAGIC Leve em consideração estas observações sobre as configurações do pipeline acima:
# MAGIC
# MAGIC - **Modo pipeline** - especifica como o pipeline será executado. Escolha o modo com base nos requisitos de latência e custo.
# MAGIC   - Os pipelines `Triggered` são executados uma vez e depois encerrados até a próxima atualização manual ou programada.
# MAGIC   - Os pipelines `Continuous` são executados continuamente, ingerindo novos dados à medida que são recebidos.
# MAGIC - **Bibliotecas de notebooks** - embora este documento seja um notebook padrão do Databricks, a sintaxe SQL é especializada para instruções de tabelas do DLT. Exploraremos a sintaxe no exercício a seguir.
# MAGIC - **Local de armazenamento** - este campo opcional permite ao usuário especificar um local para armazenar logs, tabelas e outras informações relacionadas à execução do pipeline. Se não for especificado, o DLT gerará automaticamente um diretório.
# MAGIC - **Destino** - se este campo opcional não for especificado, as tabelas não serão registradas em um metastore, mas ainda estarão disponíveis no DBFS. Consulte a <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#publish-tables" target="_blank">documentação</a> para obter mais informações sobre essa opção.
# MAGIC - **Modo cluster**, **Mínimo de workers**, **Máximo de workers** - estes campos controlam a configuração de workers para o cluster subjacente que processa o pipeline. Neste passo, definimos o número de workers como 0, que, em conjunto com o parâmetro **spark.master** definido acima, configura o cluster como single node.
# MAGIC - **fonte** - estas keys diferenciam maiúsculas e minúsculas. Confirme que a palavra "fonte" só tem letras minúsculas!

# COMMAND ----------

# DBTITLE 0,--i18n-6f8d9d42-99e2-40a5-b80e-a6e6fedd7279
# MAGIC %md
# MAGIC ## Executar um pipeline
# MAGIC
# MAGIC Depois de criar o pipeline, você o executará.
# MAGIC
# MAGIC 1. Selecione **Desenvolvimento** para executar o pipeline no modo de desenvolvimento. O modo de desenvolvimento proporciona iterações mais rápidas reutilizando o cluster (em vez de criar um cluster para cada execução) e desativando novas tentativas para que você possa identificar e corrigir erros prontamente. Consulte a <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#optimize-execution" target="_blank">documentação</a> para obter mais informações sobre esse recurso.
# MAGIC 2. Clique em **Começar**.
# MAGIC
# MAGIC A execução inicial levará vários minutos enquanto um cluster é provisionado. As execuções subsequentes serão consideravelmente mais rápidas.

# COMMAND ----------

# ANSWER

# This function is provided to start the pipeline and  
# block until it has completed, canceled or failed
DA.start_pipeline()

# COMMAND ----------

# DBTITLE 0,--i18n-75d0f6d5-17c6-419e-aacf-be7560f394b6
# MAGIC %md
# MAGIC ## Explorar o DAG
# MAGIC
# MAGIC À medida que o pipeline é concluído, o fluxo de execução é representado graficamente. 
# MAGIC
# MAGIC A seleção das tabelas mostra detalhes.
# MAGIC
# MAGIC Selecione **orders_silver**. Observe os resultados relatados na seção **Qualidade dos dados**. 
# MAGIC
# MAGIC Com cada atualização acionada, todos os dados recém-chegados serão processados pelo seu pipeline. As métricas sempre serão relatadas para a execução atual.

# COMMAND ----------

# DBTITLE 0,--i18n-4cef0694-c05f-44ba-84bf-cd14a63eda17
# MAGIC %md
# MAGIC ## Obter outro lote de dados
# MAGIC
# MAGIC Execute a célula abaixo para obter mais dados no diretório de origem e, em seguida, acione manualmente uma atualização do pipeline.

# COMMAND ----------

DA.dlt_data_factory.load()

# COMMAND ----------

# DBTITLE 0,--i18n-58129206-f245-419e-b51e-b126376a9a45
# MAGIC %md
# MAGIC À medida que continuamos no curso, você pode retornar a este notebook e usar o método fornecido acima para obter novos dados.
# MAGIC
# MAGIC Executar todo esse notebook novamente excluirá os arquivos de dados subjacentes dos dados de origem e do pipeline do DLT. 
# MAGIC
# MAGIC Se você se desconectar do cluster ou tiver algum outro evento em que deseja obter mais dados sem excluir itens, consulte o notebook <a href="$./DE 4.99 - Land New Data" target="_blank">DE 4.99 - Obter novos dados</a>.
