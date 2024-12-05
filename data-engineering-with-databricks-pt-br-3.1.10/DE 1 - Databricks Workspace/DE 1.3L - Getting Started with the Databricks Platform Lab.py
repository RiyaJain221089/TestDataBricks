# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-4212c527-b1c1-4cce-b629-b5ffb5c57d68
# MAGIC %md
# MAGIC
# MAGIC # Introdução à plataforma Databricks
# MAGIC
# MAGIC Este notebook fornece uma revisão prática de algumas funcionalidades básicas do workspace de ciência de dados e engenharia do Databricks.
# MAGIC
# MAGIC ## Objetivos de aprendizado
# MAGIC Ao final deste laboratório, você deverá ser capaz de:
# MAGIC - Renomear um notebook e alterar a linguagem default
# MAGIC - Anexar um cluster
# MAGIC - Usar o comando mágico **`%run`**
# MAGIC - Executar células Python e SQL
# MAGIC - Criar uma célula Markdown

# COMMAND ----------

# DBTITLE 0,--i18n-eb166a05-9a22-4a74-b54e-3f9e5779f342
# MAGIC %md
# MAGIC
# MAGIC ## Renomeando um notebook
# MAGIC
# MAGIC Alterar o nome de um notebook é fácil. Clique no nome na parte superior desta página e altere-o. Para facilitar a navegação de volta ao notebook, caso precise, acrescente uma string de teste curta ao final do nome existente.

# COMMAND ----------

# DBTITLE 0,--i18n-a975b60c-9871-4736-9b4f-194577d730f0
# MAGIC %md
# MAGIC
# MAGIC ## Anexando um cluster
# MAGIC
# MAGIC A execução de células em um notebook requer recursos computacionais, que são fornecidos por clusters. Ao executar uma célula em um notebook pela primeira vez, você verá uma solicitação de anexar o notebook a um cluster, caso isso ainda não tenha sido feito.
# MAGIC
# MAGIC Anexe um cluster ao notebook neste momento clicando no menu dropdown próximo ao canto superior direito da página. Selecione o cluster que você criou anteriormente. Essa ação limpará o estado de execução do notebook e o conectará ao cluster selecionado.
# MAGIC
# MAGIC Observe que o menu dropdown oferece a opção de iniciar ou reiniciar o cluster como necessário. Você também pode desanexar e reanexar o notebook a um cluster em uma única operação. Essa ação é útil para limpar o estado de execução quando necessário.

# COMMAND ----------

# DBTITLE 0,--i18n-4cd4b089-e782-4e81-9b88-5c0abd02d03f
# MAGIC %md
# MAGIC
# MAGIC ## Usando %run
# MAGIC
# MAGIC Projetos complexos de qualquer tipo podem ser divididos em componentes mais simples e reutilizáveis.
# MAGIC
# MAGIC No contexto dos notebooks do Databricks, esse recurso é fornecido por meio do comando mágico **`%run`**.
# MAGIC
# MAGIC Quando usados dessa forma, variáveis, funções e blocos de código tornam-se parte do contexto de programação atual.
# MAGIC
# MAGIC Considere este exemplo:
# MAGIC
# MAGIC O **`Notebook_A`** tem quatro comandos:
# MAGIC   1. **`name = "John"`**
# MAGIC   2. **`print(f"Hello {name}")`**
# MAGIC   3. **`%run ./Notebook_B`**
# MAGIC   4. **`print(f"Welcome back {full_name}`**
# MAGIC
# MAGIC O **`Notebook_B`** tem apenas um comando:
# MAGIC   1. **`full_name = f"{name} Doe"`**
# MAGIC
# MAGIC Executar o **`Notebook_B`** falhará porque a variável **`name`** não está definida no **`Notebook_B`**.
# MAGIC
# MAGIC Da mesma forma, poderíamos achar que o **`Notebook_A`** falharia porque usa a variável **`full_name`** que também não está definida no **`Notebook_A`**, mas isso não acontece.
# MAGIC
# MAGIC O que realmente acontece é que os dois notebooks são mesclados como mostrado abaixo, sendo **posteriormente** executados:
# MAGIC 1. **`name = "John"`**
# MAGIC 2. **`print(f"Hello {name}")`**
# MAGIC 3. **`full_name = f"{name} Doe"`**
# MAGIC 4. **`print(f"Welcome back {full_name}")`**
# MAGIC
# MAGIC Isso resulta no comportamento esperado:
# MAGIC * **`Hello John`**
# MAGIC * **`Welcome back John Doe`**

# COMMAND ----------

# DBTITLE 0,--i18n-40ca42ab-4275-4d92-b151-995429e54486
# MAGIC %md
# MAGIC
# MAGIC A pasta que contém este notebook inclui uma subpasta chamada **`ExampleSetupFolder`**, que por sua vez contém um notebook chamado **`example-setup`**.
# MAGIC
# MAGIC Esse notebook simples faz o seguinte:
# MAGIC   - Declara a variável **`my_name`** e a configura como **`None`**
# MAGIC   - Cria o DataFrame **`example_df`** e a tabela **`nyc_taxi`** (que usaremos mais tarde)
# MAGIC
# MAGIC Execute a célula a seguir para executar o notebook.
# MAGIC
# MAGIC Em seguida, abra o notebook **`example-setup`** e altere a variável **`my_name`** de **`None`** para o seu nome (ou o nome de qualquer pessoa) entre aspas. Se feito corretamente, as duas células a seguir deverão ser executadas sem gerar um **`AssertionError`**.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> Você verá referências adicionais a **`_utility-methods`** e **`DBAcademyHelper`** que são usadas neste material didático de configuração e devem ser ignoradas neste exercício.

# COMMAND ----------

# MAGIC %run ./ExampleSetupFolder/example-setup

# COMMAND ----------

assert my_name is not None, "Name is still None"
print(my_name)

# COMMAND ----------

# DBTITLE 0,--i18n-e5ef8dff-bfa6-4f9e-8ad3-d5ef322b978d
# MAGIC %md
# MAGIC
# MAGIC ## Executar uma célula Python
# MAGIC
# MAGIC Execute a célula a seguir para verificar se o notebook **`example-setup`** foi executado exibindo o DataFrame **`example_df`**. Essa tabela consiste em 16 linhas de valores crescentes.

# COMMAND ----------

display(example_df)

# COMMAND ----------

# DBTITLE 0,--i18n-6cb46bcc-9797-4782-931c-a7b8350146b2
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Mudar a linguagem
# MAGIC
# MAGIC Observe que a linguagem default deste notebook está definida como Python. Para usar outra linguagem, clique no botão **Python** à direita do nome do notebook e altere a linguagem default para SQL.
# MAGIC
# MAGIC Observe que as células Python recebem automaticamente o comando mágico <strong><code>&#37;python</code></strong> como prefixo para que sua validade seja mantida. 
# MAGIC
# MAGIC Observe que essa operação também limpa o estado de execução. Depois de alterar a linguagem default do notebook, execute novamente o script de configuração de exemplo acima para continuar.

# COMMAND ----------

# DBTITLE 0,--i18n-478faa69-6814-4725-803b-3414a1a803ae
# MAGIC %md
# MAGIC
# MAGIC ## Criar uma célula Markdown
# MAGIC
# MAGIC Adicione uma célula abaixo da atual. Preencha-a com um Markdown que inclua pelo menos os seguintes elementos:
# MAGIC * Um cabeçalho
# MAGIC * Tópicos
# MAGIC * Um link (usando as convenções de HTML ou Markdown de sua escolha)

# COMMAND ----------

# DBTITLE 0,--i18n-55b2a6c6-2fc6-4c57-8d6d-94bba244d86e
# MAGIC %md
# MAGIC
# MAGIC ## Executar uma célula SQL
# MAGIC
# MAGIC Execute a célula a seguir para consultar uma tabela Delta usando SQL. Essa ação executa uma query simples em uma tabela com suporte de um dataset de exemplo fornecido pelo Databricks e incluído em todas as instalações do DBFS.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM nyc_taxi

# COMMAND ----------

# DBTITLE 0,--i18n-9993ed50-d8cf-4f37-bc76-6b18789447d6
# MAGIC %md
# MAGIC
# MAGIC Execute a célula a seguir para visualizar os arquivos subjacentes que dão suporte a esta tabela.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.datasets}/nyctaxi-with-zipcodes/data")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-c31a3318-a114-46e8-a744-18e8f8aa071e
# MAGIC %md
# MAGIC
# MAGIC ## Limpando o estado do notebook
# MAGIC
# MAGIC Às vezes é útil limpar todas as variáveis definidas no notebook e começar do zero.  Essa ação pode ajudar quando você quer testar células isoladamente ou apenas porque deseja redefinir o estado de execução.
# MAGIC
# MAGIC Acesse o menu **Executar** e selecione a opção **Limpar estado e saídas**.
# MAGIC
# MAGIC Agora tente executar a célula abaixo e observe que as variáveis definidas anteriormente não estão mais presentes. Elas serão redefinidas quando você executar novamente as células anteriores acima.

# COMMAND ----------

# MAGIC %run ./ExampleSetupFolder/example-setup

# COMMAND ----------

print(my_name)

# COMMAND ----------

# DBTITLE 0,--i18n-9947d429-2c10-4047-811f-3f5128527c6d
# MAGIC %md
# MAGIC
# MAGIC ## Encerrando
# MAGIC
# MAGIC Após concluir este laboratório, você deverá ser capaz de manipular notebooks, criar novas células e executar notebooks dentro de notebooks com confiança.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
# MAGIC
