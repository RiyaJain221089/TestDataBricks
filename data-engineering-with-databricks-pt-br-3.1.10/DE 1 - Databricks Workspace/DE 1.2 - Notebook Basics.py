# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-2d4e57a0-a2d5-4fad-80eb-98ff30d09a37
# MAGIC %md
# MAGIC
# MAGIC # Noções básicas de notebooks
# MAGIC
# MAGIC Os notebooks são o método principal de desenvolvimento e execução de código de forma interativa no Databricks. Esta lição fornece uma introdução básica ao uso de notebooks do Databricks.
# MAGIC
# MAGIC Se você já usou notebooks do Databricks, mas esta é a primeira vez que executa um notebook no Databricks Repos, notará que a funcionalidade básica é a mesma. Na próxima lição, analisaremos algumas das funcionalidades que o Databricks Repos acrescenta aos notebooks.
# MAGIC
# MAGIC ## Objetivos de aprendizado
# MAGIC Ao final desta lição, você deverá ser capaz de:
# MAGIC * Anexar um notebook a um cluster
# MAGIC * Executar uma célula em um notebook
# MAGIC * Definir a linguagem de um notebook
# MAGIC * Descrever e usar comandos mágicos
# MAGIC * Criar e executar uma célula SQL
# MAGIC * Criar e executar uma célula Python
# MAGIC * Criar uma célula Markdown
# MAGIC * Exportar um notebook do Databricks
# MAGIC * Exportar uma coleção de notebooks do Databricks

# COMMAND ----------

# DBTITLE 0,--i18n-e7c4cc85-5ab7-46c1-9da3-f9181d77d118
# MAGIC %md
# MAGIC
# MAGIC ## Anexar a um cluster
# MAGIC
# MAGIC Na lição anterior, você criou um cluster ou identificou um cluster que uma pessoa que administra seu workspace configurou para seu uso.
# MAGIC
# MAGIC No canto superior direito da tela, clique no seletor de cluster (botão "Conectar") e escolha um cluster no menu dropdown. Quando o notebook está conectado a um cluster, esse botão mostra o nome do cluster.
# MAGIC
# MAGIC **OBSERVAÇÃO**: A implantação de um cluster pode levar vários minutos. Um círculo verde sólido aparecerá à esquerda do nome do cluster assim que os recursos forem implantados. Se o cluster tiver um círculo cinza vazio à esquerda, você precisará seguir as instruções para <a href="https://docs.databricks.com/clusters/clusters-manage.html#start-a-cluster" target="_blank">iniciar um cluster</a>.

# COMMAND ----------

# DBTITLE 0,--i18n-23aed87f-d375-4b81-8c3c-d4375cda0384
# MAGIC %md
# MAGIC
# MAGIC ## Noções básicas de notebooks
# MAGIC
# MAGIC Os notebooks permitem executar o código célula por célula. É possível combinar várias linguagens em um notebook. Os usuários podem adicionar gráficos, imagens e texto de marcação para aprimorar o código.
# MAGIC
# MAGIC Os notebooks que usamos ao longo deste curso se destinam a ser instrumentos de aprendizagem. É fácil implantar notebooks como código de produção com o Databricks, que ainda fornece um conjunto robusto de ferramentas para exploração de dados, geração de relatórios e criação de painéis.
# MAGIC
# MAGIC ### Executando uma célula
# MAGIC * Execute a célula abaixo usando uma das seguintes opções:
# MAGIC   * **CTRL+ENTER** ou **CTRL+RETURN**
# MAGIC   * **SHIFT+ENTER** ou **SHIFT+RETURN** para executar a célula e ir para a próxima
# MAGIC   * Usando as opções **Executar célula**, **Executar todos acima** ou **Executar todos abaixo**, como mostrado aqui<br/><img style="box-shadow: 5px 5px 5px 0px rgba(0,0,0,0.25); border: 1px solid rgba(0,0,0,0.25);" src="https://files.training.databricks.com/images/notebook-cell-run-cmd.png"/>

# COMMAND ----------

print("I'm running Python!")

# COMMAND ----------

# DBTITLE 0,--i18n-5b14b4c2-c009-4786-8058-a3ddb61fa41d
# MAGIC %md
# MAGIC
# MAGIC **OBSERVAÇÃO**: Quando o código é executado célula por célula, as células podem ser executadas várias vezes ou fora de ordem. A menos que receba uma instrução explícita, você deve sempre presumir que os notebooks deste curso se destinam à execução de uma célula por vez, de cima para baixo. Se você encontrar um erro, leia o texto antes e depois de uma célula para ter certeza de que o erro não foi gerado como parte intencional do aprendizado antes de tentar solucionar o problema. A maioria dos erros pode ser resolvida executando células anteriores em um notebook que foram puladas ou executando novamente todo o notebook desde o início.

# COMMAND ----------

# DBTITLE 0,--i18n-9be4ac54-8411-45a0-ad77-7173ec7402f8
# MAGIC %md
# MAGIC
# MAGIC ### Configurando a linguagem default do notebook
# MAGIC
# MAGIC A célula acima executa um comando Python, que é a linguagem default atual definida no notebook.
# MAGIC
# MAGIC Os notebooks do Databricks são compatíveis com Python, SQL, Scala e R. A linguagem pode ser selecionada quando o notebook é criado, mas isso pode ser alterado a qualquer momento.
# MAGIC
# MAGIC A linguagem default é mostrada à direita do título do notebook, na parte superior da página. Ao longo deste curso, usaremos uma combinação de notebooks SQL e Python.
# MAGIC
# MAGIC Vamos alterar a linguagem default do notebook para SQL.
# MAGIC
# MAGIC Passos:
# MAGIC * Clique em **Python** ao lado do título do notebook na parte superior da tela
# MAGIC * Na IU que aparece, selecione **SQL** na lista dropdown 
# MAGIC
# MAGIC **OBSERVAÇÃO**: Na célula imediatamente anterior a esta, você deve ver uma nova linha aparecer com <strong><code>&#37;python</code></strong>. Falaremos sobre isso em breve.

# COMMAND ----------

# DBTITLE 0,--i18n-3185e9b5-fcba-40aa-916b-5f3daa555cf5
# MAGIC %md
# MAGIC
# MAGIC ### Criar e executar uma célula SQL
# MAGIC
# MAGIC * Destaque esta célula e pressione o botão **B** no teclado para criar uma nova célula abaixo dela
# MAGIC * Copie o código a seguir na célula abaixo e execute-a
# MAGIC
# MAGIC **`%sql`**<br/>
# MAGIC **`SELECT "I'm running SQL!"`**
# MAGIC
# MAGIC **OBSERVAÇÃO**: Há vários métodos diferentes para adicionar, mover e excluir células, incluindo opções de GUI e atalhos de teclado. Consulte a <a href="https://docs.databricks.com/notebooks/notebooks-use.html#develop-notebooks" target="_blank">documentação</a> para obter detalhes.

# COMMAND ----------

# DBTITLE 0,--i18n-5046f81c-cdbf-42c3-9b39-3be0721d837e
# MAGIC %md
# MAGIC
# MAGIC ## Comandos mágicos
# MAGIC * Os comandos mágicos são específicos para os notebooks do Databricks
# MAGIC * Eles são muito semelhantes aos comandos mágicos encontrados em notebooks comparáveis
# MAGIC * São comandos integrados que fornecem o mesmo resultado, independentemente da linguagem do notebook
# MAGIC * Um único símbolo de porcentagem (%) no início de uma célula identifica um comando mágico
# MAGIC   * Você só pode ter um comando mágico por célula
# MAGIC   * O comando mágico deve ser o primeiro elemento em uma célula

# COMMAND ----------

# DBTITLE 0,--i18n-39d2c50e-4b92-46ef-968c-f358114685be
# MAGIC %md
# MAGIC
# MAGIC ### Comandos mágicos de linguagem
# MAGIC Os comandos mágicos de linguagem permitem executar código em linguagens diferentes da default do notebook. Neste curso, veremos os seguintes comandos mágicos de linguagem:
# MAGIC * <strong><code>&#37;python</code></strong>
# MAGIC * <strong><code>&#37;sql</code></strong>
# MAGIC
# MAGIC Não é necessário adicionar um comando mágico de linguagem ao tipo de notebook definido no momento.
# MAGIC
# MAGIC Quando mudamos a linguagem do notebook de Python para SQL acima, o comando <strong><code>&#37;python</code></strong> foi adicionado às células existentes escritas em Python.
# MAGIC
# MAGIC **OBSERVAÇÃO**: Em vez de alterar constantemente a linguagem default de um notebook, você deve manter uma linguagem primária como default e usar apenas comandos mágicos de linguagem quando precisar executar o código em outra linguagem.

# COMMAND ----------

print("Hello Python!")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Hello SQL!"

# COMMAND ----------

# DBTITLE 0,--i18n-94da1696-d0cf-418f-ba5a-d105a5ecdaac
# MAGIC %md
# MAGIC
# MAGIC ### Markdown
# MAGIC
# MAGIC O comando mágico **&percnt;md** permite renderizar o Markdown em uma célula:
# MAGIC * Clique duas vezes na célula para começar a editá-la
# MAGIC * Em seguida, pressione **`Esc`** para interromper a edição
# MAGIC
# MAGIC # Título um
# MAGIC ## Título dois
# MAGIC ### Título três
# MAGIC
# MAGIC Este é um teste do sistema de transmissão de emergência. Isto é apenas um teste.
# MAGIC
# MAGIC Este é um texto com uma palavra **em negrito**.
# MAGIC
# MAGIC Este é um texto com uma palavra *em itálico*.
# MAGIC
# MAGIC Esta é uma lista ordenada
# MAGIC 1. um
# MAGIC 1. dois
# MAGIC 1. três
# MAGIC
# MAGIC Esta é uma lista não ordenada
# MAGIC * maçãs
# MAGIC * pêssegos
# MAGIC * bananas
# MAGIC
# MAGIC Links/HTML incorporado: <a href="https://en.wikipedia.org/wiki/Markdown" target="_blank">Markdown - Wikipédia</a>
# MAGIC
# MAGIC Imagens:
# MAGIC ![Spark Engines](https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png)
# MAGIC
# MAGIC E, claro, tabelas:
# MAGIC
# MAGIC | nome   | valor |
# MAGIC |--------|-------|
# MAGIC | Yi     | 1     |
# MAGIC | Ali    | 2     |
# MAGIC | Selina | 3     |

# COMMAND ----------

# DBTITLE 0,--i18n-537e86bc-782f-4167-9899-edb3bd2b9e38
# MAGIC %md
# MAGIC
# MAGIC ### %run
# MAGIC * Você pode executar um notebook a partir de outro notebook usando o comando mágico **%run**
# MAGIC * Os notebooks a serem executados são especificados com caminhos relativos
# MAGIC * O notebook referenciado é executado como se fizesse parte do notebook atual; portanto, views temporárias e outras declarações locais estarão disponíveis no notebook que faz a chamada

# COMMAND ----------

# DBTITLE 0,--i18n-d5c27671-b3c8-4b8c-a559-40cf7988f92f
# MAGIC %md
# MAGIC
# MAGIC Remover o comentário e executar a seguinte célula gerará o seguinte erro:<br/>
# MAGIC **`Error in SQL statement: AnalysisException: Table or view not found: demo_tmp_vw`**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM demo_tmp_vw

# COMMAND ----------

# DBTITLE 0,--i18n-d0df4a17-abb4-42d3-ba37-c9a78f4fc9c0
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Mas podemos declarar o comentário e algumas outras variáveis e funções executando esta célula:

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01.2

# COMMAND ----------

# DBTITLE 0,--i18n-6a001755-5259-4fef-a5d3-2661d5301237
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC O notebook **`../Includes/Classroom-Setup-01.2`** que referenciamos inclui lógica para criar e **`USE`** um esquema, bem como criar a view temporária **`demo_temp_vw`**.
# MAGIC
# MAGIC Podemos confirmar se a view temporária agora está disponível na sessão atual do notebook com a query a seguir.

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM demo_tmp_vw

# COMMAND ----------

# DBTITLE 0,--i18n-c28ecc03-8919-488f-bce7-e2fc0a451870
# MAGIC %md
# MAGIC
# MAGIC Usaremos este padrão de notebooks de "configuração" ao longo do curso para ajudar a definir o ambiente para lições e laboratórios.
# MAGIC
# MAGIC As variáveis, funções e outros objetos fornecidos devem ser facilmente identificáveis, pois fazem parte do objeto **`DA`**, que é uma instância de **`DBAcademyHelper`**.
# MAGIC
# MAGIC Com isso em mente, a maioria das lições usará variáveis derivadas do seu nome de usuário para organizar arquivos e esquemas. 
# MAGIC
# MAGIC Esse padrão permite evitar colisões com outros usuários em um workspace compartilhado.
# MAGIC
# MAGIC A célula abaixo usa Python para imprimir algumas das variáveis previamente definidas no script de configuração deste notebook:

# COMMAND ----------

print(f"DA:                   {DA}")
print(f"DA.username:          {DA.username}")
print(f"DA.paths.working_dir: {DA.paths.working_dir}")
print(f"DA.schema_name:       {DA.schema_name}")

# COMMAND ----------

# DBTITLE 0,--i18n-1145175f-c51e-4cf5-a4a5-b0e3290a73a2
# MAGIC %md
# MAGIC
# MAGIC Além disso, essas mesmas variáveis são "injetadas" no contexto SQL para que possamos utilizá-las em instruções SQL.
# MAGIC
# MAGIC Falaremos mais sobre isso posteriormente, mas a célula a seguir mostra um exemplo rápido.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png"> Observe a diferença sutil, mas importante, na diferenciação de maiúsculas e minúsculas da palavra **`da`** e **`DA`** nestes dois exemplos.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${da.username}' AS current_username,
# MAGIC        '${da.paths.working_dir}' AS working_directory,
# MAGIC        '${da.schema_name}' as schema_name

# COMMAND ----------

# DBTITLE 0,--i18n-8330ad3c-8d48-42fa-9b6f-72e818426ed4
# MAGIC %md
# MAGIC
# MAGIC ## Utilitários do Databricks
# MAGIC Os notebooks do Databricks incluem um objeto **`dbutils`** que fornece vários comandos de utilitários para configurar e interagir com o ambiente: <a href="https://docs.databricks.com/user-guide/dev-tools/dbutils.html" target="_blank">documentação sobre dbutils</a>
# MAGIC
# MAGIC Ao longo deste curso, usaremos ocasionalmente **`dbutils.fs.ls()`** para listar diretórios de arquivos a partir de células Python.

# COMMAND ----------

path = f"{DA.paths.datasets}"
dbutils.fs.ls(path)

# COMMAND ----------

# DBTITLE 0,--i18n-25eb75f8-6e75-41a6-a304-d140844fa3e6
# MAGIC %md
# MAGIC
# MAGIC ## display()
# MAGIC
# MAGIC A execução de queries SQL a partir de células sempre exibe resultados em formato tabular renderizado.
# MAGIC
# MAGIC Quando dados tabulares são retornados por uma célula Python, podemos chamar **`display`** para obter o mesmo tipo de visualização.
# MAGIC
# MAGIC Neste passo, agruparemos o comando list anterior no sistema de arquivos com **`display`**.

# COMMAND ----------

path = f"{DA.paths.datasets}"
files = dbutils.fs.ls(path)
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-42b624ff-8cc9-4318-bedc-e3b340cb1b81
# MAGIC %md
# MAGIC
# MAGIC O comando **`display()`** tem os seguintes recursos e limitações:
# MAGIC * Limita a visualização dos resultados a 10.000 registros ou 2 MB, o que for menor
# MAGIC * Fornece um botão para download dos dados de resultados como CSV
# MAGIC * Permite renderizar gráficos

# COMMAND ----------

# DBTITLE 0,--i18n-c7f02dde-9b21-4edb-bded-5ce0eed56d03
# MAGIC %md
# MAGIC
# MAGIC ## Baixando notebooks
# MAGIC
# MAGIC Há várias opções para baixar notebooks individuais ou coleções de notebooks.
# MAGIC
# MAGIC A seguir, mostraremos como baixar este notebook, bem como uma coleção de todos os notebooks deste curso.
# MAGIC
# MAGIC ### Baixar um notebook
# MAGIC
# MAGIC Passos:
# MAGIC * No canto superior esquerdo do notebook, clique na opção **Arquivo** 
# MAGIC * No menu mostrado, passe o mouse sobre ** Export** e selecione **Arquivo de origem**
# MAGIC
# MAGIC O notebook será baixado para o computador. O arquivo terá o nome do notebook atual e a extensão de arquivo da linguagem default. Você pode abrir esse arquivo com qualquer editor de arquivos e visualizar o conteúdo bruto do notebook do Databricks.
# MAGIC
# MAGIC Esses arquivos de origem podem ser carregados por upload em qualquer workspace do Databricks.
# MAGIC
# MAGIC ### Baixar uma coleção de notebooks
# MAGIC
# MAGIC **OBSERVAÇÃO**: As instruções a seguir pressupõem que você importou os materiais usando **Repos**.
# MAGIC
# MAGIC Passos:
# MAGIC * Clique em ![](https://files.training.databricks.com/images/repos-icon.png) **Repos** na barra lateral esquerda
# MAGIC   * Essa ação deve mostrar os diretórios pais do notebook.
# MAGIC * No lado esquerdo da visualização do diretório, deve haver uma seta para a esquerda no meio da tela. Clique nessa seta para subir na hierarquia de arquivos.
# MAGIC * Você deverá ver um diretório chamado **Data Engineer Learning Path**. Clique na seta para baixo para abrir um menu.
# MAGIC * Nesse menu, passe o mouse sobre **Export** e selecione **Arquivo DBC**.
# MAGIC
# MAGIC O arquivo DBC (Databricks Cloud) baixado contém uma coleção compactada dos diretórios e notebooks deste curso. Esses arquivos DBC não devem ser editado localmente, mas podem ser carregados com segurança em qualquer workspace do Databricks quando o usuário deseja mover ou compartilhar o conteúdo do notebook.
# MAGIC
# MAGIC **OBSERVAÇÃO**: Quado você baixa uma coleção de DBCs, as visualizações de resultados e gráficos também são exportadas. No download dos notebooks de origem, só o código é salvo.

# COMMAND ----------

# DBTITLE 0,--i18n-30e63e01-ca85-461a-b980-ea401904731f
# MAGIC %md
# MAGIC
# MAGIC ## Aprendendo mais
# MAGIC
# MAGIC Encorajamos você a explorar a documentação para saber mais sobre os vários recursos da plataforma e dos notebooks do Databricks.
# MAGIC * <a href="https://docs.databricks.com/user-guide/index.html#user-guide" target="_blank">Guia do usuário</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/getting-started.html" target="_blank">Introdução ao Databricks</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/notebooks/index.html" target="_blank">Guia do usuário/notebooks</a>
# MAGIC * <a href="https://docs.databricks.com/notebooks/notebooks-manage.html#notebook-external-formats" target="_blank">Importando notebooks/formatos suportados</a>
# MAGIC * <a href="https://docs.databricks.com/repos/index.html" target="_blank">Repos</a>
# MAGIC * <a href="https://docs.databricks.com/administration-guide/index.html#administration-guide" target="_blank">Guia de administração</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/clusters/index.html" target="_blank">Configuração de clusters</a>
# MAGIC * <a href="https://docs.databricks.com/api/latest/index.html#rest-api-2-0" target="_blank">API REST</a>
# MAGIC * <a href="https://docs.databricks.com/release-notes/index.html#release-notes" target="_blank">Notas sobre a versão</a>

# COMMAND ----------

# DBTITLE 0,--i18n-9987fd58-1023-4dbd-8319-40332f909181
# MAGIC %md
# MAGIC
# MAGIC ## Mais uma observação! 
# MAGIC
# MAGIC Ao final de cada lição, você verá o comando **`DA.cleanup()`**.
# MAGIC
# MAGIC Esse método descarta esquemas e diretórios de trabalho específicos da lição para tentar manter o workspace limpo e preservar a estabilidade da lição.
# MAGIC
# MAGIC Execute a célula a seguir para excluir as tabelas e arquivos associados a esta lição.
# MAGIC

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
