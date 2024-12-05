# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-78c97c5e-68ec-4a38-86ce-6af6466b8bd1
# MAGIC %md
# MAGIC
# MAGIC # Informações do Projeto
# MAGIC
# MAGIC * Nome: **Engenharia de Dados com Databricks**
# MAGIC * Versão:  **3.1.10**
# MAGIC * Construído em: **27 de Novembro de 2023 às 21:53:43 UTC**

# COMMAND ----------

# MAGIC
# MAGIC %md ## Resolução de Problemas
# MAGIC A seguinte seção fornece informações gerais sobre a resolução de problemas do material didático produzido pela Academia Databricks. Cada seção correlaciona-se a uma mensagem de erro específica que um usuário final pode receber, com links diretos para conteúdo mais avançado.

# COMMAND ----------

# MAGIC
# MAGIC %md ### Admin Necessário
# MAGIC
# MAGIC A Academia Databricks frequentemente requer que o aluno tenha privilégios de administrador em um espaço de trabalho para efetuar alterações. Em alguns casos, isso é um subproduto de trabalhar com o material do curso. No entanto, em muitos casos, isso é para configurar o ambiente para usuários que de outra forma não precisariam de privilégios de administrador.
# MAGIC
# MAGIC Para obter informações mais atualizadas, por favor consulte <a href="https://files.training.databricks.com/static/troubleshooting.html#admin-required" target="_blank">Solução de problemas de Requisitos de Admin</a>

# COMMAND ----------

# MAGIC %md ### Espaço de trabalho não configurado
# MAGIC
# MAGIC Algum conteúdo produzido pela Academia Databricks requer configuração (ou reconfiguração) do espaço de trabalho e seus ativos. Essa configuração inclui tarefas completas como a criação de políticas de cluster, pools de cluster, concessão de permissões específicas de usuário e outras tarefas semelhantes.
# MAGIC
# MAGIC Esse erro geralmente indica que o notebook [Workspace-Setup]($./Includes/Workspace-Setup) não foi executado.
# MAGIC
# MAGIC Para obter informações mais atualizadas, consulte <a href="https://files.training.databricks.com/static/troubleshooting.html#workspace-setup" target="_blank">Resolução de problemas na configuração do espaço de trabalho</a>

# COMMAND ----------

# MAGIC
# MAGIC %md ### Versão do Spark
# MAGIC
# MAGIC Para garantir que todos os laboratórios sejam executados conforme o esperado, a Databricks Academy exige que o conteúdo fornecido seja executado em um cluster configurado com uma versão específica. Normalmente, essa versão é uma LTS (long-term supported).
# MAGIC
# MAGIC Este curso é testado e requer uma das seguintes versões do DBR (Databricks Runtime): **12.2.x-scala2.12, 12.2.x-photon-scala2.12, 12.2.x-cpu-ml-scala2.12**.
# MAGIC
# MAGIC Para obter informações mais atualizadas, por favor acesse <a href="https://files.training.databricks.com/static/troubleshooting.html#spark-version" target="_blank">Resolução de Problemas com a Versão do Spark</a>

# COMMAND ----------

# MAGIC
# MAGIC %md ### Não é possível escrever no DBFS
# MAGIC
# MAGIC DBFS significa Databricks File System e é um sistema de arquivos virtual que a Academia Databricks usa para ler dados e gravar dados. Existem dois locais principais e, dependendo do material do curso, alguns locais terciários.
# MAGIC  
# MAGIC O primeiro local é **dbfs:/mnt/dbacademy-datasets/**, que é um local de armazenamento para conjuntos de dados usados por este curso. Os conjuntos de dados são baixados de nossos repositórios de dados e copiados para este local para fornecer acesso mais rápido, na região, aos conjuntos de dados do curso. Arquivos gravados neste local devem ser tratados como somente leitura e compartilhados por todos os consumidores deste material do curso.
# MAGIC
# MAGIC O segundo local é **dbfs:/mnt/dbacademy-users/**, um local de armazenamento para conjuntos de dados específicos do usuário e arquivos diversos gerados ao trabalhar com o material do curso. Esta pasta é subdividida por usuário e depois por curso, criando um diretório de trabalho por usuário por curso.
# MAGIC
# MAGIC Em alguns casos, dados terciários podem ser gravados em **dbfs:/user/** como um local de armazenamento padrão para Spark e tabelas Spark. Assim como dbacademy-users, esta pasta é subdividida por usuário.
# MAGIC
# MAGIC Para obter informações mais atualizadas, consulte <a href="https://files.training.databricks.com/static/troubleshooting.html#cannot-write-dbfs" target="_blank">Solução de Problemas ao Escrever no DBFS</a>

# COMMAND ----------

# MAGIC %md ### Não é possível instalar bibliotecas
# MAGIC
# MAGIC A Databricks Academy fornece bibliotecas personalizadas para facilitar o desenvolvimento de seu material didático. Seu cluster requer acesso a essas bibliotecas hospedadas remotamente para que possam ser instaladas em seu cluster.
# MAGIC
# MAGIC Várias empresas restringem o acesso a sites externos para limitar a exfiltração de dados e lidar com outras preocupações de segurança, como vazamento de informações potencialmente sensíveis ou proprietárias.
# MAGIC
# MAGIC Para obter informações mais atualizadas, consulte <a href="https://files.training.databricks.com/static/troubleshooting.html#cannot-install-libraries" target="_blank">Resolução de problemas na instalação de bibliotecas</a>

# COMMAND ----------

# MAGIC
# MAGIC %md ### Requer Catálogo Unity
# MAGIC
# MAGIC O Catálogo Unity (UC) é um serviço fornecido pela Databricks. Este material do curso requer que o espaço de trabalho esteja habilitado para UC. No entanto, nem todos os espaços de trabalho têm (ou podem ter) essa funcionalidade ativada.
# MAGIC
# MAGIC Para obter informações mais atualizadas, consulte <a href="https://files.training.databricks.com/static/troubleshooting.html#requires-unity-catalog" target="_blank">Solução de problemas Requisitos do Catálogo Unity</a>

# COMMAND ----------

# MAGIC %md ### Não é possível criar o esquema
# MAGIC
# MAGIC Este curso exigirá que você crie um esquema (também conhecido como banco de dados), o que por sua vez requer que você tenha as permissões necessárias. Às vezes, isso é feito como parte da construção de um exemplo mais complexo. Mas na maioria dos casos, o esquema sendo criado é um esquema específico do usuário usado para fornecer isolamento em nível de usuário, à medida que vários alunos trabalham em várias lições no mesmo espaço de trabalho.
# MAGIC
# MAGIC Para obter informações mais atualizadas, consulte <a href="https://files.training.databricks.com/static/troubleshooting.html#cannot-create-schema" target="_blank">Solução de problemas ao criar esquemas</a>

# COMMAND ----------

# MAGIC %md ### Não é possível criar um catálogo
# MAGIC
# MAGIC Este curso exigirá que você crie um catálogo (geralmente em conjunto com o uso do Unity Catalog) o que, por sua vez, requer que você tenha as permissões necessárias. Na maioria dos casos, o novo catálogo é um catálogo específico do usuário usado para fornecer isolamento em nível de usuário, enquanto vários alunos trabalham em várias lições no mesmo espaço de trabalho.
# MAGIC
# MAGIC Para obter informações mais atuais, consulte <a href="https://files.training.databricks.com/static/troubleshooting.html#cannot-create-catalog" target="_blank">Solucionando Problemas ao Criar Catálogos</a>

# COMMAND ----------

# DBTITLE 0,--i18n-bd2e5e62-c077-477c-a1ff-93642a5a4db7
# MAGIC %md
# MAGIC ## Direitos Autorais
# MAGIC Esta seção documenta os vários direitos autorais relacionados aos conjuntos de dados usados neste curso.
# MAGIC
# MAGIC Execute a célula a seguir para obter informações adicionais sobre os conjuntos de dados deste curso e seus direitos autorais.

# COMMAND ----------

# MAGIC %run ./Includes/Print-Dataset-Copyrights

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
