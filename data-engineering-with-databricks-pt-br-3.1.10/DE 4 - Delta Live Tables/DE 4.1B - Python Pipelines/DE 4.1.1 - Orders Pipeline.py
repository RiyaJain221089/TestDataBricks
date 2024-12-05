# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-8c690200-3a21-4838-8d2a-15385fda557f
# MAGIC %md
# MAGIC # Fundamentos da sintaxe Python do DLT
# MAGIC
# MAGIC Este notebook demonstra o uso de Delta Live Tables (DLT) para processar dados brutos de arquivos JSON que chegam ao armazenamento de objetos em nuvem por meio de uma série de tabelas para impulsionar cargas de trabalho analíticas no lakehouse. Aqui demonstramos uma arquitetura medalhão, onde os dados são transformados e enriquecidos gradativamente à medida que fluem por um pipeline. Este notebook se concentra na sintaxe Python do DLT, e não nessa arquitetura, mas esta é uma breve visão geral do seu design:
# MAGIC
# MAGIC *A tabela bronze contém registros brutos carregados de JSON enriquecidos com dados que descrevem como os registros foram ingeridos
# MAGIC *A tabela prateada valida e enriquece os campos de interesse
# MAGIC *A tabela dourada contém dados agregados para gerar insights de negócios e painéis
# MAGIC
# MAGIC ## Objetivos de aprendizado
# MAGIC
# MAGIC Ao final deste caderno, os alunos deverão se sentir confortáveis:
# MAGIC *Declarando tabelas Delta Live
# MAGIC *Ingestão de dados com o Auto Loader
# MAGIC *Usando parâmetros em pipelines DLT
# MAGIC *Aplicando a qualidade dos dados com restrições
# MAGIC *Adicionando comentários às tabelas
# MAGIC *Descrever diferenças na sintaxe e execução de live tables e streaming live tables

# COMMAND ----------

# DBTITLE 0,--i18n-c23e126e-c267-4704-bfca-caee48728551
# MAGIC %md
# MAGIC ## Sobre os notebooks da biblioteca DLT
# MAGIC
# MAGIC A sintaxe DLT não se destina à execução interativa em um notebook. Este notebook precisará ser agendado como parte de um pipeline DLT para execução adequada. 
# MAGIC
# MAGIC No momento em que este notebook foi elaborado, o Databricks Runtime não incluía o módulo **`dlt`**. Portanto, qualquer tentativa de executar um comando do DLT em um notebook falhará. 
# MAGIC
# MAGIC Discutiremos o desenvolvimento e a solução de problemas de código DLT posteriormente neste curso.
# MAGIC
# MAGIC ## Parametrização
# MAGIC
# MAGIC Durante a configuração do pipeline DLT, diversas opções foram especificadas. Uma delas era um par de chave- valor adicionado ao** Configurações** campo.
# MAGIC
# MAGIC As configurações em pipelines do DLT são semelhantes aos parâmetros em jobs do Databricks, mas são definidas como configurações do Spark.
# MAGIC
# MAGIC No Python, podemos acessar esses valores usando **`spark.conf.get()`**.
# MAGIC
# MAGIC Ao longo destas lições, definiremos a variável **`source`** do Python no início do notebook e, em seguida, a usaremos conforme necessário no código.
# MAGIC
# MAGIC ## Observações sobre importações
# MAGIC
# MAGIC O módulo **`dlt`** deve ser importado explicitamente para suas bibliotecas de notebook do Python.
# MAGIC
# MAGIC Neste passo, devemos importar **`pyspark.sql.functions`** como **`F`**.
# MAGIC
# MAGIC Alguns desenvolvedores importam **`*`**, enquanto outros importam apenas as funções necessárias ao notebook sendo usado no momento.
# MAGIC
# MAGIC Nestas lições, usaremos **`F`** durante todo o processo para deixar claro quais métodos são importados dessa biblioteca.

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")

# COMMAND ----------

# DBTITLE 0,--i18n-d260ca6c-11c2-464d-8c16-f5ff6e56b8b0
# MAGIC %md
# MAGIC ## Tabelas como DataFrames
# MAGIC
# MAGIC Existem dois tipos distintos de tabelas persistentes que podem ser criadas com DLT:
# MAGIC ***Live tables ** são views materializadas para o lakehouse; eles retornarão os resultados atuais de qualquer consulta a cada atualização
# MAGIC ***Streaming live tables** são projetadas para serem incrementais, próximo- ao processamento de dados em tempo real
# MAGIC
# MAGIC Observe que ambos os objetos são persistidos como tabelas armazenadas com o protocolo Delta Lake (fornecendo transações ACID, controle de versão e muitos outros benefícios). Falaremos mais sobre as diferenças entre live tables e streaming live tables posteriormente neste notebook.
# MAGIC
# MAGIC O Delta Live Tables apresenta várias novas funções Python que ampliam os recursos de APIs conhecidas do PySpark.
# MAGIC
# MAGIC No centro deste design, o decorador **`@dlt.table`** é adicionado a qualquer função Python que retorne um DataFrame do Spark. (**OBSERVAÇÃO**: Isso inclui DataFrames do Koalas, mas eles não serão abordados neste curso.)
# MAGIC
# MAGIC Se você já trabalhou com o Spark e/ou com Structured Streaming, reconhecerá a maior parte da sintaxe usada no DLT. A grande diferença é que não há nenhum método ou opção para escritas do DataFrame, pois essa lógica é aplicada pelo DLT.
# MAGIC
# MAGIC Assim, a definição básica de uma tabela do DLT terá esta aparência:
# MAGIC
# MAGIC **`@dlt.table`**<br/>
# MAGIC **`def <function-name>():`**<br/>
# MAGIC **`    return (<query>)`**</br>

# COMMAND ----------

# DBTITLE 0,--i18n-969f77f0-34bf-4e23-bdc5-1f0575e99ed1
# MAGIC %md
# MAGIC
# MAGIC ## Ingestão de streaming com Auto Loader
# MAGIC
# MAGIC Databricks desenvolveu o [Auto Loader](https://docs.databricks.com/ingestion/auto- loader/index.html) para fornecer execução otimizada para carregamento incremental de dados do armazenamento de objetos em nuvem no Delta Lake. Usar o Auto Loader com DLT é simples: Basta configurar um diretório de dados de origem, fornecer algumas definições de configuração e escrever uma consulta nos dados de origem. O Auto Loader detectará automaticamente novos arquivos de dados à medida que eles chegam ao local de armazenamento de objetos na nuvem de origem, processando novos registros de forma incremental sem a necessidade de realizar varreduras caras e recalcular resultados para conjuntos de dados em crescimento infinito.
# MAGIC
# MAGIC Usando a configuração **`format("cloudFiles")`**, é possível combinar o Auto Loader com APIs do Structured Streaming para realizar a ingestão incremental de dados em todo o Databricks. No DLT, você definirá apenas as configurações associadas à leitura de dados, observando que os locais para inferência e evolução do esquema também serão configurados automaticamente se essas configurações estiverem habilitadas.
# MAGIC
# MAGIC A query abaixo retorna um DataFrame streaming de uma fonte configurada com o Auto Loader.
# MAGIC
# MAGIC Neste passo, além de passar **`cloudFiles`** como formato, também especificamos:
# MAGIC * A opção **`cloudFiles.format`** como **`json`** (isso indica o formato dos arquivos no local de armazenamento de objetos na nuvem)
# MAGIC * A opção **`cloudFiles.inferColumnTypes`** como **`True`** (para detecção automática dos tipos de cada coluna)
# MAGIC * O caminho do armazenamento de objetos em nuvem para o método **`load`**
# MAGIC * Uma instrução select que inclui algumas **`pyspark.sql.functions`** para enriquecer os dados, além de todos os campos de origem
# MAGIC
# MAGIC Por default, **`@dlt.table`** usará o nome da função como o nome da tabela de destino.

# COMMAND ----------

@dlt.table
def orders_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{source}/orders")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )

# COMMAND ----------

# DBTITLE 0,--i18n-d4d6be28-67fb-44d8-ab4a-cfae62d819ed
# MAGIC %md
# MAGIC ## Validando, enriquecendo e transformando dados
# MAGIC
# MAGIC DLT permite que os usuários declarem facilmente tabelas a partir de resultados de qualquer transformação padrão do Spark. O DLT adiciona novas funcionalidades para verificações de qualidade de dados e fornece diversas opções para permitir que os usuários enriqueçam os metadados nas tabelas criadas.
# MAGIC
# MAGIC Vamos detalhar a sintaxe da consulta abaixo.
# MAGIC
# MAGIC ### Opções para **`@dlt.table()`**
# MAGIC
# MAGIC Há <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-python-ref.html#create-table" target="_blank">várias opções</a> que podem ser especificadas durante a criação da tabela. Neste passo, usamos duas delas para anotar o dataset.
# MAGIC
# MAGIC ##### **`comment`**
# MAGIC
# MAGIC Os comentários de tabela são um padrão em bancos de dados relacionais. Eles podem ser usados para fornecer informações úteis aos usuários em toda a organização. Neste exemplo, escrevemos um pequeno humano- descrição legível da tabela que descreve como os dados estão sendo ingeridos e aplicados (que também pode ser obtido na revisão de outros metadados da tabela).
# MAGIC
# MAGIC ##### **`table_properties`**
# MAGIC
# MAGIC Este campo pode ser usado para passar qualquer número de pares key-value para marcação personalizada de dados. Aqui definimos o valor**`silver`** para a chave **`quality`**.
# MAGIC
# MAGIC Observe que, embora este campo permita que tags personalizadas sejam definidas arbitrariamente, ele também é usado para definir várias configurações que controlam o desempenho de uma tabela. Ao revisar os detalhes da tabela, você também poderá encontrar diversas configurações que são ativadas por padrão sempre que uma tabela é criada.
# MAGIC
# MAGIC ### Restrições de qualidade de dados
# MAGIC
# MAGIC A versão Python do DLT usa funções de decorador para definir <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html#delta-live-tables-data-quality-constraints" target="_blank">restrições de qualidade de dados</a>. Veremos várias delas ao longo do curso.
# MAGIC
# MAGIC O DLT usa instruções booleanas simples para permitir verificações de imposição de qualidade em dados. Na declaração abaixo, nós:
# MAGIC *Declare uma restrição chamada **`valid_date`**
# MAGIC *Defina a verificação condicional que o campo**`order_timestamp`** deve conter um valor superior a 1º de janeiro de 2021
# MAGIC * Instruiremos o DLT a impor uma falha à transação atual e se algum registro violar a restrição usando o decorador **`@dlt.expect_or_fail()`**
# MAGIC
# MAGIC Cada restrição pode ter diversas condições e diversas restrições podem ser definidas para uma única tabela. Além de falhar na atualização, a violação de restrição também pode descartar registros automaticamente ou apenas registrar o número de violações enquanto ainda processa esses registros inválidos.
# MAGIC
# MAGIC ### Métodos de leitura do DLT
# MAGIC
# MAGIC O módulo **`dlt`** do Python fornece os métodos **`read()`** e **`read_stream()`** para configurar facilmente referências a outras tabelas e views em seu pipeline do DLT. Essa sintaxe permite fazer referência a datasets por nome sem qualquer referência ao banco de dados. Você também pode usar **`spark.table("LIVE.<table_name.")`**, onde **`LIVE`** é uma palavra-chave que substitui o banco de dados referenciado no pipeline do DLT.

# COMMAND ----------

@dlt.table(
    comment = "Append only orders with valid timestamps",
    table_properties = {"quality": "silver"})
@dlt.expect_or_fail("valid_date", F.col("order_timestamp") > "2021-01-01")
def orders_silver():
    return (
        dlt.read_stream("orders_bronze")
            .select(
                "processing_time",
                "customer_id",
                "notifications",
                "order_id",
                F.col("order_timestamp").cast("timestamp").alias("order_timestamp")
            )
    )

# COMMAND ----------

# DBTITLE 0,--i18n-e6fed8ba-7028-4d19-9310-cc705b7858e4
# MAGIC %md
# MAGIC ## Live Tables vs. Streaming Live Tables
# MAGIC
# MAGIC As duas funções que analisamos até agora criaram streaming live tables. Abaixo vemos uma função simples que retorna uma live table (ou view materializada) de alguns dados agregados.
# MAGIC
# MAGIC Historicamente, o Spark diferenciou consultas em batch e consultas de streaming. Live tables e streaming live tables têm diferenças semelhantes.
# MAGIC
# MAGIC Observe que esses tipos de tabela herdam a sintaxe (bem como algumas das limitações) das APIs do PySpark e de Structured Streaming.
# MAGIC
# MAGIC Abaixo estão algumas das diferenças entre esses tipos de tabelas.
# MAGIC
# MAGIC ### Live Tables
# MAGIC *Sempre "correto", o que significa que seu conteúdo corresponderá à sua definição após qualquer atualização.
# MAGIC *Retorna os mesmos resultados como se a tabela tivesse acabado de ser definida pela primeira vez em todos os dados.
# MAGIC *Não deve ser modificado por operações externas ao pipeline DLT (você obterá respostas indefinidas ou sua alteração será simplesmente desfeita).
# MAGIC
# MAGIC ### Streaming Live Tables
# MAGIC *Suporta apenas a leitura de "append- apenas" fontes de streaming.
# MAGIC *Lê cada lote de entrada apenas uma vez, não importa o que aconteça (mesmo que as dimensões unidas sejam alteradas ou que a definição da consulta seja alterada, etc.).
# MAGIC *Pode realizar operações na tabela fora do pipeline DLT gerenciado (acrescentar dados, executar GDPR, etc.).

# COMMAND ----------

@dlt.table
def orders_by_date():
    return (
        dlt.read("orders_silver")
            .groupBy(F.col("order_timestamp").cast("date").alias("order_date"))
            .agg(F.count("*").alias("total_daily_orders"))
    )

# COMMAND ----------

# DBTITLE 0,--i18n-facc7d78-16e4-4f03-a232-bb5e4036952a
# MAGIC %md
# MAGIC ## Resumo
# MAGIC
# MAGIC Ao revisar este caderno, você agora deve se sentir confortável:
# MAGIC *Declarando tabelas Delta Live
# MAGIC *Ingestão de dados com o Auto Loader
# MAGIC *Usando parâmetros em pipelines DLT
# MAGIC *Aplicando a qualidade dos dados com restrições
# MAGIC *Adicionando comentários às tabelas
# MAGIC * Descrever diferenças na sintaxe e na execução de live tables e streaming live tables
# MAGIC
# MAGIC No próximo caderno, continuaremos aprendendo sobre essas construções sintáticas enquanto adicionamos alguns conceitos novos.
