# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-133f57e8-6ff4-4b56-be71-9598e1513fd3
# MAGIC %md
# MAGIC # Sintaxe Python adicional no DLT
# MAGIC
# MAGIC Os DLT Pipelines facilitam a combinação de vários conjuntos de dados em uma única carga de trabalho escalonável usando um ou vários notebooks.
# MAGIC
# MAGIC No último notebook, revisamos algumas das funcionalidades básicas da sintaxe do DLT durante o processamento de dados do armazenamento de objetos em nuvem por meio de uma série de queries para validar e enriquecer os registros em cada passo. Este notebook segue de forma semelhante a arquitetura medalhão, mas introduz uma série de novos conceitos.
# MAGIC *Os registros brutos representam informações de change data capture (CDC) sobre clientes 
# MAGIC *A tabela bronze usa novamente o Auto Loader para ingerir dados JSON do armazenamento de objetos em nuvem
# MAGIC *Uma tabela é definida para impor restrições antes de passar os registros para a camada silver
# MAGIC * **`dlt.apply_changes()`** é usado para processar automaticamente dados de CDC na camada silver como uma <a href="https://en.wikipedia.org/wiki/Slowly_changing_dimension" target="_blank">tabela de dimensões que mudam lentamente (SCD)</a> de Tipo 1
# MAGIC *Uma tabela gold é definida para calcular um agregado da versão atual desta tabela Tipo 1
# MAGIC *É definida uma visualização que se une a tabelas definidas em outro notebook
# MAGIC
# MAGIC ## Objetivos de aprendizado
# MAGIC
# MAGIC Ao final desta lição, os alunos deverão se sentir confortáveis:
# MAGIC * Processar dados de CDC com **`dlt.apply_changes()`**
# MAGIC *Declarando live views
# MAGIC *Juntando-se live tables
# MAGIC *Descrevendo como os notebooks da biblioteca DLT funcionam juntos em um pipeline
# MAGIC *Agendando vários notebooks em um pipeline DLT

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")

# COMMAND ----------

# DBTITLE 0,--i18n-ec15ca9a-719d-41b3-9a45-93f53bc9fb73
# MAGIC %md
# MAGIC ## Ingerir dados com Auto Loader
# MAGIC
# MAGIC Como no último notebook, definimos uma tabela bronze em relação a uma fonte de dados configurada com o Auto Loader.
# MAGIC
# MAGIC Observe que o código abaixo omite a opção Auto Loader para inferir o esquema. Quando os dados são ingeridos do JSON sem o esquema fornecido ou inferido, os campos terão os nomes corretos, mas todos serão armazenados como**`STRING`** tipo.
# MAGIC
# MAGIC ## Especificando os nomes das tabelas
# MAGIC
# MAGIC O código abaixo demonstra o uso da opção **`name`** para a declaração de tabelas do DLT. Essa opção permite que os desenvolvedores especifiquem o nome da tabela resultante separadamente da definição da função que cria o DataFrame a partir do qual a tabela é definida.
# MAGIC
# MAGIC No exemplo abaixo, usamos essa opção para seguir a convenção de nomenclatura de tabela **`<dataset-name>_<data-quality>`** e a convenção de nomenclatura de função que descreve a ação realizada pela função. (Se não especificarmos essa opção, o nome da tabela será inferido da função como **`ingest_customers_cdc`**.)

# COMMAND ----------

@dlt.table(
    name = "customers_bronze",
    comment = "Raw data from customers CDC feed"
)
def ingest_customers_cdc():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{source}/customers")
        .select(
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
            "*"
        )
    )

# COMMAND ----------

# DBTITLE 0,--i18n-ada9c9fb-cb58-46d8-9702-9ad16c95d821
# MAGIC %md
# MAGIC ## Aplicação da Qualidade Continuada
# MAGIC
# MAGIC A consulta abaixo demonstra:
# MAGIC *As 3 opções de comportamento quando as restrições são violadas
# MAGIC *Uma consulta com múltiplas restrições
# MAGIC *Múltiplas condições fornecidas para uma restrição
# MAGIC *Usando um construído- na função SQL em uma restrição
# MAGIC
# MAGIC Sobre a fonte de dados:
# MAGIC *Os dados são um feed do CDC que contém**`INSERT`**,**`UPDATE`**, e**`DELETE`** operações. 
# MAGIC *As operações de atualização e inserção devem conter entradas válidas para todos os campos.
# MAGIC *As operações de exclusão devem conter**`NULL`** valores para todos os campos, exceto o carimbo de data/hora,**`customer_id`** e campos de operação.
# MAGIC
# MAGIC Para garantir que apenas dados bons cheguem à nossa tabela silver, escreveremos uma série de regras de aplicação de qualidade que ignoram os valores nulos esperados nas operações de exclusão.
# MAGIC
# MAGIC Descreveremos cada uma dessas restrições abaixo:
# MAGIC
# MAGIC ##### **`valid_id`**
# MAGIC Esta restrição fará com que nossa transação falhe se um registro contiver um valor nulo no**`customer_id`** campo.
# MAGIC
# MAGIC ##### **`valid_operation`**
# MAGIC Esta restrição descartará os registros com um valor nulo no campo **`operation`**.
# MAGIC
# MAGIC ##### **`valid_address`**
# MAGIC Esta restrição verifica se o**`operation`** campo é**`DELETE`** ; caso contrário, ele verifica valores nulos em qualquer um dos 4 campos que compõem um endereço. Como não há instruções adicionais sobre o que fazer com registros inválidos, as linhas violadas serão registradas nas métricas, mas não descartadas.
# MAGIC
# MAGIC ##### **`valid_email`**
# MAGIC Esta restrição usa correspondência de padrões regex para verificar se o valor no**`email`** campo é um endereço de e-mail válido. Contém lógica para não aplicar isso aos registros se o**`operation`** campo é**`DELETE`** (porque estes terão um valor nulo para o**`email`** campo). Os registros violados são descartados.

# COMMAND ----------

@dlt.table
@dlt.expect_or_fail("valid_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_operation", "operation IS NOT NULL")
@dlt.expect("valid_name", "name IS NOT NULL or operation = 'DELETE'")
@dlt.expect("valid_adress", """
    (address IS NOT NULL and 
    city IS NOT NULL and 
    state IS NOT NULL and 
    zip_code IS NOT NULL) or
    operation = "DELETE"
    """)
@dlt.expect_or_drop("valid_email", """
    rlike(email, '^([a-zA-Z0-9_\\\\-\\\\.]+)@([a-zA-Z0-9_\\\\-\\\\.]+)\\\\.([a-zA-Z]{2,5})$') or 
    operation = "DELETE"
    """)
def customers_bronze_clean():
    return (
        dlt.read_stream("customers_bronze")
    )

# COMMAND ----------

# DBTITLE 0,--i18n-e3093247-1329-47b7-b06d-51284be37799
# MAGIC %md
# MAGIC ## Processando dados de CDC com **`dlt.apply_changes()`**
# MAGIC
# MAGIC DLT introduz uma nova estrutura sintática para simplificar o processamento de feeds CDC.
# MAGIC
# MAGIC **`dlt.apply_changes()`** tem as seguintes garantias e requisitos:
# MAGIC *Executa ingestão incremental/streaming de dados do CDC
# MAGIC *Fornece sintaxe simples para especificar um ou mais campos como chave primária de uma tabela
# MAGIC *A suposição padrão é que as linhas conterão inserções e atualizações
# MAGIC *Opcionalmente, pode aplicar exclusões
# MAGIC * Ordena automaticamente registros de pedidos atrasados usando um campo de sequenciamento fornecido pelo usuário
# MAGIC * Usa uma sintaxe simples para especificar colunas a serem ignoradas com **`except_column_list`**
# MAGIC *O padrão será aplicar alterações como SCD Tipo 1
# MAGIC
# MAGIC O código abaixo:
# MAGIC * Cria a tabela **`customers_silver`**; **`dlt.apply_changes()`** exige que a tabela de destino seja declarada em uma instrução separada
# MAGIC *Identifica o**`customers_silver`** tabela como o destino no qual as alterações serão aplicadas
# MAGIC * Especifica a tabela **`customers_bronze_clean`** como a fonte (**OBSERVAÇÃO**: a fonte deve ser do tipo somente inserção)
# MAGIC *Identifica o**`customer_id`** como a chave primária
# MAGIC *Especifica o**`timestamp`** campo para ordenar como as operações devem ser aplicadas
# MAGIC *Especifica que os registros onde o**`operation`** campo é**`DELETE`** deve ser aplicado como exclusões
# MAGIC *Indica que todos os campos devem ser adicionados à tabela de destino, exceto**`operation`**,**`source_file`**, e**`_rescued_data`**

# COMMAND ----------

dlt.create_target_table(
    name = "customers_silver")

dlt.apply_changes(
    target = "customers_silver",
    source = "customers_bronze_clean",
    keys = ["customer_id"],
    sequence_by = F.col("timestamp"),
    apply_as_deletes = F.expr("operation = 'DELETE'"),
    except_column_list = ["operation", "source_file", "_rescued_data"])

# COMMAND ----------

# DBTITLE 0,--i18n-43fe6e78-d4c2-46c9-9116-232b1c9bbcd9
# MAGIC %md
# MAGIC ## Consultando tabelas com alterações aplicadas
# MAGIC
# MAGIC **`dlt.apply_changes()`** por default, cria uma tabela SCD de Tipo 1, o que significa que cada key exclusiva terá no máximo um registro e que as atualizações substituirão as informações originais.
# MAGIC
# MAGIC Embora o alvo da nossa operação na célula anterior tenha sido definido como uma streaming live table, os dados estão sendo atualizados e excluídos nesta tabela (e assim quebra o anexo- apenas requisitos para fontes de streaming live tables. Como tal, as operações downstream não podem realizar consultas de streaming nesta tabela. 
# MAGIC
# MAGIC Esse padrão garante que, se alguma atualização chegar fora de ordem, os resultados downstream poderão ser recalculados adequadamente para refletir as atualizações. Também garante que quando os registros são excluídos de uma tabela de origem, esses valores não são mais refletidos nas tabelas posteriormente no pipeline.
# MAGIC
# MAGIC Abaixo, definimos uma consulta agregada simples para criar uma tabela ativa a partir dos dados na**`customers_silver`** tabela.

# COMMAND ----------

@dlt.table(
    comment="Total active customers per state")
def customer_counts_state():
    return (
        dlt.read("customers_silver")
            .groupBy("state")
            .agg( 
                F.count("*").alias("customer_count"), 
                F.first(F.current_timestamp()).alias("updated_at")
            )
    )

# COMMAND ----------

# DBTITLE 0,--i18n-18af0840-c1e0-48b5-9f79-df03ee591aad
# MAGIC %md
# MAGIC ## Views do DLT
# MAGIC
# MAGIC A query abaixo define uma view do DLT usando o decorador **`@dlt.view`**.
# MAGIC
# MAGIC As views do DLT diferem das tabelas persistentes e também podem herdar a execução de streaming a função que decoram.
# MAGIC
# MAGIC As visualizações têm as mesmas garantias de atualização que as live tables, mas os resultados das consultas não são armazenados no disco.
# MAGIC
# MAGIC Ao contrário das visualizações usadas em outros lugares no Databricks, as visualizações DLT não são persistidas no metastore, o que significa que só podem ser referenciadas no pipeline DLT do qual fazem parte. (Este é um escopo semelhante a DataFrames em notebooks do Databricks.)
# MAGIC
# MAGIC As visualizações ainda podem ser usadas para impor a qualidade dos dados, e as métricas das visualizações serão coletadas e relatadas como seriam para tabelas.
# MAGIC
# MAGIC ## Junções e referências de tabelas em bibliotecas de notebook
# MAGIC
# MAGIC O código que analisamos até agora mostrou dois conjuntos de dados de origem se propagando por meio de uma série de etapas em notebooks separados.
# MAGIC
# MAGIC DLT oferece suporte ao agendamento de vários notebooks como parte de uma única configuração de pipeline DLT. Você pode editar pipelines DLT existentes para adicionar notebooks adicionais.
# MAGIC
# MAGIC Dentro de um pipeline DLT, o código em qualquer biblioteca de notebook pode fazer referência a tabelas e visualizações criadas em qualquer outra biblioteca de notebook.
# MAGIC
# MAGIC Essencialmente, podemos considerar o escopo da referência do banco de dados pela palavra-chave **`LIVE`** como estando no nível do pipeline do DLT, e não de um notebook individual.
# MAGIC
# MAGIC Na consulta abaixo, criamos uma nova visualização juntando as tabelas silver do nosso**`orders`** e**`customers`** conjuntos de dados. Observe que esta visualização não está definida como streaming; como tal, sempre capturaremos o atual válido**`email`** para cada cliente e descartará automaticamente os registros dos clientes depois que eles forem excluídos do**`customers_silver`** mesa.

# COMMAND ----------

@dlt.view
def subscribed_order_emails_v():
    return (
        dlt.read("orders_silver").filter("notifications = 'Y'").alias("a")
            .join(
                dlt.read("customers_silver").alias("b"), 
                on="customer_id"
            ).select(
                "a.customer_id", 
                "a.order_id", 
                "b.email"
            )
    )

# COMMAND ----------

# DBTITLE 0,--i18n-3351377c-3b34-477a-8c23-150895c2ef50
# MAGIC %md
# MAGIC ## Adicionando este notebook a um pipeline DLT
# MAGIC
# MAGIC Adicionar bibliotecas de notebook adicionais a um pipeline existente é feito facilmente com a UI DLT.
# MAGIC
# MAGIC 1. Navegue até o pipeline DLT que você configurou anteriormente no curso
# MAGIC 1. Clique no** Configurações** botão no canto superior direito
# MAGIC 1. Sob** Bibliotecas de cadernos**, clique** Adicionar biblioteca de notebooks**
# MAGIC    *Use o seletor de arquivos para selecionar este bloco de notas e clique em** Selecione**
# MAGIC 1. Clique no** Salvar** botão para salvar suas atualizações
# MAGIC 1. Clique no azul** Começar** botão no canto superior direito da tela para atualizar seu pipeline e processar quaisquer novos registros
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png">O link para este notebook pode ser encontrado em [DE 4.1- Passo a passo da UI DLT]($../DE 4.1- Passo a passo da IU DLT)<br/>
# MAGIC nas instruções impressas da **Tarefa 2** na seção **Gerar a configuração do pipeline**

# COMMAND ----------

# DBTITLE 0,--i18n-6b3c538d-c036-40e7-8739-cb3c305c09c1
# MAGIC %md
# MAGIC ## Resumo
# MAGIC
# MAGIC Ao revisar este caderno, você agora deve se sentir confortável:
# MAGIC *Processando dados do CDC com**`APPLY CHANGES INTO`**
# MAGIC *Declarando live views
# MAGIC *Juntando-se live tables
# MAGIC *Descrevendo como os notebooks da biblioteca DLT funcionam juntos em um pipeline
# MAGIC *Agendando vários notebooks em um pipeline DLT
# MAGIC
# MAGIC No próximo notebook, explore a saída do nosso pipeline. Em seguida, daremos uma olhada em como desenvolver iterativamente e solucionar problemas de código DLT.
