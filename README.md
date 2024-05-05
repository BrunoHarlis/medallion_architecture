# Medallion Architecture

## Objetivo

Aqui vou dar um pequeno resumo do que me motivou a fazer esse projeto. 
A Arquitetura Medallion foi o primeiro e grande desafio da minha carreira como Engenheiro de Dados. Tive bastante dificuldade no início pois tínhamos uma estrutura de dados muito particular (coisa que vou descrever mais para frente), então tive que ler muitos artigos e documentações. Algumas soluções encontrei lendo artigos, mas sempre eram para solucionar parte do problema e geralmente partes que não eram muito complexas. A maioria descobri como fazer sozinho com muita tentativa e erro e quebrando a cabeça.

Esse repositório tem como objetivo criar uma arquitetura medallion usando diferentes técnicas que fui adquirindo no decorrer do tempo, e quem sabe ajudar alguém que esteja passando o mesmo que passei.

Minha experiência foi usando a FiveTran como ferramenta de ingestão e Databricks como plataforma de transformação dos dados, mas a ideia geral é usando Spark, mais especificamente PySpark, então essa estratégia pode ser usada em praticamente qualquer lugar que use o Spark, ambiente open-source ou alguma cloud como AWS e Azure.


# Sumário

1. [Desvendando a Arquitetura Medallion](https://github.com/BrunoHarlis/medallion_architecture?tab=readme-ov-file#desvendando-a-arquitetura-medallion)
2. [Criando a Camada Bronze](https://github.com/BrunoHarlis/medallion_architecture?tab=readme-ov-file#criando-a-camada-bronze)
    1. [Objetivo da Camada Bronze](https://github.com/BrunoHarlis/medallion_architecture?tab=readme-ov-file#objetivo-da-camada-bronze)
    2. [Descrição do Problema](https://github.com/BrunoHarlis/medallion_architecture?tab=readme-ov-file#descri%C3%A7%C3%A3o-do-problema)
    3. [Solução](https://github.com/BrunoHarlis/medallion_architecture?tab=readme-ov-file#solu%C3%A7%C3%A3o)
    4. [Conclusão](https://github.com/BrunoHarlis/medallion_architecture?tab=readme-ov-file#conclus%C3%A3o)
    5. [Script bronze layer](https://github.com/BrunoHarlis/medallion_architecture/blob/main/bronze_layer/bronze_etl.py)
4. [Criando a Camada Silver](https://github.com/BrunoHarlis/medallion_architecture?tab=readme-ov-file#criando-a-camada-silver)


# Desvendando a Arquitetura Medallion
A arquitetura medallion é um design pattern desenvolvido para organizar o fluxo dos dados, separando-os logicamente em camadas que representam a qualidade dos dados. As camadas padrão encontradas em literaturas são bronze, silver e gold. Abaixo temos a representação visual do fluxo dos dados entre as camadas.

<img src="https://github.com/BrunoHarlis/medallion_architecture/blob/main/imagens/medallion_architecture.gif" alt="drawing" width="400"/>

## Camada Bronze
A camada bronze, também conhecida como landing zone, é o lugar onde são colocados os dados das fontes de dados externas. Nessa camada os dados devem representar as bases externas de forma bruta sem grandes alterações, podendo possuir algumas colunas de controle como data e hora do carregamento, tipo de mudança etc. Essa camada tem como objetivo fornecer um histórico dos dados, permitindo realizar auditorias, reprocessamentos sem precisar de uma reimportação dos dados de origem.

## Camada Silver
As tabelas criadas nessa camada são voltadas para uma visão de negócio, fazendo mesclagens, unions e agrupamentos de várias tabelas para representar entidades de negócio. Aqui os dados que estão na camada bronze são limpos, removendo registros desnecessários, mudança de nomenclatura, traduções e aplicação de data quality. 

Do ponto de vista de modelagem de dados, as tabelas criadas aqui têm seus modedos de dados semelhantes a 3ª Forma Normal. Modelos de dados com desempenho semelhantes ao Data Vault podem ser usados nessa camada. 

## Camada Gold
A camada Gold é voltada para o consumo pela área de negócio, possuindo tabelas que auxiliando nas tomadas de decisões. Aqui podemos ter tabelas de relatórios, análise de qualidade de produto, análise de clientes, análise de estoque etc.

As tabelas são montadas para terem uma melhor performance de leitura, mais desnormalizado. Serão realizadas otimizações, refinamentos dos dados e adição outras regras de negócios e data quality. São encontrados nessa camada, modelos de dados baseados em Star Schema ou Data Marts, com agregações, cálculos, indicadores preparados para atingir uma área de negócio específica. 

Assim podemos ver como a separação lógica das camadas ajuda a termos um melhor gerenciamento do fluxo dos dados, melhor performance e possibilidade de recriar tudo a partir dos dados que estão na camada bronze. 

# Criando a Camada Bronze
## Objetivo da Camada Bronze

O objetivo é termos as tabelas na camada bronze idênticas às que estão na base de dados, com todas as atualizações e deletes. 

## Descrição do Problema

Antes de começar a mostrar o código, vou explicar como funciona a ingestão de dados do FiveTran pois é fundamental para entendimento da estratégia abordada. 
Basicamente, o FiveTran faz a ingestão de duas formas, uma em modo incremental que vai sempre adicionando novos registros ao fim da tabela, nunca apagando ou atualizando. A outra em modo soft delete que funciona quase como uma tabela em um banco relacional onde os registros que sofrem modificações são de fato atualizados, com a diferença em como o delete é feito. Em vez de apagar o registro da tabela, ele é atualizado adicionando uma flag informando que esse registro é um registro do tipo deletado (_fivetran_delete = true), sendo esse último o método escolhido aqui nesse projeto.

Tendo isso em mente, escolhemos criar uma camada adicional na arquitetura medallion chamada Raw onde ficarão nossos dados brutos. Nessa camada o FiveTran comanda, podendo fazer resync, criação de novas tabelas, atualizações etc. 

## Solução

Temos que pegar os registros que estão chegando na camada Raw e passar para a camada Bronze. O processo de ETL usando Structure Streaming do Spark é ótimo para isso, mas aqui temos nosso primeiro problema.

~~~python
df = spark.readStream.table("raw.dbo.customer")
~~~~

Estaria tudo bem se o método de ingestão fosse o incremental. O Spark não lida muito bem com atualizações de regristros que já foram lidos anteriormente, o que ocasiona um erro. A solução é fazer o streaming usando o CDF (Change Data Feed) da tabela. Para mais detalhes sobre o que é o CDF, sugiro a leitura do seguinte artigo: [link do artigo].

~~~python
df = spark.readStream \
          .option("readChangeData", "true") \
          .table("raw.dbo.customer") \
          .filter(col('_change_type').isin(["insert","delete","update_postimage"]))
~~~

Observe que adicionei um filtro no streaming para trazer somente os registros que sejam do tipo _insert_, _delete_ e _update_postimage_, deixando os do tipo _update_preimage_ de lado.

Agora precisamos gravar o resultado do streaming, mas antes é preciso fazer a deduplicação dos registros que serão gravados, pois faremos um merge na tabela bronze. Também será realizado algumas transformações e limpezas dos dados, porém nada complexo.

~~~python
def merge_raw_to_bronze(batch_df, batch_id):
    # Deduplicar os registros baseados em uma PK
    window = Window.partitionBy("Id").orderBy(col("_commit_timestamp").desc())
    batch_df = batch_df.withColumn("rank", row_number().over(window)) \
                        .where('rank = 1') \
                        .withColumnRenamed("_change_type", "_operation") \
                        .drop('_fivetran_deleted', '_fivetran_synced', '_commit_version', '_commit_timestamp', 'rank')

    # Carregue a tabela bronze em um dataframe do tipo delta
    delta_df = DeltaTable.forName(spark, "bronze.dbo.customer")

    # Execute o MERGE com Evolution Schema
    delta_df.alias("target") \
        .merge(batch_df.alias("source"), "target.Id = source.Id") \
        .whenMatchedDelete("source._operation = 'delete'") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll("source._operation != 'delete'") \
        .execute()

df.writeStream \
    .foreachBatch(merge_raw_to_bronze) \
    .option("checkpointLocation", "/save_location/_checkpoint") \
    .trigger(availableNow=True) \
.start() \
.awaitTermination()
~~~

Vou explicar o que está acontecendo em cada etapa desse processo. O spark está "estrimando" uma tabela em batch. No nosso caso, esse batch possui o hitórico de todas as mudanças que aconteceram na tabela, por isso os passos a serem seguidos são:
- Usar a função Window do pyspark para verificar a versão mais recente dos registros, particionando pelo ID da tabela e ordenando de forma descendente os commits. Em seguida criamos uma rank = 1 para assim ignorarmos versões anteriores, deduplicando os registros;
- Renomear a coluna do CDF "_change_type" para "_operation" pois esse nome é reservado para controle do CDF e não pode ser usado fora dele;
- Por fim, deletar as colunas que não são importantes. 

Aqui assumirei que a tabela bronze já está criada, portanto carregaremos ela em um DataFrame do tipo Delta para em seguida fazer o merge com o batch. Observe que estamos fazendo merge com evolution schema, ignorando se existem a mesma quantidade de colunas, isso faz com que novas colunas adicionadas na base de dados sejam passadas adiante. A camada bronze nos permite fazer isso pois ainda não estamos lidando com qualidade de dados ou tabelas de negócio.

## Conclusão
Esse método que mostrei resolve a maioria dos problemas simples de um ETL para a camada bronze, porém a coisa pode começar a complicar se você for lidar com múltiplas bases de dados com milhares de tabelas em cada base. Recomendo fazer um script "genérico" mais elaborado que englobe e automatize todas as transformações necessárias. Por exemplo, aqui assumi que a tabela bronze foi criada manualmente antes do ETL, isso se torna impraticável para um ambiente com milhares de tabelas. Seria um tempo desperdiçado criar todas elas manualmente. Será necessário criar um fluxo de criação de tabelas automatizado.

# Criando a Camada Silver
