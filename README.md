# Medallion Architecture

## Objetivo

Aqui vou dar um pequeno resumo do que me motivou a fazer esse projeto. 
A Arquitetura Medallion doi o foi o primeiro e grande desafio da minha carreira como Engenheiro de Dados. Tive bastante dificuldade no início pois tínhamos uma estrutura de dados muito particular (coisa que vou descrever mais pra frente), então tive que ler muitos artigos e documentações. Algumas soluções encontrei lendo artigos, mas sempre eram para solucionar parte do problema e geralmente partes que não eram muito complexas. A maioria descobri como fazer sozinho com muita tentativa e erro e quebrando a cabeça.

Esse repositório tem como objetivo criar uma arquitetura medallion usando diferentes técnicas que fui adiquirindo no decorrer do tempo, e quem sabe ajudar alguém que esteja passando o mesmo que passei.

Minha experiência foi usando a FiveTran como ferramenta de ingestão e Databricks como plataforma de transformação dos dados, mas a ideia geral é usando Spark, mais especificamente PySpark, então essa estratégia pode ser usada em praticamente qualquer lugar que use o Spark, ambibente open-source ou alguma cloud como AWS e Azure.


# Sumário

1. Desvendando a Arquitetura Medallion
2. Criando a Camada Bronze
    1. Descrição do Problema
    2. Script bronze layer
3. Criando a Camada Silver


# Desvendando a Arquitetura Medallion