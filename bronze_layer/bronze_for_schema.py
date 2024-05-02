from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit, when, row_number, current_timestamp, trim, coalesce, col, max
from pyspark.sql.window import Window
from delta import *
import datetime

from sys import argv


# Config do spark para não processar lotes que não contenham dados
spark.conf.set("spark.sql.streaming.noDataMicroBatches.enabled", "false")

# Config de evolução de schema na delta table
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")


# Variáveis que são definidas no momento de executar o job que estiver ligado a este script
SAVE_LOCATION = "/checkpoints"
SOURCE_CATALOG = argv[1]
TARGET_CATALOG = argv[2]
SCHEMA = argv[3]
ignore_tables = ['fivetran_audit']



# Retornar uma lista com as tabelas de um schema
def tables_of_schema(source_schema, source_catalog):
    table_name_df = spark.sql(f"""
        SELECT table_name
        FROM {source_catalog}.information_schema.tables
        WHERE table_schema = '{source_schema}'
        ORDER BY table_name
    """)

    table_list = [row['table_name'] for row in table_name_df.collect()]
    return table_list


class CDC_table:
    def __init__(self, source_catalog: str, target_catalog: str, schema: str, table: str, merge_condition: str, pk_dict: dict, fk_list: list):
        self.source_catalog = source_catalog
        self.target_catalog = target_catalog
        self.schema = schema
        self.table = table
        self.merge_condition = merge_condition
        self.pk_dict = pk_dict
        self.fk_list = fk_list
        
        
    def merge_raw_to_bronze(self, batch_DF, batchID):

        key_columns_list = []
        columns_id_cloud = ''
        for key, value in self.pk_dict.items():
            if len(self.pk_dict) == 1:
                if value not in ['int', 'bigint']:
                    columns_id_cloud += key + '_cloud BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),'

            key_columns_list.append(key)


        # Deduplicar os registros baseado em uma lista de PK
        window = Window.partitionBy(key_columns_list).orderBy(col("_commit_timestamp").desc())
        batch_DF = batch_DF.withColumn("rank", row_number().over(window)) \
                            .where('rank = 1')
        
        batch_DF = batch_DF.withColumn('_operation', when(batch_DF._fivetran_deleted == 1, lit('D')) \
                                .when(batch_DF._fivetran_deleted == True, lit('D')) \
                                .when(batch_DF._change_type == 'delete', lit('D')) \
                                .when((batch_DF._change_type == 'update_postimage') & (batch_DF._fivetran_deleted == 0), lit('U')) \
                                .when((batch_DF._change_type == 'update_postimage') & (batch_DF._fivetran_deleted == False), lit('U')) \
                                .when(batch_DF._change_type == 'insert', lit('I')) \
                                .otherwise('NULL')) \
                            .withColumn('_bronzeUpdateDate', current_timestamp()) \
                            .drop('_fivetran_deleted', '_fivetran_synced', '_change_type', '_commit_version', '_commit_timestamp', 'rank')
        # ------------------------------------------------------------------------------------------------
        


        # Teste se existe a tabela TARGET caso seja a primeira vez que o script esteja sendo executado para essa tabela
        try:
            # O Dataframe precisa ser do tipo Delta
            delta_df = DeltaTable.forName(spark, f'{self.target_catalog}.{self.schema}.{self.table}')
        except Exception as e:
            # Criação da tabela TARGET
            
            # ----------------------------- Metadados da Tabela -----------------------------
            schema = spark.table(f'{self.source_catalog}.{self.schema}.{self.table}').schema

            # Lista de colunas que servirá para a criação da variável de colunas da tabela TARGET
            lista_colunas = [] 

            # Limpeza do metadados
            for coluna in schema:
                coluna = str(coluna)
                lista_colunas.append(
                    coluna[13:].replace("DecimalType", "Decimal")
                                        .replace("',", "")
                                        .replace("Type()", "")
                                        .replace(", True)", "")
                                        .replace(", False)", "")
                )

            lista_colunas.remove('_fivetran_synced Timestamp')

            try:
                lista_colunas.remove('_fivetran_deleted Byte')
            except ValueError:
                lista_colunas.remove('_fivetran_deleted Boolean')
            # ----------------------------------------------------------------------------------


            # ----------------------- Criação das colunas ta tabela desino ---------------------
            # dicionário contendo os metadados da tabela
            dict_type_columns = dict(subString.split(" ") for subString in lista_colunas)

            # lista das colunas Primary Kkey + Foreign Key
            key_columns_list += fk_list

            # variável contendo a ordem das colunas para criação a criação da tabela destino 
            str_columns = ''
            
            # primeiro laço de repetição para ordenar as colunas (colunas chave primeiro)
            for column in key_columns_list:
                str_columns += column + ' ' + dict_type_columns[column] + ','
                dict_type_columns.pop(column)
            
            # segundo laço de repetição para ordenar as colunas (colunas normais)
            for key, value in dict_type_columns.items():
                str_columns += key + ' ' + value + ','

            str_columns = str_columns[:-1]

            # Criação da tabela de destino
            spark.sql(f'CREATE SCHEMA IF NOT EXISTS {self.target_catalog}.{self.schema}')
            spark.sql(f"""
                CREATE TABLE {self.target_catalog}.{self.schema}.{self.table}
                (
                    {columns_id_cloud}
                    {str_columns}
                )
                TBLPROPERTIES (
                    delta.enableDeletionVectors = true,
                    delta.enableChangeDataFeed = true, 
                    delta.autoOptimize.autoCompact=true,
                    delta.autoOptimize.optimizeWrite=true,
                    delta.columnMapping.mode = 'name',
                    delta.minReaderVersion = '3',
                    delta.minWriterVersion = '7',
                    delta.feature.allowColumnDefaults = 'supported'
                )
            """)
            
            delta_df = DeltaTable.forName(spark, f'{self.target_catalog}.{self.schema}.{self.table}')
            # ----------------------------------------------------------------------------------
            

        # Execute o MERGE com Evolution Schema
        delta_df.alias("target") \
            .merge(batch_DF.alias("source"), self.merge_condition) \
            .whenMatchedDelete("source._operation = 'D'") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll("source._operation != 'D'") \
            .execute()


# Função que retorna um dicionário com a Primary Key da tabela e uma lista de Foreing Key 
def get_pk_and_fk(schema: str, table: str):
    """
    Get Primary Keys (PK) and Foreign Keys (FK) from a data dictionary.
    """
    # Usar o dicionário de dados para descobrir PK
    database_name = schema[:-4]
    schema_name = schema[-3:]

    if database_name == 'acs_net_bra':
        database_name = 'acsnet_bra'

    data_dict_acs = spark.table('raw.default.data_dictionary')
    pk_table = data_dict_acs.filter((data_dict_acs.DatabaseName == database_name) & \
                                (data_dict_acs.SchemaNome == schema_name) & \
                                (data_dict_acs.TabelaNome == table) & \
                                (data_dict_acs.IsPK == 'PK'))

    fk_table = data_dict_acs.filter((data_dict_acs.DatabaseName == database_name) & \
                                (data_dict_acs.SchemaNome == schema_name) & \
                                (data_dict_acs.TabelaNome == table) & \
                                (trim(data_dict_acs.FKRef) != '') & \
                                (coalesce(data_dict_acs['IsPK'], lit('0')) == '0'))
                                    
    pk_dict = {row['ColunaNome']: row['ColunaTipo'] for row in pk_table.collect()}
    fk_list = [row['ColunaNome'] for row in fk_table.collect()]

    return pk_dict, fk_list



# Usar o catálogo fonte que foi passado como parâmetro no script
table_list = tables_of_schema(SCHEMA, SOURCE_CATALOG)
linha_integridade = []

# Laço de repetição para iniciar o streaming das tabelas que serão enviadas para a camada bronze
for table in table_list:

    # Condição se a tabela deve ou não ser enviada para a camada bronze
    # EX: Não enviar tabelas de controle do Fivetran (tabela fivetran_audit)
    if table not in ignore_tables:
        pk_dict, fk_list =  get_pk_and_fk(SCHEMA, table)
        merge_condition = ''

        for key, value in pk_dict.items():
            merge_condition += 'target.' + key + ' = source.' + key + ' AND '

        merge_condition = merge_condition[:-5]
        

        # Criar o obejeto tabela que será usada no streaming
        table_obj = CDC_table(SOURCE_CATALOG, TARGET_CATALOG, SCHEMA, table, merge_condition, pk_dict, fk_list)

        print(f'{datetime.datetime.now()} - WARNING - Start Stream: {SOURCE_CATALOG}.{SCHEMA}.{table}')

        # Tente iniciar o streaming do objeto
        try:
            # O streaming será feito a partir do CDF das tabelas usando a opção "readChangeData"
            (
                spark.readStream
                    .option("readChangeData", "true")
                    .table(f"{SOURCE_CATALOG}.{SCHEMA}.{table}")
                    .filter(col('_change_type').isin(["insert","delete","update_postimage"]))
                .writeStream
                    .foreachBatch(table_obj.merge_raw_to_bronze)
                    .option("checkpointLocation", f"{SAVE_LOCATION}/{SOURCE_CATALOG}/{SCHEMA}/{table}/_checkpoint")
                    .trigger(availableNow=True)
                .start()
                .awaitTermination()
            )

            # ----------------------- VERIFICAÇÃO DE INTEGRIDADE -----------------------
            try:
                qtd_bronze = spark.table(f'{TARGET_CATALOG}.`{SCHEMA}`.`{table}`').count()
                qtd_raw = spark.table(f'{SOURCE_CATALOG}.`{SCHEMA}`.`{table}`').filter(col('_fivetran_deleted') == 0).count()

                linha_integridade.append((SCHEMA, table, qtd_raw, qtd_bronze))
                
            except:
                linha_integridade.append((SCHEMA, table, 0, 0))
            # ---------------------------------------------------------------------------
        
        except Exception as e:
            type_error = str(e)
            cdf_error = True if 'delta.enableChangeDataFeed=true' in type_error else False
            checkpoint_error = True if 'delete your streaming query checkpoint' in type_error else False

            if cdf_error:
                print(f'{datetime.datetime.now()} - WARNING - Solving CDF Error')
                # Habilitar o CDF da tabela com erro
                spark.sql(f"""
                        ALTER TABLE {SOURCE_CATALOG}.{SCHEMA}.{table}
                        SET TBLPROPERTIES (
                            delta.enableChangeDataFeed = true,
                            delta.autoOptimize.autoCompact=true,
                            delta.autoOptimize.optimizeWrite=true
                        )
                """)
                
                # Remover possíveis checkpoints da antiga tabela 
                dbutils.fs.rm(f"{SAVE_LOCATION}/{SOURCE_CATALOG}/{SCHEMA}/{table}", True)
            elif checkpoint_error:
                print(f'{datetime.datetime.now()} - WARNING - Solving Checkpoint Error')
                dbutils.fs.rm(f"{SAVE_LOCATION}/{SOURCE_CATALOG}/{SCHEMA}/{table}", True)
                
                # Delete Registros que possivelmente estejam na camada Bronze e não foram apagados por causa de ressincronização 
                print(f'{datetime.datetime.now()} - WARNING - Deleting Pendenting Records')
                spark.sql(f"""
                    DELETE FROM {TARGET_CATALOG}.{SCHEMA}.{table} AS target
                    WHERE NOT EXISTS (SELECT * FROM {SOURCE_CATALOG}.{SCHEMA}.{table} AS source
                                    WHERE {merge_condition}) 
                """)
            else:
                assert False, f"{datetime.datetime.now()} - ERROR - Error Description: {e}"
        
        
        print(f'{datetime.datetime.now()} - WARNING - The End Stream: {SOURCE_CATALOG}.{SCHEMA}.{table}')


# Salvar tabela de integridade
schema_integridade = StructType([
                StructField('nome_schema', StringType(), True),
                StructField('nome_table', StringType(), True),
                StructField('qtd_raw', IntegerType(), True),
                StructField('qtd_bronze', IntegerType(), True)
                ])

df_integridade = spark.createDataFrame(linha_integridade, schema_integridade)
df_integridade.write.mode('overwrite').saveAsTable(f'{TARGET_CATALOG}.default.integridade_{SCHEMA}')
