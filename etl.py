#funcao de extract que le arquivos jsons
    #listar todos os arquivos json, trazer pro dataframe e concatenar
#funcao que transforma
#funcao que load parquet ou csv

import pandas as pd
import os
import glob

#funcao de extrair os arquivos json e concatenar
def extrair_dados(pasta: str):
    arquivos_json = glob.glob(os.path.join(pasta, '*.json')) #lista todos os arquivos que tem ,json na pasta dentro do windows

    #df_list = [pd.read_json(arquivo) for arquivo in arquivos_json] FAZ A MESMA COISA QUE O COMANDO DO FOR ABAIXO, POREM JA DEIXA TUDO DENTRO DA LISTA
    df_list = []
    for arquivo in arquivos_json:
        df_list.append(pd.read_json(arquivo))

    df_concatenado = pd.concat(df_list, ignore_index=True) #concatena em um dataframe unico os dataframes da lista anterior
    return df_concatenado

#funcao de transformacao dos dados, cria uma nova coluna total com o resultado de quantidade x venda
def transformacao_calcula_total_de_vendas(df: pd.DataFrame):
    df['Total'] = df['Quantidade'] * df['Venda']
    return df

#funcao de carregar os dados em um arquivo csv ou parquet


def carregar_Dados(dataframe: pd.DataFrame, formato_saida: str, nome_do_arquivo_de_saida: str):
    if formato_saida == 'csv':
        dataframe.to_csv(f"{nome_do_arquivo_de_saida}.csv",index=False)
    elif formato_saida == 'parquet':
        dataframe.to_parquet(f"{nome_do_arquivo_de_saida}.parquet",index=False)

#funcao final do usuario, que chama todas as outras
def pipeline(pasta, formato_saida,nome_do_arquivo_de_saida="dados"):
    df_concatenado =  extrair_dados(pasta)
    df_calculado = transformacao_calcula_total_de_vendas(df_concatenado)
    carregar_Dados(df_calculado,formato_saida,nome_do_arquivo_de_saida)


# if __name__ == "__main__":
#     pasta_arquivos = 'data'
#     df_concatenado =  extrair_dados(pasta=pasta_arquivos)
#     df_calculado = transformacao_calcula_total_de_vendas(df_concatenado)
#     carregar_Dados(df_calculado,"parquet","dados_csv")
