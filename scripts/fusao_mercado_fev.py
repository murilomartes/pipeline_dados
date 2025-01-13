import json
import csv
from procesamento import Dados

#funções

def get_columns(dados):                     #função responsável por buscar os nomes das colunas
    return list(dados[0].keys())

def rename_colunms(dados,key_mapping):      #função responsável por transformar os dados csv 
    new_dados_csv = []

    for old_dict in dados:
        dict_temp = {}
        for old_key, value in old_dict.items():
            dict_temp[key_mapping[old_key]] = value
        new_dados_csv.append(dict_temp)
    return new_dados_csv



def save_data(dados, path):                         #função responsável por salvar os dados juntos
    with open(path, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(dados)

#Extract

path_json = '/home/murilo/Documentos/pipeline_dados/data_raw/dados_empresaA.json'
path_csv = '/home/murilo/Documentos/pipeline_dados/data_raw/dados_empresaB.csv'

dados_empresaA = Dados(path_json,'json')
print(dados_empresaA.nome_colunas)
print(dados_empresaA.qtd_linhas)

dados_empresaB = Dados(path_csv,'csv')
print(dados_empresaB.nome_colunas)
print(dados_empresaB.qtd_linhas)

#Transform

key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}
key_mapping

dados_empresaB.rename_colunms(key_mapping)
print(dados_empresaB.nome_colunas)

dados_fusao = Dados.join(dados_empresaA, dados_empresaB)
print(dados_fusao)
print(dados_fusao.nome_colunas)
print(dados_fusao.qtd_linhas)


#Load




# print("================== LEITURA =========================================================================================================================================")

# print("-------------- Json --------------------")
# dados_json =  leitura_dados(path_json, 'json')
# nome_colunas_json = get_columns(dados_json)
# tamanho_dados_json = size_data(dados_json)
# print(f"Nome das colunas dos dados json:{nome_colunas_json}")
# print(f"Tamanho dos dados em json:{tamanho_dados_json} dados")

# print("-------------- Csv --------------------")
# dados_csv =  leitura_dados(path_csv, 'csv')
# nome_colunas_csv = get_columns(dados_csv)
# tamanho_dados_csv = size_data(dados_csv)
# # print(f"Nome das colunas dos dados csv:{nome_colunas_csv}")
# print(f"Tamanho dos dados em csv:{tamanho_dados_csv} dados")

# #Notou-se diferença entre os nomes das colunas entre os dois arquivos foi nescesário renomear

# #Transformação dos dados

# key_mapping = {'Nome do Item': 'Nome do Produto',
#                 'Classificação do Produto': 'Categoria do Produto',
#                 'Valor em Reais (R$)': 'Preço do Produto (R$)',
#                 'Quantidade em Estoque': 'Quantidade em Estoque',
#                 'Nome da Loja': 'Filial',
#                 'Data da Venda': 'Data da Venda'}
# key_mapping

# print("---------------------------------------------------------------")
# dados_csv = rename_colunms(dados_csv, key_mapping)
# nome_colunas_csv = get_columns(dados_csv)
# print(f"Nome das colunas dos dados renomeados csv:{nome_colunas_csv}")

# print("-------------- Fusao --------------------")
# dados_fusao = join(dados_json, dados_csv)
# nome_colunas_fusao = get_columns(dados_fusao)
# tamanho_dados_fusao = size_data(dados_fusao)
# print(f"Nome das colunas dos dados fusao:{nome_colunas_fusao}")
# print(f"Tamanho dos dados em fusao:{tamanho_dados_fusao} dados")


# #Salvando Dados
# dados_fusao_tabela = transoform_dados_tabela(dados_fusao, nome_colunas_fusao)
# path_dados_combinados = '/home/murilo/Documentos/pipeline_dados/data_processed/dados_combinado.csv'
# save_data(dados_fusao_tabela, path_dados_combinados,)
# print(path_dados_combinados)

