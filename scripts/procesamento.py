import json
import csv

class Dados:
    def __init__(self,path,tipo_dados):         #Método construtor
        self.path = path
        self.tipo_dados = tipo_dados
        self.dados = self.leitura_dados()
        self.nome_colunas = self.get_columns()
        self.qtd_linhas = self.size_data()
    
    def leitura_json(self):                  #Método responsável por ler os dados json
        dados_json = []
        with open(self.path, 'r') as file:
            dados_json = json.load(file)
        return dados_json

    def leitura_csv(self):                  #Método responsável por ler os dados csv
        dados_csv = []
        with open(self.path, 'r') as file:
            spamreader = csv.DictReader(file, delimiter=',')
            for row in spamreader:
                dados_csv.append(row)
        return dados_csv

    def leitura_dados(self):                 #Método responsável por ler qualquer tipo de dados
        dados = []
        if self.tipo_dados == 'csv':
            dados = self.leitura_csv()
        elif (self.tipo_dados == 'json'):
            dados = self.leitura_json()
        elif (self.tipo_dados == 'list'):
            dados = self.path
            self.path = 'lista em memória'
        return dados

    def get_columns(self):                     #Método responsável por buscar os nomes das colunas
        return list(self.dados[-1].keys())
    
    def rename_colunms(self,key_mapping):      #Método responsável por transformar os dados csv 
        new_dados = []

        for old_dict in self.dados:
            dict_temp = {}
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value
            new_dados.append(dict_temp)
        self.dados = new_dados
        self.nome_colunas = self.get_columns()

    def size_data(self):                       #Método responsável por mostra o tamanho de dados dos arquivos
        return len(self.dados)

    def join(dadosA, dadosB):                   #Método responsável por juntar as funções
        combined_list = []
        combined_list.extend(dadosA.dados)
        combined_list.extend(dadosB.dados)
        return Dados(combined_list, 'list')

    def transoform_dados_tabela(self): # Método os dados em listas de listas posibilitando a fusão
        dados_combinados_tabela = [self.nomes_colunas]
        for row in self.dados:
            linha = []
            for coluna in self.nomes_colunas:
                linha.append(row.get(coluna, 'indisponível'))
            dados_combinados_tabela.append(linha)
        return dados_combinados_tabela