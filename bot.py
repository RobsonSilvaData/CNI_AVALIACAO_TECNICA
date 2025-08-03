import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime

def obter_dados_api(url: str) -> dict: #Função responsável por buscar os dados via método GET
    
    response = requests.get(url)
    response.raise_for_status()
    
    return response.json()

def obter_valores_ipca(): #Obtém os variações do IPCA
    """
        Como forma de enriquecimento da base de dados, optei por criar essa função que busca os dados das variações do IPCA ao longo dos anos.
        
        Na API fornecida, basicamente temos informações sobre a tabela, que já veio filtrada a 1737, os períodos, unidades de medida, variáveis etc.
        No processo de descoberta, naveguei pela home do site e observei que era possível incrementar a tabela solicitada com as variações do IPCA.
        Foi então que cheguei a esta URL: https://apisidra.ibge.gov.br/home/ajuda, onde é explicado como filtrar os dados que eu precisava para fazer esse incremento.
             
        Processo de montagem do filtro:

        1 - t/1737: tabela que estamos utilizando.
        2 - n1/all: indica que vamos utilizar todo o território nacional e que vamos pegar todos os dados.
        3 - v/2265,63,2263,2264,69,2266: variáveis disponíveis na API fornecida para avaliação.
        4 - p/all: indica o período e que vamos utilizar todos os dados disponíveis.
        5 - ?formato=json: especifica que queremos a resposta em formato JSON, mas poderia ser em XML.

        De forma geral, essa documentação fornece várias possibilidades de filtros e ajuda a compreender os campos que utilizei.

    """
    
    url = "https://apisidra.ibge.gov.br/values/t/1737/n1/all/v/2265,63,2263,2264,69,2266/p/all?formato=json"
    
    try:
        #trecho responsável de fazer o GET dos dados
        response = requests.get(url)
        response.raise_for_status()
        dados = response.json()
        
        if isinstance(dados, list) and len(dados) > 1: # verificação dos dados de retorno, se é uma lista python e se tem dados
            
            # a primeira linha do retorno é os nomes das colunas
            headers = [h.replace(" ", "_") for h in dados[0]]
            
            #cria um DataFrame pandas
            df = pd.DataFrame(dados[1:], columns=headers)
                
            # Filtrar e renomear colunas
            df = df.rename(columns={
                'D1N': 'periodo',
                'D2N': 'variavel',
                'D2C': 'variavel_id',
                'V': 'valor'
            })
            
            # Mapear nomes das variáveis para IDs
            map_variaveis = {
                'IPCA - Variação mensal': 63,
                'IPCA - Variação acumulada em 3 meses': 2263,
                'IPCA - Variação acumulada em 6 meses': 2264,
                'IPCA - Variação acumulada no ano': 69,
                'IPCA - Variação acumulada em 12 meses': 2265,
                'IPCA - Número-índice (base: dezembro de 1993 = 100)': 2266
            }            
            df['variavel_id'] = df['variavel'].map(map_variaveis)
            
            # Atribuí a coluna código do período a valor de D3C. Também usarei esse campo para fazer o merge dos DataFrames na etapa de transformação
            df['codigo_periodo'] = df['D3C']
            
            return df[['codigo_periodo', 'variavel_id', 'valor']]
        
    except Exception as e:
        print(f"Erro ao acessar API de valores do IPCA: {e}")
        return None

def transformar_json_para_tabular(json_data): #Transforma os dados do IPCA no formato JSON para um DataFrame tabular.
    
    # Obtém os dados de variações do API
    df_valores = obter_valores_ipca()
    
    if df_valores is None or df_valores.empty:
        print("Não foi possível obter valores das variações")
        return None
    
    # Extrair os períodos e criar um dicionário de mapeamento Código -> Período
    periodos = {str(p['Codigo'])[-6:]: {
        'id': p['Id'],
        'mes_ano': p['Nome'],
        'disponivel': p['Disponivel'],
        'data_liberacao': p['DataLiberacao']
    } for p in json_data['Periodos']['Periodos']}
    
    # Mapear IDs de variáveis para nomes de colunas
    map_variaveis_colunas = {
        2266: 'numero_indice',
        63:   'variacao_mensal',
        2263: 'variacao_3_meses',
        2264: 'variacao_6_meses',
        69:   'variacao_acumulada_ano',
        2265: 'variacao_12_meses'
    }
    
    # Converte o dic e criar DataFrame com os períodos
    df_periodos = pd.DataFrame.from_dict(periodos, orient='index').reset_index()
    
    # Renomeando a coluna index para codigo_periodo
    df_periodos = df_periodos.rename(columns={'index': 'codigo_periodo'})
    
    # Garantir que ambos DataFrames usem string para codigo_periodo
    df_periodos['codigo_periodo'] = df_periodos['codigo_periodo'].astype(str)
    df_valores['codigo_periodo'] = df_valores['codigo_periodo'].astype(str)
    
    # Filtrar as variáveis que vamos utilizar 
    df_valores = df_valores[df_valores['variavel_id'].isin(map_variaveis_colunas.keys())]
    
    # Remover linhas duplicatas
    df_valores = df_valores.drop_duplicates(
        subset=['codigo_periodo', 'variavel_id'], 
        keep='last'
    )
    
    # Pivotar os valores para ter uma linha por período
    try:
        df_valores_pivot = df_valores.pivot(
            index='codigo_periodo',
            columns='variavel_id',
            values='valor'
        ).reset_index()
        
        # Renomear colunas de variáveis
        df_valores_pivot = df_valores_pivot.rename(columns=map_variaveis_colunas)
        
        # Juntar com os metadados dos períodos
        df_completo = pd.merge(
            df_periodos, 
            df_valores_pivot, 
            on='codigo_periodo', 
            how='left'
        )
        
        return df_completo
        
    except Exception as e:
        print(f"Erro ao transformar dados: {e}")
        return None

def gravar_parquet(df, caminho_arquivo): #Grava um DataFrame no formato Parquet
    
    if df is None:
        print("Nenhum dado para gravar")
        return
    
    try:
        # Converter DataFrame pandas para tabela pyarrow
        tabela = pa.Table.from_pandas(df)
        
        # Gravar arquivo Parquet
        pq.write_table(tabela, caminho_arquivo)
        
        print(f"Arquivo Parquet gravado com sucesso em: {caminho_arquivo}")
        
    except Exception as e:
        print(f"Erro ao gravar arquivo Parquet: {e}")

if __name__ == "__main__":
    
    # Carregar os metadados  
    URL_METADADOS = "https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1"
    
    print("Obtendo metadados da API...")
    
    try:
        json_metadados = obter_dados_api(URL_METADADOS)
        
        # Transformar para formato tabular com valores reais
        print("Obtendo e processando valores do IPCA...")
        df_ipca = transformar_json_para_tabular(json_metadados)
        
        # Verifica se o retorno não foi None
        if df_ipca is not None:                        
            # Gravar em Parquet
            gravar_parquet(df_ipca, 'dados_ipca.parquet')
        else:
            print("Falha ao processar os dados do IPCA")
    except Exception as e:
        print(f"Erro no processo principal: {e}")