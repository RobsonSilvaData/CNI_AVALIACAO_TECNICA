# CNI_AVALIACAO_TECNICA
Esse repositório trata-se de um estudo de caso para o Conselho Nacional de Indústria 

## Crie o ambiente virtual

No Linux/macOS:

```bash
python3 -m venv venv
```

## Ativando ambiente

```bash
source venv/bin/activate
```

## Instalando as dependências 

```bash
pip install -r requirements.txt
```

## Executando BOT

```bash
python3 bot.py
```
Após finalizar a execução do BOT, será gerado o arquivo chamado "dados_ipca.parquet". Caso queira ler o arquivo gerado disponibilize um jupyter notebook "ler_arquivo_parquet.ipynb"
