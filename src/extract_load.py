import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

load_dotenv()

commodities = ['CL=F', 'GC=F', 'SI=F']
DATABASE_URL = os.getenv('DATABASE_URL')

def buscar_dados_commodities(simbolo, periodo='5d', intervalo='1d'):
    ticker = yf.Ticker(simbolo)
    dados = ticker.history(period=periodo, interval=intervalo)[['Close']]
    dados['simbolo'] = simbolo
    dados.reset_index(inplace=True)
    return dados

def buscar_todos_dados_commodities(lista_simbolos):
    todos_dados = []
    for simbolo in lista_simbolos:
        dados = buscar_dados_commodities(simbolo)
        todos_dados.append(dados)
    return pd.concat(todos_dados, ignore_index=True)

def salvar_no_postgres(df):
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Cria a tabela se não existir
        cur.execute("""
            CREATE TABLE IF NOT EXISTS commodities (
                Date TIMESTAMP,
                Close FLOAT,
                simbolo TEXT
            );
        """)

        # Prepara dados para inserção
        rows = df[['Date', 'Close', 'simbolo']].values.tolist()

        # Insere os dados
        insert_query = "INSERT INTO commodities (Date, Close, simbolo) VALUES %s"
        execute_values(cur, insert_query, rows)

        conn.commit()
        cur.close()
        conn.close()
        print("✅ Dados salvos com sucesso no banco.")
    except Exception as e:
        print(f"❌ Erro ao salvar no banco: {e}")

if __name__ == "__main__":
    dados_concatenados = buscar_todos_dados_commodities(commodities)
    salvar_no_postgres(dados_concatenados)
