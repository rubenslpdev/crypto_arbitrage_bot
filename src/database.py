import sqlite3
import os
import logging
from contextlib import contextmanager
from typing import List, Optional, Union

# Configuração de log básica para o módulo de banco de dados
logger = logging.getLogger(__name__)

# DB_PATH será sempre resolvido a partir do diretório raiz do projeto (uma pasta acima de /src)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'bot_trading.db')

@contextmanager
def get_db_connection():
    """
    Context manager para gerenciar conexões com o banco de dados SQLite.
    Garante que a conexão seja fechada corretamente mesmo em caso de erros,
    evitando corrupção de dados ou 'database locked'.
    """
    # Garante que o diretório 'data' exista antes de tentar conectar
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        # Permite acessar os resultados como dicionários em consultas futuras
        conn.row_factory = sqlite3.Row
        yield conn
    except sqlite3.Error as e:
        logger.error(f"Erro no banco de dados SQLite: {e}")
        raise
    finally:
        if conn:
            conn.close()

def init_db():
    """
    Cria as tabelas market_data (preços e Z-Score) e trades (histórico de ordens)
    caso elas ainda não existam.
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Tabela para armazenar os candles (OHLCV) e Z-Score
            # A chave composta evita duplicar o mesmo candle para o mesmo par
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS market_data (
                    symbol TEXT,
                    timestamp INTEGER,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    z_score REAL,
                    PRIMARY KEY (symbol, timestamp)
                )
            ''')
            
            # Tabela para armazenar o histórico de ordens executadas/tentadas
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER,
                    symbol TEXT,
                    side TEXT,
                    price REAL,
                    amount REAL,
                    fee_bnb REAL,
                    estimated_profit REAL
                )
            ''')
            
            conn.commit()
            logger.info("Banco de dados inicializado com sucesso (tabelas verificadas/criadas).")
    except Exception as e:
        logger.error(f"Falha ao inicializar o banco de dados: {e}")
        raise

def save_candles(symbol: str, candles: List[List[Union[int, float]]]):
    """
    Salva os dados de preço (OHLCV) baixados via CCXT.
    
    Args:
        symbol (str): O par de moedas, ex: 'ETH/BTC'
        candles (list): Lista de candles no formato padrão do CCXT
                        [[timestamp, open, high, low, close, volume], ...]
    """
    if not candles:
        return
        
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Mapeia os dados brutos do CCXT para inserção apropriada
            records = []
            for candle in candles:
                timestamp, open_p, high_p, low_p, close_p, volume = candle[:6]
                records.append((symbol, timestamp, open_p, high_p, low_p, close_p, volume))
            
            # Usa INSERT OR IGNORE para pular tranquilamente candles já existentes no banco,
            # o que facilita e torna segura a sincronização incremental.
            cursor.executemany('''
                INSERT OR IGNORE INTO market_data 
                (symbol, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', records)
            
            conn.commit()
            logger.info(f"{len(records)} candles processados para {symbol}.")
    except Exception as e:
        logger.error(f"Erro ao salvar candles para {symbol}: {e}")
        raise

def get_last_timestamp(symbol: str) -> Optional[int]:
    """
    Retorna o timestamp do último candle salvo para o mercado específico.
    Usado para descobrir de onde devemos recomeçar a baixar em sincronizações incrementais.
    
    Args:
        symbol (str): O par de moedas, ex: 'ETH/BTC'
        
    Returns:
        int: Timestamp em milissegundos do último registro, ou None se a base for nova.
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT MAX(timestamp) 
                FROM market_data 
                WHERE symbol = ?
            ''', (symbol,))
            
            result = cursor.fetchone()
            # result[0] contém o MAX(timestamp), que será None se não houver dados
            return result[0] if result and result[0] is not None else None
    except Exception as e:
        logger.error(f"Erro ao buscar último timestamp para {symbol}: {e}")
        raise

if __name__ == "__main__":
    # Configuração simples de log para o teste
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    print("--- Iniciando teste do banco de dados ---")
    
    print("\n1. Testando init_db()...")
    init_db()
    
    print("\n2. Testando save_candles()...")
    # Formato ccxt passado para db: [timestamp, open, high, low, close, volume]
    dummy_candles = [
        [1672531200000, 16000.0, 16100.0, 15900.0, 16050.0, 100.5],
        [1672534800000, 16050.0, 16200.0, 16000.0, 16150.0, 120.2],
    ]
    save_candles('BTC/USDT', dummy_candles)
    
    # Testando se inserir os mesmos dados novamente não gera erro (INSERT OR IGNORE)
    print("\n   Testando inserção duplicada (incremental idêntico)...")
    save_candles('BTC/USDT', dummy_candles)
    
    print("\n3. Testando get_last_timestamp()...")
    last_ts = get_last_timestamp('BTC/USDT')
    print(f"   Último timestamp retornado para BTC/USDT: {last_ts}")
    # Deve ser igual a 1672534800000
    assert last_ts == 1672534800000, "Timestamp retornado está incorreto!"
    
    print("\n4. Testando get_last_timestamp() para par inexistente...")
    none_ts = get_last_timestamp('NADA/USDT')
    print(f"   Último timestamp retornado para NADA/USDT: {none_ts}")
    assert none_ts is None, "Deveria retornar None para pares inexistentes!"
    
    print("\n--- Todos os testes de database concluídos com sucesso! ---")
