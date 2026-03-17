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
    Cria as tabelas market_data (preços e Z-Score), trades (histórico de ordens)
    e daily_snapshots (portfolio diário) caso ainda não existam.
    Também aplica migração segura para adicionar colunas novas em daily_snapshots.
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            # Tabela para armazenar os candles (OHLCV) e Z-Score
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

            # Tabela para snapshots diários do portfolio
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_snapshots (
                    timestamp INTEGER PRIMARY KEY,
                    total_equity_btc REAL,
                    btc_balance REAL,
                    eth_balance REAL,
                    eth_price_in_btc REAL,
                    bnb_balance REAL,
                    usdt_balance REAL
                )
            ''')

            # --- Migração segura: garante colunas novas em BBs existentes ---
            # SQLite não suporta ALTER TABLE ... ADD COLUMN IF NOT EXISTS,
            # então verificamos via PRAGMA antes de tentar adicionar.
            cursor.execute("PRAGMA table_info(daily_snapshots)")
            existing_columns = {row[1] for row in cursor.fetchall()}

            for col_def in [
                ("bnb_balance",  "ALTER TABLE daily_snapshots ADD COLUMN bnb_balance REAL DEFAULT 0.0"),
                ("usdt_balance", "ALTER TABLE daily_snapshots ADD COLUMN usdt_balance REAL DEFAULT 0.0"),
            ]:
                col_name, alter_sql = col_def
                if col_name not in existing_columns:
                    cursor.execute(alter_sql)
                    logger.info(f"Migração aplicada: coluna '{col_name}' adicionada a daily_snapshots.")

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

def save_daily_snapshot(
    timestamp: int,
    total_equity_btc: float,
    btc_balance: float,
    eth_balance: float,
    eth_price_in_btc: float,
    bnb_balance: float = 0.0,
    usdt_balance: float = 0.0,
):
    """
    Salva um snapshot diário do portfolio no banco de dados.

    Args:
        timestamp (int): Timestamp em milissegundos.
        total_equity_btc (float): Patrimônio total convertido em BTC.
        btc_balance (float): Saldo livre de BTC.
        eth_balance (float): Saldo livre de ETH.
        eth_price_in_btc (float): Preço do ETH cotado em BTC.
        bnb_balance (float): Saldo livre de BNB.
        usdt_balance (float): Saldo livre de USDT.
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO daily_snapshots
                (timestamp, total_equity_btc, btc_balance, eth_balance,
                 eth_price_in_btc, bnb_balance, usdt_balance)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (timestamp, total_equity_btc, btc_balance, eth_balance,
                  eth_price_in_btc, bnb_balance, usdt_balance))
            conn.commit()
            logger.info(
                f"Snapshot diário salvo. Equity: {total_equity_btc:.6f} BTC | "
                f"BNB: {bnb_balance:.4f} | USDT: {usdt_balance:.2f}"
            )
    except Exception as e:
        logger.error(f"Erro ao salvar snapshot diário: {e}")
        raise

def get_yesterday_snapshot() -> Optional[sqlite3.Row]:
    """
    Busca o último snapshot salvo que tenha pelo menos 23 horas de diferença
    do momento atual, para funcionar como o comparativo de "ontem".
    Se não encontrar nenhum com mais de 23h, retorna o mais antigo encontrado.
    """
    import time
    current_ts = int(time.time() * 1000)
    twenty_three_hours_ms = 23 * 60 * 60 * 1000
    target_ts = current_ts - twenty_three_hours_ms

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # Primeiro tenta buscar O MAIS RECENTE que seja ANTERIOR a 23h atrás
            cursor.execute('''
                SELECT * FROM daily_snapshots 
                WHERE timestamp <= ? 
                ORDER BY timestamp DESC LIMIT 1
            ''', (target_ts,))
            
            row = cursor.fetchone()
            if row:
                return row
                
            # Se não tem nada de 23h atrás (bot novo), pega o primeiro snapshot que achar (o mais antigo)
            cursor.execute('''
                SELECT * FROM daily_snapshots 
                ORDER BY timestamp ASC LIMIT 1
            ''')
            return cursor.fetchone()
            
    except Exception as e:
        logger.error(f"Erro ao buscar snapshot de ontem: {e}")
        return None

def get_last_trade() -> Optional[sqlite3.Row]:
    """
    Retorna o último trade efetuado.
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM trades 
                ORDER BY timestamp DESC LIMIT 1
            ''')
            return cursor.fetchone()
    except Exception as e:
        logger.error(f"Erro ao buscar último trade: {e}")
        return None

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
