import os
import ccxt
import logging
from typing import Dict, Optional, List, Union
from database import get_last_timestamp, save_candles

# Configuração de log
logger = logging.getLogger(__name__)

def get_exchange() -> ccxt.binance:
    """
    Inicializa a conexão com a Binance usando as chaves do .env.
    O modo Sandbox (Testnet) é ativado APENAS se BINANCE_IS_TESTNET=True no .env.
    Se BINANCE_IS_TESTNET=False, o bot opera na Mainnet real.

    Returns:
        exchange (ccxt.binance): Objeto ccxt pronto para operações.
    """
    api_key = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_API_SECRET')

    # Validação de segurança obrigatória
    if not api_key or not api_secret:
        logger.error("Chaves de API não configuradas no ambiente.")
        raise ValueError("Chaves de API (BINANCE_API_KEY/SECRET) ausentes. Abortando execução por segurança.")

    exchange = ccxt.binance({
        'apiKey': api_key,
        'secret': api_secret,
        'enableRateLimit': True,  # Respeita os limites da API para não ser banido
        'options': {
            'defaultType': 'spot'  # Operações apenas no mercado à vista (Spot)
        }
    })

    # Modo Testnet/Mainnet dinâmico baseado na variável de ambiente
    is_testnet_str = os.getenv('BINANCE_IS_TESTNET', 'False').strip().lower()
    is_testnet = is_testnet_str == 'true'

    if is_testnet:
        exchange.set_sandbox_mode(True)
        logger.info("⚠️  Modo TESTNET (Sandbox) ativo. Nenhum capital real será utilizado.")
    else:
        logger.info("🟢 Modo MAINNET ativo. Operando com capital real.")

    return exchange

def get_balance() -> Dict[str, float]:
    """
    Consulta o saldo disponível de BTC, ETH, BNB e USDT na conta Spot.

    Returns:
        dict: Dicionário com os saldos livres (free):
              {'BTC': x, 'ETH': x, 'BNB': x, 'USDT': x}
    """
    try:
        exchange = get_exchange()
        balance_info = exchange.fetch_balance()

        # Pega a parte 'free' para saber o que realmente pode ser usado no trade
        btc_free  = float(balance_info.get('BTC',  {}).get('free', 0.0) or 0.0)
        eth_free  = float(balance_info.get('ETH',  {}).get('free', 0.0) or 0.0)
        bnb_free  = float(balance_info.get('BNB',  {}).get('free', 0.0) or 0.0)
        usdt_free = float(balance_info.get('USDT', {}).get('free', 0.0) or 0.0)

        logger.info(
            f"Saldo disponível: {btc_free:.6f} BTC | {eth_free:.4f} ETH | "
            f"{bnb_free:.4f} BNB | {usdt_free:.2f} USDT"
        )
        return {'BTC': btc_free, 'ETH': eth_free, 'BNB': bnb_free, 'USDT': usdt_free}

    except ccxt.NetworkError as e:
        logger.error(f"Erro de rede ao buscar saldo na Binance: {e}")
        raise
    except ccxt.ExchangeError as e:
        logger.error(f"Erro da Exchange ao buscar saldo: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado ao buscar saldo (possível chaves inválidas?): {e}")
        raise

def check_bnb_for_fees(min_bnb: float = 0.01) -> bool:
    """
    Verifica se o saldo de BNB é suficiente para pagar taxas de operação.
    A Binance concede ~25% de desconto quando as taxas são pagas em BNB.

    Args:
        min_bnb (float): Saldo mínimo de BNB considerado suficiente. Padrão: 0.01 BNB.

    Returns:
        bool: True se o saldo de BNB >= min_bnb, False caso contrário.
    """
    try:
        balances = get_balance()
        bnb_balance = balances.get('BNB', 0.0)

        if bnb_balance >= min_bnb:
            logger.debug(f"Saldo BNB OK para taxas: {bnb_balance:.4f} BNB (mínimo: {min_bnb}).")
            return True
        else:
            logger.warning(
                f"⚠️  Saldo BNB insuficiente para pagar taxas com desconto: "
                f"{bnb_balance:.4f} BNB (mínimo recomendado: {min_bnb} BNB). "
                f"A taxa será cobrada na moeda da operação."
            )
            return False

    except Exception as e:
        logger.error(f"Erro ao verificar saldo BNB para taxas: {e}")
        return False

def create_limit_order(symbol: str, side: str, amount: float, price: float) -> dict:
    """
    Envia uma ordem Limit (Maker) para a exchange.
    O desconto de taxa com BNB deve estar habilitado na conta da Binance.

    Args:
        symbol (str): O par de moedas, ex: 'ETH/BTC'
        side (str): 'buy' ou 'sell'
        amount (float): Quantidade a ser negociada
        price (float): Preço limite da ordem

    Returns:
        dict: Resposta da exchange contendo detalhes da ordem
    """
    try:
        exchange = get_exchange()

        logger.info(f"Enviando ordem Limit: {side.upper()} {amount} de {symbol} a {price}.")
        order = exchange.create_order(symbol, 'limit', side, amount, price)

        logger.info(f"Ordem criada com sucesso: ID {order.get('id')}")
        return order

    except ccxt.InsufficientFunds as e:
        logger.error(f"Saldo insuficiente para executar a ordem: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro ao criar ordem limit para {symbol}: {e}")
        raise

def fetch_historical_data(symbol: str, timeframe: str = '4h', limit: int = 180) -> None:
    """
    Baixa os preços e salva no banco usando sincronização incremental.
    - Se é a primeira vez (Cold Start), baixa os N últimos candles definidos em limit.
    - Se já há dados, usa o get_last_timestamp() para buscar apenas o que falta.

    Args:
        symbol (str): O par a ser negociado (ex: 'ETH/BTC').
        timeframe (str): O período do candle (ex: '4h').
        limit (int): A quantidade de candles caso não haja `since`.
                     Padrão 180 candles de 4h equivalem a 30 dias de histórico (Cold Start inicial).
    """
    try:
        exchange = get_exchange()

        # 1. Verifica no banco qual foi o último dado que nós já lemos
        last_timestamp = get_last_timestamp(symbol)

        since = None
        if last_timestamp:
            # Sincronização Incremental: buscar a partir do último timestamp gravado.
            since = last_timestamp
            logger.info(f"Fazendo atualização incremental para {symbol} desde o timestamp {since}.")
        else:
            # Partida a frio (Cold Start). Usa limit para limitar
            logger.info(f"Executando Cold Start. Baixando últimos {limit} candles ({timeframe}) para {symbol}.")

        # 2. Requisita na corretora
        candles: List[List[Union[int, float]]] = exchange.fetch_ohlcv(
            symbol,
            timeframe=timeframe,
            since=since,
            limit=limit if since is None else None
        )

        if candles:
            save_candles(symbol, candles)
            logger.info(f"Sincronização OK. {len(candles)} candles recebidos do CCXT para {symbol}.")
        else:
            logger.info(f"Nenhum candle novo de {symbol} precisava ser baixado.")

    except ccxt.NetworkError as e:
        logger.error(f"Erro de rede ao baixar os candles: {e}")
        raise
    except ccxt.ExchangeError as e:
        logger.error(f"Erro da API da Binance (symbol {symbol}): {e}")
        raise
    except Exception as e:
        logger.error(f"Falha ao executar fetch_historical_data: {e}")
        raise

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    is_testnet = os.getenv('BINANCE_IS_TESTNET', 'False').strip().lower() == 'true'
    mode_label = "Testnet" if is_testnet else "Mainnet"
    print(f"--- Testando módulo Exchange ({mode_label}) ---")

    try:
        print("\n1. Testando conexão com a Binance e verificação de saldo:")
        bal = get_balance()
        print(f"   BTC: {bal['BTC']} | ETH: {bal['ETH']} | BNB: {bal['BNB']} | USDT: {bal['USDT']}")

        print("\n2. Testando check_bnb_for_fees():")
        bnb_ok = check_bnb_for_fees()
        print(f"   BNB suficiente para taxas: {bnb_ok}")

        symbol_test = 'ETH/BTC'
        tf_test = '4h'

        print("\n3. Testando download de dados históricos (Cold start):")
        fetch_historical_data(symbol_test, timeframe=tf_test, limit=5)

        print("\n4. Repetindo download histórico para engatilhar Sync Incremental:")
        fetch_historical_data(symbol_test, timeframe=tf_test, limit=5)

        print(f"\n--- Todos os testes de exchange concluídos com sucesso! ---")
    except Exception as e:
        print(f"\n[!] Falha no teste local. Erro: {e}")
        print(f"Dica: Verifique se as chaves do .env estão corretas para o modo {mode_label}.")
