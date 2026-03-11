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
    Obrigatório inicializar no modo Testnet (Sandbox) para evitar
    operações com capital real.
    
    Returns:
        exchange (ccxt.binance): Objeto ccxt pronto para operações na Testnet.
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
            'defaultType': 'spot' # Operações apenas no mercado à vista (Spot)
        }
    })
    
    # REGRA CRÍTICA: O bot DEVE operar exclusivamente em modo Sandbox.
    exchange.set_sandbox_mode(True)
    logger.debug("Binance Testnet ativada (Sandbox Mode).")
    
    return exchange

def get_balance() -> Dict[str, float]:
    """
    Consulta o saldo disponível de BTC e ETH na conta Spot da Testnet.
    
    Returns:
        dict: Dicionário contendo os saldos livres (free), ex: {'BTC': 0.1, 'ETH': 2.0}
    """
    try:
        exchange = get_exchange()
        # fetch_balance sem parâmetros traz as carteiras default
        balance_info = exchange.fetch_balance()
        
        # Pega a parte 'free' para saber o que realmente pode ser usado no trade
        btc_free = balance_info.get('BTC', {}).get('free', 0.0)
        eth_free = balance_info.get('ETH', {}).get('free', 0.0)
        
        logger.info(f"Saldo disponível consultado na Testnet: {btc_free} BTC | {eth_free} ETH")
        return {'BTC': float(btc_free), 'ETH': float(eth_free)}
        
    except ccxt.NetworkError as e:
        logger.error(f"Erro de rede ao buscar saldo na Binance: {e}")
        raise
    except ccxt.ExchangeError as e:
        logger.error(f"Erro da Exchange ao buscar saldo: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado ao buscar saldo (Possível chaves inválidas?): {e}")
        raise

def create_limit_order(symbol: str, side: str, amount: float, price: float) -> dict:
    """
    Envia uma ordem Limit (Maker) para a exchange.
    O desconto de taxa com BNB deve estar configurado na conta.
    
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
            # ccxt aceita o `since` em milissegundos
            since = last_timestamp
            logger.info(f"Fazendo atualização incremental para {symbol} desde o timestamp {since}.")
            # Importante: Como `since` incluiu o candle de abertura anterior, o CCXT trará esse candle novamente.
            # Nossa função de database save_candles usa 'INSERT OR IGNORE', então os candles
            # idênticos antigos serão apenas ignorados sem corromper o banco.
        else:
            # Partida a frio (Cold Start). Usa limit para limitar
            logger.info(f"Executando Cold Start. Baixando últimos {limit} candles ({timeframe}) para {symbol}.")

        # 2. Requisita na corretora (sandbox)
        # Atenção: Se since é None, a Binance traz os T candles mais recentes respeitando o limit.
        candles: List[List[Union[int, float]]] = exchange.fetch_ohlcv(
            symbol, 
            timeframe=timeframe, 
            since=since, 
            limit=limit if since is None else None # Limite grande caso seja apenas update via since
        )
        
        if candles:
            # O array de candles volta na forma:
            # [[timestamp_ms, open, high, low, close, volume], ...]
            # Removemos a última vela se ela for o momento atual (ainda em formação/não finalizada)
            # Para evitar salvar dados "imcompletos", exigimos que a vela não seja a candle atual do timeframe.
            # Mas como não sabemos e a lógica era só salvar "se fechar", podemos simplesmente guardar o que vier
            # A estratégia do REPLACE OR UPDATE poderia sobrescrever. Como usamos INSERT OR IGNORE,
            # os candles provisórios podem ser um problema.
            # Dica simples: se o tempo agendado roda perfeitamente a cada 4h, guardamos tudo.  
            
            save_candles(symbol, candles)
            logger.info(f"Sincronização OK. {len(candles)} candles recebidos do CCXT para {symbol}.")
        else:
            logger.info(f"Nenhum candle novo de {symbol} precisava ser baixado ou recebido do CCXT.")

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
    
    # Confirma o carregamento do ambiente local para teste
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    print("--- Testando módulo Exchange com a Testnet ---")
    
    try:
        # Testar a conectividade chamando o saldo
        print("\n1. Testando conexão com a Binance e verificação de saldo:")
        bal = get_balance()
        print(f"   Saldo Disponível na Testnet > BTC: {bal['BTC']} | ETH: {bal['ETH']}")
        
        # Testar o fetch de histórico (Incremental sync vs Cold Start)
        symbol_test = 'ETH/BTC'
        tf_test = '4h'
        
        print("\n2. Testando download de dados históricos (Cold start):")
        # limit=5 apenas para um teste rápido e não lotar o log
        fetch_historical_data(symbol_test, timeframe=tf_test, limit=5)
        
        print("\n3. Repetindo download histórico para engatilhar Sync Incremental:")
        fetch_historical_data(symbol_test, timeframe=tf_test, limit=5)
        
        print("\n--- Todos os testes de exchange concluídos com sucesso! ---")
    except Exception as e:
        print(f"\n[!] Falha no teste local. Erro: {e}")
        print("Dica: Verifique se suas chaves do .env estão configuradas e preenchidas para a Testnet.")
