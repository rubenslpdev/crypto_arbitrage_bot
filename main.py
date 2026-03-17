import os
import sys
import logging
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

# Adiciona o diretório 'src' ao path de leitura do Python para importar os módulos
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from database import init_db, get_db_connection
from exchange import get_exchange, get_balance, fetch_historical_data, create_limit_order, check_bnb_for_fees
from indicators import calculate_zscore, calculate_kelly_size
from messenger import send_alert, send_trade_report
from portfolio_tracker import run_daily_report

# Garante que a pasta logs exista
LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)

# Configuração de logging híbrido (console + arquivo)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, 'bot.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('MainBot')

def load_data_from_db(symbol: str) -> pd.DataFrame:
    """Busca o histórico de preços salvo no banco de dados para calcular indicadores."""
    try:
        with get_db_connection() as conn:
            # Pandas já cuida da execução via read_sql_query
            df = pd.read_sql_query(
                "SELECT * FROM market_data WHERE symbol = ? ORDER BY timestamp ASC",
                conn, 
                params=(symbol,)
            )
        return df
    except Exception as e:
        logger.error(f"Erro ao buscar os dados SQLite para o Symbol {symbol}: {e}")
        return pd.DataFrame()

def save_trade_to_db(timestamp: int, symbol: str, side: str, price: float, amount: float, fee_bnb: float, estimated_profit: float):
    """Guarda o registro de um trade no SQLite."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO trades (timestamp, symbol, side, price, amount, fee_bnb, estimated_profit)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (timestamp, symbol, side, price, amount, fee_bnb, estimated_profit))
            conn.commit()
    except Exception as e:
        logger.error(f"Erro ao salvar o trade efetuado no banco de dados: {e}")

def run_cycle():
    """
    Função principal que orquestra todo o ciclo do robô (Sincronização -> Análise -> Decisão -> Execução -> Relato).
    """
    symbol = 'ETH/BTC'
    timeframe = '4h'
    logger.info("=== 🚀 Iniciando Novo Ciclo do Bot ===")
    
    try:
        # 1. Inicialização
        init_db()
        exchange_client = get_exchange()  # Modo Mainnet ou Testnet conforme BINANCE_IS_TESTNET no .env
        
        # 2. Sincronização
        fetch_historical_data(symbol, timeframe=timeframe)
        
        # 3. Análise (Z-Score)
        df_candles = load_data_from_db(symbol)
        
        if df_candles.empty:
            logger.warning("Nenhum dado encontrado no banco para calcular Z-Score.")
            send_alert("⚠️ Ciclo abortado: Sem dados no banco para análise.")
            return

        # Z-Score do Ratio (Para ETH/BTC o ratio é o próprio preço de 'close')
        df_candles['ratio'] = df_candles['close']
        
        z_score = calculate_zscore(df_candles, window=30)
        
        if z_score is None:
            logger.info("Sincronização OK, mas não há dados suficientes para calcular o Z-Score da janela escolhida.")
            return
            
        logger.info(f"O Z-Score calculado da razão ETH/BTC neste ciclo foi de: {z_score:.2f}")
        
        # 4. Decisão 
        side = None
        if z_score < -2.5:
            # Sinal de Compra: ETH/BTC afundou longe da média, esperamos reversão (compramos ETH usando BTC da carteira)
            side = 'buy'
            logger.info(f"Sinal Detectado: COMPRA (Z-Score {z_score:.2f} rompeu o chão de -2.5)")
        elif z_score > 2.5:
            # Sinal de Venda: ETH/BTC "esticou" muito além da média, vamos vender o ETH e resgatar o BTC
            side = 'sell'
            logger.info(f"Sinal Detectado: VENDA (Z-Score {z_score:.2f} furou o teto de +2.5)")
        else:
            # 7. Logs de Status (Heartbeat Silencioso sem Trades)
            logger.info(f"Ciclo concluído. O mercado segue calmo e dentro da normalidade. Nenhuma operação agendada.")
            return
            
        # 5. Dimensionamento e Filtros Finais
        # Com a remoção do filtro On-Chain (MVRV) nas suas instruções, aplicamos apenas o Kelly Criterion
        win_prob = 0.52 # Começando com 52% (Histórico de trades alimentará isso no futuro)
        payoff = 1.0    # 1:1 Gain/Loss ratio projetado
        kelly_fraction = calculate_kelly_size(win_prob, payoff)
        
        if kelly_fraction <= 0:
            logger.warning("O Kelly Fraction retornou 0%. O risco de investir superou o limiar de alocação. Abortando operação.")
            send_alert(f"ℹ️ Sinal {side.upper()} ocorreu, mas o dimensionamento de risco (Kelly = 0%) evitou a operação.")
            return
            
        # Busca Saldo Livre
        balances = get_balance()
        btc_free = balances.get('BTC', 0.0)
        eth_free = balances.get('ETH', 0.0)
        
        # Coleta das melhores cotações atuais do mercado para criarmos uma ordem LIMITada Maker.
        ticker = exchange_client.fetch_ticker(symbol)
        
        if side == 'buy':
            # Maker Buy: Limit Order no livro de lances de compra (Bid)
            limit_price = ticker['bid']
            amount_btc_to_invest = btc_free * kelly_fraction
            amount_eth_to_buy = amount_btc_to_invest / limit_price
            
            # Filtro lógico financeiro de corretora, previne ordens em que não se tem BTC suficiente ou limite mínimo irrisório
            if amount_eth_to_buy < 0.001:
                logger.warning(f"Saldo baixo ou tamanho bloqueado. Impossivel realizar compra de {amount_eth_to_buy:.4f} ETH. Requer saldo.")
                return
            
            amount = amount_eth_to_buy
            price = limit_price
            
        else: # sell
            # Maker Sell: Limit Order no livro de lances de venda (Ask)
            limit_price = ticker['ask']
            amount_eth_to_sell = eth_free * kelly_fraction
            
            if amount_eth_to_sell < 0.001:
                logger.warning(f"Saldo de ETH ínfimo ({eth_free}). Bloqueado venda de tamanho {amount_eth_to_sell:.4f} ETH.")
                return
                
            amount = amount_eth_to_sell
            price = limit_price

        # Verificação de saldo BNB para pagamento de taxas com desconto (~25%)
        use_bnb_for_fees = os.getenv('USE_BNB_FOR_FEES', 'False').strip().lower() == 'true'
        if use_bnb_for_fees:
            bnb_ok = check_bnb_for_fees(min_bnb=0.01)
            if not bnb_ok:
                send_alert(
                    "⚠️ <b>Aviso de Taxa:</b> Saldo de BNB insuficiente para o desconto de 25%. "
                    "A taxa desta operação será cobrada na moeda da ordem."
                )
        
        # Envia de fato a ordem para a corretora
        logger.info(f"Enviando ordem Binance: LIMIT {side.upper()} {amount:.4f} {symbol} a {price:.6f} via Half-Kelly ({kelly_fraction*100:.2f}%)")
        order = create_limit_order(symbol, side, amount, price)
        
        # 6. Persistência de Dados e Alertas
        fee_bnb = 0.0
        if 'fee' in order and order['fee'] and 'cost' in order['fee']:
            # Em alguns retornos a testnet pode já simular e devolver na chave 'fee' 
            fee_bnb = order['fee']['cost']
            
        # O lucro final estimado na execução
        estimated_profit_pct = (abs(z_score) - 2.5) if abs(z_score) > 2.5 else 0.0
        current_ts = int(datetime.now().timestamp() * 1000)
        order_ts = order.get('timestamp') or current_ts
        
        save_trade_to_db(
            timestamp=order_ts,
            symbol=symbol,
            side=side,
            price=price,
            amount=amount,
            fee_bnb=fee_bnb,
            estimated_profit=estimated_profit_pct
        )
        
        send_trade_report(
            symbol=symbol,
            side=side,
            price=price,
            amount=amount,
            z_score=z_score,
            estimated_profit=estimated_profit_pct
        )
        logger.info("✅ Operação do Ciclo bem-sucedida. Ordem salva e notificação emitida.")
        
    except Exception as e:
        err_msg = f"Erro letal durante o ciclo principal do bot: {e}"
        logger.error(err_msg, exc_info=True)
        send_alert(f"🚨 <b>FALHA NO BOT DE ARBITRAGEM</b>\n\nO loop de {timeframe} sofreu e foi interrompido:\n<code>{e}</code>")
        
    finally:
        # 7. Relatório Diário (Roda garantidamente 1 vez ao dia se for a hora certa, independente de trades)
        try:
            from database import get_yesterday_snapshot, get_db_connection
            # Verificamos se já existe um snapshot "hoje" (nas últimas 23h). Se não existir, gera o relatório.
            # get_yesterday_snapshot não é exatamente "ontem calendário", mas sim "há 24h atrás".
            # Para evitar floodar o bot com relatórios se rodar de 4 em 4h, podemos verificar
            # o último snapshot e se ele for mais antigo que 20-24h, chamamos o relatório.
            
            import time
            current_ts = int(time.time() * 1000)
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT MAX(timestamp) FROM daily_snapshots')
                last_snap = cursor.fetchone()[0]
                
            # Se não tem nenhum, ou se o último foi há mais de 23h, roda o relatório
            if not last_snap or (current_ts - last_snap) > (23 * 60 * 60 * 1000):
                logger.info("⏱️ Momento de gerar o Relatório Diário de Performance.")
                run_daily_report()
            else:
                logger.debug("Relatório Diário já gerado nas últimas 24h.")
                
        except Exception as e:
            logger.error(f"Erro ao tentar disparar Relatório Diário no fim do ciclo: {e}")

if __name__ == "__main__":
    load_dotenv()
    run_cycle()
