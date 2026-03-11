import logging
import time
from datetime import datetime

from exchange import get_balance, get_exchange
from messenger import send_alert
from database import save_daily_snapshot, get_yesterday_snapshot, get_last_trade
from indicators import calculate_zscore

logger = logging.getLogger(__name__)

def run_daily_report():
    """
    Gera um relatório diário de performance focado no crescimento de BTC.
    Calcula patrimônio, exposição, compara com o dia anterior e envia via Telegram.
    """
    logger.info("Iniciando geração do Relatório Diário de Performance...")
    
    try:
        # 1. Obter saldos
        balances = get_balance()
        btc_balance = balances.get('BTC', 0.0)
        eth_balance = balances.get('ETH', 0.0)
        
        # 2. Obter preço atual ETH/BTC
        exchange = get_exchange()
        ticker = exchange.fetch_ticker('ETH/BTC')
        eth_price_in_btc = ticker['last']
        
        # 3. Calcular Patrimônio Total em BTC e Alocação
        eth_value_in_btc = eth_balance * eth_price_in_btc
        total_equity_btc = btc_balance + eth_value_in_btc
        
        btc_percentage = (btc_balance / total_equity_btc * 100) if total_equity_btc > 0 else 0
        eth_percentage = (eth_value_in_btc / total_equity_btc * 100) if total_equity_btc > 0 else 0
        
        current_ts = int(time.time() * 1000)
        
        # 4. Buscar histórico de ontem ANTES de salvar o de hoje para comparar
        yesterday_snapshot = get_yesterday_snapshot()
        
        # 5. Salvar snapshot no banco de dados
        save_daily_snapshot(current_ts, total_equity_btc, btc_balance, eth_balance, eth_price_in_btc)
        
        # 6. Calcular Performance 24h
        performance_pct = 0.0
        performance_symbol = "➖"
        if yesterday_snapshot and yesterday_snapshot['total_equity_btc'] > 0:
            yesterday_equity = yesterday_snapshot['total_equity_btc']
            performance_pct = ((total_equity_btc - yesterday_equity) / yesterday_equity) * 100
            
            if performance_pct > 0:
                performance_symbol = "▲"
            elif performance_pct < 0:
                performance_symbol = "▼"
        
        # 7. Indicadores (Z-Score)
        # Import local to avoid circular imports if any, but since we are independent it's fine
        import pandas as pd
        from database import get_db_connection
        
        z_score_str = "N/A"
        z_score_status = "Desconhecido"
        try:
            with get_db_connection() as conn:
                df = pd.read_sql_query("SELECT * FROM market_data WHERE symbol = 'ETH/BTC' ORDER BY timestamp ASC", conn)
            
            if not df.empty:
                df['ratio'] = df['close']
                z = calculate_zscore(df, window=30)
                if z is not None:
                    z_score_str = f"{z:.2f}"
                    if z < -2.5:
                        z_score_status = "Oportunidade de Compra"
                    elif z > 2.5:
                        z_score_status = "Oportunidade de Venda"
                    else:
                        z_score_status = "Neutro"
        except Exception as e:
            logger.warning(f"Não foi possível calcular o Z-Score para o relatório: {e}")
            
        # 8. Status do Bot (Último Trade)
        last_trade = get_last_trade()
        last_trade_str = "Nenhum trade realizado ainda"
        if last_trade:
            trade_ts = last_trade['timestamp'] / 1000
            trade_date = datetime.fromtimestamp(trade_ts)
            days_ago = (datetime.now() - trade_date).days
            
            time_str = "Hoje" if days_ago == 0 else f"Há {days_ago} dias" if days_ago > 1 else "Ontem"
            acao = "Compra ETH" if last_trade['side'] == 'buy' else "Venda ETH"
            last_trade_str = f"_{time_str} ({acao})_"
            
        # 9. Montar Mensagem
        hoje_str = datetime.now().strftime("%d de %B de %Y")
        
        # Traduzindo meses
        meses = {
            "January": "Janeiro", "February": "Fevereiro", "March": "Março", "April": "Abril",
            "May": "Maio", "June": "Junho", "July": "Julho", "August": "Agosto",
            "September": "Setembro", "October": "Outubro", "November": "Novembro", "December": "Dezembro"
        }
        for eng, pt in meses.items():
            hoje_str = hoje_str.replace(eng, pt)
            
        msg = (
            f"📊 <b>Relatório Diário de Performance</b>\n"
            f"📅 <i>{hoje_str}</i>\n\n"
            f"💰 <b>Patrimônio:</b>\n\n"
            f"• Total: <code>{total_equity_btc:.4f} BTC</code> ({performance_symbol} {performance_pct:.2f}% nas últimas 24h)\n"
            f"• BTC: <code>{btc_balance:.4f}</code> ({btc_percentage:.0f}%)\n"
            f"• ETH: <code>{eth_balance:.4f}</code> ({eth_percentage:.0f}%)\n\n"
            f"🎯 <b>Indicadores de Mercado:</b>\n\n"
            f"• Z-Score ETH/BTC: <code>{z_score_str}</code> ({z_score_status})\n\n"
            f"🤖 <b>Status do Bot:</b>\n\n"
            f"• Último Trade: {last_trade_str}\n"
            f"• Status: <i>Monitorando oportunidades...</i>"
        )
        
        # 10. Enviar Relatório
        send_alert(msg)
        logger.info("Relatório Diário enviado com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro ao gerar relatório diário: {e}", exc_info=True)

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    print("--- Testando Portfolio Tracker ---")
    run_daily_report()
    print("--- Teste concluído ---")
