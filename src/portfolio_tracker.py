import logging
import time
from datetime import datetime

from exchange import get_balance, get_exchange
from messenger import send_alert
from database import save_daily_snapshot, get_yesterday_snapshot, get_last_trade, get_db_connection
from indicators import calculate_zscore

logger = logging.getLogger(__name__)


def _get_total_fees_spent_bnb() -> float:
    """
    Soma o total de taxas pagas em BNB registradas na tabela de trades.
    Usado para calcular a métrica 'Fees Economizadas'.

    Returns:
        float: Total acumulado de BNB gasto em taxas. 0.0 se não houver registros.
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COALESCE(SUM(fee_bnb), 0.0) FROM trades WHERE fee_bnb > 0")
            result = cursor.fetchone()
            return float(result[0]) if result else 0.0
    except Exception as e:
        logger.warning(f"Não foi possível calcular as taxas acumuladas em BNB: {e}")
        return 0.0


def run_daily_report():
    """
    Gera um relatório diário de performance focado no crescimento de BTC.
    Exibe saldos de BTC, ETH, BNB e USDT, performance 24h, Z-Score,
    e a métrica de 'Fees Economizadas' com uso de BNB.
    Envia o relatório via Telegram e salva snapshot no banco.
    """
    logger.info("Iniciando geração do Relatório Diário de Performance...")

    try:
        # 1. Obter saldos (BTC, ETH, BNB, USDT)
        balances = get_balance()
        btc_balance  = balances.get('BTC',  0.0)
        eth_balance  = balances.get('ETH',  0.0)
        bnb_balance  = balances.get('BNB',  0.0)
        usdt_balance = balances.get('USDT', 0.0)

        # 2. Obter preços atuais
        exchange = get_exchange()
        ticker_eth_btc  = exchange.fetch_ticker('ETH/BTC')
        ticker_bnb_usdt = exchange.fetch_ticker('BNB/USDT')

        eth_price_in_btc  = ticker_eth_btc['last']
        bnb_price_in_usdt = ticker_bnb_usdt['last']

        # 3. Calcular Patrimônio Total em BTC e alocações percentuais
        eth_value_in_btc   = eth_balance * eth_price_in_btc
        bnb_value_in_usdt  = bnb_balance * bnb_price_in_usdt
        total_equity_btc   = btc_balance + eth_value_in_btc

        btc_percentage = (btc_balance / total_equity_btc * 100) if total_equity_btc > 0 else 0
        eth_percentage = (eth_value_in_btc / total_equity_btc * 100) if total_equity_btc > 0 else 0

        current_ts = int(time.time() * 1000)

        # 4. Buscar histórico de ontem ANTES de salvar o de hoje (para comparação)
        yesterday_snapshot = get_yesterday_snapshot()

        # 5. Salvar snapshot no banco de dados (incluindo BNB e USDT)
        save_daily_snapshot(
            timestamp=current_ts,
            total_equity_btc=total_equity_btc,
            btc_balance=btc_balance,
            eth_balance=eth_balance,
            eth_price_in_btc=eth_price_in_btc,
            bnb_balance=bnb_balance,
            usdt_balance=usdt_balance,
        )

        # 6. Calcular Performance 24h
        performance_pct    = 0.0
        performance_symbol = "➖"
        if yesterday_snapshot and yesterday_snapshot['total_equity_btc'] > 0:
            yesterday_equity = yesterday_snapshot['total_equity_btc']
            performance_pct  = ((total_equity_btc - yesterday_equity) / yesterday_equity) * 100

            if performance_pct > 0:
                performance_symbol = "▲"
            elif performance_pct < 0:
                performance_symbol = "▼"

        # 7. Indicadores (Z-Score ETH/BTC)
        import pandas as pd

        z_score_str    = "N/A"
        z_score_status = "Desconhecido"
        try:
            with get_db_connection() as conn:
                df = pd.read_sql_query(
                    "SELECT * FROM market_data WHERE symbol = 'ETH/BTC' ORDER BY timestamp ASC",
                    conn
                )

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
        last_trade     = get_last_trade()
        last_trade_str = "Nenhum trade realizado ainda"
        if last_trade:
            trade_ts   = last_trade['timestamp'] / 1000
            trade_date = datetime.fromtimestamp(trade_ts)
            days_ago   = (datetime.now() - trade_date).days

            time_str       = "Hoje" if days_ago == 0 else f"Há {days_ago} dias" if days_ago > 1 else "Ontem"
            acao           = "Compra ETH" if last_trade['side'] == 'buy' else "Venda ETH"
            last_trade_str = f"_{time_str} ({acao})_"

        # 9. Fees Economizadas (soma de fee_bnb dos trades registrados)
        total_fees_bnb     = _get_total_fees_spent_bnb()
        fees_usdt_estimate = total_fees_bnb * bnb_price_in_usdt
        # Desconto aproximado: pagar em BNB dá ~25% de desconto vs moeda da ordem.
        # Estimamos o valor economizado como 25% do valor de taxa pago em BNB.
        fees_saved_usdt = fees_usdt_estimate * 0.25

        # 10. Montar Mensagem
        hoje_str = datetime.now().strftime("%d de %B de %Y")
        meses = {
            "January": "Janeiro", "February": "Fevereiro", "March": "Março",
            "April": "Abril", "May": "Maio", "June": "Junho",
            "July": "Julho", "August": "Agosto", "September": "Setembro",
            "October": "Outubro", "November": "Novembro", "December": "Dezembro"
        }
        for eng, pt in meses.items():
            hoje_str = hoje_str.replace(eng, pt)

        fees_line = (
            f"• Total BNB usado em taxas: <code>{total_fees_bnb:.6f} BNB</code>\n"
            f"• Desconto estimado (25%): <code>≈ ${fees_saved_usdt:.4f} USDT</code>"
        ) if total_fees_bnb > 0 else "• Nenhuma taxa em BNB registrada ainda."

        msg = (
            f"📊 <b>Relatório Diário de Performance</b>\n"
            f"📅 <i>{hoje_str}</i>\n\n"
            f"💰 <b>Patrimônio:</b>\n\n"
            f"• Total: <code>{total_equity_btc:.6f} BTC</code>  ({performance_symbol} {performance_pct:.2f}% nas últimas 24h)\n"
            f"• BTC:   <code>{btc_balance:.6f}</code>  ({btc_percentage:.0f}%)\n"
            f"• ETH:   <code>{eth_balance:.4f}</code>  ({eth_percentage:.0f}%)\n"
            f"• BNB:   <code>{bnb_balance:.4f}</code>  (≈ ${bnb_value_in_usdt:.2f} USDT)\n"
            f"• USDT:  <code>{usdt_balance:.2f}</code>\n\n"
            f"🎯 <b>Indicadores de Mercado:</b>\n\n"
            f"• Z-Score ETH/BTC: <code>{z_score_str}</code>  ({z_score_status})\n\n"
            f"💸 <b>Taxas (BNB):</b>\n\n"
            f"{fees_line}\n\n"
            f"🤖 <b>Status do Bot:</b>\n\n"
            f"• Último Trade: {last_trade_str}\n"
            f"• Status: <i>Monitorando oportunidades...</i>"
        )

        # 11. Enviar Relatório
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
