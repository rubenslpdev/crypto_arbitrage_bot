import os
import requests
import logging

# Configuração de log
logger = logging.getLogger(__name__)

def send_alert(message: str) -> bool:
    """
    Envia uma mensagem de texto simples para o Telegram configurado no .env.
    Usa um try-except silencioso para não interromper o bot em caso de falha de rede.
    
    Args:
        message (str): O texto da mensagem a ser enviada.
        
    Returns:
        bool: True se a mensagem foi enviada com sucesso, False caso contrário.
    """
    token = os.getenv('TELEGRAM_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    if not token or not chat_id:
        logger.warning("Credenciais do Telegram ausentes no .env. Alerta não enviado.")
        return False
        
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML" # Permite formatar texto com tags no envio
    }
    
    try:
        # Timeout curto para não travar o loop de negociação
        response = requests.post(url, json=payload, timeout=5)
        response.raise_for_status()
        logger.debug("Alerta enviado ao Telegram com sucesso.")
        return True
    except requests.exceptions.RequestException as e:
        # Erro capturado silenciosamente para manter o bot rodando (requisito do usário)
        logger.error(f"Falha ao enviar alerta para o Telegram: {e}")
        return False

def send_trade_report(symbol: str, side: str, price: float, amount: float, z_score: float, estimated_profit: float) -> bool:
    """
    Formata e envia uma mensagem padronizada de ordem executada.
    
    Args:
        symbol (str): O par negociado (ex: 'ETH/BTC').
        side (str): Direção da ordem ('buy' ou 'sell').
        price (float): Preço de execução.
        amount (float): Quantidade negociada.
        z_score (float): O Z-Score medido no momento da decisão.
        estimated_profit (float): Estimativa de lucro da operação.
        
    Returns:
        bool: True se o alerta foi enviado.
    """
    acao = "🟢 COMPRA" if side.lower() == 'buy' else "🔴 VENDA"
    
    # Formatação limpa usando HTML para o parse_mode do Telegram
    msg = (
        f"⚡ <b>Ordem Executada: BitBot</b>\n\n"
        f"<b>Ação:</b> {acao}\n"
        f"<b>Par:</b> {symbol}\n"
        f"<b>Preço Limit:</b> {price}\n"
        f"<b>Quantidade:</b> {amount}\n"
        f"<b>Z-Score Gatilho:</b> {z_score:.2f}\n"
        f"<b>Lucro Estimado:</b> {estimated_profit:.4f}\n"
    )
    
    logger.info(f"Enviando relatório de Trade via Telegram para o par {symbol}.")
    return send_alert(msg)

if __name__ == "__main__":
    from dotenv import load_dotenv
    
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    print("--- Testando módulo Messenger ---")
    
    print("\n1. Testando send_alert() simples:")
    # Nota: Executará silenciosamente e retornará False se você não tiver chaves reais no seu .env
    sucesso_alert = send_alert("🤖 Olá! Este é um teste do BitBot.")
    print(f"   Status do envio: {'✅ Sucesso' if sucesso_alert else '❌ Falha/Credencial Ausente'}")
    
    print("\n2. Testando send_trade_report() com dados falsos:")
    sucesso_trade = send_trade_report(
        symbol="ETH/BTC",
        side="buy",
        price=0.0543,
        amount=0.5,
        z_score=-2.65,
        estimated_profit=0.0012
    )
    print(f"   Status do envio: {'✅ Sucesso' if sucesso_trade else '❌ Falha/Credencial Ausente'}")
    
    print("\n--- Todos os testes de messenger concluídos! ---")
