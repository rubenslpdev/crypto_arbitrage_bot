# 🤖 Crypto Arbitrage Bot (BTC/ETH) - Institutional Strategy

Este é um bot de trading automatizado desenvolvido em Python para operar o par **ETH/BTC** na **Binance**. A estratégia foca em **Arbitragem Estatística (Pairs Trading)** utilizando conceitos institucionais de análise on-chain e gestão de risco.

## Funcionalidades
- **Pairs Trading (Z-Score):** Opera a distorção estatística da razão entre Bitcoin e Ethereum.
- **Filtros On-Chain:** Integração com APIs para monitorar MVRV Z-Score e Whale Inflows.
- **Gestão de Risco:** Implementação do **Half-Kelly Criterion** para dimensionamento de posição.
- **Eficiência Operacional:** Utilização de ordens **LIMIT (Maker)** e taxas pagas em BNB para máxima economia.
- **Monitoramento:** Alertas em tempo real via **Telegram** e logs persistentes em **SQLite**.

## Tecnologias Utilizadas
- **Linguagem:** Python 3.10+
- **Exchange API:** CCXT (Binance Testnet habilitada)
- **Banco de Dados:** SQLite
- **Análise de Dados:** Pandas

## Pré-requisitos
Antes de começar, você precisará de:
1. Chaves de API da **Binance Spot Testnet**.
2. Tokens de API da **Glassnode** ou **CryptoQuant**.
3. Um Bot no **Telegram** (via BotFather).

## Instalação
1. Clone este repositório:

   ```bash
   git clone https://github.com/rubenslpdev/crypt_arbitrage_bot.git

    Crie e ative seu ambiente virtual:
    python -m venv venv
    source venv/bin/activate  # Windows: venv\Scripts\activate

    Instale as dependências:
    pip install -r requirements.txt

    Configure suas credenciais no arquivo .env (use o .env.example como base).
    ```

## Estratégia de Execução

O bot opera em ciclos de 4 horas, verificando a razão ETH/BTC. A execução ocorre apenas quando a anomalia estatística (Z-Score) é validada pelos filtros de saúde macro do mercado (MVRV e Fluxo de Baleias).

---

Aviso: Este software é para fins educacionais e de teste em ambiente Sandbox. Criptoativos envolvem alto risco.
