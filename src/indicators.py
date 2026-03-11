import os
import requests
import logging
import pandas as pd
from typing import Optional, Union

# Configuração de log
logger = logging.getLogger(__name__)

def calculate_zscore(df: pd.DataFrame, window: int = 30) -> Optional[float]:
    """
    Calcula o Z-Score da razão ETH/BTC.
    
    Args:
        df (pd.DataFrame): DataFrame contendo as colunas de preços. 
                           Obrigatório conter a coluna 'ratio' (ETH/BTC).
        window (int): Janela de observação para média e desvio padrão.
        
    Returns:
        float: O Z-Score do candle mais recente, ou None se dados insuficientes.
    """
    if 'ratio' not in df.columns:
        logger.error("A coluna 'ratio' (ETH/BTC) não foi encontrada no DataFrame.")
        raise ValueError("DataFrame deve conter uma coluna chamada 'ratio'.")
        
    if len(df) < window:
        logger.warning(f"Dados insuficientes para calcular Z-Score. Requer {window}, temos {len(df)}.")
        return None
        
    # Usando .copy() para evitar SettingWithCopyWarning
    df = df.copy()
    
    # Calcular Média Móvel (SMA) e Desvio Padrão (STD) para a janela de N períodos
    df['rolling_mean'] = df['ratio'].rolling(window=window).mean()
    df['rolling_std'] = df['ratio'].rolling(window=window).std()
    
    # Z-Score = (Valor Atual - Média da Janela) / Desvio Padrão da Janela
    df['z_score'] = (df['ratio'] - df['rolling_mean']) / df['rolling_std']
    
    # Pega o último registro válido
    current_zscore = df['z_score'].iloc[-1]
    
    if pd.isna(current_zscore):
        return None
        
    return float(current_zscore)

def calculate_kelly_size(win_prob: float, payoff: float) -> float:
    """
    Calcula a fração do capital a ser alocada baseada no Critério de Kelly.
    Usa especificamente a variação 'Half-Kelly' para maior conservadorismo.
    
    Fórmula Kelly = W - ((1 - W) / R)
    Onde:
      W = Probabilidade de vitória (win_prob, ex: 0.52 para 52%)
      R = Razão de Payoff (Lucro médio / Prejuízo médio, ex: 1.0 para 1:1)
      
    Args:
        win_prob (float): Probabilidade de acerto (ex: 0.52)
        payoff (float): Risco/Retorno (ex: 1.0 para 1:1)
        
    Returns:
        float: Fração recomendada do capital para entrar na operação. Mínimo 0.0 (não operar).
    """
    if payoff <= 0:
        logger.warning(f"Payoff inválido ou zero ({payoff}). Retornando fração alocável 0.0.")
        return 0.0
        
    kelly_fraction = win_prob - ((1.0 - win_prob) / payoff)
    
    # Aplica estratégia conservadora (Half-Kelly)
    half_kelly = kelly_fraction / 2.0
    
    # Previne que o bot recommende apostar "negativo" (venda a descoberto onde não deveria)
    final_fraction = max(0.0, half_kelly)
    
    logger.info(f"Cálculo Risco: WinProb={win_prob*100}%, Payoff={payoff} -> Alocar: {final_fraction*100:.2f}% do saldo.")
    return final_fraction

if __name__ == "__main__":
    from dotenv import load_dotenv
    import numpy as np
    
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    print("--- Testando módulo Indicators ---")
    
    print("\n1. Testando Z-Score (cenário simulado):")
    # Gerar um pequeno DF falso (30 preços randômicos + 1 de pico)
    ratios = list(np.random.normal(0.05, 0.005, 30))
    # Forçar o último para gerar um zscore gritante (ex: +3.0)
    ratios.append(ratios[-1] + (0.005 * 3)) 
    
    df_test = pd.DataFrame({'ratio': ratios})
    
    z_score = calculate_zscore(df_test, window=30)
    print(f"   Z-Score retornado da série: {z_score:.2f}")
    assert z_score is not None, "Falha: Z-score não deveria ser None aqui."
    
    print("\n3. Testando Gestão de Risco (Half-Kelly):")
    # Instrução manda 52% (0.52) acerto com 1:1 payoff
    fracao = calculate_kelly_size(win_prob=0.52, payoff=1.0)
    print(f"   Half-Kelly para 52% Win e 1:1 Payoff: {fracao*100:.2f}% (Esperado 2.00%)")
    # Kelly normal seria 0.52 - (0.48 / 1) = 0.04. Metade = 0.02 (2%)
    assert abs(fracao - 0.02) < 0.0001, "Cálculo matemático do Kelly está incorreto."
    
    print("\n--- Todos os testes de indicators concluídos com sucesso! ---")
