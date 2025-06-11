import pandas as pd
import numpy as np
import glob
import matplotlib.pyplot as plt
import os
import time
import multiprocessing
from functools import reduce

# ==============================================================================
# FUNÇÕES DE CÁLCULO DE METAS (ADAPTADAS PARA RECEBER DICIONÁRIO DE SOMAS)
# ==============================================================================

def calcular_meta_generica(somas, col_julg, col_dist, col_susp, fator_mult):
    """Função genérica para calcular a maioria das metas a partir de somas pré-calculadas."""
    julgados = somas.get(col_julg, 0)
    distribuidos = somas.get(col_dist, 0)
    suspensos = somas.get(col_susp, 0)
    
    denominador = distribuidos - suspensos
    if denominador == 0:
        return np.nan
    return (julgados / denominador) * fator_mult

def calcular_metas_justica_estadual(somas):
    """Calcula as metas da Justiça Estadual a partir de um dicionário de somas."""
    metas = {}
    julgados = somas.get('julgados_2025_sum', 0)
    casos_novos = somas.get('casos_novos_2025_sum', 0)
    dessobrestados = somas.get('dessobrestados_2025_sum', 0)
    suspensos_m1 = somas.get('suspensos_2025_sum', 0)
    denominador_m1 = casos_novos + dessobrestados - suspensos_m1
    metas['meta1'] = (julgados / denominador_m1) * 100 if denominador_m1 > 0 else np.nan
    
    metas['meta2a'] = calcular_meta_generica(somas, 'julgadom2_a_sum', 'dism2_a_sum', 'susm2_a_sum', (1000/8))
    metas['meta2b'] = calcular_meta_generica(somas, 'julgadom2_b_sum', 'dism2_b_sum', 'susm2_b_sum', (1000/9))
    metas['meta4a'] = calcular_meta_generica(somas, 'julgadom4_a_sum', 'dism4_a_sum', 'susm4_a_sum', (1000/6.5))
    metas['meta4b'] = calcular_meta_generica(somas, 'julgadom4_b_sum', 'dism4_b_sum', 'susm4_b_sum', 100)
    metas['meta6'] = calcular_meta_generica(somas, 'julgadom6_sum', 'dism6_sum', 'susm6_sum', 100)
    metas['meta8a'] = calcular_meta_generica(somas, 'julgadom8_a_sum', 'dism8_a_sum', 'susm8_a_sum', (1000/7.5))
    metas['meta8b'] = calcular_meta_generica(somas, 'julgadom8_b_sum', 'dism8_b_sum', 'susm8_b_sum', (1000/9))
    return metas

# ... (Funções de cálculo para outros ramos da justiça seriam adaptadas de forma similar) ...
# Para manter o exemplo conciso, as outras funções seguem o mesmo padrão.
# O código completo teria todas as funções adaptadas.

def calcular_metas_finais(args):
    """Worker (Reduce): Recebe somas agregadas e calcula as metas finais."""
    nome_tribunal, dados_agregados = args
    ramo_justica = dados_agregados['ramo_justica']
    somas = dados_agregados['somas']

    mapa_calculo = {
        'Justiça Estadual': calcular_metas_justica_estadual,
        # Adicionar outras funções adaptadas aqui
        # 'Justiça do Trabalho': calcular_metas_justica_trabalho_adaptada,
        # etc.
    }

    resultado_final = {'sigla_tribunal': nome_tribunal, 'ramo_justica': ramo_justica}
    funcao_calculo = mapa_calculo.get(ramo_justica)
    
    if funcao_calculo:
        metas_calculadas = funcao_calculo(somas)
        resultado_final.update(metas_calculadas)
        
    return resultado_final

# ==============================================================================
# FUNÇÕES WORKER PARA PARALELIZAÇÃO (ESTRATÉGIA MAP-REDUCE)
# ==============================================================================

def processar_arquivo_para_somas(caminho_arquivo):
    """Worker (Map): Lê um arquivo CSV, agrupa por tribunal e retorna somas parciais."""
    try:
        df = pd.read_csv(caminho_arquivo, sep=',', low_memory=False)
        # Identifica colunas numéricas que precisam ser somadas
        colunas_para_soma = [col for col in df.columns if 'julg' in col or 'dist' in col or 'susp' in col or 'casos' in col or 'dessobrestados' in col]
        
        # Agrupa por tribunal dentro do arquivo
        grouped = df.groupby('sigla_tribunal')
        
        resultados_parciais = {}
        for nome, grupo in grouped:
            somas = grupo[colunas_para_soma].sum().to_dict()
            # Adiciona sufixo '_sum' para clareza
            somas = {f"{k}_sum": v for k, v in somas.items()}
            resultados_parciais[nome] = {
                'somas': somas,
                'ramo_justica': grupo['ramo_justica'].iloc[0] # Pega o ramo da justiça
            }
        return resultados_parciais
    except Exception as e:
        print(f"Erro ao processar arquivo {caminho_arquivo}: {e}")
        return {}

# ==============================================================================
# FUNÇÕES PRINCIPAIS DO PROCESSO (ESTRATÉGIA OTIMIZADA)
# ==============================================================================

def gerar_consolidado_eficiente(caminho_glob):
    """Gera o Consolidado.csv de forma eficiente em memória, lendo em pedaços."""
    print("Iniciando a geração do 'Consolidado.csv' (modo eficiente em memória)...")
    lista_arquivos = glob.glob(caminho_glob)
    if not lista_arquivos:
        return

    output_file = 'Consolidado.csv'
    
    # Escreve o cabeçalho usando o primeiro arquivo
    try:
        df_header = pd.read_csv(lista_arquivos[0], nrows=0, sep=',')
        df_header.to_csv(output_file, index=False, sep=';', decimal=',')

        # Processa cada arquivo em pedaços (chunks) para não sobrecarregar a memória
        chunksize = 100_000
        for f in lista_arquivos:
            with pd.read_csv(f, chunksize=chunksize, sep=',') as reader:
                for chunk in reader:
                    chunk.to_csv(output_file, mode='a', header=False, index=False, sep=';', decimal=',')
        print("'Consolidado.csv' gerado com sucesso.")
    except Exception as e:
        print(f"Falha ao gerar 'Consolidado.csv': {e}")


def gerar_grafico(df_resumo):
    """Gera um gráfico de barras para comparar a Meta 1 entre os tribunais."""
    if df_resumo is None or df_resumo.empty:
        print("Não há dados para gerar o gráfico.")
        return
    print("\nIniciando a Etapa 3: Geração do Gráfico...")
    
    df_grafico = df_resumo[df_resumo['meta1'].notna() & (df_resumo['meta1'] != 'NA')].copy()
    df_grafico['meta1'] = pd.to_numeric(df_grafico['meta1'])
    df_grafico = df_grafico.sort_values('meta1', ascending=False)

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(18, 10))
    
    bars = ax.bar(df_grafico['sigla_tribunal'], df_grafico['meta1'], color='rebeccapurple')

    ax.set_ylabel('Índice de Desempenho (%)', fontsize=12)
    ax.set_title('Desempenho da Meta 1 por Tribunal (Estratégia Otimizada)', fontsize=16, weight='bold', pad=20)
    ax.tick_params(axis='x', rotation=90)
    
    ax.axhline(100, color='crimson', linestyle='--', linewidth=2, label='Meta (100%)')
    ax.legend()
    
    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2.0, yval + 1, f'{yval:.1f}', ha='center', va='bottom', fontsize=8, rotation=90)

    plt.ylim(0, max(df_grafico['meta1'].max() * 1.1, 110))
    plt.tight_layout()
    
    caminho_grafico = 'desempenho_metas_otimizado.png'
    plt.savefig(caminho_grafico)
    print(f"Gráfico salvo com sucesso em '{caminho_grafico}'.")

# ==============================================================================
# EXECUÇÃO PRINCIPAL
# ==============================================================================

if __name__ == '__main__':
    multiprocessing.freeze_support()
    start_time = time.time()
    
    caminho_csvs_glob = os.path.join('Dados', 'teste_*.csv')
    num_processos = os.cpu_count()
    print(f"Iniciando processamento otimizado com {num_processos} processos...")

    # ETAPA 0: Gerar Consolidado.csv de forma eficiente (sequencial, mas leve)
    gerar_consolidado_eficiente(caminho_csvs_glob)

    # ETAPA 1 (MAP): Processar cada arquivo em paralelo para obter somas parciais
    print("\nETAPA 1 (MAP): Lendo arquivos e calculando somas parciais em paralelo...")
    lista_arquivos = glob.glob(caminho_csvs_glob)
    with multiprocessing.Pool(processes=num_processos) as pool:
        resultados_parciais_lista = pool.map(processar_arquivo_para_somas, lista_arquivos)

    # ETAPA 1.5 (COMBINE): Agregar os resultados parciais (rápido, sequencial)
    print("Combinando resultados parciais...")
    somas_agregadas = {}
    for dic_parcial in resultados_parciais_lista:
        for tribunal, dados in dic_parcial.items():
            if tribunal not in somas_agregadas:
                somas_agregadas[tribunal] = {'ramo_justica': dados['ramo_justica'], 'somas': {}}
            
            for chave_soma, valor_soma in dados['somas'].items():
                somas_agregadas[tribunal]['somas'][chave_soma] = somas_agregadas[tribunal]['somas'].get(chave_soma, 0) + valor_soma

    # ETAPA 2 (REDUCE): Calcular as metas finais em paralelo usando as somas agregadas
    print("\nETAPA 2 (REDUCE): Calculando metas finais em paralelo...")
    with multiprocessing.Pool(processes=num_processos) as pool:
        resultados_finais = pool.map(calcular_metas_finais, somas_agregadas.items())
    
    # Finalização
    df_resumo = pd.DataFrame(resultados_finais)
    df_resumo = df_resumo.fillna('NA')
    df_resumo.to_csv('ResumoMetas.csv', index=False, sep=';', decimal=',')
    print("Arquivo 'ResumoMetas.csv' gerado com sucesso.")

    gerar_grafico(df_resumo)

    end_time = time.time()
    tempo_total = end_time - start_time
    print(f"\nProcesso OTIMIZADO concluído em {tempo_total:.2f} segundos.")
