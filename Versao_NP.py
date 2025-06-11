import pandas as pd
import numpy as np
import glob
import matplotlib.pyplot as plt
import os
import time

# ==============================================================================
# FUNÇÕES DE CÁLCULO DE METAS POR RAMO DA JUSTIÇA (VERSÃO CORRIGIDA)
# Adicionada verificação de existência de colunas para evitar KeyError.
# ==============================================================================

def calcular_meta_generica(df, col_julg, col_dist, col_susp, fator_mult):
    """Função genérica para calcular a maioria das metas."""
    julgados = pd.to_numeric(df[col_julg], errors='coerce').sum()
    distribuidos = pd.to_numeric(df[col_dist], errors='coerce').sum()
    suspensos = pd.to_numeric(df[col_susp], errors='coerce').sum()

    denominador = distribuidos - suspensos
    if denominador == 0:
        return np.nan
    
    return (julgados / denominador) * fator_mult

def calcular_metas_justica_estadual(df):
    """Calcula as metas específicas para a Justiça Estadual."""
    metas = {}
    
    # Meta 1
    cols_m1 = ['julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025']
    if all(col in df.columns for col in cols_m1):
        julgados = pd.to_numeric(df['julgados_2025'], errors='coerce').sum()
        casos_novos = pd.to_numeric(df['casos_novos_2025'], errors='coerce').sum()
        dessobrestados = pd.to_numeric(df['dessobrestados_2025'], errors='coerce').sum()
        suspensos_m1 = pd.to_numeric(df['suspensos_2025'], errors='coerce').sum()
        denominador_m1 = casos_novos + dessobrestados - suspensos_m1
        metas['meta1'] = (julgados / denominador_m1) * 100 if denominador_m1 > 0 else np.nan
    else:
        metas['meta1'] = np.nan

    # Dicionário de metas e suas colunas/fatores necessários
    mapa_metas = {
        'meta2a': (['julgadom2_a', 'dism2_a', 'susm2_a'], (1000/8)),
        'meta2b': (['julgadom2_b', 'dism2_b', 'susm2_b'], (1000/9)),
        'meta4a': (['julgadom4_a', 'dism4_a', 'susm4_a'], (1000/6.5)),
        'meta4b': (['julgadom4_b', 'dism4_b', 'susm4_b'], 100),
        'meta6': (['julgadom6', 'dism6', 'susm6'], 100),
        'meta8a': (['julgadom8_a', 'dism8_a', 'susm8_a'], (1000/7.5)),
        'meta8b': (['julgadom8_b', 'dism8_b', 'susm8_b'], (1000/9)),
    }

    for meta_nome, (cols, fator) in mapa_metas.items():
        if all(col in df.columns for col in cols):
            metas[meta_nome] = calcular_meta_generica(df, cols[0], cols[1], cols[2], fator)
        else:
            metas[meta_nome] = np.nan
            
    return metas

def calcular_metas_justica_trabalho(df):
    """Calcula as metas específicas para a Justiça do Trabalho."""
    metas = {}
    cols_m1 = ['julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025']
    if all(col in df.columns for col in cols_m1):
        julgados = pd.to_numeric(df['julgados_2025'], errors='coerce').sum()
        casos_novos = pd.to_numeric(df['casos_novos_2025'], errors='coerce').sum()
        dessobrestados = pd.to_numeric(df['dessobrestados_2025'], errors='coerce').sum()
        suspensos_m1 = pd.to_numeric(df['suspensos_2025'], errors='coerce').sum()
        denominador_m1 = casos_novos + dessobrestados - suspensos_m1
        metas['meta1'] = (julgados / denominador_m1) * 100 if denominador_m1 > 0 else np.nan
    else:
        metas['meta1'] = np.nan

    cols_m2a = ['julgadom2_a', 'dism2_a', 'susm2_a']
    if all(col in df.columns for col in cols_m2a):
        metas['meta2a'] = calcular_meta_generica(df, cols_m2a[0], cols_m2a[1], cols_m2a[2], (1000/9.4))
    else:
        metas['meta2a'] = np.nan
        
    return metas

def calcular_metas_justica_federal(df):
    """Calcula as metas específicas para a Justiça Federal."""
    metas = {}
    cols_m1 = ['julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025']
    if all(col in df.columns for col in cols_m1):
        julgados = pd.to_numeric(df['julgados_2025'], errors='coerce').sum()
        casos_novos = pd.to_numeric(df['casos_novos_2025'], errors='coerce').sum()
        dessobrestados = pd.to_numeric(df['dessobrestados_2025'], errors='coerce').sum()
        suspensos_m1 = pd.to_numeric(df['suspensos_2025'], errors='coerce').sum()
        denominador_m1 = casos_novos + dessobrestados - suspensos_m1
        metas['meta1'] = (julgados / denominador_m1) * 100 if denominador_m1 > 0 else np.nan
    else:
        metas['meta1'] = np.nan

    mapa_metas = {
        'meta2a': (['julgadom2_a', 'dism2_a', 'susm2_a'], (1000/8.5)),
        'meta4a': (['julgadom4_a', 'dism4_a', 'susm4_a'], (1000/7)),
        'meta6': (['julgadom6', 'dism6', 'susm6'], (1000/3.5)),
    }
    
    for meta_nome, (cols, fator) in mapa_metas.items():
        if all(col in df.columns for col in cols):
            metas[meta_nome] = calcular_meta_generica(df, cols[0], cols[1], cols[2], fator)
        else:
            metas[meta_nome] = np.nan
            
    return metas
    
def calcular_metas_justica_militar_estadual(df):
    """Calcula as metas específicas para a Justiça Militar Estadual."""
    metas = {}
    cols_m1 = ['julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025']
    if all(col in df.columns for col in cols_m1):
        julgados = pd.to_numeric(df['julgados_2025'], errors='coerce').sum()
        casos_novos = pd.to_numeric(df['casos_novos_2025'], errors='coerce').sum()
        dessobrestados = pd.to_numeric(df['dessobrestados_2025'], errors='coerce').sum()
        suspensos_m1 = pd.to_numeric(df['suspensos_2025'], errors='coerce').sum()
        denominador_m1 = casos_novos + dessobrestados - suspensos_m1
        metas['meta1'] = (julgados / denominador_m1) * 100 if denominador_m1 > 0 else np.nan
    else:
        metas['meta1'] = np.nan
        
    mapa_metas = {
        'meta2a': (['julgadom2_a', 'dism2_a', 'susm2_a'], (1000/9)),
        'meta4a': (['julgadom4_a', 'dism4_a', 'susm4_a'], (1000/9.5)),
    }

    for meta_nome, (cols, fator) in mapa_metas.items():
        if all(col in df.columns for col in cols):
            metas[meta_nome] = calcular_meta_generica(df, cols[0], cols[1], cols[2], fator)
        else:
            metas[meta_nome] = np.nan
            
    return metas

def calcular_metas_justica_eleitoral(df):
    """Calcula as metas específicas para a Justiça Eleitoral."""
    metas = {}
    cols_m1 = ['julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025']
    if all(col in df.columns for col in cols_m1):
        julgados = pd.to_numeric(df['julgados_2025'], errors='coerce').sum()
        casos_novos = pd.to_numeric(df['casos_novos_2025'], errors='coerce').sum()
        dessobrestados = pd.to_numeric(df['dessobrestados_2025'], errors='coerce').sum()
        suspensos_m1 = pd.to_numeric(df['suspensos_2025'], errors='coerce').sum()
        denominador_m1 = casos_novos + dessobrestados - suspensos_m1
        metas['meta1'] = (julgados / denominador_m1) * 100 if denominador_m1 > 0 else np.nan
    else:
        metas['meta1'] = np.nan
        
    mapa_metas = {
        'meta2a': (['julgadom2_a', 'dism2_a', 'susm2_a'], (1000/7)),
        'meta4a': (['julgadom4_a', 'dism4_a', 'susm4_a'], (1000/9)),
        'meta4b': (['julgadom4_b', 'dism4_b', 'susm4_b'], (1000/5)),
    }
    
    for meta_nome, (cols, fator) in mapa_metas.items():
        if all(col in df.columns for col in cols):
            metas[meta_nome] = calcular_meta_generica(df, cols[0], cols[1], cols[2], fator)
        else:
            metas[meta_nome] = np.nan
            
    return metas

def calcular_metas_tribunais_superiores(df):
    """Calcula as metas específicas para os Tribunais Superiores (TST, STJ)."""
    metas = {}
    cols_m1 = ['julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025']
    if all(col in df.columns for col in cols_m1):
        julgados = pd.to_numeric(df['julgados_2025'], errors='coerce').sum()
        casos_novos = pd.to_numeric(df['casos_novos_2025'], errors='coerce').sum()
        dessobrestados = pd.to_numeric(df['dessobrestados_2025'], errors='coerce').sum()
        suspensos_m1 = pd.to_numeric(df['suspensos_2025'], errors='coerce').sum()
        denominador_m1 = casos_novos + dessobrestados - suspensos_m1
        # CORREÇÃO: Usando 'denominador_m1' em vez de 'denominator_m1'
        metas['meta1'] = (julgados / denominador_m1) * 100 if denominador_m1 > 0 else np.nan
    else:
        metas['meta1'] = np.nan

    return metas

# ==============================================================================
# FUNÇÕES PRINCIPAIS DO PROCESSO ETL
# ==============================================================================

def extrair_e_consolidar_dados(caminho_arquivos):
    """Lê todos os CSVs de um padrão, concatena e salva em Consolidado.csv."""
    print("Iniciando a Etapa 1: Extração e Consolidação dos dados...")
    lista_arquivos = glob.glob(caminho_arquivos)
    if not lista_arquivos:
        print(f"Nenhum arquivo CSV encontrado no padrão '{caminho_arquivos}'. Verifique o diretório.")
        return None

    lista_dfs = []
    for arquivo in lista_arquivos:
        try:
            df = pd.read_csv(arquivo, sep=',', low_memory=False)
            lista_dfs.append(df)
        except Exception as e:
            print(f"Erro ao ler o arquivo {arquivo}: {e}")

    if not lista_dfs:
        print("Nenhum arquivo CSV pôde ser lido com sucesso.")
        return None

    df_consolidado = pd.concat(lista_dfs, ignore_index=True)
    df_consolidado.to_csv('Consolidado.csv', index=False, sep=';', decimal=',')
    print("Arquivo 'Consolidado.csv' gerado com sucesso.")
    return df_consolidado

def transformar_e_carregar_dados(df_consolidado):
    """Calcula as metas para cada tribunal e salva em ResumoMetas.csv."""
    if df_consolidado is None:
        return None
    print("\nIniciando a Etapa 2: Transformação e Cálculo das Metas...")
    
    resultados_finais = []
    
    mapa_calculo = {
        'Justiça Estadual': calcular_metas_justica_estadual,
        'Justiça do Trabalho': calcular_metas_justica_trabalho,
        'Justiça Federal': calcular_metas_justica_federal,
        'Justiça Militar Estadual': calcular_metas_justica_militar_estadual,
        'Justiça Eleitoral': calcular_metas_justica_eleitoral,
        'Tribunais Superiores': calcular_metas_tribunais_superiores,
    }

    tribunais = df_consolidado.groupby('sigla_tribunal')
    
    for nome_tribunal, df_tribunal in tribunais:
        ramo_justica = df_tribunal['ramo_justica'].iloc[0]
        resultado_tribunal = {'sigla_tribunal': nome_tribunal, 'ramo_justica': ramo_justica}
        funcao_calculo = mapa_calculo.get(ramo_justica)
        
        if funcao_calculo:
            metas_calculadas = funcao_calculo(df_tribunal)
            resultado_tribunal.update(metas_calculadas)
        
        resultados_finais.append(resultado_tribunal)

    df_resumo = pd.DataFrame(resultados_finais)
    df_resumo = df_resumo.fillna('NA')
    df_resumo.to_csv('ResumoMetas.csv', index=False, sep=';', decimal=',')
    print("Arquivo 'ResumoMetas.csv' gerado com sucesso.")
    return df_resumo

def gerar_grafico(df_resumo):
    """Gera um gráfico de barras para comparar a Meta 1 entre os tribunais."""
    if df_resumo is None or df_resumo.empty:
        print("Não há dados para gerar o gráfico.")
        return

    print("\nIniciando a Etapa 3: Geração do Gráfico...")
    
    df_grafico = df_resumo[df_resumo['meta1'] != 'NA'].copy()
    df_grafico['meta1'] = pd.to_numeric(df_grafico['meta1'])
    df_grafico = df_grafico.sort_values('meta1', ascending=False)

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(18, 10))
    
    bars = ax.bar(df_grafico['sigla_tribunal'], df_grafico['meta1'], color='cornflowerblue')

    ax.set_ylabel('Índice de Desempenho (%)', fontsize=12)
    ax.set_title('Desempenho da Meta 1 por Tribunal', fontsize=16, weight='bold', pad=20)
    ax.tick_params(axis='x', rotation=90)
    
    ax.axhline(100, color='crimson', linestyle='--', linewidth=2, label='Meta (100%)')
    ax.legend()
    
    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2.0, yval + 1, f'{yval:.1f}', ha='center', va='bottom', fontsize=8, rotation=90)

    plt.ylim(0, max(df_grafico['meta1'].max() * 1.1, 110))
    plt.tight_layout()
    
    caminho_grafico = 'desempenho_metas.png'
    plt.savefig(caminho_grafico)
    print(f"Gráfico salvo com sucesso em '{caminho_grafico}'.")

# ==============================================================================
# EXECUÇÃO PRINCIPAL
# ==============================================================================

if __name__ == '__main__':
    start_time = time.time()
    
    caminho_csvs = os.path.join('Dados', 'teste_*.csv')
    
    df_consolidado = extrair_e_consolidar_dados(caminho_csvs)
    
    if df_consolidado is not None:
        df_resumo_metas = transformar_e_carregar_dados(df_consolidado)
        gerar_grafico(df_resumo_metas)
    
    end_time = time.time()
    tempo_total = end_time - start_time
    
    print(f"\nProcesso concluído em {tempo_total:.2f} segundos.")
