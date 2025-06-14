# Versao_NP.py
# Autor: Gemini (Aluno Dedicado)
# Data: 14/06/2025
# Descrição: VERSÃO FINAL CORRIGIDA do script NÃO-PARALELO (serial).
#            - Contém todas as correções de leitura de arquivo e de lógica de seleção de tribunais.
#            - Serve como base de comparação de desempenho com a Versão_P.py.

import pandas as pd
import os
import glob
import numpy as np
import matplotlib.pyplot as plt
import time

# --- FUNÇÕES UTILITÁRIAS ---
def safe_sum(df, column_name):
    """
    Soma uma coluna de forma segura, retornando 0 se a coluna não existir no DataFrame.
    Isso torna o script robusto para diferentes arquivos CSV que podem não ter todas as metas.
    """
    if column_name in df.columns:
        return pd.to_numeric(df[column_name], errors='coerce').fillna(0).sum()
    return 0

def calculate_performance(numerator, denominator):
    """Calcula a performance, tratando a divisão por zero para evitar erros."""
    if denominator == 0:
        return 0.0
    return numerator / denominator

def apply_formula(df, result_dict, meta_name, num_col, den_cols, multiplier, den_type='sub'):
    """Função genérica para aplicar uma fórmula de cálculo e armazenar o resultado."""
    numerator = safe_sum(df, num_col)
    if den_type == 'add':
        denominator = safe_sum(df, den_cols[0]) + safe_sum(df, den_cols[1]) - safe_sum(df, den_cols[2])
    else:
        denominator = safe_sum(df, den_cols[0]) - safe_sum(df, den_cols[1])
    performance = calculate_performance(numerator, denominator)
    result_dict[meta_name] = performance * multiplier

# --- FUNÇÕES DE CÁLCULO PARA CADA RAMO DA JUSTIÇA ---
def calcular_metas_estadual(df_tribunal):
    """Calcula todas as metas para a Justiça Estadual."""
    results = {'sigla_tribunal': df_tribunal['sigla_tribunal'].iloc[0]}
    apply_formula(df_tribunal, results, 'Meta 1', 'julgados_2025', ['casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025'], 100, den_type='add')
    apply_formula(df_tribunal, results, 'Meta 2A', 'julgm2_a', ['distm2_a', 'suspm2_a'], 1000 / 8)
    apply_formula(df_tribunal, results, 'Meta 2B', 'julgm2_b', ['distm2_b', 'suspm2_b'], 1000 / 9)
    apply_formula(df_tribunal, results, 'Meta 2C', 'julgm2_c', ['distm2_c', 'suspm2_c'], 1000 / 9.5)
    apply_formula(df_tribunal, results, 'Meta 2ANT', 'julgm2_ant', ['distm2_ant', 'suspm2_ant'], 100)
    apply_formula(df_tribunal, results, 'Meta 4A', 'julgm4_a', ['distm4_a', 'suspm4_a'], 1000 / 6.5)
    apply_formula(df_tribunal, results, 'Meta 4B', 'julgm4_b', ['distm4_b', 'suspm4_b'], 100)
    apply_formula(df_tribunal, results, 'Meta 6', 'julgm6_a', ['distm6_a', 'suspm6_a'], 100)
    apply_formula(df_tribunal, results, 'Meta 7A', 'julgm7_a', ['distm7_a', 'suspm7_a'], 1000 / 5)
    apply_formula(df_tribunal, results, 'Meta 7B', 'julgm7_b', ['distm7_b', 'suspm7_b'], 1000 / 5)
    apply_formula(df_tribunal, results, 'Meta 8A', 'julgm8_a', ['distm8_a', 'suspm8_a'], 1000 / 7.5)
    apply_formula(df_tribunal, results, 'Meta 8B', 'julgm8_b', ['distm8_b', 'suspm8_b'], 1000 / 9)
    apply_formula(df_tribunal, results, 'Meta 10A', 'julgm10_a', ['distm10_a', 'suspm10_a'], 1000 / 9)
    apply_formula(df_tribunal, results, 'Meta 10B', 'julgm10_b', ['distm10_b', 'suspm10_b'], 1000 / 10)
    return results

def calcular_metas_eleitoral(df_tribunal):
    """Calcula todas as metas para a Justiça Eleitoral."""
    results = {'sigla_tribunal': df_tribunal['sigla_tribunal'].iloc[0]}
    apply_formula(df_tribunal, results, 'Meta 1', 'julgados_2025', ['casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025'], 100, den_type='add')
    apply_formula(df_tribunal, results, 'Meta 2A', 'julgm2_a', ['distm2_a', 'suspm2_a'], 1000 / 7.0)
    apply_formula(df_tribunal, results, 'Meta 2B', 'julgm2_b', ['distm2_b', 'suspm2_b'], 1000 / 9.9)
    apply_formula(df_tribunal, results, 'Meta 2ANT', 'julgm2_ant', ['distm2_ant', 'suspm2_ant'], 100)
    apply_formula(df_tribunal, results, 'Meta 4A', 'julgm4_a', ['distm4_a', 'suspm4_a'], 1000 / 9)
    apply_formula(df_tribunal, results, 'Meta 4B', 'julgm4_b', ['distm4_b', 'suspm4_b'], 1000 / 5)
    return results

def calcular_metas_trabalho(df_tribunal):
    """Calcula todas as metas para a Justiça do Trabalho."""
    results = {'sigla_tribunal': df_tribunal['sigla_tribunal'].iloc[0]}
    apply_formula(df_tribunal, results, 'Meta 1', 'julgados_2025', ['casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025'], 100, den_type='add')
    apply_formula(df_tribunal, results, 'Meta 2A', 'julgm2_a', ['distm2_a', 'suspm2_a'], 1000 / 9.4)
    apply_formula(df_tribunal, results, 'Meta 2ANT', 'julgm2_ant', ['distm2_ant', 'suspm2_ant'], 100)
    return results

def calcular_metas_federal(df_tribunal):
    """Calcula todas as metas para a Justiça Federal."""
    results = {'sigla_tribunal': df_tribunal['sigla_tribunal'].iloc[0]}
    apply_formula(df_tribunal, results, 'Meta 1', 'julgados_2025', ['casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025'], 100, den_type='add')
    apply_formula(df_tribunal, results, 'Meta 2A', 'julgm2_a', ['distm2_a', 'suspm2_a'], 1000 / 8.5)
    apply_formula(df_tribunal, results, 'Meta 2B', 'julgm2_b', ['distm2_b', 'suspm2_b'], 100)
    apply_formula(df_tribunal, results, 'Meta 2ANT', 'julgm2_ant', ['distm2_ant', 'suspm2_ant'], 100)
    apply_formula(df_tribunal, results, 'Meta 4A', 'julgm4_a', ['distm4_a', 'suspm4_a'], 1000 / 7)
    apply_formula(df_tribunal, results, 'Meta 4B', 'julgm4_b', ['distm4_b', 'suspm4_b'], 100)
    apply_formula(df_tribunal, results, 'Meta 6', 'julgm6_a', ['distm6_a', 'suspm6_a'], 1000 / 3.5)
    apply_formula(df_tribunal, results, 'Meta 7A', 'julgm7_a', ['distm7_a', 'suspm7_a'], 1000 / 3.5)
    apply_formula(df_tribunal, results, 'Meta 7B', 'julgm7_b', ['distm7_b', 'suspm7_b'], 1000 / 3.5)
    apply_formula(df_tribunal, results, 'Meta 8A', 'julgm8_a', ['distm8_a', 'suspm8_a'], 1000 / 7.5)
    apply_formula(df_tribunal, results, 'Meta 8B', 'julgm8_b', ['distm8_b', 'suspm8_b'], 1000 / 9)
    apply_formula(df_tribunal, results, 'Meta 10A', 'julgm10_a', ['distm10_a', 'suspm10_a'], 100)
    return results

def calcular_metas_militar_uniao(df_tribunal):
    """Calcula todas as metas para a Justiça Militar da União."""
    results = {'sigla_tribunal': df_tribunal['sigla_tribunal'].iloc[0]}
    apply_formula(df_tribunal, results, 'Meta 1', 'julgados_2025', ['casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025'], 100, den_type='add')
    apply_formula(df_tribunal, results, 'Meta 2A', 'julgm2_a', ['distm2_a', 'suspm2_a'], 1000 / 9.5)
    apply_formula(df_tribunal, results, 'Meta 2B', 'julgm2_b', ['distm2_b', 'suspm2_b'], 1000 / 9.9)
    apply_formula(df_tribunal, results, 'Meta 2ANT', 'julgm2_ant', ['distm2_ant', 'suspm2_ant'], 100)
    apply_formula(df_tribunal, results, 'Meta 4A', 'julgm4_a', ['distm4_a', 'suspm4_a'], 1000 / 9.5)
    apply_formula(df_tribunal, results, 'Meta 4B', 'julgm4_b', ['distm4_b', 'suspm4_b'], 1000 / 9.9)
    return results

def calcular_metas_militar_estadual(df_tribunal):
    """Calcula todas as metas para a Justiça Militar Estadual."""
    results = {'sigla_tribunal': df_tribunal['sigla_tribunal'].iloc[0]}
    apply_formula(df_tribunal, results, 'Meta 1', 'julgados_2025', ['casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025'], 100, den_type='add')
    apply_formula(df_tribunal, results, 'Meta 2A', 'julgm2_a', ['distm2_a', 'suspm2_a'], 1000 / 9)
    apply_formula(df_tribunal, results, 'Meta 2B', 'julgm2_b', ['distm2_b', 'suspm2_b'], 1000 / 9.5)
    apply_formula(df_tribunal, results, 'Meta 2ANT', 'julgm2_ant', ['distm2_ant', 'suspm2_ant'], 100)
    apply_formula(df_tribunal, results, 'Meta 4A', 'julgm4_a', ['distm4_a', 'suspm4_a'], 1000 / 9.5)
    apply_formula(df_tribunal, results, 'Meta 4B', 'julgm4_b', ['distm4_b', 'suspm4_b'], 1000 / 9.9)
    return results

def calcular_metas_tst(df_tribunal):
    """Calcula todas as metas para o Tribunal Superior do Trabalho."""
    results = {'sigla_tribunal': df_tribunal['sigla_tribunal'].iloc[0]}
    apply_formula(df_tribunal, results, 'Meta 1', 'julgados_2025', ['casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025'], 100, den_type='add')
    apply_formula(df_tribunal, results, 'Meta 2A', 'julgm2_a', ['distm2_a', 'suspm2_a'], 1000 / 9.5)
    apply_formula(df_tribunal, results, 'Meta 2B', 'julgm2_b', ['distm2_b', 'suspm2_b'], 1000 / 9.9)
    apply_formula(df_tribunal, results, 'Meta 2ANT', 'julgm2_ant', ['distm2_ant', 'suspm2_ant'], 100)
    return results

def calcular_metas_stj(df_tribunal):
    """Calcula todas as metas para o Superior Tribunal de Justiça."""
    results = {'sigla_tribunal': df_tribunal['sigla_tribunal'].iloc[0]}
    apply_formula(df_tribunal, results, 'Meta 1', 'julgados_2025', ['casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025'], 100, den_type='add')
    apply_formula(df_tribunal, results, 'Meta 2ANT', 'julgm2_ant', ['distm2_ant', 'suspm2_ant'], 100)
    apply_formula(df_tribunal, results, 'Meta 4A', 'julgm4_a', ['distm4_a', 'suspm4_a'], 1000 / 9)
    apply_formula(df_tribunal, results, 'Meta 4B', 'julgm4_b', ['distm4_b', 'suspm4_b'], 100)
    apply_formula(df_tribunal, results, 'Meta 6', 'julgm6_a', ['distm6_a', 'suspm6_a'], 1000 / 7.5)
    apply_formula(df_tribunal, results, 'Meta 7A', 'julgm7_a', ['distm7_a', 'suspm7_a'], 1000 / 7.5)
    apply_formula(df_tribunal, results, 'Meta 7B', 'julgm7_b', ['distm7_b', 'suspm7_b'], 1000 / 7.5)
    apply_formula(df_tribunal, results, 'Meta 8', 'julgm8_a', ['distm8_a', 'suspm8_a'], 1000 / 10)
    apply_formula(df_tribunal, results, 'Meta 10', 'julgm10_a', ['distm10_a', 'suspm10_a'], 1000 / 10)
    return results

# --- LÓGICA PRINCIPAL DE EXECUÇÃO ---
def main():
    start_time = time.time()
    
    data_path = 'Dados/'
    if not os.path.exists(data_path) or not os.listdir(data_path):
         print(f"Erro: A pasta '{data_path}' não foi encontrada ou está vazia.")
         print("Por favor, crie a pasta 'data' e coloque seus arquivos CSV dentro dela antes de executar o script.")
         return

    all_files = glob.glob(os.path.join(data_path, "*.csv"))
    
    print("Passo 1: Lendo e consolidando os arquivos CSV...")
    try:
        # Leitura correta com separador de vírgula e codificação UTF-8
        df_list = [pd.read_csv(file, sep=',', encoding='utf-8', engine='python') for file in all_files]
        df_consolidado = pd.concat(df_list, ignore_index=True)
    except Exception as e:
        print(f"Ocorreu um erro ao ler os arquivos CSV: {e}")
        return

    consolidado_filename = 'Consolidado.csv'
    df_consolidado.to_csv(consolidado_filename, index=False, sep=';', encoding='utf-8-sig')
    print(f"O arquivo '{consolidado_filename}' foi criado com sucesso.")

    print("\nPasso 2: Transformando os dados e calculando as metas em modo sequencial...")
    
    # Dicionário de funções de cálculo
    funcoes_calculo = {
        'Justiça Estadual': calcular_metas_estadual,
        'Justiça Eleitoral': calcular_metas_eleitoral,
        'Justiça do Trabalho': calcular_metas_trabalho,
        'Justiça Federal': calcular_metas_federal,
        'Justiça Militar da União': calcular_metas_militar_uniao,
        'Justiça Militar Estadual': calcular_metas_militar_estadual,
        'Superior Tribunal de Justiça': calcular_metas_stj,
        'Tribunal Superior do Trabalho': calcular_metas_tst
    }
    
    tribunais = df_consolidado['sigla_tribunal'].unique()
    all_results = []
    
    # Laço FOR sequencial para processar um tribunal de cada vez
    for sigla in tribunais:
        df_tribunal = df_consolidado[df_consolidado['sigla_tribunal'] == sigla].copy()
        ramo = df_tribunal['ramo_justica'].iloc[0]
        
        funcao_calculo = None
        # Lógica de seleção inteligente para lidar com o ramo "Tribunais Superiores"
        if ramo == 'Tribunais Superiores':
            if sigla == 'STJ':
                funcao_calculo = funcoes_calculo.get('Superior Tribunal de Justiça')
            elif sigla == 'TST':
                funcao_calculo = funcoes_calculo.get('Tribunal Superior do Trabalho')
        else:
            # Mapeamento direto para todos os outros ramos
            funcao_calculo = funcoes_calculo.get(ramo)

        if funcao_calculo:
            print(f"  - Processando: {sigla} (Ramo: {ramo})")
            resultados_tribunal = funcao_calculo(df_tribunal)
            all_results.append(resultados_tribunal)
        else:
            print(f"    - Aviso: Nenhuma função de cálculo definida para o Ramo da Justiça: '{ramo}'. Tribunal '{sigla}' será ignorado.")

    print("Transformação concluída.")

    print("\nPasso 3: Gerando o arquivo 'Resumo Metas.CSV'...")
    
    if all_results:
        df_resumo = pd.DataFrame(all_results)
        df_resumo.rename(columns={'tribunal': 'sigla_tribunal'}, inplace=True)

        all_meta_columns = [
            'sigla_tribunal', 'Meta 1', 'Meta 2A', 'Meta 2B', 'Meta 2C', 'Meta 2ANT',
            'Meta 4A', 'Meta 4B', 'Meta 6', 'Meta 7A', 'Meta 7B', 'Meta 8',
            'Meta 8A', 'Meta 8B', 'Meta 10', 'Meta 10A', 'Meta 10B'
        ]
        df_resumo = df_resumo.reindex(columns=all_meta_columns)
        df_resumo.fillna('NA', inplace=True)
        
        resumo_filename = 'Resumo Metas.CSV'
        df_resumo.to_csv(resumo_filename, sep=';', encoding='utf-8-sig', index=False)
        print(f"O arquivo '{resumo_filename}' foi criado com sucesso.")
    else:
        print("Aviso: Nenhum resultado foi calculado. O arquivo 'Resumo Metas.CSV' não será gerado.")
        df_resumo = pd.DataFrame()

    print("\nPasso 4: Gerando o gráfico de comparação...")
    if not df_resumo.empty and 'Meta 1' in df_resumo.columns:
        plot_data = df_resumo[['sigla_tribunal', 'Meta 1']].copy()
        plot_data['Meta 1'] = pd.to_numeric(plot_data['Meta 1'], errors='coerce').fillna(0)
        plot_data = plot_data[plot_data['Meta 1'] > 0]
        plot_data.sort_values('Meta 1', inplace=True)

        plt.style.use('seaborn-v0_8-whitegrid')
        fig, ax = plt.subplots(figsize=(12, max(8, len(plot_data) * 0.5)))
        
        bars = ax.barh(plot_data['sigla_tribunal'], plot_data['Meta 1'], color='teal')
        ax.set_title('Desempenho da Meta 1 por Tribunal', fontsize=16, weight='bold')
        ax.set_xlabel('Índice de Desempenho (%)', fontsize=12)
        ax.set_ylabel('Tribunal', fontsize=12)
        
        ax.bar_label(bars, fmt='%.2f', padding=3, fontsize=10)
        
        if not plot_data.empty:
            ax.set_xlim(right=plot_data['Meta 1'].max() * 1.15)
        
        plt.tight_layout()
        
        graph_filename = 'comparativo_metas.png'
        plt.savefig(graph_filename)
        print(f"O gráfico '{graph_filename}' foi criado com sucesso.")
    else:
        print("Não foi possível gerar o gráfico, pois não há dados válidos para a 'Meta 1'.")
        
    end_time = time.time()
    print(f"\nExecução NÃO-PARALELA concluída em {end_time - start_time:.4f} segundos.")


if __name__ == '__main__':
    main()