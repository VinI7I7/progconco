import pandas as pd
import os
import glob
import numpy as np
import matplotlib.pyplot as plt
import time
import multiprocessing
from collections import defaultdict

def safe_sum(df, column_name):
    """Soma uma coluna de forma segura, retornando 0 se a coluna não existir."""
    if column_name in df.columns:
        return pd.to_numeric(df[column_name], errors='coerce').fillna(0).sum()
    return 0

def calculate_performance(numerator, denominator):
    """Calcula a performance, tratando a divisão por zero."""
    if denominator == 0:
        return 0.0
    return numerator / denominator

def apply_formula(df, result_dict, meta_name, num_col, den_cols, multiplier, den_type='sub'):
    """Aplica uma fórmula de cálculo e armazena o resultado."""
    numerator = safe_sum(df, num_col)
    if den_type == 'add':
        denominator = safe_sum(df, den_cols[0]) + safe_sum(df, den_cols[1]) - safe_sum(df, den_cols[2])
    else:
        denominator = safe_sum(df, den_cols[0]) - safe_sum(df, den_cols[1])
    performance = calculate_performance(numerator, denominator)
    result_dict[meta_name] = performance * multiplier

def calcular_metas_estadual(df_tribunal):
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
    results = {'sigla_tribunal': df_tribunal['sigla_tribunal'].iloc[0]}
    apply_formula(df_tribunal, results, 'Meta 1', 'julgados_2025', ['casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025'], 100, den_type='add')
    apply_formula(df_tribunal, results, 'Meta 2A', 'julgm2_a', ['distm2_a', 'suspm2_a'], 1000 / 7.0)
    apply_formula(df_tribunal, results, 'Meta 2B', 'julgm2_b', ['distm2_b', 'suspm2_b'], 1000 / 9.9)
    apply_formula(df_tribunal, results, 'Meta 2ANT', 'julgm2_ant', ['distm2_ant', 'suspm2_ant'], 100)
    apply_formula(df_tribunal, results, 'Meta 4A', 'julgm4_a', ['distm4_a', 'suspm4_a'], 1000 / 9)
    apply_formula(df_tribunal, results, 'Meta 4B', 'julgm4_b', ['distm4_b', 'suspm4_b'], 1000 / 5)
    return results

def calcular_metas_trabalho(df_tribunal):
    results = {'sigla_tribunal': df_tribunal['sigla_tribunal'].iloc[0]}
    apply_formula(df_tribunal, results, 'Meta 1', 'julgados_2025', ['casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025'], 100, den_type='add')
    apply_formula(df_tribunal, results, 'Meta 2A', 'julgm2_a', ['distm2_a', 'suspm2_a'], 1000 / 9.4)
    apply_formula(df_tribunal, results, 'Meta 2ANT', 'julgm2_ant', ['distm2_ant', 'suspm2_ant'], 100)
    return results

def calcular_metas_federal(df_tribunal):
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
    results = {'sigla_tribunal': df_tribunal['sigla_tribunal'].iloc[0]}
    apply_formula(df_tribunal, results, 'Meta 1', 'julgados_2025', ['casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025'], 100, den_type='add')
    apply_formula(df_tribunal, results, 'Meta 2A', 'julgm2_a', ['distm2_a', 'suspm2_a'], 1000 / 9.5)
    apply_formula(df_tribunal, results, 'Meta 2B', 'julgm2_b', ['distm2_b', 'suspm2_b'], 1000 / 9.9)
    apply_formula(df_tribunal, results, 'Meta 2ANT', 'julgm2_ant', ['distm2_ant', 'suspm2_ant'], 100)
    apply_formula(df_tribunal, results, 'Meta 4A', 'julgm4_a', ['distm4_a', 'suspm4_a'], 1000 / 9.5)
    apply_formula(df_tribunal, results, 'Meta 4B', 'julgm4_b', ['distm4_b', 'suspm4_b'], 1000 / 9.9)
    return results

def calcular_metas_militar_estadual(df_tribunal):
    results = {'sigla_tribunal': df_tribunal['sigla_tribunal'].iloc[0]}
    apply_formula(df_tribunal, results, 'Meta 1', 'julgados_2025', ['casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025'], 100, den_type='add')
    apply_formula(df_tribunal, results, 'Meta 2A', 'julgm2_a', ['distm2_a', 'suspm2_a'], 1000 / 9)
    apply_formula(df_tribunal, results, 'Meta 2B', 'julgm2_b', ['distm2_b', 'suspm2_b'], 1000 / 9.5)
    apply_formula(df_tribunal, results, 'Meta 2ANT', 'julgm2_ant', ['distm2_ant', 'suspm2_ant'], 100)
    apply_formula(df_tribunal, results, 'Meta 4A', 'julgm4_a', ['distm4_a', 'suspm4_a'], 1000 / 9.5)
    apply_formula(df_tribunal, results, 'Meta 4B', 'julgm4_b', ['distm4_b', 'suspm4_b'], 1000 / 9.9)
    return results

def calcular_metas_tst(df_tribunal):
    results = {'sigla_tribunal': df_tribunal['sigla_tribunal'].iloc[0]}
    apply_formula(df_tribunal, results, 'Meta 1', 'julgados_2025', ['casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025'], 100, den_type='add')
    apply_formula(df_tribunal, results, 'Meta 2A', 'julgm2_a', ['distm2_a', 'suspm2_a'], 1000 / 9.5)
    apply_formula(df_tribunal, results, 'Meta 2B', 'julgm2_b', ['distm2_b', 'suspm2_b'], 1000 / 9.9)
    apply_formula(df_tribunal, results, 'Meta 2ANT', 'julgm2_ant', ['distm2_ant', 'suspm2_ant'], 100)
    return results

def calcular_metas_stj(df_tribunal):
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

# --- DICIONÁRIO GLOBAL DE FUNÇÕES ---
FUNCOES_CALCULO = {
    'Justiça Estadual': calcular_metas_estadual, 'Justiça Eleitoral': calcular_metas_eleitoral,
    'Justiça do Trabalho': calcular_metas_trabalho, 'Justiça Federal': calcular_metas_federal,
    'Justiça Militar da União': calcular_metas_militar_uniao, 'Justiça Militar Estadual': calcular_metas_militar_estadual,
    'Superior Tribunal de Justiça': calcular_metas_stj, 'Tribunal Superior do Trabalho': calcular_metas_tst
}

# --- NOVA FUNÇÃO "WORKER" OTIMIZADA ---
def processar_arquivos_do_tribunal(tarefa):
    sigla_tribunal, lista_arquivos = tarefa
    try:
        df_list = [pd.read_csv(file, sep=',', encoding='utf-8', engine='python') for file in lista_arquivos]
        if not df_list: return None, None
        df_tribunal = pd.concat(df_list, ignore_index=True)
    except Exception as e:
        print(f"Erro ao ler arquivos para o tribunal {sigla_tribunal}: {e}")
        return None, None
    ramo = df_tribunal['ramo_justica'].iloc[0]
    funcao_calculo = None
    if ramo == 'Tribunais Superiores':
        if sigla_tribunal == 'STJ': funcao_calculo = FUNCOES_CALCULO.get('Superior Tribunal de Justiça')
        elif sigla_tribunal == 'TST': funcao_calculo = FUNCOES_CALCULO.get('Tribunal Superior do Trabalho')
    else:
        funcao_calculo = FUNCOES_CALCULO.get(ramo)
    if funcao_calculo:
        print(f"  - Processando: {sigla_tribunal} (Lendo {len(lista_arquivos)} arquivo(s))")
        resultados_dict = funcao_calculo(df_tribunal)
        return resultados_dict, df_tribunal
    else:
        print(f"    - Aviso: Nenhuma função de cálculo definida para '{ramo}'. Tribunal '{sigla_tribunal}' ignorado.")
        return None, None

def main():
    start_time = time.time()
    
    data_path = 'Dados/'
    if not os.path.exists(data_path) or not os.listdir(data_path):
         print(f"Erro: A pasta '{data_path}' não foi encontrada ou está vazia.")
         return

    print("Passo 1: Mapeando arquivos para cada tribunal...")
    all_files = glob.glob(os.path.join(data_path, "*.csv"))
    tarefas_por_tribunal = defaultdict(list)
    for f in all_files:
        nome_base = os.path.basename(f)
        sigla = nome_base.replace('teste_', '').replace('.csv', '')
        tarefas_por_tribunal[sigla].append(f)
    lista_de_tarefas = list(tarefas_por_tribunal.items())

    print(f"{len(lista_de_tarefas)} tribunais encontrados para processar.")
    print("\nPasso 2: Executando leitura e cálculo em paralelo...")
    
    all_results = []
    lista_dfs_consolidados = []
    try:
        with multiprocessing.Pool(processes=os.cpu_count()) as pool:
            resultados_processamento = pool.map(processar_arquivos_do_tribunal, lista_de_tarefas)
        for res_dict, res_df in resultados_processamento:
            if res_dict: all_results.append(res_dict)
            if res_df is not None: lista_dfs_consolidados.append(res_df)
    except Exception as e:
        print(f"Ocorreu um erro durante o processamento paralelo: {e}")

    print("Transformação concluída.")

    print("\nPasso 3: Gerando arquivos de saída...")
    if lista_dfs_consolidados:
        df_consolidado = pd.concat(lista_dfs_consolidados, ignore_index=True)
        df_consolidado.to_csv('Consolidado.csv', index=False, sep=';', encoding='utf-8-sig')
        print("O arquivo 'Consolidado.csv' foi criado com sucesso.")
    else:
        print("Aviso: Nenhum dado foi lido, arquivo 'Consolidado.csv' não gerado.")

    if all_results:
        df_resumo = pd.DataFrame(all_results)
        all_meta_columns = [
            'sigla_tribunal', 'Meta 1', 'Meta 2A', 'Meta 2B', 'Meta 2C', 'Meta 2ANT',
            'Meta 4A', 'Meta 4B', 'Meta 6', 'Meta 7A', 'Meta 7B', 'Meta 8',
            'Meta 8A', 'Meta 8B', 'Meta 10', 'Meta 10A', 'Meta 10B'
        ]
        df_resumo = df_resumo.reindex(columns=all_meta_columns)
        

        colunas_de_metas = [col for col in df_resumo.columns if 'Meta' in col]
        df_resumo['Desempenho Geral'] = df_resumo[colunas_de_metas].mean(axis=1, skipna=True)

        all_display_columns = all_meta_columns + ['Desempenho Geral']
        df_resumo = df_resumo.reindex(columns=all_display_columns)

        for col in df_resumo.columns:
            if col != 'sigla_tribunal':
                df_resumo[col] = df_resumo[col].astype(object)
        
        df_resumo.fillna('NA', inplace=True)
        
        df_resumo.to_csv('Resumo Metas.CSV', sep=';', encoding='utf-8-sig', index=False)
        print("O arquivo 'Resumo Metas.CSV' foi criado com sucesso.")
    else:
        print("Aviso: Nenhum resultado foi calculado. O arquivo 'Resumo Metas.CSV' não será gerado.")
        df_resumo = pd.DataFrame()

    print("\nPasso 4: Gerando o gráfico de comparação...")
    if not df_resumo.empty and 'Desempenho Geral' in df_resumo.columns:
        plot_data = df_resumo[['sigla_tribunal', 'Desempenho Geral']].copy()
        plot_data['Desempenho Geral'] = pd.to_numeric(plot_data['Desempenho Geral'], errors='coerce').fillna(0)
        plot_data = plot_data[plot_data['Desempenho Geral'] > 0]
        plot_data.sort_values('Desempenho Geral', inplace=True)

        plt.style.use('seaborn-v0_8-whitegrid')
        fig, ax = plt.subplots(figsize=(14, max(8, len(plot_data) * 0.4)))
        
        bars = ax.barh(plot_data['sigla_tribunal'], plot_data['Desempenho Geral'], color='rebeccapurple')
        ax.set_title('Desempenho Geral por Tribunal', fontsize=18, weight='bold', pad=20)
        ax.set_xlabel('Índice de Desempenho Geral (Média das Metas)', fontsize=12)
        ax.set_ylabel('Tribunal', fontsize=12)
        
        ax.bar_label(bars, fmt='%.2f', padding=5, fontsize=10, color='dimgray')
        
        if not plot_data.empty:
            ax.set_xlim(right=plot_data['Desempenho Geral'].max() * 1.18)
        
        ax.tick_params(axis='x', labelsize=10)
        ax.tick_params(axis='y', labelsize=10)
        ax.grid(axis='x', linestyle='--', alpha=0.7)
        fig.tight_layout()
        
        graph_filename = 'comparativo_desempenho_geral.png' 
        plt.savefig(graph_filename, dpi=150)
        print(f"O gráfico '{graph_filename}' foi criado com sucesso.")
    else:
        print("Não foi possível gerar o gráfico, pois não há dados válidos de Desempenho Geral.")
        
    end_time = time.time()
    print(f"\nExecução OTIMIZADA PARALELA concluída em {end_time - start_time:.4f} segundos.")

if __name__ == '__main__':
    main()