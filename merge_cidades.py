
# ------------------------------
# Bloco: importação de bibliotecas necessárias
# ------------------------------
import pandas as pd
import glob
import os
import sys
import unicodedata

# ------------------------------
# Função utilitária: normalizar nome de coluna
# remove acentos, converte para minúscula, substitui caracteres não alfanuméricos por underscore
# ------------------------------
def normalize_colname(col: str) -> str:
    if not isinstance(col, str):
        return col
    # remove acentos
    nfkd = unicodedata.normalize("NFKD", col)
    without_accents = "".join([c for c in nfkd if not unicodedata.combining(c)])
    # minúsculas e caracteres válidos
    lowered = without_accents.lower().strip()
    # substituir espaços e outros caracteres por underscore
    cleaned = []
    for ch in lowered:
        if ch.isalnum():
            cleaned.append(ch)
        else:
            cleaned.append("_")
    # colapsar underscores múltiplos
    cleaned_s = "".join(cleaned)
    while "__" in cleaned_s:
        cleaned_s = cleaned_s.replace("__", "_")
    return cleaned_s.strip("_")

def main():
    # ------------------------------
    # Bloco: definir pastas de entrada e saída
    # ------------------------------
    folder = "dados_cidades"
    out_folder = os.path.join("output", "dados_processados")
    output_file = os.path.join(out_folder, "merged_cidades.csv")

    # ------------------------------
    # Bloco: checar existência da pasta de entrada e listar arquivos CSV
    # ------------------------------
    if not os.path.isdir(folder):
        print(f"A pasta '{folder}' não foi encontrada. Verifique o caminho.")
        sys.exit(1)

    csv_files = glob.glob(os.path.join(folder, "*.csv"))
    if not csv_files:
        print(f"Nenhum arquivo .csv encontrado em '{folder}'.")
        sys.exit(1)

    # ------------------------------
    # Bloco: ler todos os CSVs em uma lista de DataFrames
    # tenta parsear a coluna 'data' se existir
    # ------------------------------
    dfs = []
    for f in csv_files:
        try:
            df = pd.read_csv(f, parse_dates=["data"], dayfirst=True)
        except Exception:
            df = pd.read_csv(f)
        # salva arquivo de origem para rastreio (opcional)
        df["__source_file"] = os.path.basename(f)
        dfs.append(df)

    # ------------------------------
    # Bloco: concatenar todos os DataFrames em um único DataFrame
    # ------------------------------
    combined = pd.concat(dfs, ignore_index=True, sort=False)

    # ------------------------------
    # Bloco: normalizar nomes de colunas e criar mapeamento original->normalizado
    # ------------------------------
    original_cols = list(combined.columns)
    normalized_cols = [normalize_colname(c) if isinstance(c, str) else c for c in original_cols]
    # renomear no DataFrame (temporariamente) para trabalhar com nomes normalizados
    rename_map = dict(zip(original_cols, normalized_cols))
    combined = combined.rename(columns=rename_map)

    # ------------------------------
    # Bloco: identificar colunas alvo (com base nos nomes normalizados)
    # suportamos variações como "temperatura_maxima", "temperatura_maxima_absoluta",
    # "precipitacao", "temperatura_minima" etc.
    # ------------------------------
    # nomes alvo canônicos
    target_bases = {
        "precipitacao": None,
        "temperatura_maxima": None,
        "temperatura_maxima_absoluta": None,
        "temperatura_minima": None,
        "temperatura_minima_absoluta": None,
    }
    # para cada coluna existente, veja se corresponde a um dos alvos (prefixo/igual)
    for col in combined.columns:
        if not isinstance(col, str):
            continue
        for base in target_bases.keys():
            if col == base or col.startswith(base + "_") or col.endswith("_" + base) or base in col:
                # se já mapeado, preferimos igualdade exata; caso contrário associa
                # prioriza mapeamento exato
                if target_bases[base] is None:
                    target_bases[base] = col
                else:
                    # se encontrar exato, substitui
                    if col == base:
                        target_bases[base] = col

    # Mostrar quais colunas foram encontradas (útil para debug)
    print("Mapeamento de colunas alvo detectadas:")
    for k, v in target_bases.items():
        print(f"  {k} -> {v}")

    # ------------------------------
    # Bloco: converter as colunas numéricas alvo para float (coerção de erros para NaN)
    # e arredondar para 1 casa decimal
    # ------------------------------
    numeric_targets_to_round = [
        "precipitacao",
        "temperatura_maxima",
        "temperatura_maxima_absoluta",
        "temperatura_minima",
    ]
    for base in numeric_targets_to_round:
        col = target_bases.get(base)
        if col and col in combined.columns:
            combined[col] = pd.to_numeric(combined[col], errors="coerce")
            combined[col] = combined[col].round(1)  # arredonda numericamente para 1 casa

    # ------------------------------
    # Bloco: formatar estas colunas para "até 1 casa decimal"
    # 12.0 -> "12", 12.3 -> "12.3", NaN -> ""
    # ------------------------------
    def format_until_one_decimal(v):
        if pd.isna(v):
            return ""
        try:
            fv = float(v)
        except Exception:
            return str(v)
        if fv.is_integer():
            return str(int(fv))
        return f"{fv:.1f}"

    for base in numeric_targets_to_round:
        col = target_bases.get(base)
        if col and col in combined.columns:
            combined[col] = combined[col].apply(format_until_one_decimal)

    # ------------------------------
    # Bloco: opcional — ordenar por cidade e data se existirem (usando nomes normalizados)
    # ------------------------------
    cidade_col = None
    data_col = None
    # procurar colunas no DataFrame normalizado
    for c in combined.columns:
        if c == "cidade" or "cidade" in c:
            cidade_col = c
            break
    for c in combined.columns:
        if c == "data" or "data" in c:
            data_col = c
            break

    if cidade_col and data_col:
        try:
            combined = combined.sort_values(by=[cidade_col, data_col]).reset_index(drop=True)
        except Exception:
            pass

    # ------------------------------
    # Bloco: criar pasta de saída e salvar CSV final
    # ------------------------------
    os.makedirs(out_folder, exist_ok=True)
    try:
        # salvamos com index=False; já formatamos as colunas alvo como strings "até 1 decimal"
        combined.to_csv(output_file, index=False)
        print(f"Arquivo combinado salvo em: {output_file}")
    except Exception as e:
        print("Erro ao salvar o arquivo de saída:", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
