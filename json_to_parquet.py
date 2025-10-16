import pandas as pd

# Ler JSON
df = pd.read_json("casos_tjmg_jan2025.json")

# Salvar em Parquet
df.to_parquet("casos_tjmg_jan2025.parquet", engine='pyarrow', index=False)
print("Arquivo Parquet salvo com sucesso!")

# Ler arquivo Parquet
df = pd.read_parquet("casos_tjmg_jan2025.parquet", engine='pyarrow')

# Comparando json x Parquet na prática (visual e com tempo)
import pandas as pd
import numpy as np
import time
import os

# ----------------------------------------------------------
# Mostrar tamanho de cada arquivo
print(f" Tamanho json: {os.path.getsize('casos_tjmg_jan2025.json')/1e6:.2f} MB")
print(f" Tamanho Parquet: {os.path.getsize('casos_tjmg_jan2025.parquet')/1e6:.2f} MB\n")

# ----------------------------------------------------------
# Ler ambos e medir tempo
t0 = time.time()
json_df = pd.read_json("casos_tjmg_jan2025.json")
json_time = time.time() - t0

t0 = time.time()
parquet_df = pd.read_parquet("casos_tjmg_jan2025.parquet")
parquet_time = time.time() - t0

print(f" Tempo leitura json: {json_time:.2f} s")
print(f" Tempo leitura Parquet: {parquet_time:.2f} s\n")

# ----------------------------------------------------------
# Ler apenas uma coluna (para ver diferença colunar)
t0 = time.time()
json_valor = pd.read_json("casos_tjmg_jan2025.json")
json_col_time = time.time() - t0

t0 = time.time()
parquet_valor = pd.read_parquet("casos_tjmg_jan2025.parquet")
parquet_col_time = time.time() - t0

print(f"Tempo leitura de 1 coluna (json): {json_col_time:.2f} s")
print(f"Tempo leitura de 1 coluna (Parquet): {parquet_col_time:.2f} s\n")

# ----------------------------------------------------------
# Visualização comparativa simples
import matplotlib.pyplot as plt

tempos = [json_time, parquet_time, json_col_time, parquet_col_time]
labels = ["json (tudo)", "Parquet (tudo)", "json (1 col)", "Parquet (1 col)"]

plt.bar(labels, tempos, color=["#d62728","#2ca02c","#ff7f0e","#1f77b4"])
plt.ylabel("Tempo (segundos)")
plt.title("Comparativo de leitura: json x Parquet")
plt.show()

# ----------------------------------------------------------
# Resumo textual
print("Comparativo final:")
print(f"- Parquet é {(os.path.getsize('casos_tjmg_jan2025.json')/os.path.getsize('casos_tjmg_jan2025.parquet')):.1f}x menor.")
print(f"- Leitura total foi {(json_time/parquet_time):.1f}x mais rápida.")
print(f"- Leitura de 1 coluna foi {(json_col_time/parquet_col_time):.1f}x mais rápida.")
