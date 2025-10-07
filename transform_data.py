import pandas as pd

# -----------------------------
# 1. Cargar el archivo CSV
# -----------------------------
file_path = r"C:\Users\analistaaplicacion\Documents\Web_scraping\Scraping\Tesis\rmcab_reportes.csv"
df = pd.read_csv(file_path)

# -----------------------------
# 2. Normalizar nombres de columnas
# -----------------------------
df.columns = (
    df.columns.str.strip()
    .str.lower()
    .str.replace("á", "a", regex=False)
    .str.replace("é", "e", regex=False)
    .str.replace("í", "i", regex=False)
    .str.replace("ó", "o", regex=False)
    .str.replace("ú", "u", regex=False)
)

# -----------------------------
# 3. Eliminar columnas innecesarias
# -----------------------------
df = df.drop(columns=["unnamed: 0", "max", "min"], errors="ignore")

# -----------------------------
# 4. Detectar columnas de hora (números del 1 al 24)
# -----------------------------
columnas_horas = [col for col in df.columns if col.isdigit() and 1 <= int(col) <= 24]

# -----------------------------
# 5. Crear columna combinada parametro + unidad
# -----------------------------
if "parametros" in df.columns and "unidades" in df.columns:
    df["parametro_unidad"] = df["parametros"] + " (" + df["unidades"] + ")"
else:
    raise KeyError(f"No se encontraron columnas 'parametros' o 'unidades'. Columnas disponibles: {df.columns.tolist()}")

# -----------------------------
# 6. Transformar de formato ancho → largo
# -----------------------------
df_long = df.melt(
    id_vars=["fecha", "estacion", "parametro_unidad"],  # columnas fijas
    value_vars=columnas_horas,                          # columnas con las 24 horas
    var_name="hora",
    value_name="valor"
)

# -----------------------------
# 7. Crear columna datetime con Fecha + Hora
# -----------------------------
df_long["hora"] = df_long["hora"].astype(int) - 1
df_long["fechahora"] = pd.to_datetime(df_long["fecha"], errors="coerce") + pd.to_timedelta(df_long["hora"], unit="h")

# -----------------------------
# 8. Pivotear para que cada parámetro sea una columna
# -----------------------------
df_pivot = df_long.pivot_table(
    index=["estacion", "fechahora"],
    columns="parametro_unidad",
    values="valor"
).reset_index()

# -----------------------------
# 9. Ordenar y limpiar
# -----------------------------
df_pivot = df_pivot.sort_values(["estacion", "fechahora"])
df_pivot = df_pivot.set_index(["estacion", "fechahora"])

# -----------------------------
# 10. Revisar resultado
# -----------------------------
print(df_pivot.head())
print(f"\n✅ Total de filas: {len(df_pivot)}")

# -----------------------------
# 11. Exportar resultado final
# -----------------------------
output_path = r"C:\Users\analistaaplicacion\Documents\Web_scraping\Scraping\Tesis\serie_tiempo_estaciones.csv"
df_pivot.to_csv(output_path, encoding="utf-8-sig")

print(f"📁 Archivo guardado en:\n{output_path}")
