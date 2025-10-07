import pandas as pd
import os
from datetime import datetime

# -----------------------------
# 1. Cargar el archivo CSV
# -----------------------------
file_path = r"C:\Users\analistaaplicacion\Documents\Web_scraping\Scraping\Tesis\rmcab_reportes.csv"
df = pd.read_csv(file_path, sep=";")

# -----------------------------
# 2. Crear carpeta de resultados (si no existe)
# -----------------------------
output_dir = r"C:\Users\analistaaplicacion\Documents\Web_scraping\Scraping\Tesis\Resultados_Tesis"
os.makedirs(output_dir, exist_ok=True)

# -----------------------------
# 3. Generar nombre dinámico con fecha y hora
# -----------------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # formato AAAAMMDD_HHMMSS
output_filename = f"serie_tiempo_estaciones_{timestamp}.csv"
output_path = os.path.join(output_dir, output_filename)

# -----------------------------
# 4. Limpiar columnas innecesarias
# -----------------------------
df = df.drop(columns=["Unnamed: 0", "Máx", "Mín"], errors="ignore")

# -----------------------------
# 5. Detectar columnas de hora (1 al 24)
# -----------------------------
columnas_horas = [str(i) for i in range(1, 25) if str(i) in df.columns]

# -----------------------------
# 6. Crear columna combinada parámetro + unidad
# -----------------------------
# Evitar errores por nombre con tilde o mayúsculas
df.columns = df.columns.str.strip().str.lower()
if "parámetros" in df.columns:
    param_col = "parámetros"
else:
    param_col = "parametros"

df["parametro_unidad"] = df[param_col] + " (" + df["unidades"] + ")"

# -----------------------------
# 7. Transformar formato ancho → largo
# -----------------------------
df_long = df.melt(
    id_vars=["fecha", "estacion", "parametro_unidad"],
    value_vars=columnas_horas,
    var_name="hora",
    value_name="valor"
)

# -----------------------------
# 8. Crear columna datetime
# -----------------------------
df_long["hora"] = df_long["hora"].astype(int) - 1
df_long["fechahora"] = pd.to_datetime(df_long["fecha"]) + pd.to_timedelta(df_long["hora"], unit="h")

# -----------------------------
# 9. Pivotear: cada parámetro como columna
# -----------------------------
df_pivot = df_long.pivot_table(
    index=["estacion", "fechahora"],
    columns="parametro_unidad",
    values="valor"
).reset_index()

# -----------------------------
# 10. Ordenar y exportar
# -----------------------------
df_pivot = df_pivot.sort_values(["estacion", "fechahora"])
df_pivot.to_csv(output_path, index=False, encoding="utf-8-sig")

print(f"✅ Archivo exportado con éxito:\n{output_path}")
