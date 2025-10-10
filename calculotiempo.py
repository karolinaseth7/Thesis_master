import pandas as pd
import os
from datetime import datetime

# =====================================================
# 1️⃣ Cargar el archivo original descargado del RMCAB
# =====================================================
file_path = r"C:\Users\analistaaplicacion\Documents\Web_scraping\Scraping\Tesis\rmcab_reportes.csv"
df = pd.read_csv(file_path, sep=";", dtype=str)

# =====================================================
# 2️⃣ Crear carpeta de resultados (si no existe)
# =====================================================
output_dir = r"C:\Users\analistaaplicacion\Documents\Web_scraping\Scraping\Tesis\Resultados_Tesis"
os.makedirs(output_dir, exist_ok=True)

# =====================================================
# 3️⃣ Generar nombre único para el archivo de salida
# =====================================================
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_filename = f"serie_tiempo_estaciones_{timestamp}.csv"
output_path = os.path.join(output_dir, output_filename)

# =====================================================
# 4️⃣ Normalizar columnas y limpiar
# =====================================================
# Quitar espacios, pasar a minúsculas y reemplazar tildes
df.columns = (
    df.columns.str.strip()
    .str.lower()
    .str.replace("á", "a")
    .str.replace("é", "e")
    .str.replace("í", "i")
    .str.replace("ó", "o")
    .str.replace("ú", "u")
)

# Eliminar columnas innecesarias
df = df.drop(columns=["unnamed: 0", "max", "min"], errors="ignore")

# =====================================================
# 5️⃣ Detectar columnas que representan horas (1–24)
# =====================================================
columnas_horas = [str(i) for i in range(1, 25) if str(i) in df.columns]

# =====================================================
# 6️⃣ Crear columna combinada “parametro (unidad)”
# =====================================================
if "parametros" in df.columns and "unidades" in df.columns:
    df["parametro_unidad"] = df["parametros"] + " (" + df["unidades"] + ")"
else:
    raise ValueError("No se encontraron columnas 'parametros' o 'unidades' en el archivo CSV")

# =====================================================
# 7️⃣ Convertir de formato ancho → largo
# =====================================================
df_long = df.melt(
    id_vars=["fecha", "estacion", "parametro_unidad"],
    value_vars=columnas_horas,
    var_name="hora",
    value_name="valor"
)

# =====================================================
# 8️⃣ Limpiar y convertir valores numéricos
# =====================================================
# Reemplazar comas por puntos para asegurar formato decimal correcto
df_long["valor"] = (
    df_long["valor"]
    .astype(str)
    .str.replace(",", ".", regex=False)
    .str.replace(" ", "", regex=False)
)

# Convertir a número si es posible
df_long["valor"] = pd.to_numeric(df_long["valor"], errors="coerce")

# =====================================================
# 9️⃣ Crear columna datetime (Fecha + Hora)
# =====================================================
df_long["hora"] = df_long["hora"].astype(int) - 1  # '1' -> 00:00
df_long["fechahora"] = pd.to_datetime(df_long["fecha"], errors="coerce") + pd.to_timedelta(df_long["hora"], unit="h")

# =====================================================
# 🔟 Pivotear: cada parámetro como una columna
# =====================================================
df_pivot = df_long.pivot_table(
    index=["estacion", "fechahora"],
    columns="parametro_unidad",
    values="valor"
).reset_index()

# =====================================================
# 11️⃣ Ordenar y exportar resultados
# =====================================================
df_pivot = df_pivot.sort_values(["estacion", "fechahora"])
df_pivot.to_csv(output_path, index=False, encoding="utf-8-sig")

print("✅ Transformación completada con éxito.")
print(f"📁 Archivo guardado en:\n{output_path}")
print(f"📊 Filas exportadas: {len(df_pivot):,}")
