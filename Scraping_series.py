import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
from tqdm import tqdm
import os
from datetime import datetime

# ==============================
# CONFIGURACIÓN DE FECHAS
# ==============================
start_date = datetime(2023, 10, 10)
end_date = datetime(2024, 10, 10)
# end_date = datetime.today() ##<---- descomentar si es necesario.

# Carpeta de salida
output_dir = r"C:\Users\analistaaplicacion\Documents\Web_scraping\Scraping\Tesis"

# ==============================
# SCRAPING RMCAB
# ==============================
all_data = []

for current_date in tqdm(pd.date_range(start_date, end_date), desc="Descargando datos"):
    date_str = current_date.strftime("%Y-%m-%d")
    url = f"http://rmcab.ambientebogota.gov.co/Report/HourlyReports?id=1&UserDateString={date_str}"

    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        titles = [h2.get_text(strip=True) for h2 in soup.find_all("h2")]
        tables = soup.find_all("table")

        for title, table in zip(titles, tables):
            df = pd.read_html(StringIO(str(table)), decimal=",", thousands=".")[0]

            # Limpieza de encabezados
            df.columns = [str(c).strip().replace("\n", " ") for c in df.columns]

            # Conversión de valores con coma a punto
            for col in df.columns:
                if df[col].dtype == "object":
                    df[col] = (
                        df[col]
                        .astype(str)
                        .str.replace(".", "", regex=False)  # separador de miles
                        .str.replace(",", ".", regex=False)  # coma a punto decimal
                    )
                    # Conversión segura de columnas numéricas (sin usar errors="ignore")
                    for col in df.columns:
                        try:
                            df[col] = pd.to_numeric(df[col])
                        except (ValueError, TypeError):
                            pass  # Ignora columnas que no pueden convertirse


            df["Fecha"] = date_str
            df["Estacion"] = title
            all_data.append(df)

    except Exception as e:
        print(f"⚠️ Error al procesar {date_str}: {e}")

# ==============================
# CONSOLIDAR RESULTADOS
# ==============================
if not all_data:
    raise SystemExit("⚠️ No se extrajo ninguna tabla. El script se detiene aquí.")

final_df = pd.concat(all_data, ignore_index=True)
print(f"\n✅ Datos descargados correctamente. Total de filas: {len(final_df):,}")

# ======================================================
# 4️⃣ NORMALIZAR NOMBRES DE COLUMNAS
# ======================================================
df = final_df.copy()
df.columns = (
    df.columns.str.strip()
    .str.lower()
    .str.replace("á", "a", regex=False)
    .str.replace("é", "e", regex=False)
    .str.replace("í", "i", regex=False)
    .str.replace("ó", "o", regex=False)
    .str.replace("ú", "u", regex=False)
    .str.replace("ñ", "n", regex=False)
)

# ======================================================
# 5️⃣ ELIMINAR COLUMNAS INNECESARIAS
# ======================================================
df = df.drop(columns=["unnamed: 0", "max", "min"], errors="ignore")

# ======================================================
# 6️⃣ DETECTAR COLUMNAS DE HORA (1–24)
# ======================================================
columnas_horas = [col for col in df.columns if col.isdigit() and 1 <= int(col) <= 24]

# ======================================================
# 7️⃣ CREAR COLUMNA COMBINADA “parametro (unidad)”
# ======================================================
param_cols = [c for c in df.columns if "param" in c]
unit_cols = [c for c in df.columns if "unidad" in c or "unit" in c]

if not param_cols or not unit_cols:
    raise KeyError(f"No se encontraron columnas 'parametros' o 'unidades'. Columnas disponibles: {df.columns.tolist()}")

param_col = param_cols[0]
unit_col = unit_cols[0]

df["parametro_unidad"] = df[param_col] + " (" + df[unit_col] + ")"

# ======================================================
# 8️⃣ CONVERTIR DE FORMATO ANCHO → LARGO
# ======================================================
df_long = df.melt(
    id_vars=["fecha", "estacion", "parametro_unidad"],
    value_vars=columnas_horas,
    var_name="hora",
    value_name="valor"
)

# ======================================================
# 9️⃣ LIMPIAR DECIMALES Y CONVERTIR TIPOS
# ======================================================
df_long["valor"] = (
    df_long["valor"]
    .astype(str)
    .str.replace(",", ".", regex=False)
    .str.replace(" ", "", regex=False)
)
df_long["valor"] = pd.to_numeric(df_long["valor"], errors="coerce")

# ======================================================
# 🔟 CREAR COLUMNA FECHA-HORA COMPLETA
# ======================================================
df_long["hora"] = df_long["hora"].astype(int) - 1
df_long["fechahora"] = pd.to_datetime(df_long["fecha"], errors="coerce") + pd.to_timedelta(df_long["hora"], unit="h")

# ======================================================
# 11️⃣ PIVOTEAR PARA FORMATO FINAL
# ======================================================
df_pivot = df_long.pivot_table(
    index=["estacion", "fechahora"],
    columns="parametro_unidad",
    values="valor"
).reset_index()

df_pivot = df_pivot.sort_values(["estacion", "fechahora"])

# ======================================================
# 12️⃣ GUARDAR RESULTADO FINAL
# ======================================================
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f"serie_tiempo_estaciones_{timestamp}.csv"
output_path = os.path.join(output_dir, output_file)
df_pivot.to_csv(output_path, index=False, encoding="utf-8-sig")

print("\n✅ Transformación completada.")
print(f"📊 Total de filas finales: {len(df_pivot):,}")
print(f"📁 Archivo guardado en:\n{output_path}")
