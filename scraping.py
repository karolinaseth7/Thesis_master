import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from io import StringIO
from tqdm import tqdm

# ==============================
# CONFIGURACIÓN DE FECHAS
# ==============================
start_date = datetime(2025, 10, 5)
end_date = datetime.today()

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

            # Conversión de valores con coma a punto (si pandas no lo detectó)
            for col in df.columns:
                if df[col].dtype == "object":
                    df[col] = (
                        df[col]
                        .astype(str)
                        .str.replace(".", "", regex=False)  # quita separadores de miles
                        .str.replace(",", ".", regex=False)  # convierte coma en punto
                    )
                    df[col] = pd.to_numeric(df[col], errors="ignore")

            df["Fecha"] = date_str
            df["Estacion"] = title
            all_data.append(df)

    except Exception as e:
        print(f"⚠️ Error al procesar {date_str}: {e}")

# ==============================
# CONSOLIDAR Y GUARDAR RESULTADOS
# ==============================
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)

    output_path = r"C:\Users\analistaaplicacion\Documents\Web_scraping\Scraping\Tesis\rmcab_reportes.csv"
    final_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"\n✅ Archivo guardado correctamente en:\n{output_path}")
    print(f"Total de filas: {len(final_df)}")
else:
    print("⚠️ No se extrajo ninguna tabla.")
