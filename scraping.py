import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from io import StringIO

# ==============================
# Configuración de fechas
# ==============================
start_date = datetime(2025, 10, 7)
end_date = datetime.today()

# ==============================
# Scraping con requests
# ==============================
all_data = []
current_date = start_date

while current_date <= end_date:
    date_str = current_date.strftime("%Y-%m-%d")
    url = f"http://rmcab.ambientebogota.gov.co/Report/HourlyReports?id=1&UserDateString={date_str}"
    
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        
        soup = BeautifulSoup(r.text, "lxml")
        
        # Extraer títulos (estaciones) y tablas
        titles = [h2.get_text(strip=True) for h2 in soup.find_all("h2")]
        tables = soup.find_all("table")
        
        # Emparejar título con tabla
        for title, table in zip(titles, tables):
            df = pd.read_html(StringIO(str(table)))[0]
            df["Fecha"] = date_str
            df["Estacion"] = title  # ✅ asociar título
            all_data.append(df)
        
        print(f"✅ {date_str} - {len(tables)} tablas descargadas ({len(titles)} títulos)")
    
    except Exception as e:
        print(f"⚠️ Error en {date_str}: {e}")
    
    current_date += timedelta(days=1)

# ==============================
# CONSOLIDAR Y GUARDAR RESULTADOS
# ==============================
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)

    # Limpieza adicional por si hay valores residuales con coma
    for col in final_df.columns:
        if final_df[col].astype(str).str.contains(r'\d', regex=True).any():
            final_df[col] = (
                final_df[col]
                .astype(str)
                .str.replace(".", "", regex=False)   # elimina separador de miles
                .str.replace(",", ".", regex=False)  # convierte coma en punto
            )
            final_df[col] = pd.to_numeric(final_df[col], errors="ignore")


# ==============================
# Guardar resultados
# ==============================
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_csv("rmcab_reportes.csv", index=False, encoding="utf-8-sig")
    print("📁 Archivo guardado como rmcab_reportes.csv")
else:
    print("⚠️ No se extrajo ninguna tabla")
