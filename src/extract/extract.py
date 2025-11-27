"""
PASO 1: EXTRACCIÓN DE DATOS
Extrae datos de la API del IADB o genera datos de ejemplo
"""
import requests
import pandas as pd
import numpy as np
from config import RAW_DIR, COUNTRIES

def generar_datos_ejemplo():
    """Genera datos sintéticos para demostración"""
    print("\nGenerando datos de ejemplo...")
    
    np.random.seed(42)
    years = list(range(2010, 2024))
    
    # Dataset 1: Pobreza
    data_pobreza = []
    for country in COUNTRIES:
        base = np.random.uniform(15, 45)
        for year in years:
            trend = -0.5 * (year - 2010)
            noise = np.random.normal(0, 2)
            pobreza = max(5, base + trend + noise)
            
            data_pobreza.append({
                'country_code': country,
                'year': year,
                'poverty_rate': round(pobreza, 2),
                'extreme_poverty': round(pobreza * 0.4, 2),
                'gini_index': round(np.random.uniform(0.40, 0.55), 3)
            })
    
    df_pobreza = pd.DataFrame(data_pobreza)
    df_pobreza.to_csv(RAW_DIR / 'poverty_data.csv', index=False)
    print(f"✓ Pobreza: {len(df_pobreza)} registros")
    
    # Dataset 2: Educación
    data_edu = []
    for country in COUNTRIES:
        base = np.random.uniform(7, 11)
        for year in years:
            trend = 0.1 * (year - 2010)
            avg_years = base + trend + np.random.normal(0, 0.3)
            
            data_edu.append({
                'country_code': country,
                'year': year,
                'avg_years_education': round(avg_years, 2),
                'primary_attendance': round(np.random.uniform(85, 98), 2),
                'secondary_attendance': round(np.random.uniform(65, 90), 2),
                'dropout_rate': round(np.random.uniform(5, 20), 2)
            })
    
    df_edu = pd.DataFrame(data_edu)
    df_edu.to_csv(RAW_DIR / 'education_data.csv', index=False)
    print(f"✓ Educación: {len(df_edu)} registros")
    
    # Dataset 3: Empleo
    data_emp = []
    for country in COUNTRIES:
        for year in years:
            data_emp.append({
                'country_code': country,
                'year': year,
                'unemployment_rate': round(np.random.uniform(3, 12), 2),
                'employment_rate': round(np.random.uniform(55, 75), 2),
                'informal_rate': round(np.random.uniform(30, 60), 2)
            })
    
    df_emp = pd.DataFrame(data_emp)
    df_emp.to_csv(RAW_DIR / 'employment_data.csv', index=False)
    print(f"✓ Empleo: {len(df_emp)} registros")
    
    # Dataset 4: Infraestructura
    data_infra = []
    for country in COUNTRIES:
        for year in years:
            data_infra.append({
                'country_code': country,
                'year': year,
                'electricity_access': round(np.random.uniform(85, 99), 2),
                'water_access': round(np.random.uniform(80, 98), 2),
                'internet_access': round(np.random.uniform(30, 85), 2)
            })
    
    df_infra = pd.DataFrame(data_infra)
    df_infra.to_csv(RAW_DIR / 'infrastructure_data.csv', index=False)
    print(f"✓ Infraestructura: {len(df_infra)} registros")
    
    return {
        'poverty': df_pobreza,
        'education': df_edu,
        'employment': df_emp,
        'infrastructure': df_infra
    }

def extraer_datos_iadb():
    """
    Intenta extraer datos reales de IADB API
    Si falla, genera datos de ejemplo
    """
    print("="*60)
    print("PASO 1: EXTRACCIÓN DE DATOS")
    print("="*60)
    
    try:
        print("\nIntentando conectar a IADB API...")
        url = "https://data.iadb.org/api/3/action/package_show"
        params = {'id': 'social-indicators-of-latin-america-and-the-caribbean'}
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                print("✓ API conectada")
                resources = data['result'].get('resources', [])
                print(f"  Recursos disponibles: {len(resources)}")
                
                # Intentar descargar primer CSV
                csv_resources = [r for r in resources if r.get('format', '').lower() == 'csv']
                
                if csv_resources:
                    print(f"\n  Descargando primer CSV...")
                    csv_url = csv_resources[0]['url']
                    df = pd.read_csv(csv_url)
                    df.to_csv(RAW_DIR / 'iadb_data.csv', index=False)
                    print(f"  ✓ Descargado: {len(df)} filas")
                    print("\n✓ Datos reales del IADB descargados")
                    return {'iadb': df}
                else:
                    print("  ⚠ No se encontraron archivos CSV")
                    raise Exception("No CSV disponible")
        else:
            raise Exception(f"API respondió con status {response.status_code}")
            
    except Exception as e:
        print(f"\n⚠ No se pudo conectar a IADB API: {e}")
        print("  Generando datos de ejemplo en su lugar...\n")
        return generar_datos_ejemplo()

if __name__ == '__main__':
    datos = extraer_datos_iadb()
    print("\n" + "="*60)
    print("EXTRACCIÓN COMPLETADA")
    print("="*60)
    print(f"Archivos guardados en: {RAW_DIR}")
    print(f"Total de datasets: {len(datos)}")