"""
PASO 1: EXTRACCIÓN DE DATOS
Extrae datos de la API del IADB o genera datos de ejemplo
"""
import requests
import pandas as pd
import numpy as np
import json
import traceback
from http import HTTPStatus
from config import RAW_DIR, COUNTRIES, ALLOWED_NAMES, FIELDS, YEARS

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
        csv_resources = get_resources(url, params)
                
        if csv_resources:
            for resource in csv_resources:
                df = get_data_from_api(resource['id'], limit=10000, filters=create_filter(year=2022))
                filename = resource['name'].replace(' ', '').lower()
                df.to_csv(RAW_DIR / f"{filename}.csv", index=False)
            return {'filename': df}
            
    except Exception as e:
        print(f"\nNo se pudo conectar a IADB API: {e} {traceback.extract_tb(e.__traceback__)[-1].lineno}")
        print("  Generando datos de ejemplo en su lugar...\n")
        return generar_datos_ejemplo()
    
def get_resources(url: str, params: dict[str, str]) -> list[dict[str, any]]:
    response = requests.get(url, params=params)

    if response.status_code >= HTTPStatus.OK and response.status_code < HTTPStatus.BAD_REQUEST:
        has_succeeded = response.json()['success']
        if has_succeeded:
            result = response.json()['result']
            resources = []
            for r in result['resources']:
                if r['is_indicator'] \
                   and r['name'] in ALLOWED_NAMES \
                   and r['format'].lower() == 'csv':
                    resources.append({'id': r['id'], 'name': r['name']})
            print(f"Number of resources: {len(resources)}")
            return resources
        else:
            raise Exception("No se encontraron archivos CSV")
    else:
        raise Exception(f"Error accessing API. Code {response.status_code}")

    
def get_data_from_api(resource_id: str, limit: int = 1000, filters: list[dict[str, str]] = []):
    filter_str = parse_filter(filters)
    sort = "year%20desc"

    url = f"https://data.iadb.org/api/action/datastore_search?resource_id={resource_id}&limit={limit}&filters={filter_str}&distinct=True&include_total=False&sort={sort}"
    response = requests.get(url)

    if response.status_code >= HTTPStatus.OK and response.status_code < HTTPStatus.BAD_REQUEST:
        result = response.json().get('result')
        data = pd.DataFrame(result['records'])[FIELDS]
        print(f"Download {resource_id} completed successfully")
        data = data[data['isoalpha3'].isin(COUNTRIES)]
        data = data[data['year'].isin(YEARS)]
        data.rename(columns={'isoalpha3': 'country_code'}, inplace=True)
        return data
    else:
        raise Exception(f"Code {response.status_code}: {response.text}")
    
def parse_filter(filters: list[dict[str, str]] = []) -> str:
    filter_str = "%7B"
    for f in filters:
        key, value = next(iter(f.items()))
        if isinstance(value, str):
            value = f"%22{value}%22"
        filter_str += f"%22{key}%22%3A{value}%2C"
    filter_str = filter_str[:-3]
    filter_str += "%7D"
    return filter_str
    
def create_filter(year: int) -> list[dict[str, str]]:
    filters = [{'education_level': 'Total'}
             , {'ethnicity': 'Total'}
             , {'language': 'Total'}
             , {'disability': 'Total'}
             , {'migration': 'Total'}
             , {'management': 'Total'}
             , {'quintile': 'Total'}]
    return filters

if __name__ == '__main__':
    datos = extraer_datos_iadb()
    print("\n" + "="*60)
    print("EXTRACCIÓN COMPLETADA")
    print("="*60)
    print(f"Archivos guardados en: {RAW_DIR}")
    print(f"Total de datasets: {len(datos)}")