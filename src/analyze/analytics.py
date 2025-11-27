"""
PASO 3: PROCESAMIENTO ANALÍTICO
Calcula KPIs y métricas agregadas
"""
import pandas as pd
from config import PROCESSED_DIR, ANALYTICS_DIR

def calcular_kpis():
    """Calcula KPIs principales a partir de datos limpios"""
    print("="*60)
    print("PASO 3: PROCESAMIENTO ANALÍTICO (KPIs)")
    print("="*60)
    
    # Leer datos limpios
    input_file = PROCESSED_DIR / 'unified_data.csv'
    if not input_file.exists():
        raise FileNotFoundError(f"No existe {input_file}. Ejecuta 2_transform.py primero.")
    
    df = pd.read_csv(input_file)
    print(f"\nDatos cargados: {len(df)} filas")
    print(f"Columnas: {list(df.columns)}\n")
    
    kpis = {}
    
    # KPI 1: Promedio por País
    print("Calculando KPI 1: Promedios por País...")
    kpi1 = df.groupby('country_code').agg({
        'country_name': 'first',
        **{col: 'mean' for col in df.columns if col not in ['country_code', 'country_name', 'year']}
    }).round(2).reset_index()
    
    kpi1.to_csv(ANALYTICS_DIR / 'kpi_promedios_pais.csv', index=False)
    print(f"  ✓ Guardado: {len(kpi1)} países")
    kpis['promedios_pais'] = kpi1
    
    # KPI 2: Evolución Temporal
    print("Calculando KPI 2: Evolución Temporal...")
    kpi2 = df.groupby('year').agg({
        col: 'mean' for col in df.columns if col not in ['country_code', 'country_name', 'year']
    }).round(2).reset_index()
    
    kpi2.to_csv(ANALYTICS_DIR / 'kpi_evolucion_temporal.csv', index=False)
    print(f"  ✓ Guardado: {len(kpi2)} años")
    kpis['evolucion_temporal'] = kpi2
    
    # KPI 3: Datos por País y Año
    print("Calculando KPI 3: Matriz País-Año...")
    kpi3 = df.copy()
    kpi3.to_csv(ANALYTICS_DIR / 'kpi_pais_año.csv', index=False)
    print(f"  ✓ Guardado: {len(kpi3)} registros")
    kpis['pais_año'] = kpi3
    
    # KPI 4: Rankings (si hay columnas numéricas)
    print("Calculando KPI 4: Rankings...")
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    numeric_cols = [col for col in numeric_cols if col != 'year']
    
    if len(numeric_cols) > 0:
        # Usar último año disponible
        ultimo_año = df['year'].max()
        df_ultimo = df[df['year'] == ultimo_año].copy()
        
        kpi4 = df_ultimo[['country_code', 'country_name'] + list(numeric_cols)].copy()
        
        # Calcular rankings para cada indicador
        for col in numeric_cols:
            if col in kpi4.columns:
                # Determinar si menor es mejor o mayor es mejor
                if any(word in col.lower() for word in ['poverty', 'unemployment', 'dropout', 'gini']):
                    # Menor es mejor
                    kpi4[f'{col}_rank'] = kpi4[col].rank(ascending=True)
                else:
                    # Mayor es mejor
                    kpi4[f'{col}_rank'] = kpi4[col].rank(ascending=False)
        
        kpi4.to_csv(ANALYTICS_DIR / 'kpi_rankings.csv', index=False)
        print(f"  ✓ Guardado: rankings de {len(numeric_cols)} indicadores")
        kpis['rankings'] = kpi4
    
    # KPI 5: Comparación Regional
    print("Calculando KPI 5: Comparación Regional...")
    kpi5 = df.copy()
    
    # Calcular promedios regionales por año
    for col in numeric_cols:
        if col in df.columns:
            regional_avg = df.groupby('year')[col].transform('mean')
            kpi5[f'{col}_regional_avg'] = regional_avg
            kpi5[f'{col}_vs_regional'] = (df[col] - regional_avg).round(2)
    
    kpi5.to_csv(ANALYTICS_DIR / 'kpi_comparacion_regional.csv', index=False)
    print(f"  ✓ Guardado: comparaciones regionales")
    kpis['comparacion_regional'] = kpi5
    
    # Resumen
    print("\n" + "="*60)
    print("KPIs CALCULADOS")
    print("="*60)
    for nombre, df_kpi in kpis.items():
        print(f"  {nombre}: {len(df_kpi)} filas, {len(df_kpi.columns)} columnas")
    
    return kpis

if __name__ == '__main__':
    kpis = calcular_kpis()
    print("\n" + "="*60)
    print("PROCESAMIENTO ANALÍTICO COMPLETADO")
    print("="*60)
    print(f"Archivos guardados en: {ANALYTICS_DIR}")