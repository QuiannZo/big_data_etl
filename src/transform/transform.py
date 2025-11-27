"""
PASO 2: TRANSFORMACIÓN Y LIMPIEZA
Lee datos raw, limpia y crea dataset unificado
"""
import pandas as pd
from config import RAW_DIR, PROCESSED_DIR, COUNTRIES_MAP

def limpiar_datos():
    """Lee y limpia todos los CSVs del directorio raw"""
    print("="*60)
    print("PASO 2: TRANSFORMACIÓN Y LIMPIEZA")
    print("="*60)
    
    # Leer todos los CSVs
    archivos = list(RAW_DIR.glob('*.csv'))
    
    if not archivos:
        raise FileNotFoundError("No hay archivos CSV en data/raw/. Ejecuta 1_extract.py primero.")
    
    print(f"\nArchivos encontrados: {len(archivos)}")
    
    dataframes = {}
    for archivo in archivos:
        nombre = archivo.stem
        df = pd.read_csv(archivo)
        print(f"  ✓ {nombre}: {len(df)} filas, {len(df.columns)} columnas")
        dataframes[nombre] = df
    
    # Unificar datasets
    print("\nUnificando datasets...")
    
    if len(dataframes) == 1:
        # Si solo hay un archivo (IADB completo)
        df_unified = list(dataframes.values())[0]
    else:
        # Si hay múltiples archivos (datasets separados)
        # Hacer merge por country_code y year
        df_unified = None
        for nombre, df in dataframes.items():
            if df_unified is None:
                df_unified = df
            else:
                df_unified = df_unified.merge(df, on=['country_code', 'year'], how='outer')
    
    print(f"  Dataset unificado: {len(df_unified)} filas")
    
    # Limpieza
    print("\nLimpiando datos...")
    
    # Eliminar duplicados
    antes = len(df_unified)
    df_unified = df_unified.drop_duplicates(subset=['country_code', 'year'])
    print(f"  Duplicados eliminados: {antes - len(df_unified)}")
    
    # Agregar nombre del país
    if 'country_code' in df_unified.columns:
        df_unified['country_name'] = df_unified['country_code'].map(COUNTRIES_MAP)
        print(f"  ✓ Nombres de países agregados")
    
    # Ordenar
    df_unified = df_unified.sort_values(['country_code', 'year']).reset_index(drop=True)
    
    # Guardar
    output_path = PROCESSED_DIR / 'unified_data.csv'
    df_unified.to_csv(output_path, index=False)
    
    print(f"\n✓ Dataset limpio guardado: {output_path}")
    print(f"  Filas finales: {len(df_unified)}")
    print(f"  Columnas: {len(df_unified.columns)}")
    
    # Estadísticas
    if 'country_code' in df_unified.columns:
        print(f"  Países: {df_unified['country_code'].nunique()}")
    if 'year' in df_unified.columns:
        print(f"  Años: {df_unified['year'].min()} - {df_unified['year'].max()}")
    
    # Mostrar columnas disponibles
    print(f"\nColumnas disponibles:")
    for i, col in enumerate(df_unified.columns, 1):
        print(f"  {i}. {col}")
    
    return df_unified

if __name__ == '__main__':
    df = limpiar_datos()
    print("TRANSFORMACIÓN COMPLETADA")
    print(f"Archivo guardado en: {PROCESSED_DIR / 'unified_data.csv'}")