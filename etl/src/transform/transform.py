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
    
    archivos = list(RAW_DIR.glob('*.csv'))
    
    if not archivos:
        raise FileNotFoundError("No hay archivos CSV en data/raw/. Ejecuta 1_extract.py primero.")
    
    df_unified = pd.DataFrame()
    for archivo in archivos:
        df = pd.read_csv(archivo)
        indicator = archivo.stem
    
        df.rename(columns={'value': indicator}, inplace=True)
        if df_unified.empty:
            df_unified = df
        else:
            df_unified = df_unified.merge(
                df, 
                on=['year', 'country_code', 'area', 'sex'],
                how='outer'
            )
    
    df_unified = df_unified.dropna()
    # if len(dataframes) == 1:
    #     # Si solo hay un archivo (IADB completo)
    #     df_unified = list(dataframes.values())[0]
    # else:
    #     # Si hay múltiples archivos (datasets separados)
    #     # Hacer merge por country_code y year
    #     df_unified = None
    #     for nombre, df in dataframes.items():
    #         if df_unified is None:
    #             df_unified = df
    #         else:
    #             # df_unified = df_unified.merge(df, on=['country_code', 'year'], how='outer')
    #             df_unified = df_unified.merge(df, on=['country_code', 'year'], how='outer')
    
    # Eliminar duplicados
    antes = len(df_unified)
    df_unified = df_unified.drop_duplicates(subset=['country_code', 'year', 'area', 'sex'])
    # print(f"  Duplicados eliminados: {antes - len(df_unified)}")
    
    # Agregar nombre del país
    if 'country_code' in df_unified.columns:
        df_unified['country_name'] = df_unified['country_code'].map(COUNTRIES_MAP)
        print(f"  ✓ Nombres de países agregados")
    
    # Ordenar
    # df_unified = df_unified.sort_values(['country_code', 'year']).reset_index(drop=True)
    
    # Guardar
    output_path = PROCESSED_DIR / 'unified_data.csv'
    df_unified.to_csv(output_path, index=False)
    
    print(f"Dataset limpio guardado: {output_path}")
    return df_unified

if __name__ == '__main__':
    df = limpiar_datos()