"""
PASO 4: CARGA A POSTGRESQL
Carga datos procesados y KPIs a la base de datos
"""
import pandas as pd
from sqlalchemy import create_engine, text
from config import DATABASE_URL, PROCESSED_DIR, ANALYTICS_DIR
import time

def cargar_a_postgres():
    """Carga todos los CSVs a PostgreSQL"""
    print("="*60)
    print("PASO 4: CARGA A POSTGRESQL")
    print("="*60)
    
    # Conectar a PostgreSQL
    print(f"\nConectando a PostgreSQL...")
    print(f"  URL: {DATABASE_URL.split('@')[1]}")  # Ocultar password
    
    try:
        time.sleep(5)
        engine = create_engine(DATABASE_URL)
        
        # Test conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        print("  ✓ Conexión exitosa")
    except Exception as e:
        print(f"\n✗ ERROR: No se pudo conectar a PostgreSQL")
        print(f"  {e}")
        print("\nVerifica que:")
        print("  1. PostgreSQL esté corriendo")
        print("  2. La base de datos exista: createdb -U postgres iadb_data")
        print("  3. Las credenciales en .env sean correctas")
        raise
    
    # Cargar datos procesados
    print("\nCargando datos procesados...")
    unified_file = PROCESSED_DIR / 'unified_data.csv'
    
    if unified_file.exists():
        df_unified = pd.read_csv(unified_file)
        df_unified.to_sql('unified_data', engine, if_exists='replace', index=False)
        print(f"  ✓ Tabla 'unified_data': {len(df_unified)} filas")
    else:
        print("  ⚠ No se encontró unified_data.csv")
    
    # Cargar KPIs
    print("\nCargando KPIs...")
    archivos_kpi = list(ANALYTICS_DIR.glob('kpi_*.csv'))
    
    if not archivos_kpi:
        print("  ⚠ No se encontraron archivos KPI")
    else:
        for archivo in archivos_kpi:
            nombre_tabla = archivo.stem  # nombre sin extensión
            df_kpi = pd.read_csv(archivo)
            df_kpi.to_sql(nombre_tabla, engine, if_exists='replace', index=False)
            print(f"  ✓ Tabla '{nombre_tabla}': {len(df_kpi)} filas")
    
    # Crear índices
    print("\nCreando índices...")
    try:
        with engine.connect() as conn:
            # Índice en unified_data
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_unified_country 
                ON unified_data(country_code)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_unified_year 
                ON unified_data(year)
            """))
            conn.commit()
        print("  ✓ Índices creados")
    except Exception as e:
        print(f"  ⚠ Error creando índices: {e}")
    
    # Verificar tablas
    print("\nVerificando tablas en base de datos...")
    query = """
        SELECT tablename, 
               pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
        FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY tablename
    """
    
    try:
        df_tablas = pd.read_sql(query, engine)
        print("\nTablas en la base de datos:")
        for _, row in df_tablas.iterrows():
            print(f"  - {row['tablename']} ({row['size']})")
    except Exception as e:
        print(f"  ⚠ No se pudo verificar tablas: {e}")
    
    # Contar registros
    print("\nConteo de registros:")
    try:
        with engine.connect() as conn:
            # unified_data
            result = conn.execute(text("SELECT COUNT(*) FROM unified_data"))
            count = result.scalar()
            print(f"  unified_data: {count:,} registros")
            
            # KPIs
            for archivo in archivos_kpi:
                tabla = archivo.stem
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {tabla}"))
                    count = result.scalar()
                    print(f"  {tabla}: {count:,} registros")
                except:
                    pass
    except Exception as e:
        print(f"  ⚠ Error contando registros: {e}")
    
    print("\n✓ Carga completada exitosamente")
    return True

if __name__ == '__main__':
    cargar_a_postgres()
    print("\n" + "="*60)
    print("CARGA A POSTGRESQL COMPLETADA")
    print("="*60)
    print("\nAhora puedes conectar Power BI a PostgreSQL:")
    print("  Servidor: localhost")
    print("  Base de datos: iadb_data")
    print("  Tablas: unified_data, kpi_*")