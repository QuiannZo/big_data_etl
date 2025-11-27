"""
EJECUTAR TODO EL PIPELINE
Ejecuta todos los pasos en orden
"""
from datetime import datetime
import sys

def run_pipeline():
    """Ejecuta el pipeline completo ETL"""
    start = datetime.now()
    
    print("PIPELINE ETL - INDICADORES SOCIALES IADB")
    
    try:
        # Paso 1: Extracción
        print("\n>>> EJECUTANDO PASO 1: EXTRACCIÓN")
        import importlib
        extract = importlib.import_module('src.extract.extract')
        extract.extraer_datos_iadb()
        
        # Paso 2: Transformación
        print("\n>>> EJECUTANDO PASO 2: TRANSFORMACIÓN")
        transform = importlib.import_module('src.transform.transform')
        transform.limpiar_datos()
        
        # Paso 3: Analytics
        print("\n>>> EJECUTANDO PASO 3: ANALYTICS")
        analytics = importlib.import_module('src.analyze.analytics')
        analytics.calcular_kpis()
        
        # Paso 4: Carga
        print("\n>>> EJECUTANDO PASO 4: CARGA A POSTGRESQL")
        load = importlib.import_module('src.load.load')
        load.cargar_a_postgres()

        print("PIPELINE COMPLETADO EXITOSAMENTE")
        
        return True
        
    except Exception as e:
        print(f"\n ERROR EN PIPELINE: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = run_pipeline()
    sys.exit(0 if success else 1)