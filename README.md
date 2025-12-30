# Proyecto ETL - Indicadores Sociales IADB

Proyecto de Big Data para análisis de indicadores sociales de América Latina - Educacion.

### 1. Instalar Dependencias
```bash
pip install -r requirements.txt
```

### 2. Configurar PostgreSQL
```bash
# Crear base de datos
createdb -U postgres iadb_data
```

### 3. Configurar .env

Editar el `.env` con tu contraseña de PostgreSQL:
```
DB_PASSWORD=...
```

### 4. Ejecutar Pipeline

**A: Todo junto**
```bash
python run_all.py
```

**B: Paso a paso**
```bash
python 1_extract.py      # Extraer datos
python 2_transform.py    # Limpiar
python 3_analytics.py    # Calcular KPIs
python 4_load.py        # Cargar a PostgreSQL
```

## KPIs Calculados

1. **kpi_promedios_pais** - Promedios históricos por país
2. **kpi_evolucion_temporal** - Tendencias año a año
3. **kpi_pais_año** - Matriz completa país-año
4. **kpi_rankings** - Rankings de países
5. **kpi_comparacion_regional** - País vs promedio regional

## Conectar Power BI

1. Abrir Power BI Desktop
2. Obtener datos → PostgreSQL database
3. Servidor: `localhost`
4. Base de datos: `iadb_data`
5. Importar tablas: `unified_data`, `kpi_*`

---

**Proyecto Big Data - Universidad de Costa Rica**