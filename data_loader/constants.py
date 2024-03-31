import pyarrow as pa

# PIPELINE SETTINGS
RESOLUTION_CONSTANT = 8
N_DASK_PARTITIONS_CONSTANT = 10

# DATA CONSTANT
LATITUDE_CONSTANT = 'latitude'
LONGITUDE_CONSTANT = 'longitude'
TIME_CONSTANT = 'time'
TP_CONSTANT = 'tp'
TP_MM_CONSTANT = 'tp_mm'
H3_INDEX_CONSTANT = 'h3_index'

# PROCESSED COLUMNS
DATE_CONSTANT = 'date'
TOTAL_DAILY_PRECIPITATIONS_MM_CONSTANT = 'total_daily_precipitation_mm'


# PARQUET SCHEMA DEFINITION (possibly in production can be a separated file)
RAW_ERA5_PARQUET_SCHEMA_CONSTANT = pa.schema([
    (LATITUDE_CONSTANT, pa.float64()),
    (TIME_CONSTANT, pa.timestamp('s')),
    (LONGITUDE_CONSTANT, pa.float64()),
    (TP_CONSTANT, pa.float64()),
    (TP_MM_CONSTANT, pa.float64()),
    (H3_INDEX_CONSTANT, pa.string())
])


PROCESSED_ERA5_PARQUET_SCHEMA_CONSTANT = pa.schema([
    (DATE_CONSTANT, pa.date32()),
    (LATITUDE_CONSTANT, pa.float64()),
    (LONGITUDE_CONSTANT, pa.float64()),
    (H3_INDEX_CONSTANT, pa.string()),
    (TOTAL_DAILY_PRECIPITATIONS_MM_CONSTANT, pa.float64())
])
