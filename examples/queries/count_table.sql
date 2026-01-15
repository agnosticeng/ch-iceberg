select
    _file,
    count(*) as rows,
    formatReadableSize(any(_size)) as size
from iceberg('http://localhost:9001/test01/table01', 'minio', 'minio123', settings iceberg_use_version_hint=true)
group by _file
settings 
    input_format_parquet_use_native_reader=1