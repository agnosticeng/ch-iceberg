select 
    * 
from iceberg('http://localhost:9001/test01/t01', 'minio', 'minio123', settings iceberg_use_version_hint=true)