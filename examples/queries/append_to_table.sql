select 
    result
from executable(
    'ch-iceberg table-function iceberg-append-static-table --table-location=s3://test01/t01#aws_access_key_id=minio&aws_secret_access_key=minio123&aws_region=us-east-1&aws_endpoint_url=http://localhost:9001&aws_allow_http=true&aws_virtual_hosted_style_request=false',
    ArrowStream, 
    'result String',
    (
        select 
            *
        from generateRandom($SQL$
            timestamp DateTime64(6, 'UTC'), 
            text String, 
            scrore Float64
        $SQL$) 
        limit 10000000
    ),
    settings 
        stderr_reaction='log', 
        check_exit_code=true,
        command_read_timeout=100000
)
settings output_format_arrow_string_as_string=0