with 
    values as (
        select iceberg_field_bound_values(
            'type=file&storage_path=s3://test01&aws_access_key_id=minio&aws_secret_access_key=minio123&aws_region=us-east-1&aws_endpoint_url=http://localhost:9001&aws_allow_http=true&aws_virtual_hosted_style_request=false',
            'table01',
            'score'
        ) as res
    )

select 
    arrayMin(res.value[].lower::Array(Float64)),
    arrayMax(res.value[].upper::Array(Float64)),
from values

settings output_format_arrow_string_as_string=0