select iceberg_create_static_table(
    's3://test01/t01#aws_access_key_id=minio&aws_secret_access_key=minio123&aws_region=us-east-1&aws_endpoint_url=http://localhost:9001&aws_allow_http=true&aws_virtual_hosted_style_request=false',
    $JSON$
    {
        "name": "",
        "current-schema-id": 0,
        "schema": {
            "schema-id": 0,
            "type": "struct",
            "fields": [
                {
                    "id": 1,
                    "name": "timestamp",
                    "type": "timestamptz",
                    "required": true
                },
                {
                    "id": 2,
                    "name": "text",
                    "type": "binary",
                    "required": true
                },
                {
                    "id": 3,
                    "name": "score",
                    "type": "double",
                    "required": true
                }
            ]
        }
    }
    $JSON$
)

settings output_format_arrow_string_as_string=0