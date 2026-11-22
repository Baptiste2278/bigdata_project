export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export S3_ENDPOINT_URL=$(aws configure get endpoint_url --profile 'default' || echo "https://$AWS_S3_ENDPOINT")
export BRONZE_PREFIX=bronze/openflights
export SOURCE_BASE_URL=https://raw.githubusercontent.com/jpatokal/openflights/1db9191f2cd626088b5846eef15cb22961e42ac8/data
export POLARIS_URI=http://polaris:8181
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export AWS_REGION=us-east-1
export TRINO_HOST=trino TRINO_PORT=8080 TRINO_USER="$KUBERNETES_NAMESPACE"
export ICEBERG_PACKAGE=org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0
