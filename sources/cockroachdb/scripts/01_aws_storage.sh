#!/usr/bin/env bash

set -u

# Get git root first to check for existing credentials
GIT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$GIT_ROOT" ]; then
    echo "❌ Not in a git repository"
    exit 1
fi

COCKROACH_DIR="$GIT_ROOT/sources/cockroachdb"
ENV_DIR="$COCKROACH_DIR/.env"
JSON_FILE="$ENV_DIR/cockroachdb_cdc_aws.json"

# Global associative array for credentials (Python-style dictionary in bash)

# Default configuration
BUCKET_PREFIX="cockroachdb-cdc"

# #############################################################################
# AWS Cloud

AWS_INIT

# read or default credentials
declare -A credentials
if [ -f "$JSON_FILE" ]; then
    json_to_associative_array credentials "$JSON_FILE"
fi

# Single timestamp for consistent resource naming (all resources in this run share same suffix)
credentials[timestamp]=${credentials[timestamp]:-$(date +%s)}

# S3 bucket names must be globally unique and lowercase
credentials[s3_bucket]=${credentials[s3_bucket]:-${BUCKET_PREFIX}-${credentials[timestamp]}}
credentials[s3_region]=${credentials[s3_region]:-$AWS_REGION}

# create S3 bucket
if ! AWS s3api head-bucket --bucket "${credentials[s3_bucket]}"; then
    # Create bucket with appropriate location constraint
    if [[ "${credentials[s3_region]}" == "us-east-1" ]]; then
        DB_EXIT_ON_ERROR="PRINT_EXIT" AWS s3api create-bucket \
            --bucket "${credentials[s3_bucket]}" \
            --region "${credentials[s3_region]}"
    else
        DB_EXIT_ON_ERROR="PRINT_EXIT" AWS s3api create-bucket \
            --bucket "${credentials[s3_bucket]}" \
            --region "${credentials[s3_region]}" \
            --create-bucket-configuration LocationConstraint="${credentials[s3_region]}"
    fi
    
    # Enable versioning (recommended for CDC)
    DB_EXIT_ON_ERROR="PRINT_EXIT" AWS s3api put-bucket-versioning \
        --bucket "${credentials[s3_bucket]}" \
        --versioning-configuration Status=Enabled
    
    # Block public access
    DB_EXIT_ON_ERROR="PRINT_EXIT" AWS s3api put-public-access-block \
        --bucket "${credentials[s3_bucket]}" \
        --public-access-block-configuration \
            BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    
    associative_array_to_json_file credentials "$JSON_FILE"
fi

# IAM role for Databricks access (if using Unity Catalog with IAM roles)
credentials[iam_role_name]=${credentials[iam_role_name]:-cockroachdb-cdc-databricks-role-${credentials[timestamp]}}

# Create IAM role trust policy (allows Databricks to assume the role)
if ! AWS iam get-role --role-name "${credentials[iam_role_name]}"; then
    # Get Databricks account ID for trust policy (user should set this)
    DATABRICKS_ACCOUNT_ID=${DATABRICKS_ACCOUNT_ID:-"414351767826"}  # Default Databricks AWS account
    
    trust_policy=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::${DATABRICKS_ACCOUNT_ID}:root"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "${DATABRICKS_EXTERNAL_ID:-}"
        }
      }
    }
  ]
}
EOF
)
    
    DB_EXIT_ON_ERROR="PRINT_EXIT" AWS iam create-role \
        --role-name "${credentials[iam_role_name]}" \
        --assume-role-policy-document "$trust_policy" \
        --description "IAM role for Databricks Unity Catalog to access CockroachDB CDC data"
    
    # Get role ARN
    DB_EXIT_ON_ERROR="PRINT_EXIT" AWS iam get-role --role-name "${credentials[iam_role_name]}"
    credentials[iam_role_arn]=$(jq -r '.Role.Arn' /tmp/aws_stdout.$$)
    
    # Create and attach S3 access policy
    policy_document=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::${credentials[s3_bucket]}",
        "arn:aws:s3:::${credentials[s3_bucket]}/*"
      ]
    }
  ]
}
EOF
)
    
    credentials[iam_policy_name]="cockroachdb-cdc-s3-access-${credentials[timestamp]}"
    DB_EXIT_ON_ERROR="PRINT_EXIT" AWS iam create-policy \
        --policy-name "${credentials[iam_policy_name]}" \
        --policy-document "$policy_document" \
        --description "S3 access policy for CockroachDB CDC data"
    
    credentials[iam_policy_arn]=$(jq -r '.Policy.Arn' /tmp/aws_stdout.$$)
    
    # Attach policy to role
    DB_EXIT_ON_ERROR="PRINT_EXIT" AWS iam attach-role-policy \
        --role-name "${credentials[iam_role_name]}" \
        --policy-arn "${credentials[iam_policy_arn]}"
    
    associative_array_to_json_file credentials "$JSON_FILE"
fi

# Get IAM role ARN if not already set
if [[ -z "${credentials[iam_role_arn]:-}" ]]; then
    DB_EXIT_ON_ERROR="PRINT_EXIT" AWS iam get-role --role-name "${credentials[iam_role_name]}"
    credentials[iam_role_arn]=$(jq -r '.Role.Arn' /tmp/aws_stdout.$$)
fi

# Get AWS access credentials for CockroachDB changefeed (use IAM user or temporary credentials)
# Note: For production, use IAM roles. For testing, you can use access keys.
if [[ -z "${credentials[aws_access_key_id]:-}" ]] && [[ -n "${AWS_ACCESS_KEY_ID:-}" ]]; then
    credentials[aws_access_key_id]="${AWS_ACCESS_KEY_ID}"
    credentials[aws_secret_access_key]="${AWS_SECRET_ACCESS_KEY}"
fi

# build URLs
credentials[changefeed_uri]="s3://${credentials[s3_bucket]}?AWS_ACCESS_KEY_ID=${credentials[aws_access_key_id]}&AWS_SECRET_ACCESS_KEY=${credentials[aws_secret_access_key]}&AWS_REGION=${credentials[s3_region]}"
credentials[s3_base_url]="s3://${credentials[s3_bucket]}"
credentials[s3_parquet_url]="${credentials[s3_base_url]}/parquet-cdc"
credentials[s3_json_url]="${credentials[s3_base_url]}/json-cdc"

# save to JSON
associative_array_to_json_file credentials "$JSON_FILE"

# #############################################################################
# Unity Catalog Setup
#
# This creates:
# - Unity Catalog storage credential (using IAM role)
# - External locations for Parquet and JSON CDC data
# - Permissions for the current Databricks user
#
# Resource names include timestamp suffix for association with AWS resources
#
# Requirements:
# - Databricks CLI installed and configured
# - Unity Catalog enabled in your workspace
# - Account admin or metastore admin privileges
#
# If you lack these requirements:
# - Create storage credential manually via Databricks UI
# - Use the notebook with embedded credentials
# #############################################################################

# Initialize Databricks CLI and get current user
DBX_INIT

# Configuration - use timestamp suffix to associate with AWS resources
credentials[unity_catalog__storage_credential_name]="cockroachdb_cdc_storage_credential_${credentials[timestamp]}"
credentials[unity_catalog__parquet_location_name]="cockroachdb_cdc_parquet_${credentials[timestamp]}"
credentials[unity_catalog__json_location_name]="cockroachdb_cdc_json_${credentials[timestamp]}"

if ! DBX storage-credentials get "${credentials[unity_catalog__storage_credential_name]}"; then    
    storage_cred_json=$(cat <<EOF
{
  "name": "${credentials[unity_catalog__storage_credential_name]}",
  "comment": "CockroachDB CDC changefeeds",
  "aws_iam_role": {
    "role_arn": "${credentials[iam_role_arn]}"
  },
  "read_only": false,
  "skip_validation": false
}
EOF
)
    DB_EXIT_ON_ERROR="PRINT_EXIT" DBX storage-credentials create --json "$storage_cred_json"
fi

if ! DBX external-locations get "${credentials[unity_catalog__parquet_location_name]}"; then
    DB_EXIT_ON_ERROR="PRINT_EXIT" DBX external-locations create \
        --name "${credentials[unity_catalog__parquet_location_name]}" \
        --url "${credentials[s3_parquet_url]}" \
        --credential-name "${credentials[unity_catalog__storage_credential_name]}" \
        --comment "CockroachDB CDC Parquet" \
        --read-only false \
        --skip-validation false
fi

if ! DBX external-locations get "${credentials[unity_catalog__json_location_name]}"; then
    DB_EXIT_ON_ERROR="PRINT_EXIT" DBX external-locations create \
        --name "${credentials[unity_catalog__json_location_name]}" \
        --url "${credentials[s3_json_url]}" \
        --credential-name "${credentials[unity_catalog__storage_credential_name]}" \
        --comment "CockroachDB CDC JSON" \
        --read-only false \
        --skip-validation false
fi

for location in "${credentials[unity_catalog__parquet_location_name]}" "${credentials[unity_catalog__json_location_name]}"; do
    DB_EXIT_ON_ERROR="PRINT_EXIT" DBX grants update \
        --securable-type EXTERNAL_LOCATION \
        --name "$location" \
        --principal "$DBX_USERNAME" \
        --changes '[{"add": ["READ_FILES"]}]'
done

# Save all credentials to JSON
associative_array_to_json_file credentials "$JSON_FILE"







