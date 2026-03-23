#!/bin/bash

# Script to move all files from S3 folder prefixes
# into partitions based on a fixed timestamp path
# Region: us-east-1
# Version: 6.1 - Process all files in each folder (LOAD*.parquet and others)

# AWS region
AWS_REGION="us-east-1"

# AWS account ID (must match the account where the bronze bucket was created)
AWS_ACCOUNT_ID="086997587178"

# AWS CLI profile (WSL uses ~/.aws in Linux — not the same as Windows unless you share it)
AWS_PROFILE="${AWS_PROFILE:-iac_admin}"

# Base S3 location (trailing slash optional; stripped below)
BASE_LOCATION="s3://datalake-1-bronze-us-east-1-${AWS_ACCOUNT_ID}-sandbox-dlh/commercial/erp/public/"

# Target partition path (YYYY/MM/DD/HH)
PARTITION_PATH="2026/01/01/00"

echo "Starting move of all files from folder prefixes..."
echo "AWS region: $AWS_REGION"
echo "AWS account ID: $AWS_ACCOUNT_ID"
echo "AWS CLI profile: $AWS_PROFILE"
echo "Base location: $BASE_LOCATION"
echo "Target partition: $PARTITION_PATH"

# Strip trailing slash from base location if present
BASE_LOCATION=${BASE_LOCATION%/}

# Bucket from URI (errors below refer to this name)
S3_BUCKET=$(echo "$BASE_LOCATION" | sed -E 's|^s3://([^/]+).*|\1|')

# Why this can fail even if you "can see buckets":
# - Console "see bucket" often uses broad console permissions; CLI needs s3:ListBucket on THIS bucket.
# - s3:ListAllMyBuckets (list names) is NOT enough to run aws s3 ls s3://bucket/prefix/
# - Wrong profile in WSL (~/.aws/credentials) vs where you tested (Windows / another user)
# - Wrong AWS_ACCOUNT_ID → bucket name does not exist in the account (404)
# - Bucket policy / SCP / explicit Deny on ListBucket for your principal
if ! aws_err=$(aws s3api head-bucket --bucket "$S3_BUCKET" --region "$AWS_REGION" --profile "$AWS_PROFILE" 2>&1); then
    echo "ERROR: Cannot access bucket '$S3_BUCKET'."
    echo "AWS CLI: $aws_err"
    echo ""
    echo "Verify identity matches this script's account id:"
    echo "  aws sts get-caller-identity --profile $AWS_PROFILE"
    echo "List this bucket (same call the script uses):"
    echo "  aws s3 ls \"s3://$S3_BUCKET/commercial/erp/public/\" --region $AWS_REGION --profile $AWS_PROFILE"
    exit 1
fi

echo "Listing folder prefixes under base location..."

# List all folder prefixes under the base location
folders=$(aws s3 ls "$BASE_LOCATION/" --region "$AWS_REGION" --profile "$AWS_PROFILE" | grep "PRE" | awk '{print $2}' | sed 's|/$||')

if [ -z "$folders" ]; then
    echo "No folder prefixes found under base location."
    exit 0
fi

# Counters for statistics
processed_folders=0
skipped_folders=0
moved_files=0

# For each folder, process files (e.g. LOAD*.parquet and others)
for folder in $folders; do
    echo "Processing folder: $folder"
    
    # Full path to the folder prefix
    folder_path="$BASE_LOCATION/$folder"
    
    echo "  Folder path: $folder_path"
    
    # Check for objects in the folder (exclude subfolder markers)
    files_in_folder=$(aws s3 ls "$folder_path/" --region $AWS_REGION --profile "$AWS_PROFILE" | grep -v "PRE" | awk '{$1=$2=$3=""; print substr($0,4)}' | sed 's/^[ \t]*//')
    
    if [ -z "$files_in_folder" ]; then
        echo "  No files in folder. Skipping..."
        ((skipped_folders++))
        continue
    fi
    
    # Target path using the fixed partition
    target_partition="$folder_path/$PARTITION_PATH"
    
    echo "  Target path: $target_partition"
    
    # Create target partition prefix if it does not exist
    aws s3 ls "$target_partition/" --region $AWS_REGION --profile "$AWS_PROFILE" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "  Creating target prefix: $target_partition"
        
        # Parse bucket and key prefix from S3 URI
        bucket=$(echo $target_partition | sed -E 's|s3://([^/]+)/.*|\1|')
        prefix=$(echo $target_partition | sed -E 's|s3://[^/]+/(.*)|\1|')
        
        # Create placeholder object for the prefix (S3 has no real directories)
        aws s3api put-object --bucket "$bucket" --key "${prefix}/" --region $AWS_REGION --profile "$AWS_PROFILE" > /dev/null
        
        if [ $? -eq 0 ]; then
            echo "  Target prefix created successfully."
        else
            echo "  ERROR: Failed to create target prefix!"
            continue
        fi
    else
        echo "  Target prefix already exists."
    fi
    
    # Files moved in this folder
    folder_moved_files=0
    
    # For each file, copy then delete source (move)
    for file in $files_in_folder; do
        source_path="$folder_path/$file"
        target_path="$target_partition/$file"
        
        echo "  Moving file: $file"
        echo "    From: $source_path"
        echo "    To: $target_path"
        
        # Skip if destination already exists
        aws s3 ls "$target_path" --region $AWS_REGION --profile "$AWS_PROFILE" > /dev/null 2>&1
        if [ $? -eq 0 ]; then
            echo "    WARNING: File already exists at destination. Skipping..."
            continue
        fi
        
        # Copy into partition
        aws s3 cp "$source_path" "$target_path" --region $AWS_REGION --profile "$AWS_PROFILE"
        
        # On success, remove source
        if [ $? -eq 0 ]; then
            aws s3 rm "$source_path" --region $AWS_REGION --profile "$AWS_PROFILE"
            if [ $? -eq 0 ]; then
                echo "    File moved successfully."
                ((moved_files++))
                ((folder_moved_files++))
            else
                echo "    ERROR: Failed to remove source file!"
                # Remove copy to avoid duplicate / inconsistent state
                aws s3 rm "$target_path" --region $AWS_REGION --profile "$AWS_PROFILE" > /dev/null 2>&1
            fi
        else
            echo "    ERROR: Failed to copy file!"
        fi
    done
    
    if [ $folder_moved_files -gt 0 ]; then
        echo "  Finished folder: $folder ($folder_moved_files file(s) moved)"
        ((processed_folders++))
    else
        echo "  No files were moved from folder: $folder"
        ((skipped_folders++))
    fi
done

# Final statistics
echo ""
echo "File move completed."
echo "Statistics:"
echo "  Folders processed: $processed_folders"
echo "  Folders skipped: $skipped_folders"
echo "  Total files moved: $moved_files"
echo ""
echo "Target partition used: $PARTITION_PATH"
