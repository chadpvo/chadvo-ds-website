import boto3
from botocore.config import Config

# IMPORTANT: Keep these secret! Don't commit to git!
R2_ACCESS_KEY = '066f81b45bafd6ccc7d7996a45056a0e'  # Paste your Access Key ID
R2_SECRET_KEY = '3f5cbee546b553ca3a2ecef10f43c003d2b94c710bb26e62f400ade1ec8f4e2a'  # Paste your Secret Access Key
R2_ENDPOINT = 'https://163cf1341dc78d4065fd61b38b62e386.r2.cloudflarestorage.com'
BUCKET_NAME = 'redfin-data'

# Local file path
LOCAL_FILE = r'C:\personal_projects\chadvo-ds-website\projects\map_viz\data\redfin\raw\redfin_zip_all_states.tsv'
R2_FILE_NAME = 'redfin_zip_all_states.tsv'

# Create S3 client for R2
s3 = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

print(f"Uploading {LOCAL_FILE} to R2...")
print("This will take 10-20 minutes for a 4.35GB file...")

try:
    s3.upload_file(LOCAL_FILE, BUCKET_NAME, R2_FILE_NAME)
    print("\n✅ Upload complete!")
    print(f"File available in your R2 bucket!")
except Exception as e:
    print(f"❌ Error: {e}")