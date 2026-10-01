import boto3

# Masukkan Access Key & Secret Key DigitalOcean Spaces Anda
DO_ACCESS_KEY = "DO006AXTN8CYW4XVRM6N"
DO_SECRET_KEY = "twlQMzYjf1OT+1kNQMgQFnk8nrO9MO6FjZIlmYGCeJg"
session = boto3.session.Session()
client = session.client(
    's3',
    region_name='sgp1',
    endpoint_url='https://sgp1.digitaloceanspaces.com',
    aws_access_key_id=DO_ACCESS_KEY,
    aws_secret_access_key=DO_SECRET_KEY
)

bucket_name = "scola-school-archives"

try:
    # Mengambil daftar file di dalam bucket
    response = client.list_objects_v2(Bucket=bucket_name)
    
    if 'Contents' not in response:
        print("Bucket kosong / tidak ada file.")
    else:
        print(f"{'NAMA FILE / KEY':<50} | {'STORAGE CLASS':<15}")
        print("-" * 70)
        
        for item in response['Contents']:
            key = item['Key']
            # Ambil StorageClass (Default adalah STANDARD jika tidak tercantum)
            storage_class = item.get('StorageClass', 'STANDARD')
            print(f"{key:<50} | {storage_class:<15}")

except Exception as e:
    prin