from hdfs import InsecureClient
from hdfs.util import HdfsError
import json
import time
import sys

# Wait for HDFS to start (increased time)
print("⏳ Waiting for HDFS to initialize...")
time.sleep(30)  # Give HDFS more time to start

# Connect to HDFS
print("🔌 Connecting to HDFS NameNode...")
try:
    hdfs_client = InsecureClient('http://namenode:9870', user='root')
    # Test connection
    hdfs_client.status('/')
    print("✅ Successfully connected to HDFS!")
except Exception as e:
    print(f"❌ Failed to connect to HDFS: {e}")
    print("   Possible reasons:")
    print("   - NameNode is still starting up (wait longer)")
    print("   - Network issue between containers")
    print("   - NameNode failed to start (check logs: docker logs namenode)")
    sys.exit(1)

# Base directory for our data
base_dir = '/user/data'
users_file = f'{base_dir}/users.json'

print("=" * 50)
print("HDFS CRUD Operations Demo")
print("=" * 50)

# Create base directory if it doesn't exist
try:
    hdfs_client.makedirs(base_dir)
    print(f"✅ Created directory: {base_dir}")
except HdfsError:
    print(f"ℹ️  Directory {base_dir} already exists")
except Exception as e:
    print(f"⚠️  Error creating directory: {e}")

# --- CREATE ---
print("\n--- CREATE Operation ---")
user = {"name": "Ali", "age": 22, "city": "Lahore"}

try:
    # Write user data to HDFS as JSON
    with hdfs_client.write(users_file, encoding='utf-8', overwrite=True) as writer:
        json.dump([user], writer)
    print(f"✅ Created user: {user}")
except Exception as e:
    print(f"❌ Error creating user: {e}")
    sys.exit(1)

# --- READ ---
print("\n--- READ Operation ---")
print("Users in HDFS:")
try:
    with hdfs_client.read(users_file, encoding='utf-8') as reader:
        users = json.load(reader)
        for u in users:
            print(f"  {u}")
except Exception as e:
    print(f"❌ Error reading: {e}")

# --- UPDATE ---
print("\n--- UPDATE Operation ---")
try:
    # Read existing data
    with hdfs_client.read(users_file, encoding='utf-8') as reader:
        users = json.load(reader)
    
    # Update Ali's city
    for u in users:
        if u['name'] == 'Ali':
            old_city = u['city']
            u['city'] = 'Karachi'
            print(f"  Updating {u['name']}'s city: {old_city} → Karachi")
    
    # Write back to HDFS
    with hdfs_client.write(users_file, encoding='utf-8', overwrite=True) as writer:
        json.dump(users, writer)
    
    print("✅ Update completed successfully")
    
    # Verify update
    with hdfs_client.read(users_file, encoding='utf-8') as reader:
        users = json.load(reader)
        print(f"  Verified data: {users}")
except Exception as e:
    print(f"❌ Error updating: {e}")

# --- DELETE ---
print("\n--- DELETE Operation ---")
try:
    # Read existing data
    with hdfs_client.read(users_file, encoding='utf-8') as reader:
        users = json.load(reader)
    
    original_count = len(users)
    
    # Remove Ali from list
    users = [u for u in users if u['name'] != 'Ali']
    
    # Write back to HDFS
    with hdfs_client.write(users_file, encoding='utf-8', overwrite=True) as writer:
        json.dump(users, writer)
    
    print(f"✅ Deleted user Ali")
    print(f"  Users before: {original_count}, Users after: {len(users)}")
except Exception as e:
    print(f"❌ Error deleting: {e}")

# --- LIST FILES ---
print("\n--- List Files in HDFS ---")
try:
    files = hdfs_client.list(base_dir)
    print(f"Files in {base_dir}:")
    for f in files:
        file_status = hdfs_client.status(f'{base_dir}/{f}')
        print(f"  - {f} (size: {file_status['length']} bytes)")
except Exception as e:
    print(f"❌ Error listing files: {e}")

print("\n" + "=" * 50)
print("✅ HDFS CRUD operations completed successfully!")
print("=" * 50)
print("\n💡 You can view HDFS Web UI at: http://localhost:9870")