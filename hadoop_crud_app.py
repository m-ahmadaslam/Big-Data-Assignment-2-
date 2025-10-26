import time
from hdfs import InsecureClient

HDFS_URL = "http://namenode:9870"
HDFS_USER = "root"
HDFS_DIR = "/user/root"
LOCAL_FILE = "example.txt"

print("⏳ Waiting for HDFS to be ready...")
time.sleep(20)

try:
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    print(f"✅ Connected to HDFS at {HDFS_URL} as user '{HDFS_USER}'")

    # Ensure directory exists
    client.makedirs(HDFS_DIR)
    print(f"📂 Directory {HDFS_DIR} created or already exists.")

    # --------------------------
    # 🧱 CREATE
    # --------------------------
    with open(LOCAL_FILE, "w") as f:
        f.write("Hello from Dockerized Python and HDFS!\n")
    print("📝 Local file created successfully.")

    client.upload(HDFS_DIR, LOCAL_FILE, overwrite=True)
    print(f"⬆️ Uploaded '{LOCAL_FILE}' to HDFS at {HDFS_DIR}")

    # --------------------------
    # 📖 READ
    # --------------------------
    hdfs_file_path = f"{HDFS_DIR}/{LOCAL_FILE}"
    with client.read(hdfs_file_path, encoding="utf-8") as reader:
        contents = reader.read()
        print(f"📤 Read from HDFS:\n{contents}")

    # --------------------------
    # ✏️ UPDATE
    # --------------------------
    updated_content = contents + "This line was appended from Python update operation.\n"
    with open(LOCAL_FILE, "w") as f:
        f.write(updated_content)
    client.upload(HDFS_DIR, LOCAL_FILE, overwrite=True)
    print("🔁 File updated successfully in HDFS.")

    # Verify update
    with client.read(hdfs_file_path, encoding="utf-8") as reader:
        new_data = reader.read()
        print(f"🆕 Updated HDFS file content:\n{new_data}")

    # --------------------------
    # 🗑️ DELETE
    # --------------------------
    client.delete(hdfs_file_path)
    print(f"🧹 File '{hdfs_file_path}' deleted successfully from HDFS.")

except Exception as e:
    print(f"❌ Error during HDFS CRUD operations: {e}")

# Keep container alive for logs/debug
print("✅ CRUD operations complete. Keeping container alive...")
while True:
    time.sleep(60)
