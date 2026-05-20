import os
import sys
import httpx
from pymongo import MongoClient

def main():
    # Load .env.gateway env variables
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env.gateway")
    mongodb_uri = "mongodb://127.0.0.1:27017"
    mongodb_db = "poe_gateway"
    
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("=", 1)
                if len(parts) == 2:
                    key = parts[0].strip()
                    val = parts[1].strip()
                    if key == "MONGODB_URI":
                        mongodb_uri = val
                    elif key == "MONGODB_DB":
                        mongodb_db = val

    print(f"Connecting to MongoDB: {mongodb_uri} | DB: {mongodb_db}")
    client = MongoClient(mongodb_uri)
    db = client[mongodb_db]
    accounts_col = db["accounts"]
    
    # Query accounts with status: invalid
    invalid_accounts = list(accounts_col.find({"status": "invalid"}))
    total_invalid = len(invalid_accounts)
    print(f"Found {total_invalid} accounts with status 'invalid'.")
    
    if total_invalid == 0:
        print("No invalid accounts found to refresh.")
        return
        
    succeeded = 0
    failed = 0
    
    # Request endpoint on 127.0.0.1:8004
    api_url = "http://127.0.0.1:8004/admin/accounts/refresh-points"
    
    with httpx.Client(timeout=60.0) as http_client:
        for idx, acc in enumerate(invalid_accounts, start=1):
            email = acc.get("email")
            if not email:
                continue
            
            print(f"[{idx}/{total_invalid}] Refreshing {email} ... ", end="", flush=True)
            try:
                resp = http_client.post(api_url, json={"email": email})
                if resp.status_code == 200:
                    res_data = resp.json()
                    new_status = res_data.get("status")
                    new_balance = res_data.get("message_point_balance")
                    print(f"SUCCESS (status: {new_status}, points: {new_balance})")
                    succeeded += 1
                else:
                    print(f"FAILED (HTTP {resp.status_code}: {resp.text.strip()})")
                    failed += 1
            except Exception as e:
                print(f"ERROR ({e})")
                failed += 1
                
    print(f"\nRefresh complete. Total: {total_invalid}, Succeeded: {succeeded}, Failed: {failed}")

if __name__ == "__main__":
    main()
