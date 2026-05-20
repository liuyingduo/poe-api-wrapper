import httpx

def main():
    base_url = "http://207.180.218.216:8004"
    print(f"Fetching invalid accounts page by page from: {base_url}")
    
    invalid_accounts = []
    page = 1
    size = 100
    
    while True:
        stats_url = f"{base_url}/admin/dashboard-stats?page={page}&size={size}&status=invalid"
        try:
            resp = httpx.get(stats_url, timeout=30.0)
            if resp.status_code != 200:
                print(f"Failed to fetch stats for page {page}: HTTP {resp.status_code}")
                break
            data = resp.json()
        except Exception as e:
            print(f"Error fetching stats page {page}: {e}")
            break
            
        accounts = data.get("accounts", [])
        if not accounts:
            break
            
        invalid_accounts.extend(accounts)
        
        total_pages = data.get("total_pages", 1)
        print(f"Loaded page {page}/{total_pages} (found {len(accounts)} accounts on this page).")
        
        if page >= total_pages:
            break
        page += 1

    total_invalid = len(invalid_accounts)
    print(f"Found total {total_invalid} accounts with status 'invalid'.")

    if total_invalid == 0:
        print("No invalid accounts found to refresh.")
        return

    refresh_url = f"{base_url}/admin/accounts/refresh-points"
    succeeded = 0
    failed = 0

    # Set a timeout for each refresh request
    with httpx.Client(timeout=60.0) as client:
        for idx, acc in enumerate(invalid_accounts, start=1):
            email = acc.get("email")
            if not email:
                continue
            
            print(f"[{idx}/{total_invalid}] Refreshing {email} ... ", end="", flush=True)
            try:
                r = client.post(refresh_url, json={"email": email})
                if r.status_code == 200:
                    res_data = r.json()
                    new_status = res_data.get("status")
                    new_balance = res_data.get("message_point_balance")
                    print(f"SUCCESS (status: {new_status}, points: {new_balance})")
                    succeeded += 1
                else:
                    print(f"FAILED (HTTP {r.status_code}: {r.text.strip()})")
                    failed += 1
            except Exception as e:
                print(f"ERROR ({e})")
                failed += 1

    print(f"\nRefresh complete. Total: {total_invalid}, Succeeded: {succeeded}, Failed: {failed}")

if __name__ == "__main__":
    main()
