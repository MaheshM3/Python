"""
Get exact DBU usage for a specific Azure Databricks Job Run ID
Updated for:
- Serverless SQL Warehouse
- system.billing.usage in a custom catalog (e.g. system_billing)
- Latest SQL Statement Execution API (2025)
"""

import os
import time
import requests
import json
from typing import List, Dict, Any

# ================================================================
# CONFIGURATION — REPLACE THESE 5 VALUES ONLY
# ================================================================

# 1. Your Azure Databricks workspace URL
WORKSPACE_URL = "https://adb-1234567890123456.7.azuredatabricks.net"   # change

# 2. Personal Access Token (workspace or account level)
#    User Settings → Access Tokens → Generate new token
DATABRICKS_TOKEN = ""               # change

# 3. Serverless SQL Warehouse HTTP Path
#    Go to SQL Warehouses → pick your Serverless warehouse → Connection Details → HTTP Path
#    Example: /sql/1.0/warehouses/11111111-2222-3333-4444-555555555555
SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/11111111-2222-3333-4444-555555555555"  # change

# 4. Catalog where system.billing.usage lives
#    Common values: system_billing (most Azure workspaces in 2024–2025)
#                  system       (legacy, still works if not moved)
BILLING_CATALOG = "system_billing"   # ← change only if different

# 5. The Job Run ID you want to analyze
JOB_RUN_ID = "987654321"             # ← change to your real run_id

# ================================================================
# End of configuration
# ================================================================

def poll_statement(statement_id: str) -> Dict[str, Any]:
    url = f"{WORKSPACE_URL}/api/2.0/sql/statements/{statement_id}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

    while True:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        state = data["status"]["state"]

        if state in ["SUCCEEDED", "FAILED", "CANCELED", "ERROR"]:
            return data

        print(f"   Query still running... ({state}) – sleeping 4s")
        time.sleep(4)

def execute_query(query: str) -> List[List[Any]]:
    url = f"{WORKSPACE_URL}/api/2.0/sql/statements"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "warehouse_id": SQL_WAREHOUSE_HTTP_PATH.split("/")[-1],
        "statement": query,
        "wait_timeout": "0s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
        "on_wait_timeout": "CONTINUE"
    }

    print("Submitting query to Serverless SQL Warehouse...")
    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    statement_id = resp.json()["statement_id"]
    print(f"Statement ID: {statement_id}")

    result = poll_statement(statement_id)
    state = result["status"]["state"]

    if state != "SUCCEEDED":
        print(f"Query failed with state: {state}")
        if "error" in result["status"]:
            print(result["status"]["error"]["message"])
        return []

    rows = result.get("result", {}).get("data_array", [])
    return rows


# ================================================================
# Main SQL query – works with custom catalog + serverless
# ================================================================

SQL = f"""
SELECT
    usage_start_time,
    usage_end_time,
    sku_name,
    usage_quantity AS dbu,
    usage_metadata.job_id,
    usage_metadata.run_name,
    custom_tags:job_type AS job_type,
    cloud
FROM {BILLING_CATALOG}.billing.usage
WHERE usage_metadata.job_run_id = '{JOB_RUN_ID}'
  AND usage_unit = 'DBU'
  AND usage_quantity != 0          -- ignore pure correction rows
ORDER BY usage_start_time
"""

if __name__ == "__main__":
    print("="*80)
    print("Databricks DBU Usage Lookup (Serverless + Custom Catalog)")
    print(f"Job Run ID : {JOB_RUN_ID}")
    print(f"Billing catalog : {BILLING_CATALOG}.billing.usage")
    print("="*80)

    rows = execute_query(SQL)

    if not rows:
        print("No DBU records found.")
        print("\nPossible causes:")
        print("   • Job run too recent (< 3 hours)")
        print("   • system.billing.usage not enabled or in wrong catalog")
        print("   • Run used no billable compute (instant failure)")
        print("   • Wrong JOB_RUN_ID")
        exit(0)

    total_dbu = sum(float(row[3]) for row in rows)

    print(f"\nFound {len(rows)} billing row(s) → Total DBU = {total_dbu:.6f}\n")
    print(f"{'Start':<22} {'End':<22} {'SKU':<28} {'DBU':>10} {'Job Type':<12}")
    print("-" * 90)

    for r in rows:
        start = str(r[0])[:19].replace("T", " ")
        end   = str(r[1])[:19].replace("T", " ")
        sku   = r[2]
        dbu   = float(r[3])
        jtype = r[6] if r[6] else "N/A"
        print(f"{start}  {end}  {sku:<28} {dbu:10.6f} {jtype}")

    print("-" * 90)
    print(f"{'TOTAL DBU':>74} {total_dbu:10.6f}")
    print("="*80)

    # Optional: rough cost estimate (example rates as of Nov 2025 – change as needed)
    rates = {
        "AUTOMATED_JOBS_COMPUTE": 0.20,
        "INTERACTIVE_JOBS_COMPUTE": 0.35,
        "SERVERLESS_JOBS_COMPUTE": 0.55,
        "PREMIUM_JOBS_COMPUTE": 0.40
    }
    estimated_cost = sum(float(r[3]) * rates.get(r[2].split("_")[0] + "_JOBS_COMPUTE", 0.30) for r in rows)
    print(f"Estimated cost (USD): ≈ ${estimated_cost:.4f} (rates are approximate)")

