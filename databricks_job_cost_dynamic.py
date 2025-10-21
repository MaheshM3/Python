import requests
import csv
import io
from datetime import datetime, timedelta

# ===== CONFIG =====
DATABRICKS_WORKSPACE = "https://adb-xxxxxxxxxx.azuredatabricks.net"
ACCOUNT_ID = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
TOKEN = "dapi-xxxxxxxxxxxxxxxxxxxxxxxx"
REGION = "uksouth"  # e.g., eastus, uksouth, westeurope

SPOT_DISCOUNT = 0.7  # fallback discount if spot rate not found

# Example DBU rate map (adjust for your SKU)
DBU_RATES = {
    "standard": 0.55,
    "premium": 0.65,
    "photon": 0.75,
}

# Cache for VM prices to avoid multiple calls
VM_PRICE_CACHE = {}

# ==============================

def get_job_run_details(run_id):
    """Fetch job run details via REST API."""
    url = f"{DATABRICKS_WORKSPACE}/api/2.1/jobs/runs/get?run_id={run_id}"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def download_usage_csv(start, end):
    """Download billable usage CSV for the month(s) covering the job run."""
    url = f"https://accounts.azuredatabricks.net/api/2.0/accounts/{ACCOUNT_ID}/usage/download"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    params = {
        "start_month": start.strftime("%Y-%m"),
        "end_month": end.strftime("%Y-%m"),
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.content.decode("utf-8")


def get_azure_vm_price(vm_size, region, spot=False):
    """Fetch VM price from Azure Retail Prices API."""
    key = (vm_size, region, spot)
    if key in VM_PRICE_CACHE:
        return VM_PRICE_CACHE[key]

    sku_name = vm_size.replace("_", " ")
    price_url = f"https://prices.azure.com/api/retail/prices"
    params = {
        "$filter": f"armRegionName eq '{region}' and serviceFamily eq 'Compute' and armSkuName eq '{vm_size}'"
    }
    resp = requests.get(price_url, params=params)
    resp.raise_for_status()
    data = resp.json()

    retail_price = None
    for item in data.get("Items", []):
        if "Windows" not in item["productName"] and "Low Priority" not in item["productName"]:
            retail_price = float(item["retailPrice"])
            if not spot:
                break
        if spot and "Spot" in item["skuName"]:
            retail_price = float(item["retailPrice"])
            break

    if retail_price is None:
        # fallback to on-demand * discount
        retail_price = get_azure_vm_price(vm_size, region, spot=False) * SPOT_DISCOUNT if spot else 0.5

    VM_PRICE_CACHE[key] = retail_price
    return retail_price


def estimate_cost(run_id):
    run = get_job_run_details(run_id)
    cluster = run["cluster_spec"]["new_cluster"]
    start = datetime.fromtimestamp(run["start_time"] / 1000)
    end = datetime.fromtimestamp(run["end_time"] / 1000)

    node_type = cluster["node_type_id"]
    num_workers = cluster["num_workers"]
    aws_attr = cluster.get("aws_attributes", {})
    is_spot = aws_attr.get("availability", "").upper() == "SPOT"

    print(f"\n=== Job Run {run_id} ===")
    print(f"Cluster Type: {node_type}")
    print(f"Workers: {num_workers} | Spot: {is_spot}")
    print(f"Start: {start} | End: {end}")
    print("-" * 60)

    # Fetch usage data for the time window
    csv_data = download_usage_csv(start, end)
    usage_reader = csv.DictReader(io.StringIO(csv_data))

    total_dbu = 0.0
    photon_dbu = 0.0

    for row in usage_reader:
        if "usageDate" in row:
            usage_start = datetime.strptime(row["usageDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
            if start <= usage_start <= end:
                dbu = float(row.get("dbu", 0))
                total_dbu += dbu
                if "photon" in row.get("sku", "").lower():
                    photon_dbu += dbu

    dbu_rate = DBU_RATES["photon"] if photon_dbu > 0 else DBU_RATES["standard"]
    dbu_cost = total_dbu * dbu_rate

    # VM pricing from Azure Retail API
    vm_price = get_azure_vm_price(node_type, REGION, spot=is_spot)
    hours = (end - start).total_seconds() / 3600
    vm_cost = vm_price * hours * num_workers
    total_cost = dbu_cost + vm_cost

    print(f"DBU Used: {total_dbu:.2f}")
    print(f"DBU Cost (@${dbu_rate}/DBU): ${dbu_cost:.2f}")
    print(f"VM Price per Hour: ${vm_price:.4f}")
    print(f"VM Cost ({num_workers} nodes × {hours:.2f} h): ${vm_cost:.2f}")
    print(f"TOTAL ESTIMATED COST: ${total_cost:.2f}")

    return {
        "run_id": run_id,
        "total_dbu": total_dbu,
        "dbu_cost": dbu_cost,
        "vm_cost": vm_cost,
        "total_cost": total_cost,
        "photon_dbu": photon_dbu,
        "spot": is_spot,
    }


if __name__ == "__main__":
    run_id = input("Enter Databricks Job Run ID: ").strip()
    estimate_cost(run_id)