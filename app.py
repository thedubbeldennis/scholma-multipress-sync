"""
Scholma MultiPress → HubSpot Deal Status Sync - Web Service

Endpoints:
    GET  /health     - Health check
    GET  /sync       - Run sync (all active stages)
    GET  /sync?stage=voorstel  - Only check 'Voorstel verstuurd' stage
"""
import os
import re
import time
import requests
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from fastapi import FastAPI, Query, Header, HTTPException

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = FastAPI(title="Scholma Deal Status Sync")

# --- Config from environment ---
HUBSPOT_API_KEY = os.environ.get("HUBSPOT_API_KEY", "")
MULTIPRESS_URL = os.environ.get("MULTIPRESS_URL", "https://e-commerce.scholma.nl/connector")
MULTIPRESS_USER = os.environ.get("MULTIPRESS_USER", "zwartekraai")
MULTIPRESS_PASS = os.environ.get("MULTIPRESS_PASS", "")
API_SECRET = os.environ.get("API_SECRET", "")

HS_BASE = "https://api.hubapi.com"

# HubSpot pipeline stages
STAGES = {
    "3594129634": "Inventarisatie",
    "3594129635": "Voorstel maken",
    "3594129636": "Voorstel verstuurd",
    "3594129637": "Onderhandeling",
    "4087013618": "Productie info",
    "4073418949": "Niet opvolgen",
}
STAGE_GEWONNEN = "3594129638"
STAGE_VERLOREN = "3594129640"

# MultiPress status mapping
STATUS_WON = {"Order"}
STATUS_LOST = {"Order ao", "Vervallen", "Niet gegund", "Te duur >10%"}


def hs_headers():
    return {"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"}


def get_deals_by_stage(stage_ids: list[str]) -> list[dict]:
    """Fetch all deals from given stages."""
    all_deals = []
    for stage_id in stage_ids:
        after = None
        while True:
            payload = {
                "filterGroups": [{"filters": [{"propertyName": "dealstage", "operator": "EQ", "value": stage_id}]}],
                "properties": ["dealname", "dealstage", "client_system_deal_id", "offerte_id", "amount"],
                "limit": 100,
            }
            if after:
                payload["after"] = after
            r = requests.post(f"{HS_BASE}/crm/v3/objects/deals/search", headers=hs_headers(), json=payload)
            r.raise_for_status()
            data = r.json()
            for deal in data.get("results", []):
                deal["_stage"] = STAGES.get(stage_id, stage_id)
            all_deals.extend(data.get("results", []))
            paging = data.get("paging", {}).get("next", {})
            after = paging.get("after")
            if not after:
                break
            time.sleep(0.1)
    return all_deals


def extract_qn(deal: dict) -> str | None:
    """Extract quotation number from deal."""
    props = deal.get("properties", {})
    name = props.get("dealname", "")
    match = re.search(r"#(\d+)", name)
    if match:
        return match.group(1)
    csdi = props.get("client_system_deal_id", "")
    if csdi and str(csdi).isdigit():
        return str(csdi)
    oid = props.get("offerte_id", "")
    if oid and str(oid).isdigit():
        return str(oid)
    return None


def check_mp_status(qn: str) -> tuple[str | None, str]:
    """Check quotation status in MultiPress."""
    try:
        r = requests.get(
            f"{MULTIPRESS_URL}/jobs/handleJobsQuotationsDetails",
            params={"quotation_number": qn},
            auth=(MULTIPRESS_USER, MULTIPRESS_PASS),
            verify=False,
            timeout=30,
        )
        if r.status_code == 200:
            data = r.json()
            return data.get("quotation_status", ""), data.get("company", "")
        return None, f"HTTP {r.status_code}"
    except Exception as e:
        return None, str(e)


def delete_task_for_qn(qn: str, updated_qns_tasks: dict):
    """Delete task matching quotation number."""
    if qn not in updated_qns_tasks:
        return 0
    deleted = 0
    for task_id in updated_qns_tasks[qn]:
        r = requests.delete(f"{HS_BASE}/crm/v3/objects/tasks/{task_id}", headers=hs_headers())
        if r.status_code in (200, 204):
            deleted += 1
        time.sleep(0.1)
    return deleted


def find_opvolgen_tasks(qn_set: set[str]) -> dict[str, list[str]]:
    """Find all 'Opvolgen - Offerte #' tasks matching given quotation numbers."""
    all_tasks = []
    after = None
    while True:
        payload = {
            "filterGroups": [{"filters": [{"propertyName": "hs_task_subject", "operator": "CONTAINS_TOKEN", "value": "Opvolgen"}]}],
            "properties": ["hs_task_subject"],
            "limit": 100,
        }
        if after:
            payload["after"] = after
        r = requests.post(f"{HS_BASE}/crm/v3/objects/tasks/search", headers=hs_headers(), json=payload)
        data = r.json()
        all_tasks.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
        time.sleep(0.1)

    # Map qn → [task_ids]
    result = {}
    for task in all_tasks:
        subject = task["properties"].get("hs_task_subject", "")
        match = re.search(r"Offerte\s*#(\d+)", subject)
        if match and match.group(1) in qn_set:
            qn = match.group(1)
            result.setdefault(qn, []).append(task["id"])
    return result


@app.get("/health")
def health():
    return {"status": "ok", "service": "scholma-deal-sync"}


@app.get("/sync")
def sync(
    stage: str = Query("all", description="'all' for all active stages, 'voorstel' for only Voorstel verstuurd"),
    x_api_key: str = Header(None),
):
    # Auth check
    if API_SECRET and x_api_key != API_SECRET:
        raise HTTPException(status_code=401, detail="Invalid API key")

    if not HUBSPOT_API_KEY or not MULTIPRESS_PASS:
        raise HTTPException(status_code=500, detail="Missing environment variables")

    started = datetime.utcnow().isoformat()

    # Determine which stages to check
    if stage == "voorstel":
        stage_ids = ["3594129636"]
    else:
        stage_ids = list(STAGES.keys())

    # Step 1: Get deals
    deals = get_deals_by_stage(stage_ids)

    # Step 2: Check MultiPress (parallel)
    won = []
    lost = []
    skip = 0
    errors = 0
    no_qn = 0

    # Build work items
    work = []
    for deal in deals:
        qn = extract_qn(deal)
        if not qn:
            no_qn += 1
            continue
        work.append((deal, qn))

    # Run MP checks in parallel (20 workers)
    def _check(item):
        deal, qn = item
        mp_status, company = check_mp_status(qn)
        return deal, qn, mp_status, company

    with ThreadPoolExecutor(max_workers=20) as pool:
        for deal, qn, mp_status, company in pool.map(_check, work):
            if mp_status is None:
                errors += 1
            elif mp_status in STATUS_WON:
                won.append({"deal_id": deal["id"], "qn": qn, "company": company, "from_stage": deal.get("_stage", "?")})
            elif mp_status in STATUS_LOST:
                lost.append({"deal_id": deal["id"], "qn": qn, "company": company, "status": mp_status, "from_stage": deal.get("_stage", "?")})
            else:
                skip += 1

    # Step 3: Update deals
    updated_won = 0
    for item in won:
        r = requests.patch(
            f'{HS_BASE}/crm/v3/objects/deals/{item["deal_id"]}',
            headers=hs_headers(),
            json={"properties": {"dealstage": STAGE_GEWONNEN}},
        )
        if r.status_code == 200:
            updated_won += 1
        time.sleep(0.1)

    updated_lost = 0
    for item in lost:
        r = requests.patch(
            f'{HS_BASE}/crm/v3/objects/deals/{item["deal_id"]}',
            headers=hs_headers(),
            json={"properties": {"dealstage": STAGE_VERLOREN}},
        )
        if r.status_code == 200:
            updated_lost += 1
        time.sleep(0.1)

    # Step 4: Delete tasks
    all_updated_qns = set(d["qn"] for d in won + lost)
    tasks_map = find_opvolgen_tasks(all_updated_qns) if all_updated_qns else {}
    tasks_deleted = 0
    for qn in all_updated_qns:
        tasks_deleted += delete_task_for_qn(qn, tasks_map)

    return {
        "started": started,
        "finished": datetime.utcnow().isoformat(),
        "deals_checked": len(deals),
        "no_quotation_number": no_qn,
        "api_errors": errors,
        "unchanged": skip,
        "won": updated_won,
        "lost": updated_lost,
        "tasks_deleted": tasks_deleted,
        "won_details": [{"qn": d["qn"], "company": d["company"], "from": d["from_stage"]} for d in won],
        "lost_details": [{"qn": d["qn"], "company": d["company"], "status": d["status"], "from": d["from_stage"]} for d in lost],
    }
