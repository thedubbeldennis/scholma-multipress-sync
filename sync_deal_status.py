"""
Scholma MultiPress → HubSpot Deal Status Sync

Checks all HubSpot deals in "Voorstel verstuurd" stage against MultiPress API.
Moves deals to Won/Lost based on quotation_status. Removes follow-up tasks.

Usage:
    python sync_deal_status.py              # Dry run (default)
    python sync_deal_status.py --execute    # Actually update HubSpot
"""
import os
import re
import sys
import time
import argparse
import requests
import urllib3
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Config ---
HUBSPOT_API_KEY = os.environ.get("HUBSPOT_API_KEY", "")
MULTIPRESS_URL = os.environ.get("MULTIPRESS_URL", "https://e-commerce.scholma.nl/connector")
MULTIPRESS_USER = os.environ.get("MULTIPRESS_USER", "zwartekraai")
MULTIPRESS_PASS = os.environ.get("MULTIPRESS_PASS", "")

HS_BASE = "https://api.hubapi.com"
HS_HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_API_KEY}",
    "Content-Type": "application/json",
}

# HubSpot pipeline stages (Hoofdpijplijn)
STAGE_VOORSTEL_VERSTUURD = "3594129636"
STAGE_GEWONNEN = "3594129638"
STAGE_VERLOREN = "3594129640"

# MultiPress status → HubSpot action
STATUS_WON = {"Order"}
STATUS_LOST = {"Order ao", "Vervallen", "Niet gegund", "Te duur >10%"}
STATUS_SKIP = {"Offerte uitgebracht", "Nieuwe calculatie"}


def get_hubspot_deals():
    """Get all deals in 'Voorstel verstuurd' stage."""
    all_deals = []
    after = None

    while True:
        payload = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "dealstage",
                    "operator": "EQ",
                    "value": STAGE_VOORSTEL_VERSTUURD,
                }]
            }],
            "properties": ["dealname", "dealstage", "client_system_deal_id", "offerte_id", "amount"],
            "limit": 100,
        }
        if after:
            payload["after"] = after

        r = requests.post(f"{HS_BASE}/crm/v3/objects/deals/search", headers=HS_HEADERS, json=payload)
        r.raise_for_status()
        data = r.json()

        all_deals.extend(data.get("results", []))

        paging = data.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after:
            break
        time.sleep(0.1)

    return all_deals


def extract_quotation_number(deal):
    """Extract quotation number from deal name or properties."""
    props = deal.get("properties", {})
    name = props.get("dealname", "")

    # Try dealname pattern: "Offerte #320450 - €2600"
    match = re.search(r"#(\d+)", name)
    if match:
        return match.group(1)

    # Try client_system_deal_id
    csdi = props.get("client_system_deal_id", "")
    if csdi and str(csdi).isdigit():
        return str(csdi)

    # Try offerte_id
    oid = props.get("offerte_id", "")
    if oid and str(oid).isdigit():
        return str(oid)

    return None


def check_multipress_status(quotation_number):
    """Check quotation status in MultiPress API."""
    r = requests.get(
        f"{MULTIPRESS_URL}/jobs/handleJobsQuotationsDetails",
        params={"quotation_number": quotation_number},
        auth=(MULTIPRESS_USER, MULTIPRESS_PASS),
        verify=False,
        timeout=30,
    )
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}"

    data = r.json()
    return data.get("quotation_status", ""), data.get("company", "")


def find_and_delete_task(deal_id, quotation_number, dry_run=True):
    """Find task with title 'Opvolgen - Offerte #XXXXXX' linked to deal and delete it."""
    # Search tasks containing the quotation number
    payload = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "hs_task_subject",
                "operator": "CONTAINS_TOKEN",
                "value": f"Offerte",
            }]
        }],
        "properties": ["hs_task_subject", "hs_task_status"],
        "limit": 100,
    }

    r = requests.post(f"{HS_BASE}/crm/v3/objects/tasks/search", headers=HS_HEADERS, json=payload)
    if r.status_code != 200:
        return 0

    deleted = 0
    for task in r.json().get("results", []):
        subject = task["properties"].get("hs_task_subject", "")
        # Match pattern: "Opvolgen - Offerte #320450"
        if f"#{quotation_number}" not in subject:
            continue

        # Verify task is associated with this deal
        assoc_r = requests.get(
            f"{HS_BASE}/crm/v4/objects/tasks/{task['id']}/associations/deals",
            headers=HS_HEADERS,
        )
        if assoc_r.status_code == 200:
            linked_deals = [str(a["toObjectId"]) for a in assoc_r.json().get("results", [])]
            if str(deal_id) not in linked_deals:
                continue

        if dry_run:
            print(f"    [DRY-RUN] Zou taak verwijderen: '{subject}' (id={task['id']})")
        else:
            del_r = requests.delete(f"{HS_BASE}/crm/v3/objects/tasks/{task['id']}", headers=HS_HEADERS)
            if del_r.status_code in (200, 204):
                print(f"    Taak verwijderd: '{subject}' (id={task['id']})")
                deleted += 1
            else:
                print(f"    FOUT bij verwijderen taak {task['id']}: {del_r.status_code}")
        time.sleep(0.1)

    return deleted


def update_deal_stage(deal_id, new_stage, dry_run=True):
    """Update deal stage in HubSpot."""
    if dry_run:
        return True

    r = requests.patch(
        f"{HS_BASE}/crm/v3/objects/deals/{deal_id}",
        headers=HS_HEADERS,
        json={"properties": {"dealstage": new_stage}},
    )
    return r.status_code == 200


def main():
    parser = argparse.ArgumentParser(description="Sync MultiPress quotation status to HubSpot deals")
    parser.add_argument("--execute", action="store_true", help="Actually update HubSpot (default: dry-run)")
    args = parser.parse_args()

    dry_run = not args.execute
    mode = "DRY-RUN" if dry_run else "LIVE"

    print(f"{'='*60}")
    print(f"Scholma Deal Status Sync - {mode}")
    print(f"Tijd: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    if not HUBSPOT_API_KEY:
        print("FOUT: HUBSPOT_API_KEY niet gevonden in environment")
        sys.exit(1)
    if not MULTIPRESS_PASS:
        print("FOUT: MULTIPRESS_PASS niet gevonden in environment")
        sys.exit(1)

    # Step 1: Get deals
    print("\n[1/4] HubSpot deals ophalen (stage: Voorstel verstuurd)...")
    deals = get_hubspot_deals()
    print(f"  Gevonden: {len(deals)} deals")

    # Step 2: Check each deal against MultiPress
    print(f"\n[2/4] MultiPress status checken...")
    results = {"won": [], "lost": [], "skip": [], "error": [], "no_qn": []}

    for i, deal in enumerate(deals):
        deal_id = deal["id"]
        props = deal.get("properties", {})
        name = props.get("dealname", "?")
        qn = extract_quotation_number(deal)

        if not qn:
            results["no_qn"].append({"deal_id": deal_id, "name": name})
            continue

        mp_status, company = check_multipress_status(qn)

        if mp_status is None:
            results["error"].append({"deal_id": deal_id, "name": name, "qn": qn, "error": company})
        elif mp_status in STATUS_WON:
            results["won"].append({"deal_id": deal_id, "name": name, "qn": qn, "status": mp_status, "company": company})
        elif mp_status in STATUS_LOST:
            results["lost"].append({"deal_id": deal_id, "name": name, "qn": qn, "status": mp_status, "company": company})
        else:
            results["skip"].append({"deal_id": deal_id, "name": name, "qn": qn, "status": mp_status})

        if (i + 1) % 25 == 0:
            print(f"  ...{i+1}/{len(deals)}")
        time.sleep(0.05)

    # Step 3: Update deals
    print(f"\n[3/4] Deals updaten...")
    updated_won = 0
    updated_lost = 0

    for item in results["won"]:
        label = f"  #{item['qn']} [{item['status']}] {item['company'][:35]}"
        if dry_run:
            print(f"  [DRY-RUN] → Gewonnen: {label}")
        else:
            if update_deal_stage(item["deal_id"], STAGE_GEWONNEN, dry_run=False):
                print(f"  → Gewonnen: {label}")
                updated_won += 1
            else:
                print(f"  FOUT: {label}")
        time.sleep(0.1)

    for item in results["lost"]:
        label = f"  #{item['qn']} [{item['status']}] {item['company'][:35]}"
        if dry_run:
            print(f"  [DRY-RUN] → Verloren: {label}")
        else:
            if update_deal_stage(item["deal_id"], STAGE_VERLOREN, dry_run=False):
                print(f"  → Verloren: {label}")
                updated_lost += 1
            else:
                print(f"  FOUT: {label}")
        time.sleep(0.1)

    # Step 4: Delete follow-up tasks
    print(f"\n[4/4] Opvolg-taken verwijderen...")
    tasks_deleted = 0
    all_changed = results["won"] + results["lost"]
    for item in all_changed:
        tasks_deleted += find_and_delete_task(item["deal_id"], item["qn"], dry_run=dry_run)

    # Summary
    print(f"\n{'='*60}")
    print(f"RESULTAAT ({mode})")
    print(f"{'='*60}")
    print(f"  Totaal deals gecheckt:  {len(deals)}")
    print(f"  Zonder offerte nummer:  {len(results['no_qn'])}")
    print(f"  API errors:             {len(results['error'])}")
    print(f"  Ongewijzigd (correct):  {len(results['skip'])}")
    print(f"  → Gewonnen:             {len(results['won'])}")
    print(f"  → Verloren:             {len(results['lost'])}")
    print(f"  Taken verwijderd:       {tasks_deleted}")

    if dry_run:
        print(f"\n  Dit was een DRY-RUN. Gebruik --execute om echt te updaten.")

    if results["error"]:
        print(f"\n  Errors:")
        for e in results["error"][:5]:
            print(f"    #{e['qn']}: {e['error']}")


if __name__ == "__main__":
    main()
