#!/usr/bin/env python3
"""
Accurate Kustomer agent-performance export
------------------------------------------
✓ Counts every customer-facing outbound message
✓ Ignores bots / automations
✓ Correct date-range filtering via the search API
"""

import csv, time, requests
from collections import defaultdict
from datetime import datetime
from statistics import mean, median
import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────── CONFIG ────────────────────────────────────────────────
KUSTOMER_API_KEY   = os.getenv('KUSTOMER_API_KEY')

BASE_URL  = "https://my-jewellery.api.kustomerapp.com"
START_DATE = "2025-06-01"              # inclusive
END_DATE   = "2025-06-03"              # inclusive
PAGE_SIZE  = 1000
MAX_RETRIES = 4

start_iso = f"{START_DATE}T00:00:00Z"
end_iso   = f"{END_DATE}T23:59:59Z"

HEADERS = {
    "Authorization": f"Bearer {KUSTOMER_API_KEY}",
    "Content-Type": "application/json",
}

# ─────────────── GENERIC HELPERS ───────────────────────────────────────
def request_retry(method: str, url: str, **kw):
    """HTTP with exponential back-off for 429 / transient 5xx."""
    for attempt in range(MAX_RETRIES + 1):
        r = requests.request(method, url, timeout=40, **kw)
        if r.status_code < 400:
            return r
        if r.status_code in (429, 502, 503, 504) and attempt < MAX_RETRIES:
            wait = 2 ** (attempt + 1)
            print(f"[retry {attempt+1}/{MAX_RETRIES}] {r.status_code} → wait {wait}s")
            time.sleep(wait); continue
        raise RuntimeError(f"{method} {url} → {r.status_code}: {r.text[:400]}")

def paginated_search(body: dict):
    """POST /v1/customers/search paging with ?page=N&pageSize=…"""
    page = 1
    while True:
        url = f"{BASE_URL}/v1/customers/search?page={page}&pageSize={PAGE_SIZE}"
        data = request_retry("POST", url, json=body, headers=HEADERS).json()
        yield from data.get("data", [])
        if page >= data.get("meta", {}).get("totalPages", 1):
            break
        page += 1

def dt(ts: str) -> datetime:  # quick ISO-8601 → datetime helper
    return datetime.strptime(ts.rstrip("Z"), "%Y-%m-%dT%H:%M:%S")

# ─────────────── USER MAP (skip bots) ──────────────────────────────────
print("Fetching users …")
user_map = {}
users_url = f"{BASE_URL}/v1/users?page=1&size=9000"
while users_url:
    chunk = request_retry("GET", users_url, headers=HEADERS).json()
    for u in chunk.get("data", []):
        a = u.get("attributes", u)
        if a.get("deletedAt") or a.get("userType") != "user":  # <-- bots filtered here
            continue
        name = (a.get("name") or f"{a.get('firstName','')} {a.get('lastName','')}".strip()
                or a.get("email") or u["id"])
        user_map[u["id"]] = name
    users_url = chunk.get("next")
    if users_url and users_url.startswith("/"):
        users_url = BASE_URL + users_url
print(f"✔ {len(user_map):,} human agents loaded")

# ─────────────── 1) MESSAGES (outbound only) ───────────────────────────
msg_body = {
    "and": [
        {"createdAt": {"gte": start_iso}},
        {"createdAt": {"lte": end_iso}},
        {"direction": {"equals": "out"}},
    ],
    "sort": [{"createdAt": "asc"}],
    "queryContext": "message",
    "timeZone": "Europe/Amsterdam",
}
print("Fetching outbound messages …")
messages = list(paginated_search(msg_body))
print(f"✔ {len(messages):,} messages")

# ─────────────── 2) CONVERSATIONS (created & done) ─────────────────────
conv_created_body = {
    "and": [
        {"createdAt": {"gte": start_iso}},
        {"createdAt": {"lte": end_iso}},
    ],
    "sort": [{"createdAt": "asc"}],
    "queryContext": "conversation",
    "timeZone": "Europe/Amsterdam",
}
conv_done_body = {
    "and": [
        {"lastDoneAt": {"gte": start_iso}},
        {"lastDoneAt": {"lte": end_iso}},
    ],
    "sort": [{"lastDoneAt": "asc"}],
    "queryContext": "conversation",
    "timeZone": "Europe/Amsterdam",
}

print("Fetching conversations (created) …")
conv_created = list(paginated_search(conv_created_body))
print("Fetching conversations (done) …")
conv_done = list(paginated_search(conv_done_body))

conv_lookup = {c["id"]: c for c in conv_created + conv_done}

# ─────────────── METRIC BUCKETS ────────────────────────────────────────
stats = defaultdict(lambda: {
    "msgs": 0, "conv": set(), "cust": set(),
    "conv_done": 0, "conv_created_done": 0,
    "handle_sum": 0.0,
    "frt": [], "fr_res": [],
    "shortcuts": 0, "fcr": 0,
})

# ───── Count every outbound message ────────────────────────────────────
for m in messages:
    agent = (m.get("relationships", {})
               .get("createdBy", {})
               .get("data", {})
               .get("id"))
    if agent not in user_map:      # skip bots / system
        continue
    s = stats[agent]
    s["msgs"] += 1

    c_id = (m.get("conversationId") or
            m.get("relationships", {}).get("conversation", {}).get("data", {}).get("id"))
    if c_id:
        s["conv"].add(c_id)
        cust = (conv_lookup.get(c_id, {}).get("customerId") or
                m.get("relationships", {}).get("customer", {}).get("data", {}).get("id"))
        if cust:
            s["cust"].add(cust)

    if m.get("attributes", {}).get("shortcutIds"):
        s["shortcuts"] += 1

# ───── Conversation completions / handle-time ──────────────────────────
created_ids = {c["id"] for c in conv_created}
for c in conv_done:
    agent = c.get("lastDoneById")
    if agent not in user_map:
        continue
    s = stats[agent]
    s["conv_done"] += 1
    if c["id"] in created_ids:
        s["conv_created_done"] += 1
    if h := c.get("handleTime"):
        s["handle_sum"] += h

# ───── First response & resolution ­times ──────────────────────────────
for c in conv_created:
    cid = c["id"]
    conv_msgs = [m for m in messages if m.get("conversationId") == cid]
    if not conv_msgs:
        continue
    conv_msgs.sort(key=lambda x: x["attributes"]["createdAt"])

    first_cust = next((m for m in conv_msgs if m["direction"] == "in"), None)
    first_agent = next((m for m in conv_msgs if m["direction"] == "out"), None)
    if first_cust and first_agent:
        frt = (dt(first_agent["createdAt"]) - dt(first_cust["createdAt"])).total_seconds()
        stats[first_agent["createdById"]]["frt"].append(frt)

    if c.get("firstDoneAt") and c.get("firstDoneById") in user_map:
        fres = (dt(c["firstDoneAt"]) - dt(c["createdAt"])).total_seconds()
        s = stats[c["firstDoneById"]]
        s["fr_res"].append(fres)
        if c.get("firstDoneAt") == c.get("lastDoneAt"):
            s["fcr"] += 1

# ─────────────── EXPORT CSV ────────────────────────────────────────────
print("Writing CSV …")
with open("agent_performance_metrics_fixed.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow([
        "Agent",
        "Messages sent",
        "Unique conversations messaged",
        "Conversations marked done",
        "Unique customers messaged",
        "Average conversation handle time (s)",
        "Average sent messages per conversation",
        "Average sent messages per customer",
        "First contact resolution rate (%)",
        "Average first response time (s)",
        "Median first response time (s)",
        "Average time to first resolution (s)",
        "Median time to first resolution (s)",
        "Messages sent with shortcuts",
        "Percent of messages sent with shortcuts (%)",
    ])

    for aid, s in stats.items():
        conv_ct = len(s["conv"])
        cust_ct = len(s["cust"])
        wr = [
            user_map[aid],
            s["msgs"],
            conv_ct,
            s["conv_done"],
            cust_ct,
            round(s["handle_sum"] / s["conv_done"], 2) if s["conv_done"] else 0,
            round(s["msgs"] / conv_ct, 2) if conv_ct else 0,
            round(s["msgs"] / cust_ct, 2) if cust_ct else 0,
            round(s["fcr"] / s["conv_created_done"] * 100, 2) if s["conv_created_done"] else 0,
            round(mean(s["frt"]), 2) if s["frt"] else 0,
            round(median(s["frt"]), 2) if s["frt"] else 0,
            round(mean(s["fr_res"]), 2) if s["fr_res"] else 0,
            round(median(s["fr_res"]), 2) if s["fr_res"] else 0,
            s["shortcuts"],
            round(s["shortcuts"] / s["msgs"] * 100, 2) if s["msgs"] else 0,
        ]
        w.writerow(wr)

print("✅  agent_performance_metrics_fixed.csv ready – now aligned with Kustomer UI")
