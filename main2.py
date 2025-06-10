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
from datetime import datetime, timedelta
from copy import deepcopy
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
    # The Kustomer search API uses zero-indexed pages and returns the
    # total number of pages in the response metadata. Track ``total_pages``
    # so we never request past the last available page which would cause a
    # 400 error.
    page = 0
    total_pages = 1
    while page < total_pages:
        url = f"{BASE_URL}/v1/customers/search?page={page}&pageSize={PAGE_SIZE}"
        data = request_retry("POST", url, json=body, headers=HEADERS).json()
        yield from data.get("data", [])
        total_pages = data.get("meta", {}).get("totalPages", total_pages)
        page += 1

def search_by_day(base_body: dict, field: str):
    """Split the search into per-day chunks to avoid large page counts."""
    results = []
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    day = timedelta(days=1)
    while start <= end:
        day_start = f"{start.strftime('%Y-%m-%d')}T00:00:00Z"
        day_end = f"{start.strftime('%Y-%m-%d')}T23:59:59Z"
        body = deepcopy(base_body)
        body["and"] = [
            {field: {"gte": day_start}},
            {field: {"lte": day_end}},
            *[c for c in base_body.get("and", []) if field not in c],
        ]
        results.extend(paginated_search(body))
        start += day
    return results

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
        {"auto": {"equals": False}},
        {"direction": {"equals": "out"}},
    ],
    "sort": [{"createdAt": "asc"}],
    "queryContext": "message",
    "timeZone": "Europe/Amsterdam",
}
print("Fetching outbound messages …")
messages = search_by_day(msg_body, "createdAt")
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
        {"deleted": {"equals": False}},
        {"messageCount": {"gt": 0}},
    ],
    "sort": [{"lastDoneAt": "asc"}],
    "queryContext": "conversation",
    "timeZone": "Europe/Amsterdam",
}

print("Fetching conversations (created) …")
conv_created = search_by_day(conv_created_body, "createdAt")
print("Fetching conversations (done) …")
conv_done = search_by_day(conv_done_body, "lastDoneAt")

conv_lookup = {c["id"]: c for c in conv_created + conv_done}

# ─────────────── 3) USER TIME (logged in) ──────────────────────────────
user_time_body = {
    "and": [
        {"docAt": {"gte": start_iso}},
        {"docAt": {"lte": end_iso}},
    ],
    "sort": [{"docAt": "asc"}],
    "queryContext": "userTime",
    "timeZone": "Europe/Amsterdam",
}
print("Fetching user time …")
user_times = search_by_day(user_time_body, "docAt")
print(f"✔ {len(user_times):,} user time entries")

# ─────────────── METRIC BUCKETS ────────────────────────────────────────
stats = defaultdict(lambda: {
    "msgs": 0,
    "conv": set(),
    "cust": set(),
    "conv_done": 0,
    "handle_sum": 0.0,
    "resp": [],
    "frt": [],
    "fr_res": [],
    "shortcuts": 0,
    "fcr_hits": 0,
    "fcr_total": 0,
    "login": 0.0,
})

# ───── Count every outbound message ────────────────────────────────────
for m in messages:
    direction = m.get("direction") or m.get("attributes", {}).get("direction")
    auto = m.get("auto") if "auto" in m else m.get("attributes", {}).get("auto")
    if direction != "out" or auto:
        continue

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

    resp_bt = (m.get("responseBusinessTime") or
               m.get("attributes", {}).get("responseBusinessTime"))
    if isinstance(resp_bt, (int, float)):
        s["resp"].append(resp_bt)

    if m.get("attributes", {}).get("shortcutIds"):
        s["shortcuts"] += 1

# ───── Conversation completions / handle-time ──────────────────────────
for c in conv_done:
    if c.get("deleted"):
        continue
    if c.get("messageCount", 0) <= 0:
        continue

    agent = c.get("lastDoneById")
    if agent not in user_map:
        continue
    s = stats[agent]
    s["conv_done"] += 1
    if h := c.get("handleTime"):
        s["handle_sum"] += h

    fd_at = c.get("firstDoneAt")
    fd_by = c.get("firstDoneById")
    if fd_at and fd_by in user_map:
        if start_iso <= fd_at <= end_iso:
            sf = stats[fd_by]
            sf["fcr_total"] += 1
            if (
                c.get("status") == "done" and
                c.get("direction") == "in" and
                c.get("messageCount", 0) > 0 and
                c.get("reopenCount", 0) <= 0 and
                not c.get("mergedTarget")
            ):
                sf["fcr_hits"] += 1
            if c.get("createdAt"):
                try:
                    fres = (dt(fd_at) - dt(c["createdAt"])).total_seconds()
                    sf["fr_res"].append(fres)
                except Exception:
                    pass

# ───── First response & resolution ­times ──────────────────────────────
for c in conv_created:
    cid = c["id"]
    conv_msgs = [m for m in messages if (m.get("conversationId") or m.get("relationships", {}).get("conversation", {}).get("data", {}).get("id")) == cid]
    if not conv_msgs:
        continue
    conv_msgs.sort(key=lambda x: x.get("attributes", {}).get("createdAt") or x.get("createdAt"))

    first_cust = next((m for m in conv_msgs if (m.get("direction") or m.get("attributes", {}).get("direction")) == "in"), None)
    first_agent = next((m for m in conv_msgs if (m.get("direction") or m.get("attributes", {}).get("direction")) == "out" and not (m.get("auto") if "auto" in m else m.get("attributes", {}).get("auto"))), None)
    if first_cust and first_agent:
        try:
            frt = (dt(first_agent.get("createdAt") or first_agent["attributes"]["createdAt"]) - dt(first_cust.get("createdAt") or first_cust["attributes"]["createdAt"])).total_seconds()
            stats[first_agent.get("createdById")]["frt"].append(frt)
        except Exception:
            pass

# ───── Logged-in time ─────────────────────────────────────────────────
for ut in user_times:
    uid = (ut.get("userId") or
           ut.get("relationships", {}).get("user", {}).get("data", {}).get("id"))
    if uid not in user_map:
        continue
    logged = ut.get("loggedIn", {}).get("timeTotal")
    if isinstance(logged, (int, float)):
        stats[uid]["login"] += logged

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
        "Average response time (s)",
        "Average first response time (s)",
        "Median first response time (s)",
        "Average time to first resolution (s)",
        "Median time to first resolution (s)",
        "Total time logged in (s)",
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
            round(s["fcr_hits"] / s["fcr_total"] * 100, 2) if s["fcr_total"] else 0,
            round(mean(s["resp"]), 2) if s["resp"] else 0,
            round(mean(s["frt"]), 2) if s["frt"] else 0,
            round(median(s["frt"]), 2) if s["frt"] else 0,
            round(mean(s["fr_res"]), 2) if s["fr_res"] else 0,
            round(median(s["fr_res"]), 2) if s["fr_res"] else 0,
            round(s["login"], 2),
            s["shortcuts"],
            round(s["shortcuts"] / s["msgs"] * 100, 2) if s["msgs"] else 0,
        ]
        w.writerow(wr)

print("✅  agent_performance_metrics_fixed.csv ready – now aligned with Kustomer UI")
