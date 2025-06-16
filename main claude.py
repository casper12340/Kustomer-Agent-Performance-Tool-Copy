#!/usr/bin/env python3
"""
Accurate Kustomer agent-performance export (Updated Version)
-----------------------------------------------------------
✓ Counts every customer-facing outbound message
✓ Includes team-based metrics (createdByTeams)
✓ Uses proper field mappings per requirements
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
END_DATE   = "2025-06-01"              # inclusive
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
    page = 0
    total_pages = 1
    while page < total_pages:
        url = f"{BASE_URL}/v1/customers/search?page={page}&pageSize={PAGE_SIZE}"
        data = request_retry("POST", url, json=body, headers=HEADERS).json()
        yield from data.get("data", [])
        total_pages = data.get("meta", {}).get("totalPages", total_pages)
        print("Total pages", total_pages)
        page += 1

def search_by_day(base_body: dict, field: str):
    """Split the search into per-day 12-hour chunks to avoid large page counts."""
    results = []
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    day = timedelta(days=1)
    
    while start <= end:
        day_str = start.strftime('%Y-%m-%d')
        # First half of the day
        first_half_start = f"{day_str}T00:00:00Z"
        first_half_end = f"{day_str}T11:59:59Z"
        print(f"First half: {first_half_start} → {first_half_end}")
        
        body1 = deepcopy(base_body)
        body1["and"] = [
            {field: {"gte": first_half_start}},
            {field: {"lte": first_half_end}},
            *[c for c in base_body.get("and", []) if field not in c],
        ]
        results.extend(paginated_search(body1))
        
        # Second half of the day
        second_half_start = f"{day_str}T12:00:00Z"
        second_half_end = f"{day_str}T23:59:59Z"
        print(f"Second half: {second_half_start} → {second_half_end}")
        
        body2 = deepcopy(base_body)
        body2["and"] = [
            {field: {"gte": second_half_start}},
            {field: {"lte": second_half_end}},
            *[c for c in base_body.get("and", []) if field not in c],
        ]
        results.extend(paginated_search(body2))
        
        start += day
    return results

def dt(ts: str) -> datetime:
    """Quick ISO-8601 → datetime helper"""
    return datetime.strptime(ts.rstrip("Z"), "%Y-%m-%dT%H:%M:%S")

def get_agent_or_team_id(item):
    """Extract agent ID from createdBy or team ID from createdByTeams"""
    # Try createdBy first
    if "createdBy" in item.get("relationships", {}):
        return item["relationships"]["createdBy"]["data"]["id"], "user"
    elif "createdBy" in item:
        return item["createdBy"], "user"
    
    # Try createdByTeams
    if "createdByTeams" in item.get("relationships", {}):
        teams = item["relationships"]["createdByTeams"]["data"]
        if teams and len(teams) > 0:
            return teams[0]["id"], "team"  # Take first team
    elif "createdByTeams" in item:
        if item["createdByTeams"] and len(item["createdByTeams"]) > 0:
            return item["createdByTeams"][0], "team"
    
    return None, None

# ─────────────── USER & TEAM MAP ───────────────────────────────────────
print("Fetching users …")
user_map = {}
users_url = f"{BASE_URL}/v1/users?page=1&size=9000"
while users_url:
    chunk = request_retry("GET", users_url, headers=HEADERS).json()
    for u in chunk.get("data", []):
        a = u.get("attributes", u)
        if a.get("deletedAt") or a.get("userType") != "user":
            continue
        name = (a.get("name") or f"{a.get('firstName','')} {a.get('lastName','')}".strip()
                or a.get("email") or u["id"])
        user_map[u["id"]] = name
    users_url = chunk.get("next")
    if users_url and users_url.startswith("/"):
        users_url = BASE_URL + users_url

print("Fetching teams …")
team_map = {}
teams_url = f"{BASE_URL}/v1/teams?page=1&size=9000"
while teams_url:
    chunk = request_retry("GET", teams_url, headers=HEADERS).json()
    for t in chunk.get("data", []):
        a = t.get("attributes", t)
        if a.get("deletedAt"):
            continue
        name = a.get("name") or t["id"]
        team_map[t["id"]] = name
    teams_url = chunk.get("next")
    if teams_url and teams_url.startswith("/"):
        teams_url = BASE_URL + teams_url

print(f"✔ {len(user_map):,} human agents loaded")
print(f"✔ {len(team_map):,} teams loaded")

# ─────────────── 1) MESSAGES (outbound only) ───────────────────────────
msg_body = {
    "and": [
        {"message_created_at": {"gte": start_iso}},
        {"message_created_at": {"lte": end_iso}},
        {"auto": {"equals": "False"}},
        {"direction": {"equals": "out"}},
    ],
    "sort": [{"message_created_at": "asc"}],
    "queryContext": "message",
    "timeZone": "Europe/Amsterdam",
}

print("Fetching outbound messages …")
messages = search_by_day(msg_body, "message_created_at")
print(f"✔ {len(messages):,} messages")

# ─────────────── 2) CONVERSATIONS ──────────────────────────────────────
# For conversations marked done (using lastDone.createdAt)
conv_done_body = {
    "and": [
        {"conversation_last_done_created_at": {"gte": start_iso}},
        {"conversation_last_done_created_at": {"lte": end_iso}},
        {"deleted": {"equals": False}},
        {"conversation_message_count": {"gt": 0}},
    ],
    "sort": [{"conversation_last_done_created_at": "asc"}],
    "queryContext": "conversation",
    "timeZone": "Europe/Amsterdam",
}

# For FCR calculations (using firstDone.createdAt)
conv_first_done_body = {
    "and": [
        {"conversation_first_done_created_at": {"gte": start_iso}},
        {"conversation_first_done_created_at": {"lte": end_iso}},
        {"deleted": {"equals": False}},
        {"conversation_message_count": {"gt": 0}},
    ],
    "sort": [{"conversation_first_done_created_at": "asc"}],
    "queryContext": "conversation",
    "timeZone": "Europe/Amsterdam",
}

# For first response time (using firstResponse.createdAt)
conv_first_response_body = {
    "and": [
        {"conversation_first_response_created_at": {"gte": start_iso}},
        {"conversation_first_response_created_at": {"lte": end_iso}},
        {"deleted": {"equals": False}},
        {"conversation_message_count": {"gt": 0}},
    ],
    "sort": [{"conversation_first_response_created_at": "asc"}],
    "queryContext": "conversation",
    "timeZone": "Europe/Amsterdam",
}

print("Fetching conversations (done) …")
conv_done = search_by_day(conv_done_body, "conversation_last_done_created_at")

print("Fetching conversations (first done) …")
conv_first_done = search_by_day(conv_first_done_body, "conversation_first_done_created_at")

print("Fetching conversations (first response) …")
conv_first_response = search_by_day(conv_first_response_body, "conversation_first_response_created_at")

# ─────────────── 3) CONVERSATION TIMES ─────────────────────────────────
conv_time_body = {
    "and": [
        {"conversation_time_created_at": {"gte": start_iso}},
        {"conversation_time_created_at": {"lte": end_iso}},
    ],
    "sort": [{"conversation_time_created_at": "asc"}],
    "queryContext": "conversation_time",
    "timeZone": "Europe/Amsterdam",
}

print("Fetching conversation times …")
conv_times = search_by_day(conv_time_body, "conversation_time_created_at")
print(f"✔ {len(conv_times):,} conversation time entries")

# ─────────────── 4) USER TIME (logged in) ──────────────────────────────
# user_time_body = {
#     "and": [
#         {"docAt": {"gte": start_iso}},
#         {"docAt": {"lte": end_iso}},
#     ],
#     "sort": [{"docAt": "asc"}],
#     "queryContext": "userTime",
#     "timeZone": "Europe/Amsterdam",
# }

# print("Fetching user time …")
# user_times = search_by_day(user_time_body, "docAt")
# print(f"✔ {len(user_times):,} user time entries")

# ─────────────── METRIC BUCKETS ────────────────────────────────────────
stats = defaultdict(lambda: {
    "msgs": 0,
    "conv": set(),
    "cust": set(),
    "conv_done": 0,
    "handle_times": [],
    "resp_times": [],
    "first_resp_times": [],
    "first_resolution_times": [],
    "shortcuts": 0,
    "fcr_hits": 0,
    "fcr_total": 0,
    "login": 0.0,
    "type": "user"  # or "team"
})

# ───── Process Messages ────────────────────────────────────────────────
print("Processing messages...")
for m in messages:
    # Verify message criteria
    direction = m.get("direction") or m.get("attributes", {}).get("direction")
    auto = m.get("auto") if "auto" in m else m.get("attributes", {}).get("auto")
    
    if direction != "out" or auto:
        continue
    
    agent_id, agent_type = get_agent_or_team_id(m)
    if not agent_id:
        continue
    
    # Check if agent/team is in our maps
    if agent_type == "user" and agent_id not in user_map:
        continue
    if agent_type == "team" and agent_id not in team_map:
        continue
    
    s = stats[agent_id]
    s["type"] = agent_type
    s["msgs"] += 1
    
    # Track conversation and customer
    conv_id = (m.get("conversationId") or
               m.get("relationships", {}).get("conversation", {}).get("data", {}).get("id"))
    if conv_id:
        s["conv"].add(conv_id)
    
    cust_id = (m.get("customerId") or
               m.get("relationships", {}).get("customer", {}).get("data", {}).get("id"))
    if cust_id:
        s["cust"].add(cust_id)
    
    # Response time
    resp_bt = (m.get("responseBusinessTime") or
               m.get("attributes", {}).get("responseBusinessTime"))
    if isinstance(resp_bt, (int, float)):
        s["resp_times"].append(resp_bt)
    
    # Shortcuts
    shortcuts = (m.get("shortcuts") or
                 m.get("attributes", {}).get("shortcuts") or
                 m.get("attributes", {}).get("shortcutIds"))
    if shortcuts:
        s["shortcuts"] += 1

# ───── Process Conversations Done ──────────────────────────────────────
print("Processing conversations done...")
for c in conv_done:
    if c.get("deleted") or c.get("messageCount", 0) <= 0:
        continue
    
    # Get agent from lastDone
    last_done = c.get("lastDone", {})
    agent_id, agent_type = get_agent_or_team_id({"relationships": {"createdBy": {"data": {"id": last_done.get("createdBy")}}}}) if last_done.get("createdBy") else (None, None)
    
    if not agent_id:
        continue
    
    if agent_type == "user" and agent_id not in user_map:
        continue
    if agent_type == "team" and agent_id not in team_map:
        continue
    
    s = stats[agent_id]
    s["type"] = agent_type
    s["conv_done"] += 1

# ───── Process Conversation Times ──────────────────────────────────────
print("Processing conversation times...")
for ct in conv_times:
    agent_id, agent_type = get_agent_or_team_id(ct)
    if not agent_id:
        continue
    
    if agent_type == "user" and agent_id not in user_map:
        continue
    if agent_type == "team" and agent_id not in team_map:
        continue
    
    handle_time = ct.get("handleTime") or ct.get("attributes", {}).get("handleTime")
    if isinstance(handle_time, (int, float)):
        s = stats[agent_id]
        s["type"] = agent_type
        s["handle_times"].append(handle_time)
        

# ───── Process First Contact Resolution ────────────────────────────────
print("Processing FCR...")
for c in conv_first_done:
    # Try multiple ways to get firstDone data
    attributess = c.get("attributes", {})

    first_done = c.get("firstDone", {})
    if not first_done:
        first_done = c.get("attributes", {}).get("firstDone", {})
    
    if not first_done:
        # Try getting from firstDoneBy field directly
        first_done_by = c.get("firstDoneBy") or c.get("attributes", {}).get("firstDoneBy")
        if first_done_by:
            first_done = {"createdBy": first_done_by}
    
    if not first_done:
        continue
    
    print(first_done)
    # Get agent ID from firstDone
    agent_id = None
    agent_type = None
    
    # Try different ways to extract agent ID
    if first_done.get("createdBy"):
        agent_id = first_done["createdBy"]
        agent_type = "user"
    elif first_done.get("createdByTeams"):
        teams = first_done["createdByTeams"]
        if teams and len(teams) > 0:
            agent_id = teams[0] if isinstance(teams[0], str) else teams[0].get("id")
            agent_type = "team"
    
    if not agent_id:
        continue
    
    if agent_type == "user" and agent_id not in user_map:
        continue
    if agent_type == "team" and agent_id not in team_map:
        continue
    
    s = stats[agent_id]
    s["type"] = agent_type
    s["fcr_total"] += 1
    
    print(f"FCR Debug - Agent: {user_map.get(agent_id, team_map.get(agent_id, agent_id))}, Status: {c.get('status')}, Direction: {c.get('direction')}, MsgCount: {c.get('messageCount')}, ReopenCount: {c.get('reopenCount')}, MergedTarget: {c.get('mergedTarget')}")
    
    # Check FCR criteria
    status = c.get("status") or c.get("attributes", {}).get("status")
    direction = c.get("direction") or c.get("attributes", {}).get("direction")
    message_count = c.get("messageCount") or c.get("attributes", {}).get("messageCount", 0)
    reopen_count = c.get("reopenCount") or c.get("attributes", {}).get("reopenCount", 0)
    merged_target = c.get("mergedTarget") or c.get("attributes", {}).get("mergedTarget")
    
    if (status == "done" and
        direction == "in" and
        message_count > 0 and
        reopen_count <= 0 and
        not merged_target):
        s["fcr_hits"] += 1
        print(f"FCR HIT for {user_map.get(agent_id, team_map.get(agent_id, agent_id))}")
    
    # First resolution time
    first_done_bt = first_done.get("businessTime")
    if isinstance(first_done_bt, (int, float)):
        s["first_resolution_times"].append(first_done_bt)

# ───── Process First Response Times ────────────────────────────────────
print("Processing first response times...")
for c in conv_first_response:
    if c.get("deleted") or c.get("messageCount", 0) <= 0:
        continue
    
    first_response = c.get("firstResponse", {})
    if not first_response:
        continue
    
    agent_id, agent_type = get_agent_or_team_id({"relationships": {"createdBy": {"data": {"id": first_response.get("createdBy")}}}}) if first_response.get("createdBy") else (None, None)
    
    if not agent_id:
        continue
    
    if agent_type == "user" and agent_id not in user_map:
        continue
    if agent_type == "team" and agent_id not in team_map:
        continue
    
    first_resp_bt = first_response.get("businessTime")
    if isinstance(first_resp_bt, (int, float)):
        s = stats[agent_id]
        s["type"] = agent_type
        s["first_resp_times"].append(first_resp_bt)

# ───── Process User Time ───────────────────────────────────────────────
# print("Processing user time...")
# for ut in user_times:
#     # Check for userId or teams
#     user_id = ut.get("userId") or ut.get("relationships", {}).get("user", {}).get("data", {}).get("id")
#     teams = ut.get("teams") or ut.get("relationships", {}).get("teams", {}).get("data", [])
    
#     logged_time = ut.get("loggedIn", {}).get("timeTotal")
#     if not isinstance(logged_time, (int, float)):
#         continue
    
#     # Add to user stats
#     if user_id and user_id in user_map:
#         s = stats[user_id]
#         s["type"] = "user"
#         s["login"] += logged_time
    
#     # Add to team stats
#     for team in teams:
#         team_id = team.get("id") if isinstance(team, dict) else team
#         if team_id and team_id in team_map:
#             s = stats[team_id]
#             s["type"] = "team"
#             s["login"] += logged_time

# ─────────────── EXPORT CSV ────────────────────────────────────────────
print("Writing CSV …")
with open("agent_performance_metrics_fixed.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow([
        "Agent/Team",
        "Type",
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
    
    for agent_id, s in stats.items():
        conv_ct = len(s["conv"])
        cust_ct = len(s["cust"])
        
        # Get name based on type
        if s["type"] == "user":
            name = user_map.get(agent_id, agent_id)
        else:
            name = team_map.get(agent_id, agent_id)
        
        wr = [
            name,
            s["type"].title(),
            s["msgs"],
            conv_ct,
            s["conv_done"],
            cust_ct,
            round(mean(s["handle_times"]), 2) if s["handle_times"] else 0,
            round(s["msgs"] / conv_ct, 2) if conv_ct else 0,
            round(s["msgs"] / cust_ct, 2) if cust_ct else 0,
            round(s["fcr_hits"] / s["fcr_total"] * 100, 2) if s["fcr_total"] else 0,
            round(mean(s["resp_times"]), 2) if s["resp_times"] else 0,
            round(mean(s["first_resp_times"]), 2) if s["first_resp_times"] else 0,
            round(median(s["first_resp_times"]), 2) if s["first_resp_times"] else 0,
            round(mean(s["first_resolution_times"]), 2) if s["first_resolution_times"] else 0,
            round(median(s["first_resolution_times"]), 2) if s["first_resolution_times"] else 0,
            round(s["login"], 2),
            s["shortcuts"],
            round(s["shortcuts"] / s["msgs"] * 100, 2) if s["msgs"] else 0,
        ]
        w.writerow(wr)

print("✅  agent_performance_metrics_fixed.csv ready – now fully aligned with specifications")