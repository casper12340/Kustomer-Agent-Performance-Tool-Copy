import os
import time
import csv
from datetime import datetime
from statistics import mean, median
import requests
import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────── CONFIG ────────────────────────────────────────────────
KUSTOMER_API_KEY   = os.getenv('KUSTOMER_API_KEY')
BASE_URL  = "https://my-jewellery.api.kustomerapp.com"
START_DATE = "2025-06-01"   # inclusive
END_DATE   = "2025-06-03"   # inclusive
PAGE_SIZE  = 1000           # max items per page (Kustomer’s soft limit)
MAX_RETRIES = 3             # for 429/5xx

start_iso = f"{START_DATE}T00:00:00Z"
end_iso   = f"{END_DATE}T23:59:59Z"

HEADERS = {
    "Authorization": f"Bearer {KUSTOMER_API_KEY}",
    "Content-Type": "application/json"
}

def ensure(agent_id):
    if agent_id not in user_name_map:
        print(f"Agent ID {agent_id} not found in user_name_map.")

# ──────────────────────────── HELPERS ──────────────────────────────────
def parse_time(ts: str) -> datetime:
    """Convert Kustomer UTC ISO-8601 timestamp → datetime."""
    return datetime.strptime(ts.replace("Z", ""), "%Y-%m-%dT%H:%M:%S")

def request_with_retry(method: str, url: str, **kwargs):
    """Make an HTTP request with basic back-off on 429 or 5xx errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.request(method, url, timeout=30, **kwargs)
        if resp.status_code < 400:
            return resp
        if resp.status_code in (429, 502, 503, 504):
            wait = 2 ** attempt
            print(f"[retry {attempt}/{MAX_RETRIES}] {resp.status_code} – waiting {wait}s")
            time.sleep(wait)
            continue
        # Unrecoverable error
        raise RuntimeError(f"{method} {url} → {resp.status_code}: {resp.text}")
    raise RuntimeError(f"Gave up after {MAX_RETRIES} retries for {url}")



import copy

def paginated_search(body: dict):
    """Yield items across all pages for a /search POST using page-based pagination."""
    page = 1
    while True:
        url = f"{BASE_URL}/v1/customers/search?page={page}&pageSize={PAGE_SIZE}"
        try:
            resp = request_with_retry("POST", url, json=body, headers=HEADERS)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 404:
                print(f"404 Not Found: {url} — stopping pagination.")
                break
            else:
                raise

        data = resp.json()
        items = data.get("data", [])
        print(f"Fetched {len(items)} items from page {page}")
        yield from items

        meta = data.get("meta", {})
        total_pages = meta.get("totalPages", 1)
        print(total_pages, "total pages")
        if page >= total_pages:
            print("No more pages.")
            break
        page += 1



from datetime import datetime, timedelta

def fetch_messages_per_day(start_date: str, end_date: str):
    """
    Fetch messages from the Kustomer API for each day in the specified date range.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)

    all_messages = []

    while start <= end:
        day_start_iso = start.strftime("%Y-%m-%dT00:00:00Z")
        day_end_iso = start.strftime("%Y-%m-%dT23:59:59Z")

        message_body = {
            "and": [
                {"conversation_updated_at": {"gte": day_start_iso}},
                {"conversation_updated_at": {"lte": day_end_iso}},
                { "conversation_direction": { "equals": "out" } }
            ],
            "sort": [{"conversation_updated_at": "asc"}],
            "queryContext": "message",
            "timeZone": "Europe/Amsterdam"
        }

        print(f"Fetching messages for {start.strftime('%Y-%m-%d')}...")
        messages = list(paginated_search(message_body))
        all_messages.extend(messages)

        start += delta

    return all_messages




def fetch_conversations_done_per_day(start_date: str, end_date: str):
    """
    Fetch conversations marked as done from the Kustomer API for each day in the specified date range.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)

    all_conversations_done = []

    while start <= end:
        day_start_iso = start.strftime("%Y-%m-%dT00:00:00Z")
        day_end_iso = start.strftime("%Y-%m-%dT23:59:59Z")

        conv_done_body = {
            "and": [
                {"conversation_last_done_created_at": {"gte": day_start_iso}},
                {"conversation_last_done_created_at": {"lte": day_end_iso}}
            ],
            "sort": [{"conversation_last_done_created_at": "asc"}],
            "queryContext": "conversation",
            "timeZone": "Europe/Amsterdam"
        }

        print(f"Fetching conversations marked as done for {start.strftime('%Y-%m-%d')}...")
        conversations_done = list(paginated_search(conv_done_body))
        all_conversations_done.extend(conversations_done)

        start += delta

    return all_conversations_done








def paginated_get(url: str, params: dict):
    """Yield items across all pages for a GET request using cursor or page-based pagination."""
    while True:
        resp = request_with_retry("GET", url, params=params, headers=HEADERS)
        data = resp.json()
        yield from data.get("data", [])

        # Cursor-based pagination
        cursor = data.get("cursor", {}).get("after")
        if cursor:
            params["after"] = cursor
            continue

        # Page-based pagination
        next_link = data.get("links", {}).get("next")
        if next_link:
            # If 'next' is a full URL, use it directly; otherwise, construct the full URL
            if next_link.startswith("http"):
                url = next_link
                params = {}  # Reset params if URL already contains query parameters
            else:
                url = BASE_URL + next_link
                params = {}  # Reset params if URL already contains query parameters
            continue

        # No more pages
        break




# ─────────────────────────── PULL USERS ────────────────────────────────
print("Fetching users …")
users = []
next_url = f"{BASE_URL}/v1/users?page=1&size=9000"
while next_url:
    resp = request_with_retry("GET", next_url, headers=HEADERS)
    chunk = resp.json()
    users.extend(chunk.get("data", []))
    next_url = chunk.get("next")
    if next_url and next_url.startswith("/"):
        next_url = BASE_URL + next_url

user_name_map = {}
for u in users:
    attrs = u.get("attributes", u)
    name = (
        attrs.get("name") or
        f"{attrs.get('firstName','')} {attrs.get('lastName','')}".strip() or
        attrs.get("email") or
        attrs["id"]
    )
    if attrs.get("deletedAt") or attrs.get('userType') != "user":
        print(attrs.get('name'))
    else:
        user_name_map[u["id"]] = name
        













# ──────────────────────── SEARCH DEFINITIONS ───────────────────────────

message_params = {
    "pageSize": 100000,
    "sort": "createdAt",
    "order": "asc",
    "createdAt[gte]": start_iso,
    "createdAt[lte]": end_iso
}


conv_done_body = {
    "and": [
        {"lastDoneAt": {"gte": start_iso}},
        {"lastDoneAt": {"lte": end_iso}}
    ],
    "sort": [{"lastDoneAt": "asc"}],
    "queryContext": "conversation"
}

conv_created_body = {
    "and": [
        {"conversation_updated_at": {"gte": start_iso}},
        {"conversation_updated_at": {"lte": end_iso}}
    ],
    "sort": [{"conversation_updated_at": "asc"}],
    "queryContext": "conversation"
}

# ─────────────────────────── FETCH DATA ────────────────────────────────
# print("Fetching messages …")
# messages = list(paginated_search(message_body))

print("Fetching messages …")
# messages = list(paginated_get(f"{BASE_URL}/v1/messages", message_params))
START_DATE = "2025-06-01"
END_DATE = "2025-06-03"

messages = fetch_messages_per_day(START_DATE, END_DATE)

print("Fetching conversations (done) …")
# conversations_done = list(paginated_search(conv_done_body))
conversations_done = fetch_conversations_done_per_day(START_DATE, END_DATE)


print("Fetching conversations (created) …")
conversations_created = list(paginated_search(conv_created_body))

# Merge created + done conversations for fast lookup
conv_data = {}
for c in conversations_created + conversations_done:
    if c_id := c.get("id"):
        conv_data.setdefault(c_id, {}).update(c)

# ──────────────────────── METRIC AGGREGATION ───────────────────────────
agent_stats = {}
for agent_id in user_name_map:
    agent_stats[agent_id] = {
        "messages_sent": 0,
        "unique_conversations": set(),
        "unique_customers": set(),
        "conversations_done": 0,
        "conv_created_done": 0,
        "total_handle_time": 0.0,
        "first_response_times": [],
        "first_resolution_times": [],
        "messages_shortcut": 0,
        "fcr_count": 0
    }


# ----- 1. Process messages -----
for m in messages:
    direction = (
        m.get("attributes", {}).get("direction") or
        m.get("direction")
    )
    if direction != "out":
        continue  # only agent-sent

    # agent ID
    sender = (
        m.get("relationships", {}).get("createdBy", {}).get("data", {}).get("id") or
        m.get("createdById") or
        m.get("ownerId")
    )
    if not sender or sender not in user_name_map:
        continue  
    ensure(sender)

    stats = agent_stats[sender]
    stats["messages_sent"] += 1

    conv_id = (
        m.get("conversationId") or
        m.get("relationships", {}).get("conversation", {}).get("data", {}).get("id")
    )
    if conv_id:
        stats["unique_conversations"].add(conv_id)
        cust_id = conv_data.get(conv_id, {}).get("customerId") or \
                  m.get("relationships", {}).get("customer", {}).get("data", {}).get("id")
        if cust_id:
            stats["unique_customers"].add(cust_id)

    # shortcut?
    if (
        m.get("attributes", {}).get("shortcutIds") or
        m.get("shortcuts") or
        m.get("shortcutId") or
        m.get("via") == "shortcut"
    ):
        stats["messages_shortcut"] += 1

# ----- 2. Conversation resolutions -----
created_ids = {c["id"] for c in conversations_created if c.get("id")}
for c in conversations_done:
    c_id = c.get("id")
    agent_id = (
        c.get("lastDoneById") or
        c.get("relationships", {}).get("lastDoneBy", {}).get("data", {}).get("id")
    )
    if not agent_id:
        continue
    ensure(agent_id)
    stats = agent_stats[agent_id]
    stats["conversations_done"] += 1
    if c_id in created_ids:
        stats["conv_created_done"] += 1

    handle_time = c.get("handleTime") or c.get("attributes", {}).get("handleTime")
    if isinstance(handle_time, (int, float)):
        stats["total_handle_time"] += handle_time

# ----- 3. First response & first resolution -----
for c in conversations_created:
    c_id = c.get("id")
    if not c_id:
        continue

    conv_msgs = [
        m for m in messages
        if (m.get("conversationId") or
            m.get("relationships", {}).get("conversation", {}).get("data", {}).get("id")) == c_id
    ]
    if not conv_msgs:
        continue
    conv_msgs.sort(key=lambda x: x.get("attributes", {}).get("createdAt") or x.get("createdAt"))
    first_msg = conv_msgs[0]
    if (first_msg.get("attributes", {}).get("direction") or first_msg.get("direction")) == "out":
        continue  # outbound convo, skip FRT

    # find first agent response
    first_agent = next((m for m in conv_msgs
                        if (m.get("attributes", {}).get("direction") or m.get("direction")) == "out"), None)
    if not first_agent:
        continue

    # compute FRT
    try:
        frt = (
            parse_time(first_agent["attributes"].get("createdAt") or first_agent["createdAt"]) -
            parse_time(first_msg["attributes"].get("createdAt") or first_msg["createdAt"])
        ).total_seconds()
    except Exception:
        frt = None

    responder = (
        first_agent.get("relationships", {}).get("createdBy", {}).get("data", {}).get("id") or
        first_agent.get("createdById") or
        first_agent.get("ownerId")
    )
    if responder and frt is not None:
        ensure(responder)
        agent_stats[responder]["first_response_times"].append(frt)

    # first resolution time
    if c.get("firstDoneAt") and c.get("firstDoneById"):
        try:
            frt_res = (
                parse_time(c["firstDoneAt"]) -
                parse_time(c["createdAt"])
            ).total_seconds()
        except Exception:
            frt_res = None
        if frt_res is not None:
            resolver = c["firstDoneById"]
            ensure(resolver)
            stats = agent_stats[resolver]
            stats["first_resolution_times"].append(frt_res)

            # FCR check
            if c.get("lastDoneAt") == c.get("firstDoneAt") and \
               c.get("lastDoneById") == resolver:
                stats["fcr_count"] += 1

# ──────────────────────────── CSV OUTPUT ───────────────────────────────
print("Writing CSV …")
with open("agent_performance_metrics.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
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
        "Percent of messages sent with shortcuts (%)"
    ])

    for aid, s in agent_stats.items():
        conv_ct  = len(s["unique_conversations"])
        cust_ct  = len(s["unique_customers"])
        writer.writerow([
            user_name_map.get(aid, aid),
            s["messages_sent"],
            conv_ct,
            s["conversations_done"],
            cust_ct,
            round(s["total_handle_time"] / s["conversations_done"], 2) if s["conversations_done"] else 0,
            round(s["messages_sent"] / conv_ct, 2) if conv_ct else 0,
            round(s["messages_sent"] / cust_ct, 2) if cust_ct else 0,
            round(s["fcr_count"] / s["conv_created_done"] * 100, 2) if s["conv_created_done"] else 0,
            round(mean(s["first_response_times"]), 2) if s["first_response_times"] else 0,
            round(median(s["first_response_times"]), 2) if s["first_response_times"] else 0,
            round(mean(s["first_resolution_times"]), 2) if s["first_resolution_times"] else 0,
            round(median(s["first_resolution_times"]), 2) if s["first_resolution_times"] else 0,
            s["messages_shortcut"],
            round(s["messages_shortcut"] / s["messages_sent"] * 100, 2) if s["messages_sent"] else 0
        ])

print("✓ agent_performance_metrics.csv created")