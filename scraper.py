"""
LeafLink → sales_data.json scraper for Chill Medicated New Jersey.

Pulls orders + line items from the LeafLink V2 API and writes a
sales_data.json file in the same shape the (existing Apex-based)
dashboard already expects. The dashboard.html in this repo doesn't
know or care that the data came from LeafLink instead of Apex.

Architecture
------------
1. Authenticate using LEAFLINK_TOKEN from .env (header: `Authorization: Token <key>`)
2. Paginate GET /orders-received/?include_children=line_items,customer,sales_reps
3. For each order:
     - Filter to New Jersey (delivery_address.state == "NJ")
     - For each line item in that order, emit one row in the output
       array with normalized field names matching the Apex shape
4. Write sales_data.json atomically (write to .tmp then rename) so
   the dashboard never reads a half-finished file mid-refresh

Usage
-----
    python scraper.py

Environment variables (set in .env):
    LEAFLINK_TOKEN       (required)  API token from Developer Options
    LEAFLINK_DAYS_BACK   (optional)  how many days of history to pull, default 365
    LEAFLINK_PAGE_SIZE   (optional)  results per page, default 100
    LEAFLINK_STATE       (optional)  state code to filter to, default "NJ"
    LEAFLINK_DOMAIN      (optional)  API host, default app.leaflink.com
                                     (use www.sandbox.leaflink.com for sandbox)
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import requests
from dotenv import load_dotenv

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #

load_dotenv()

TOKEN = os.environ.get("LEAFLINK_TOKEN", "").strip()
DOMAIN = os.environ.get("LEAFLINK_DOMAIN", "app.leaflink.com").strip()
STATE_FILTER = os.environ.get("LEAFLINK_STATE", "NJ").strip().upper()
DAYS_BACK = int(os.environ.get("LEAFLINK_DAYS_BACK", "365"))
PAGE_SIZE = int(os.environ.get("LEAFLINK_PAGE_SIZE", "100"))
# Optional absolute start date. If set, overrides DAYS_BACK.
# Format: YYYY-MM-DD (e.g. "2025-05-01"). Useful for "everything since launch."
START_DATE = os.environ.get("LEAFLINK_START_DATE", "").strip()
# Optional brand name filter (case-insensitive substring match against the resolved
# brand name). Useful when the seller account sells multiple brands and you only
# want one. Set "" to disable.
BRAND_FILTER = os.environ.get("LEAFLINK_BRAND_FILTER", "").strip()

BASE_URL = f"https://{DOMAIN}/api/v2"
OUTPUT_PATH = Path(__file__).parent / "sales_data.json"

# Polite request pacing. The docs don't specify limits, but a small sleep
# between pages keeps us well under any reasonable rate cap.
SLEEP_BETWEEN_PAGES_SEC = 0.4
REQUEST_TIMEOUT_SEC = 30
MAX_RETRIES = 3
BACKOFF_BASE_SEC = 2.0


# --------------------------------------------------------------------------- #
# HTTP helpers                                                                #
# --------------------------------------------------------------------------- #

def _session() -> requests.Session:
    if not TOKEN:
        sys.exit(
            "ERROR: LEAFLINK_TOKEN is not set. Create a .env file with your\n"
            "API token (see .env.example). You can generate one in LeafLink at\n"
            "Developer Options."
        )
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Token {TOKEN}",
        "Accept": "application/json",
        "User-Agent": "chill-medicated-nj-dashboard/1.0",
    })
    return s


def _get(session: requests.Session, path: str, params: dict | None = None) -> dict:
    """GET with retries + helpful error messages."""
    url = f"{BASE_URL}{path}"
    if not url.endswith("/") and "?" not in url:
        url += "/"  # LeafLink requires trailing slash on paths
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, params=params or {}, timeout=REQUEST_TIMEOUT_SEC)
        except requests.RequestException as e:
            last_exc = e
            time.sleep(BACKOFF_BASE_SEC * (2 ** attempt))
            continue

        if r.status_code == 200:
            return r.json()
        if r.status_code == 401:
            sys.exit(
                "ERROR: LeafLink rejected the request (status 401).\n"
                "Your API token is missing, invalid, or expired.\n"
                "Generate a new one in LeafLink → Developer Options and update .env."
            )
        if r.status_code == 403:
            sys.exit(
                "ERROR: LeafLink returned 403 (forbidden).\n"
                "The token exists but lacks permission for this endpoint.\n"
                "Confirm the user has 'Manage Orders Received' permission."
            )
        if r.status_code == 429:
            # Rate limited — back off and try again
            wait = BACKOFF_BASE_SEC * (2 ** attempt)
            print(f"  rate-limited; waiting {wait}s and retrying...", file=sys.stderr)
            time.sleep(wait)
            continue
        if 500 <= r.status_code < 600:
            wait = BACKOFF_BASE_SEC * (2 ** attempt)
            print(f"  server error {r.status_code}; retrying in {wait}s...", file=sys.stderr)
            time.sleep(wait)
            continue
        # Anything else — fail hard with the body so we can debug
        sys.exit(
            f"ERROR: LeafLink returned unexpected status {r.status_code}\n"
            f"URL: {url}\nResponse: {r.text[:500]}"
        )
    sys.exit(f"ERROR: gave up after {MAX_RETRIES} retries. Last error: {last_exc}")


def paginate(session: requests.Session, path: str, params: dict) -> Iterator[dict]:
    """Yield each result across all pages of a paginated endpoint."""
    p = dict(params)
    p.setdefault("limit", PAGE_SIZE)
    offset = 0
    page = 1
    while True:
        p["offset"] = offset
        data = _get(session, path, p)
        results = data.get("results", [])
        total = data.get("count", 0)
        print(f"  page {page}: {len(results)} results (offset {offset} / total {total})")
        for row in results:
            yield row
        if not data.get("next") or len(results) == 0:
            return
        offset += len(results)
        page += 1
        time.sleep(SLEEP_BETWEEN_PAGES_SEC)


# --------------------------------------------------------------------------- #
# Normalization                                                               #
# --------------------------------------------------------------------------- #

def _money_to_cents(m: Any) -> int:
    """LeafLink money objects look like {amount: 594, currency: "USD"}.
    Some endpoints return amount as number, some as string. Always return cents (int)."""
    if m is None:
        return 0
    if isinstance(m, dict):
        amount = m.get("amount", 0)
    else:
        amount = m
    try:
        return int(round(float(amount) * 100))
    except (TypeError, ValueError):
        return 0


def _to_decimal(v: Any) -> float:
    """Parse a string/number into a float safely."""
    if v is None or v == "":
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _safe_state(addr: dict | None) -> str:
    """Extract 2-letter state code from a delivery/corporate address."""
    if not isinstance(addr, dict):
        return ""
    raw = (addr.get("state") or "").strip().upper()
    # LeafLink stores 2-letter codes directly (CO, NJ, etc.)
    return raw if len(raw) == 2 else raw[:2]


def _sales_rep_id(reps: list | None) -> int | None:
    """Pick the first sales rep ID from the order's sales_reps array.
    Handles both shapes:
      - [{id: 13, user: "John Doe"}, ...] (expanded)
      - [13, ...] (just IDs)
      - [{id: 13}, ...] (partial)
    """
    if not reps:
        return None
    first = reps[0]
    if isinstance(first, dict):
        return first.get("id")
    if isinstance(first, int):
        return first
    return None


def _sales_rep_name_from_expanded(reps: list | None) -> str:
    """Pick the first sales rep name if it's in the expanded form.
    Returns empty string if reps array contains only IDs."""
    if not reps:
        return ""
    first = reps[0]
    if isinstance(first, dict):
        return first.get("user") or first.get("name") or ""
    return ""


def _credits_to_payment_dates(credits_str: str, paid_date: str | None) -> list:
    """The MA dashboard parses credits out of `payment_dates` strings like
    'Credit of $1,045.22 on 03/17/2026'. We reconstruct the same format
    from LeafLink's `credits` (a string dollar amount on the order)."""
    amount = _to_decimal(credits_str)
    if amount <= 0:
        return []
    # Use paid_date if available; otherwise leave the date blank
    when = "—"
    if paid_date:
        try:
            d = datetime.fromisoformat(paid_date.replace("Z", "+00:00"))
            when = d.strftime("%m/%d/%Y")
        except (ValueError, AttributeError):
            when = paid_date[:10] if isinstance(paid_date, str) else "—"
    return [f"Credit of ${amount:,.2f} on {when}"]


def _compute_line_total_cents(line_item: dict, order_discount_dollars: float,
                              order_discount_type: str, order_subtotal_dollars: float) -> tuple[int, int]:
    """
    Compute the line item's (line_total_cents, discount_cents) given:
      - line item's quantity × sale_price (or ordered_unit_price if no sale_price)
      - the order's order-level discount, allocated proportionally across lines

    LeafLink's order has ONE discount field (either % or $) that applies to the
    whole order. The MA dashboard's data shape expects per-line discounts. So we
    allocate the order discount across lines proportionally by line subtotal.
    """
    qty = _to_decimal(line_item.get("quantity"))
    sale = line_item.get("sale_price")
    ordered = line_item.get("ordered_unit_price")
    # Prefer sale_price if it's nonzero; fall back to ordered_unit_price
    sale_cents = _money_to_cents(sale)
    ordered_cents = _money_to_cents(ordered)
    unit_cents = sale_cents if sale_cents > 0 else ordered_cents
    line_subtotal_cents = int(round(unit_cents * qty))

    # Allocate this order's discount to this line, proportional to subtotal
    if order_discount_dollars <= 0 or order_subtotal_dollars <= 0:
        discount_cents = 0
    else:
        share = (line_subtotal_cents / 100.0) / order_subtotal_dollars
        if order_discount_type == "%":
            # Percentage discount: applies uniformly, so per-line discount =
            # line_subtotal × pct
            line_disc_dollars = (line_subtotal_cents / 100.0) * (order_discount_dollars / 100.0)
        else:
            # Flat dollar discount: divide proportionally
            line_disc_dollars = order_discount_dollars * share
        discount_cents = int(round(line_disc_dollars * 100))

    # Net line total = subtotal − discount
    line_total_cents = max(0, line_subtotal_cents - discount_cents)
    return line_total_cents, discount_cents


def _order_subtotal_dollars(order: dict) -> float:
    """Compute the pre-discount subtotal of an order from its line items.
    Used to allocate the order-level discount across lines."""
    subtotal = 0.0
    for li in order.get("line_items") or []:
        qty = _to_decimal(li.get("quantity"))
        sale_cents = _money_to_cents(li.get("sale_price"))
        ordered_cents = _money_to_cents(li.get("ordered_unit_price"))
        unit_cents = sale_cents if sale_cents > 0 else ordered_cents
        subtotal += (unit_cents / 100.0) * qty
    return subtotal


def normalize_order_to_rows(order: dict) -> list[dict]:
    """Convert one LeafLink order (with embedded line_items) into a list of
    dashboard-shaped rows, one per line item.
    Returns [] if the order doesn't match the state filter."""
    delivery_addr = order.get("delivery_address") or {}
    state = _safe_state(delivery_addr)
    if STATE_FILTER and state != STATE_FILTER:
        return []

    # Order-level fields used on every row
    order_num = order.get("number") or order.get("order_number") or ""
    short_id = order.get("short_id") or order.get("order_short_number") or ""
    created_on = order.get("created_on") or ""
    ship_date = order.get("ship_date") or ""
    paid_date = order.get("paid_date") or ""
    status = order.get("status") or ""
    classification = order.get("classification") or ""

    # `customer` field can be either:
    #   - a full dict {id, display_name, ...} (when include_children expands it)
    #   - an integer ID alone (when it doesn't)
    # We capture both the name (if available) and the ID for later /customers/{id}/ lookup.
    customer_raw = order.get("customer")
    if isinstance(customer_raw, dict):
        customer_id = customer_raw.get("id")
        buyer_name = customer_raw.get("display_name") or ""
    elif isinstance(customer_raw, int):
        customer_id = customer_raw
        buyer_name = ""  # will be filled by enrichment pass
    else:
        customer_id = None
        buyer_name = ""

    # `sales_reps` field similarly comes in two shapes.
    sales_rep_id = _sales_rep_id(order.get("sales_reps") or [])
    sales_rep = _sales_rep_name_from_expanded(order.get("sales_reps") or [])

    payment_dates = _credits_to_payment_dates(order.get("credits") or "0", paid_date)

    # Order-level discount allocation context
    discount_dollars = _to_decimal(order.get("discount"))
    discount_type = (order.get("discount_type") or "$").strip()
    order_subtotal = _order_subtotal_dollars(order)

    rows = []
    for li in order.get("line_items") or []:
        qty = _to_decimal(li.get("quantity"))
        if qty <= 0 and not li.get("is_sample"):
            continue
        line_total_cents, discount_cents = _compute_line_total_cents(
            li, discount_dollars, discount_type, order_subtotal
        )
        sale_cents = _money_to_cents(li.get("sale_price"))
        ordered_cents = _money_to_cents(li.get("ordered_unit_price"))
        unit_cents = sale_cents if sale_cents > 0 else ordered_cents

        # Pull product display info. LeafLink returns `product` as either an id
        # (string) or, when expanded, a full object. The orders endpoint with
        # include_children=line_items returns it as id only. We carry a placeholder
        # name and resolve real names in a follow-up pass.
        product_id = li.get("product")
        product_name = li.get("product_name") or f"Product #{product_id}" if product_id else "—"

        # Normalize dates: strip fractional seconds from ISO timestamps so the
        # dashboard's JS Date parser handles them cleanly.
        # "2026-05-26T10:16:33.250708-04:00"  →  "2026-05-26T10:16:33-04:00"
        clean_created = _strip_microseconds(created_on)
        clean_ship = _strip_microseconds(ship_date)

        rows.append({
            # Identity
            "order_id": order_num,           # canonical key for invoice grouping
            "order_number": short_id or order_num,
            "number": order_num,
            # Dates (raw + iso variants — dashboard uses the most-specific available)
            "order_date_utc": clean_created,
            "order_date_utc_raw": clean_created,
            "order_date_localized": clean_created,
            "order_date_localized_raw": clean_created,
            "order_date": _fmt_date(clean_created),
            "order_date_raw": clean_created,
            "delivery_date": _fmt_date(clean_ship),
            "delivery_date_raw": clean_ship,
            "paid_date": paid_date,
            # Buyer
            "buyer_name": buyer_name,
            "buyer_state": state,
            "buyer_city": delivery_addr.get("city") or "",
            "buyer_zip": delivery_addr.get("zipcode") or "",
            # Product
            "product_id": product_id,
            "product_name": product_name,
            "product_brand": "",  # filled by enrichment pass if available
            "brand": "",          # ditto
            "product_type": "",   # ditto (category name)
            # Quantity & money (all monetary fields in CENTS for *_raw variants)
            "quantity_raw": qty,
            "quantity": qty,
            "order_quantity": qty,
            "unit_multiplier": li.get("unit_multiplier") or 1,
            "line_total_raw": line_total_cents,
            "line_total": f"${line_total_cents/100:,.2f}",
            "computed_sale_price": f"${unit_cents/100:,.2f}",
            "product_listing_price": f"${ordered_cents/100:,.2f}",
            "unit_price": f"${unit_cents/100:,.2f}",
            "discounts": discount_cents,  # integer cents — matches Apex shape
            "additional_discounts": 0,    # LeafLink has only one discount field
            "batch_cost_of_goods": "",    # not exposed by LeafLink — modeled at dashboard
            # Order state
            "order_status": status,
            "payment_status": status,
            "classification": classification,
            "sales_rep": sales_rep,
            "sales_reps": sales_rep,
            "sales_reps_display": sales_rep,
            # Credits (in the format the dashboard's payment_dates parser expects)
            "payment_dates": payment_dates,
            # Misc passthrough
            "is_sample": bool(li.get("is_sample")),
            "notes": (li.get("notes") or order.get("notes") or "")[:500],
            # Private fields used by enrichment pass — stripped before final write
            "_customer_id": customer_id,
            "_sales_rep_id": sales_rep_id,
        })
    return rows


def _strip_microseconds(iso: str) -> str:
    """Strip microseconds from an ISO timestamp so JS Date parsers don't choke.
    '2026-05-26T10:16:33.250708-04:00' → '2026-05-26T10:16:33-04:00'"""
    if not iso or not isinstance(iso, str):
        return iso
    # Find the position of the dot in the time portion (after the 'T')
    t_idx = iso.find("T")
    if t_idx < 0:
        return iso
    dot_idx = iso.find(".", t_idx)
    if dot_idx < 0:
        return iso
    # Find the next non-digit character after the dot (the timezone separator)
    end_idx = dot_idx + 1
    while end_idx < len(iso) and iso[end_idx].isdigit():
        end_idx += 1
    return iso[:dot_idx] + iso[end_idx:]


def _fmt_date(iso: str) -> str:
    """Format an ISO datetime into MM/DD/YYYY for human-readable date columns."""
    if not iso:
        return ""
    try:
        d = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return d.strftime("%m/%d/%Y")
    except (ValueError, AttributeError):
        return iso[:10] if isinstance(iso, str) else ""


# --------------------------------------------------------------------------- #
# Product enrichment                                                          #
# --------------------------------------------------------------------------- #

def fetch_product_lookup(session: requests.Session, product_ids: set) -> dict:
    """Fetch product display info for a set of product ids and return a
    dict: id -> {name, brand, category_name, ...}. Done in a separate pass
    so we batch the requests instead of N per-order calls."""
    if not product_ids:
        return {}
    print(f"Resolving {len(product_ids)} unique product(s)...")
    lookup: dict = {}
    # LeafLink's products endpoint accepts comma-separated ids via the `id` filter
    ids_list = sorted(p for p in product_ids if p)
    BATCH = 50
    for i in range(0, len(ids_list), BATCH):
        batch = ids_list[i:i + BATCH]
        params = {"id": ",".join(str(x) for x in batch), "limit": BATCH}
        try:
            data = _get(session, "/products/", params)
        except SystemExit:
            # Don't kill the whole run if products endpoint fails — just skip enrichment
            print("  warning: product lookup failed; product names will show as IDs", file=sys.stderr)
            return lookup
        for prod in data.get("results", []):
            pid = prod.get("id")
            if pid is None:
                continue
            lookup[pid] = {
                "name": prod.get("display_name") or prod.get("name") or f"Product #{pid}",
                "brand_id": prod.get("brand"),
                "category_id": prod.get("category"),
                "sub_category_id": prod.get("sub_category"),
                "sku": prod.get("sku") or "",
                "wholesale_price_cents": _money_to_cents(prod.get("wholesale_price")),
            }
        time.sleep(SLEEP_BETWEEN_PAGES_SEC)
    return lookup


def fetch_brand_lookup(session: requests.Session, brand_ids: set) -> dict:
    """Resolve brand IDs to human-readable names. id -> name."""
    if not brand_ids:
        return {}
    print(f"Resolving {len(brand_ids)} brand(s)...")
    lookup: dict = {}
    for bid in sorted(b for b in brand_ids if b):
        try:
            data = _get(session, f"/brands/{bid}/", {})
        except SystemExit:
            print(f"  warning: brand {bid} lookup failed; will show as blank", file=sys.stderr)
            continue
        name = data.get("name") or data.get("display_name") or ""
        if name:
            lookup[bid] = name
        time.sleep(SLEEP_BETWEEN_PAGES_SEC / 2)
    return lookup


def fetch_category_lookup(session: requests.Session) -> dict:
    """Fetch the full product-categories list. id -> name.
    Categories are a small fixed set, so we pull all of them once."""
    print("Resolving product categories...")
    lookup: dict = {}
    try:
        # No batching needed — usually < 20 categories
        first_logged = False
        for cat in paginate(session, "/product-categories/", {"limit": PAGE_SIZE}):
            if not first_logged:
                print(f"  DEBUG category response keys: {sorted(cat.keys())}", file=sys.stderr)
                print(f"  DEBUG category sample: {json.dumps(cat, indent=2)[:500]}", file=sys.stderr)
                first_logged = True
            cid = cat.get("id")
            # Try every plausible field name
            name = (cat.get("name") or cat.get("display_name") or
                    cat.get("category_name") or cat.get("label") or
                    cat.get("title") or "")
            if cid and name:
                lookup[cid] = name
    except SystemExit:
        print("  warning: category lookup failed; category column will be blank", file=sys.stderr)
    print(f"  Category lookup resolved {len(lookup)} name(s)")
    return lookup


def fetch_customer_lookup(session: requests.Session, customer_ids: set) -> dict:
    """Resolve customer IDs to display names. id -> display_name.
    Customers are usually a small set (one row per dispensary), so individual lookups
    work fine. The /customers/{id}/ endpoint requires a single ID per call."""
    if not customer_ids:
        return {}
    ids = sorted(c for c in customer_ids if c)
    print(f"Resolving {len(ids)} customer(s)...")
    lookup: dict = {}
    first_logged = False
    for cid in ids:
        try:
            data = _get(session, f"/customers/{cid}/", {})
        except SystemExit:
            # Don't kill the whole run; print and continue
            print(f"  warning: customer {cid} lookup failed; will show as blank", file=sys.stderr)
            continue
        if not first_logged:
            print(f"  DEBUG customer response keys: {sorted(data.keys())}", file=sys.stderr)
            print(f"  DEBUG customer sample: {json.dumps(data, indent=2)[:800]}", file=sys.stderr)
            first_logged = True
        # Try every plausible field name LeafLink might use for the customer's name
        name = (data.get("display_name") or data.get("name") or
                data.get("company_name") or data.get("nickname") or
                data.get("dba_name") or "")
        # Some endpoints nest the name under "company" or "buyer"
        if not name and isinstance(data.get("company"), dict):
            name = data["company"].get("name") or data["company"].get("display_name") or ""
        if not name and isinstance(data.get("buyer"), dict):
            name = data["buyer"].get("name") or data["buyer"].get("display_name") or ""
        if name:
            lookup[cid] = name
        time.sleep(SLEEP_BETWEEN_PAGES_SEC / 2)
    print(f"  Customer lookup resolved {len(lookup)} of {len(ids)} name(s)")
    return lookup


def fetch_sales_rep_lookup(session: requests.Session, rep_ids: set) -> dict:
    """Resolve sales rep user IDs to names. id -> display name.
    Sales reps are accessed via /company-staff/{id}/. Usually a tiny set
    (you only have a handful of reps), so individual GETs are fine."""
    if not rep_ids:
        return {}
    ids = sorted(r for r in rep_ids if r)
    print(f"Resolving {len(ids)} sales rep(s)...")
    lookup: dict = {}
    first_logged = False
    for rid in ids:
        try:
            data = _get(session, f"/company-staff/{rid}/", {})
        except SystemExit:
            print(f"  warning: sales rep {rid} lookup failed; will show as blank", file=sys.stderr)
            continue
        if not first_logged:
            print(f"  DEBUG company-staff response keys: {sorted(data.keys())}", file=sys.stderr)
            print(f"  DEBUG company-staff sample: {json.dumps(data, indent=2)[:800]}", file=sys.stderr)
            first_logged = True
        # Try every plausible field combination
        name = ""
        # First, try first+last name combinations
        first = (data.get("user_first_name") or data.get("first_name") or
                 data.get("firstName") or "")
        last = (data.get("user_last_name") or data.get("last_name") or
                data.get("lastName") or "")
        if first or last:
            name = f"{first} {last}".strip()
        # If still nothing, look for a nested user object
        if not name and isinstance(data.get("user"), dict):
            u = data["user"]
            ufirst = u.get("first_name") or u.get("firstName") or ""
            ulast = u.get("last_name") or u.get("lastName") or ""
            if ufirst or ulast:
                name = f"{ufirst} {ulast}".strip()
            if not name:
                name = u.get("full_name") or u.get("display_name") or u.get("username") or u.get("email") or ""
        # Last resort: scalar user field or other names
        if not name:
            user_val = data.get("user")
            if isinstance(user_val, str):
                name = user_val
        if not name:
            name = (data.get("full_name") or data.get("display_name") or
                    data.get("name") or data.get("username") or
                    data.get("email") or "")
        if name:
            lookup[rid] = name
        time.sleep(SLEEP_BETWEEN_PAGES_SEC / 2)
    print(f"  Sales rep lookup resolved {len(lookup)} of {len(ids)} name(s)")
    return lookup


# --------------------------------------------------------------------------- #
# Main                                                                        #
# --------------------------------------------------------------------------- #

def main() -> None:
    started = time.time()
    session = _session()

    # Date window. START_DATE (absolute) overrides DAYS_BACK (rolling) if set.
    if START_DATE:
        # Pad with time so the API gets a full ISO datetime
        since = f"{START_DATE}T00:00:00+00:00"
        window_desc = f"since {START_DATE} (absolute)"
    else:
        since = (datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)).isoformat()
        window_desc = f"since {since[:10]} (last {DAYS_BACK} days)"
    print(f"Pulling orders from LeafLink ({DOMAIN}) {window_desc}...")
    print(f"Filtering to state: {STATE_FILTER or '(all)'}")
    if BRAND_FILTER:
        print(f"Filtering to brand:  {BRAND_FILTER}")

    # Step 1 — pull orders with embedded line items, customer, sales_reps
    order_params = {
        "include_children": "line_items,customer,sales_reps",
        "created_on__gte": since,
        # status filter — we want everything except draft (drafts aren't real sales)
        "status__not": "draft,cancelled,rejected",
    }
    raw_rows: list[dict] = []
    seen_product_ids: set = set()
    seen_customer_ids: set = set()
    seen_rep_ids: set = set()
    orders_seen = 0
    orders_kept = 0

    for order in paginate(session, "/orders-received/", order_params):
        orders_seen += 1
        rows = normalize_order_to_rows(order)
        if rows:
            orders_kept += 1
            raw_rows.extend(rows)
            for r in rows:
                if r["product_id"]:
                    seen_product_ids.add(r["product_id"])
                if r.get("_customer_id"):
                    seen_customer_ids.add(r["_customer_id"])
                if r.get("_sales_rep_id"):
                    seen_rep_ids.add(r["_sales_rep_id"])

    print(f"Fetched {orders_seen} order(s); kept {orders_kept} in state {STATE_FILTER}.")
    print(f"Produced {len(raw_rows)} line-item row(s).")

    # Step 2 — enrich product names
    product_lookup = fetch_product_lookup(session, seen_product_ids)
    # Collect brand IDs from products so we can resolve brand names
    brand_ids = {info["brand_id"] for info in product_lookup.values() if info.get("brand_id")}

    # Step 3 — enrich category, brand, customer, and sales-rep names
    category_lookup = fetch_category_lookup(session)
    brand_lookup = fetch_brand_lookup(session, brand_ids)
    customer_lookup = fetch_customer_lookup(session, seen_customer_ids)
    sales_rep_lookup = fetch_sales_rep_lookup(session, seen_rep_ids)

    # Step 4 — apply enrichment to every row, and filter by brand if requested
    final_rows = []
    for r in raw_rows:
        pid = r["product_id"]
        if pid and pid in product_lookup:
            info = product_lookup[pid]
            r["product_name"] = info["name"]
            cat_name = category_lookup.get(info["category_id"], "")
            brand_name = brand_lookup.get(info["brand_id"], "")
            r["product_type"] = cat_name
            r["product_brand"] = brand_name
            r["brand"] = brand_name
            if not r["unit_price"] or r["unit_price"] == "$0.00":
                if info["wholesale_price_cents"]:
                    r["unit_price"] = f"${info['wholesale_price_cents']/100:,.2f}"

        # Resolve customer name from id if it's still blank
        if not r.get("buyer_name"):
            cid = r.get("_customer_id")
            if cid and cid in customer_lookup:
                r["buyer_name"] = customer_lookup[cid]

        # Resolve sales rep name from id if it's still blank
        if not r.get("sales_rep") or isinstance(r.get("sales_rep"), int):
            rid = r.get("_sales_rep_id")
            if rid and rid in sales_rep_lookup:
                name = sales_rep_lookup[rid]
                r["sales_rep"] = name
                r["sales_reps"] = name
                r["sales_reps_display"] = name
            else:
                # No resolution available — clear the integer ID so we don't show "83128"
                r["sales_rep"] = ""
                r["sales_reps"] = ""
                r["sales_reps_display"] = ""

        # Strip private bookkeeping fields before writing
        r.pop("_customer_id", None)
        r.pop("_sales_rep_id", None)

        # Apply brand filter (case-insensitive substring match) AFTER enrichment
        if BRAND_FILTER:
            if BRAND_FILTER.lower() not in (r.get("brand") or "").lower():
                continue
        final_rows.append(r)

    if BRAND_FILTER:
        print(f"Brand filter '{BRAND_FILTER}' kept {len(final_rows)} of {len(raw_rows)} rows.")
    raw_rows = final_rows

    # Step 3 — write atomically
    output = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": "leaflink-v2-api",
        "state": STATE_FILTER,
        "days_back": DAYS_BACK,
        "orders_seen": orders_seen,
        "orders_kept": orders_kept,
        "rows": raw_rows,
    }
    tmp = OUTPUT_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(output, indent=2, default=str))
    tmp.replace(OUTPUT_PATH)

    elapsed = time.time() - started
    print(f"\n✓ Wrote {OUTPUT_PATH.name} ({len(raw_rows)} rows) in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
