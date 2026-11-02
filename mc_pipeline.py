"""Moneycontrol earnings collector: public HTTP -> raw archives -> versioned SQLite.

Python standard library only. No browser cookies, logins, proxy rotation, access
control workarounds or invented historical forecast timestamps.
"""
from __future__ import annotations

import argparse
import calendar
import csv
import gzip
import hashlib
import io
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import re
import sqlite3
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from urllib.error import HTTPError, URLError
from urllib.parse import quote as quote_path, urlencode, urlsplit
from urllib.request import Request, urlopen

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
BASE = "https://api.moneycontrol.com/mcapi/v1/"
PUBLIC_PAGE = "https://www.moneycontrol.com/markets/earnings/estimates/"
METRICS = {"Revenue": ("revenue", "INR_crore"), "Net Profit": ("net_profit", "INR_crore"), "EPS": ("eps", "INR_per_share")}
UNIVERSES = {
    "current_nifty100": {"label": "Nifty 100", "code": "NIFTY100", "nominal_count": 100, "index_id": "28", "member_url": "https://www.niftyindices.com/IndexConstituent/ind_nifty100list.csv"},
    "current_nifty500": {"label": "Nifty 500", "code": "NIFTY500", "nominal_count": 500, "index_id": "7", "member_url": "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"},
}


class PipelineError(Exception):
    pass


class SchemaError(PipelineError):
    pass


def utcnow():
    return datetime.now(UTC).isoformat(timespec="microseconds")


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(value if isinstance(value, bytes) else canonical(value).encode()).hexdigest()


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp, path)


def number(value):
    """Exact decimal text, retaining missing versus genuine zero."""
    if value is None:
        return None
    s = str(value).strip().replace("\u00a0", "")
    if s.lower() in {"", "-", "--", "n/a", "na", "n.a.", "null", "none"}:
        return None
    s = s.replace(",", "").replace("%", "").strip()
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        n = Decimal(s)
    except InvalidOperation as exc:
        raise SchemaError(f"Unrecognized numeric representation: {value!r}") from exc
    if not n.is_finite():
        raise SchemaError(f"Non-finite number: {value!r}")
    return "0" if not n else format(n.normalize(), "f")


def surprise(actual, estimate):
    if actual is None or estimate is None or Decimal(estimate) == 0:
        return None
    return format(((Decimal(actual) - Decimal(estimate)) / abs(Decimal(estimate)) * 100).quantize(Decimal("0.000001")), "f")


def quarter_period(label):
    m = re.fullmatch(r"Q([1-4])\s+FY(\d{2}|\d{4})-(\d{2}|\d{4})", str(label).strip())
    if not m:
        raise SchemaError(f"Unsupported fiscal-quarter label: {label!r}")
    q, start, end = (int(x) for x in m.groups())
    start = start + 2000 if start < 100 else start
    end = end + 2000 if end < 100 else end
    if end != start + 1:
        raise SchemaError(f"Invalid fiscal-year span: {label!r}")
    month = {1: 6, 2: 9, 3: 12, 4: 3}[q]
    year = start if q < 4 else end
    return date(year, month, calendar.monthrange(year, month)[1]).isoformat()


def month_period(label):
    try:
        dt = datetime.strptime(str(label), "%b %y")
    except ValueError as exc:
        raise SchemaError(f"Unsupported period label: {label!r}") from exc
    return date(dt.year, dt.month, calendar.monthrange(dt.year, dt.month)[1]).isoformat()


def source_date(value):
    if value in (None, "", "-", "--"):
        return None
    try:
        return datetime.strptime(str(value), "%b %d, %Y").date().isoformat()
    except ValueError as exc:
        raise SchemaError(f"Unrecognized source event date: {value!r}") from exc


def normalize_name(value):
    value = re.sub(r"\b(?:limited|ltd)\b", "", str(value).lower().replace("&", "and"))
    return re.sub(r"[^a-z0-9]", "", value)


def universe_info(c):
    try:
        return UNIVERSES[c.get("scope", "current_nifty100")]
    except KeyError as exc:
        raise PipelineError("Unsupported index universe") from exc


def parse_members(text, spec):
    members = list(csv.DictReader(io.StringIO(text)))
    required = {"Company Name", "Industry", "Symbol", "ISIN Code"}
    if not members or not required.issubset(members[0]):
        raise SchemaError("Official constituent download has changed its columns")
    # Temporary extra constituents can appear in the official file, for example
    # during corporate actions. Keep them, flag the count, never drop an arbitrary row.
    expected = spec["nominal_count"]
    if not expected <= len(members) <= expected + 5:
        raise SchemaError(f"Official {spec['label']} reference has an unexpected {len(members)} rows")
    for key in ("Symbol", "ISIN Code"):
        values = [x.get(key) for x in members]
        if any(not value for value in values) or len(set(values)) != len(values):
            raise SchemaError(f"Official constituent reference contains missing or duplicate {key}")
    return members


def decode_feed(document, feed, observed_at, controls=()):
    if document.get("success") not in (1, True):
        raise SchemaError("Source did not return success=1")
    d = document.get("data")
    if not isinstance(d, dict):
        raise SchemaError("Missing data object")
    header, rows, table = d.get("header"), d.get("list"), d.get("tableHeader")
    if not isinstance(header, list) or not isinstance(rows, list) or not isinstance(table, list):
        raise SchemaError("Missing header, list or tableHeader")
    if not rows:
        return [], []
    names = [h.get("name") for h in header if isinstance(h, dict)]
    required = {"sc_id", "stockName", "quarterData", "exchange", "financialType", "mtgdate"}
    if len(names) != len(header) or len(set(names)) != len(names) or not required.issubset(names):
        raise SchemaError("Unexpected row header schema")
    if len(table) < 3:
        raise SchemaError("Missing period/comparison headers")
    period = quarter_period(table[0])
    actual_view = feed == "actual"
    if actual_view:
        if month_period(table[1]) != period or table[2] != "Estimates":
            raise SchemaError("Actual/estimate columns or quarter have changed")
    elif table[1] != "Estimates" or len(table) != 4 or table[3] != "Est Growth":
        raise SchemaError("Upcoming forecast columns have changed")
    elif month_period(table[2]) != date.fromisoformat(period).replace(year=date.fromisoformat(period).year - 1).isoformat():
        raise SchemaError("Upcoming comparison is not the prior-year quarter")
    capture_date = datetime.fromisoformat(observed_at).astimezone(IST).date().isoformat()
    normalized, quarantined = [], []
    for index, row in enumerate(rows):
        try:
            if not isinstance(row, list) or len(row) != len(names):
                raise SchemaError("Row width does not match source header")
            item = dict(zip(names, row))
            if not isinstance(item["quarterData"], list) or not item["sc_id"]:
                raise SchemaError("Invalid company/quarterData")
            event = source_date(item["mtgdate"])
            basis = str(item["financialType"])
            if basis not in {"Consolidated", "Standalone"}:
                raise SchemaError(f"Unexpected reporting basis {basis!r}")
            for metric_row in item["quarterData"]:
                if not isinstance(metric_row, list) or len(metric_row) != (3 if actual_view else 4):
                    raise SchemaError("Invalid metric row width")
                metric_label = metric_row[0]
                if metric_label not in METRICS:
                    raise SchemaError(f"Unknown earnings metric {metric_label!r}")
                metric, unit = METRICS[metric_label]
                a = number(metric_row[1]) if actual_view else None
                e = number(metric_row[2] if actual_view else metric_row[1])
                flags = ["SOURCE_DISPLAY_PRECISION", "ESTIMATE_ASOF_NOT_SUPPLIED", "EARNINGS_ADJUSTMENT_BASIS_UNVERIFIED", "SOURCE_EVENT_TIME_UNVERIFIED"]
                if e is None:
                    flags.append("MISSING_ESTIMATE")
                if actual_view and a is None:
                    flags.append("MISSING_ACTUAL")
                if e is not None and Decimal(e) == 0:
                    flags.append("ZERO_ESTIMATE_PERCENT_UNDEFINED")
                if actual_view:
                    timing = "POST_RESULT_COMPARISON_NOT_A_PRE_RELEASE_SNAPSHOT"
                elif event and capture_date < event:
                    timing = "BEFORE_SOURCE_SCHEDULED_DATE_RELEASE_TIME_STILL_UNVERIFIED"
                else:
                    timing = "SOURCE_EVENT_DATE_SAME_DAY_OR_PAST_TIMING_UNRESOLVED"
                validation = []
                for rule in controls:
                    if actual_view and item["sc_id"] == rule["company_id"] and period == rule["period_end"] and metric == rule["metric"] and a is not None:
                        if abs(Decimal(a) - Decimal(rule["reference_value"])) > Decimal(rule["tolerance"]):
                            flags.append("COMPANY_RELEASE_ACTUAL_MISMATCH")
                            validation.append(rule)
                payload = {
                    "company_id": str(item["sc_id"]), "company_name": str(item["stockName"]),
                    "nse_symbol": item.get("symbol"), "exchange": str(item["exchange"]),
                    "financial_basis": basis, "fiscal_quarter_source": table[0], "period_end": period,
                    "source_displayed_event_date": event, "source_date_kind": "DISPLAYED_DATE_NOT_VERIFIED_RELEASE_TIMESTAMP",
                    "metric": metric, "unit": unit, "observation_kind": "reported_comparison" if actual_view else "upcoming_forecast",
                    "source_view": feed, "actual": a, "estimate": e,
                    "prior_year_actual": None if actual_view else number(metric_row[2]),
                    "source_estimated_yoy_growth_pct": None if actual_view else number(metric_row[3]),
                    "source_expectation_label": item.get("expectations") if actual_view else None,
                    "source_expectation_pct": number(item.get("expectationsPer")) if actual_view else None,
                    "source_label_metric": "NOT_SPECIFIED_DO_NOT_ASSUME_EPS",
                    "displayed_values_surprise_pct": surprise(a, e) if actual_view else None,
                    "estimate_asof": None, "timing_status": timing,
                    "raw_metric_cells": metric_row, "source_table_headers": table,
                    "source_page": PUBLIC_PAGE, "quality_flags": flags, "validation_references": validation,
                }
                payload["entity_key"] = "|".join(payload[k] for k in ("company_id", "exchange", "financial_basis", "period_end", "metric", "observation_kind"))
                normalized.append(payload)
        except (SchemaError, KeyError, TypeError, ValueError) as exc:
            # Never silently turn a malformed row into an empty healthy result.
            quarantined.append({"row_index": index, "error": str(exc), "row": row})
    return normalized, quarantined


class Database:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS runs (
          id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
          status TEXT NOT NULL, report_json TEXT);
        CREATE TABLE IF NOT EXISTS responses (
          id INTEGER PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id),
          feed TEXT NOT NULL, page INTEGER, url TEXT NOT NULL, observed_at TEXT NOT NULL,
          status INTEGER NOT NULL, sha256 TEXT NOT NULL, archive_path TEXT NOT NULL,
          schema_hash TEXT, row_count INTEGER);
        CREATE TABLE IF NOT EXISTS records (
          id INTEGER PRIMARY KEY, entity_key TEXT NOT NULL, content_hash TEXT NOT NULL,
          payload_json TEXT NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
          seen_count INTEGER NOT NULL DEFAULT 0, UNIQUE(entity_key,content_hash));
        CREATE TABLE IF NOT EXISTS observations (
          id INTEGER PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id),
          response_id INTEGER NOT NULL REFERENCES responses(id), record_id INTEGER NOT NULL REFERENCES records(id),
          observed_at TEXT NOT NULL, UNIQUE(run_id,record_id));
        CREATE INDEX IF NOT EXISTS obs_time ON observations(observed_at);
        CREATE INDEX IF NOT EXISTS rec_entity ON records(entity_key);
        CREATE TABLE IF NOT EXISTS quarantine (
          id INTEGER PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), response_id INTEGER NOT NULL REFERENCES responses(id),
          reason TEXT NOT NULL, payload_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value_json TEXT NOT NULL);
        """)

    def state(self, key, default=None):
        row = self.db.execute("SELECT value_json FROM state WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_state(self, key, value):
        self.db.execute("INSERT INTO state VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json", (key, canonical(value)))
        self.db.commit()

    def record(self, payload, run_id, response_id, observed_at):
        financial_content = {k: v for k, v in payload.items() if k not in {"membership_reference_collected_at", "identifier_match_method"}}
        h = digest(financial_content)
        c = self.db.execute("INSERT OR IGNORE INTO records(entity_key,content_hash,payload_json,first_seen,last_seen) VALUES(?,?,?,?,?)", (payload["entity_key"], h, canonical(payload), observed_at, observed_at))
        new = c.rowcount == 1
        rid = self.db.execute("SELECT id FROM records WHERE entity_key=? AND content_hash=?", (payload["entity_key"], h)).fetchone()[0]
        seen = self.db.execute("INSERT OR IGNORE INTO observations(run_id,response_id,record_id,observed_at) VALUES(?,?,?,?)", (run_id, response_id, rid, observed_at)).rowcount
        if seen:
            self.db.execute("UPDATE records SET last_seen=?,seen_count=seen_count+1 WHERE id=?", (observed_at, rid))
        return new

    def latest(self, asof=None):
        cutoff = asof or utcnow()
        rows = self.db.execute("""
        SELECT payload_json,observed_at,run_id FROM (
          SELECT r.payload_json,o.observed_at,o.run_id,
          ROW_NUMBER() OVER(PARTITION BY r.entity_key ORDER BY o.observed_at DESC,o.id DESC) rn
          FROM observations o JOIN records r ON r.id=o.record_id
          WHERE o.observed_at<=?) WHERE rn=1 ORDER BY observed_at DESC
        """, (cutoff,)).fetchall()
        return [dict(json.loads(r["payload_json"]), collected_at_utc=r["observed_at"], run_id=r["run_id"]) for r in rows]


class HttpClient:
    def __init__(self, config, root, database, run_id):
        self.c, self.root, self.store, self.run = config, root, database, run_id
        self.last_request = 0.0
        self.logger = logging.getLogger("moneycontrol")

    def fetch(self, url, feed, page=None, json_response=True):
        host = urlsplit(url).hostname
        if host not in {"api.moneycontrol.com", "priceapi.moneycontrol.com", "www.niftyindices.com"}:
            raise PipelineError("Host is outside the collector allowlist")
        blocked = self.store.state("cooldown:" + host)
        if blocked and blocked["until"] > utcnow():
            raise PipelineError(f"{host} is in cooldown until {blocked['until']}: {blocked['reason']}")
        for attempt in range(self.c["max_attempts"]):
            time.sleep(max(0, self.c["request_gap_seconds"] - (time.monotonic() - self.last_request)))
            self.last_request = time.monotonic()
            try:
                req = Request(url, headers={"User-Agent": self.c["user_agent"], "Accept": "application/json" if json_response else "text/csv,*/*;q=0.5"})
                with urlopen(req, timeout=self.c["timeout_seconds"]) as r:
                    body = r.read(12_000_001)
                    status = r.status
                if len(body) > 12_000_000:
                    raise PipelineError("Response exceeds 12 MB safety limit")
                observed = utcnow()
                sha = digest(body)
                relative = Path("raw") / sha[:2] / (sha + ".gz")
                dest = self.root / relative
                dest.parent.mkdir(parents=True, exist_ok=True)
                if not dest.exists():
                    with gzip.open(dest, "wb") as out:
                        out.write(body)
                value = json.loads(body) if json_response else body.decode("utf-8-sig")
                d = value.get("data") if isinstance(value, dict) else None
                schema = digest(d.get("header")) if isinstance(d, dict) and "header" in d else None
                count = len(d["list"]) if isinstance(d, dict) and isinstance(d.get("list"), list) else None
                cur = self.store.db.execute("INSERT INTO responses(run_id,feed,page,url,observed_at,status,sha256,archive_path,schema_hash,row_count) VALUES(?,?,?,?,?,?,?,?,?,?)", (self.run, feed, page, url, observed, status, sha, str(relative), schema, count))
                self.store.db.commit()
                return value, cur.lastrowid, observed
            except HTTPError as exc:
                if exc.code in {401, 403, 429}:
                    delay = 86400 if exc.code in {401, 403} else 3600
                    retry = exc.headers.get("Retry-After", "")
                    if retry.isdigit():
                        delay = max(delay, int(retry))
                    self.store.set_state("cooldown:" + host, {"until": (datetime.now(UTC) + timedelta(seconds=delay)).isoformat(timespec="microseconds"), "reason": f"HTTP {exc.code}"})
                    raise PipelineError(f"HTTP {exc.code}; stopped without trying cookies, proxies or another identity") from exc
                if exc.code < 500 or attempt + 1 == self.c["max_attempts"]:
                    raise PipelineError(f"HTTP {exc.code} from {url}") from exc
            except (URLError, TimeoutError, OSError) as exc:
                if attempt + 1 == self.c["max_attempts"]:
                    raise PipelineError(f"Network failure: {exc}") from exc
            except (json.JSONDecodeError, UnicodeError) as exc:
                raise SchemaError("Source returned an invalid data document; raw response retained") from exc
            time.sleep(min(2 ** attempt, 8))
        raise PipelineError("Request exhausted retries")


def reference_data(c, store, client):
    spec = universe_info(c)
    fingerprint = digest({"scope": c.get("scope", "current_nifty100"), "member_url": c["member_url"], "index_id": c["moneycontrol_index_id"]})
    cached = store.state("reference")
    if cached and cached.get("schema_version") == 3 and cached.get("config_fingerprint") == fingerprint and datetime.fromisoformat(cached["fetched_at"]) > datetime.now(UTC) - timedelta(hours=c["reference_cache_hours"]):
        return cached
    text, _, observed = client.fetch(c["member_url"], spec["code"].lower() + "_members", json_response=False)
    members = parse_members(text, spec)
    grouped_names = {}
    for member in members:
        grouped_names.setdefault(normalize_name(member["Company Name"]), []).append(member)
    by_name = {name: group[0] for name, group in grouped_names.items() if len(group) == 1}
    by_isin = {x["ISIN Code"]: x for x in members}
    today = datetime.now(IST).date()
    fy_start = today.year if today.month >= 4 else today.year - 1
    # The calendar is a candidate identifier map, never the membership authority.
    params = {"indexId": c["moneycontrol_index_id"], "page": 1, "startDate": f"{fy_start}-04-01", "endDate": f"{fy_start+1}-03-31", "sector": "", "limit": 150, "sortBy": "marketcap", "search": ""}
    identifiers, unresolved, excluded, seen, checked_ids = {}, [], [], set(), set()
    for page in range(1, c["max_pages"] + 1):
        params["page"] = page
        j, _, _ = client.fetch(BASE + "earnings/get-earnings-data?" + urlencode(params), "scope_calendar", page)
        if j.get("success") not in (1, True) or not isinstance(j.get("data", {}).get("list"), list):
            raise SchemaError("Invalid scope-calendar response")
        rows = j["data"]["list"]
        signature = digest(sorted((x.get("scId"), x.get("resultType"), x.get("date")) for x in rows))
        if rows and signature in seen:
            raise SchemaError("Scope calendar repeated a page")
        seen.add(signature)
        for row in rows:
            scid = row.get("scId")
            if not scid or scid in checked_ids:
                continue
            checked_ids.add(scid)
            member = by_name.get(normalize_name(row.get("stockName", "")))
            method = "exact_unique_company_name"
            isin = None
            if member is None:
                old = (cached or {}).get("identifiers", {}).get(scid)
                if old and old.get("isin") in by_isin:
                    member = by_isin[old["isin"]]
                    method = "cached_verified_isin"
                else:
                    quote, _, _ = client.fetch("https://priceapi.moneycontrol.com/pricefeed/nse/equitycash/" + scid, "identifier_lookup")
                    q = quote.get("data", {})
                    isin = q.get("isinid")
                    member = by_isin.get(isin)
                    method = "provider_isin_join_official_members"
            if member:
                identifiers[scid] = {"symbol": member["Symbol"], "isin": member["ISIN Code"], "industry": member["Industry"], "company_name": member["Company Name"], "method": method}
            elif isin:
                excluded.append({"company_id": scid, "source_name": row.get("stockName"), "isin": isin, "reason": "NOT_IN_CURRENT_OFFICIAL_MEMBERSHIP"})
            else:
                unresolved.append({"company_id": scid, "source_name": row.get("stockName")})
        if len(rows) < 150:
            break
    else:
        raise PipelineError("Scope-calendar pagination cap reached")
    result = {"schema_version": 3, "config_fingerprint": fingerprint, "scope": c.get("scope", "current_nifty100"), "nominal_member_count": spec["nominal_count"], "official_member_count": len(members), "warnings": ["OFFICIAL_COMPONENT_COUNT_DIFFERS_FROM_INDEX_NAME"] if len(members) != spec["nominal_count"] else [], "fetched_at": observed, "membership_effective_date": None, "membership_basis": "CURRENT_CONSTITUENT_DOWNLOAD_NOT_HISTORICAL_MEMBERSHIP", "members": members, "identifiers": identifiers, "unresolved": unresolved, "excluded": excluded, "members_without_calendar_identifier": sorted(set(x["Symbol"] for x in members) - set(x["symbol"] for x in identifiers.values()))}
    store.set_state("reference", result)
    atomic_json(client.root / "exports" / "universe_reference.json", result)
    return result


def feed_url(feed, page, c):
    common = {"page": page, "search": "", "indexId": c.get("feed_index_id", c["moneycontrol_index_id"]), "sector": ""}
    if feed == "actual":
        path = "earnings/actual-estimate"
        common.update(sortBy="all", type="all")
    else:
        path = "earnings/estimate-performance"
        common.update(type=feed, subType="all")
    return BASE + path + "?" + urlencode(common)


def member_identity(payload, reference, client):
    """Use official membership even when Moneycontrol's own index filter is stale."""
    if payload["exchange"] != "N":
        return None
    identity = reference["identifiers"].get(payload["company_id"])
    if identity:
        return identity
    members = reference["members"]
    member = next((x for x in members if x["Symbol"] == payload.get("nse_symbol")), None)
    method = "source_symbol_join_official_members"
    if member is None:
        matches = [x for x in members if normalize_name(x["Company Name"]) == normalize_name(payload["company_name"])]
        member = matches[0] if len(matches) == 1 else None
        method = "exact_unique_company_name"
    if member is None:
        known_exclusion = next((x for x in reference.get("excluded", []) if x["company_id"] == payload["company_id"]), None)
        cached = client.store.state("identity:" + payload["company_id"])
        if known_exclusion:
            isin = known_exclusion["isin"]
        elif cached and datetime.fromisoformat(cached["fetched_at"]) > datetime.now(UTC) - timedelta(hours=client.c["reference_cache_hours"]):
            isin = cached["isin"]
        else:
            document, _, observed = client.fetch("https://priceapi.moneycontrol.com/pricefeed/nse/equitycash/" + quote_path(payload["company_id"], safe=""), "feed_identifier_lookup")
            data = document.get("data", {})
            if not isinstance(data, dict) or not data.get("isinid"):
                raise SchemaError(f"Cannot verify source company identifier {payload['company_id']}")
            isin = data["isinid"]
            client.store.set_state("identity:" + payload["company_id"], {"isin": isin, "fetched_at": observed})
        member = next((x for x in members if x["ISIN Code"] == isin), None)
        method = "feed_isin_join_official_members"
    if member is None:
        return None
    return {"symbol": member["Symbol"], "isin": member["ISIN Code"], "industry": member["Industry"], "company_name": member["Company Name"], "method": method}


def collect_feed(feed, c, store, client, reference):
    report = {"feed": feed, "status": "success", "pages": 0, "source_cards": 0, "metrics_stored": 0, "new_versions": 0, "excluded_out_of_scope": 0, "quarantined": 0}
    signatures, cards_seen = set(), set()
    identity_cache = {}
    for page in range(1, c["max_pages"] + 1):
        document, response_id, observed = client.fetch(feed_url(feed, page, c), feed, page)
        normalized, quarantined = decode_feed(document, feed, observed, c.get("quality_controls", []))
        rows = document["data"]["list"]
        report["pages"] += 1
        report["source_cards"] += len(rows)
        # Ignore changing market quotes when detecting a repeated pagination page.
        signature = digest([r[0] for r in rows])
        if rows and signature in signatures:
            raise PipelineError(f"{feed}: source repeated a pagination page")
        signatures.add(signature)
        for bad in quarantined:
            store.db.execute("INSERT INTO quarantine(run_id,response_id,reason,payload_json) VALUES(?,?,?,?)", (client.run, response_id, bad["error"], canonical(bad)))
        report["quarantined"] += len(quarantined)
        for payload in normalized:
            identity_key = (payload["company_id"], payload["exchange"])
            if identity_key not in identity_cache:
                identity_cache[identity_key] = member_identity(payload, reference, client)
            identity = identity_cache[identity_key]
            if identity is None:
                report["excluded_out_of_scope"] += 1
                continue
            payload.update(nse_symbol=identity["symbol"], isin=identity["isin"], industry=identity["industry"], universe=universe_info(c)["code"] + "_CURRENT_AT_REFERENCE_COLLECTION", membership_reference_collected_at=reference["fetched_at"], identifier_match_method=identity["method"])
            report["new_versions"] += int(store.record(payload, client.run, response_id, observed))
            report["metrics_stored"] += 1
            cards_seen.add(payload["company_id"])
        store.db.commit()
        if len(rows) < c["page_size"]:
            break
    else:
        raise PipelineError(f"{feed}: page cap reached; the scan is incomplete")
    report["companies_stored"] = len(cards_seen)
    if report["quarantined"]:
        report["status"] = "partial"
    if not report["source_cards"]:
        report["warning"] = "EMPTY_FEED_REVIEW_REQUIRED"
    return report


def build_coverage(reference, rows, run_id):
    observed = {}
    for row in rows:
        if row["run_id"] == run_id:
            observed.setdefault(row.get("isin"), []).append(row)
    records = []
    for member in sorted(reference["members"], key=lambda x: x["Symbol"]):
        current = observed.get(member["ISIN Code"], [])
        records.append({
            "nse_symbol": member["Symbol"], "company_name": member["Company Name"],
            "isin": member["ISIN Code"], "industry": member["Industry"],
            "seen_in_latest_run": bool(current),
            "reported_comparison_card": any(x["observation_kind"] == "reported_comparison" for x in current),
            "upcoming_forecast_card": any(x["observation_kind"] == "upcoming_forecast" for x in current),
            "has_nonmissing_estimate": any(x["estimate"] is not None for x in current),
            "captured_metrics": sorted(set(x["metric"] for x in current)),
            "captured_periods": sorted(set(x["period_end"] for x in current)),
            "availability": "CARD_CAPTURED_THIS_RUN" if current else "NO_CARD_IN_COLLECTED_ESTIMATE_FEEDS_THIS_RUN",
        })
    summary = {"official_members_tracked": len(records), "companies_with_data_this_run": sum(x["seen_in_latest_run"] for x in records), "companies_with_reported_comparison": sum(x["reported_comparison_card"] for x in records), "companies_with_upcoming_forecast": sum(x["upcoming_forecast_card"] for x in records), "companies_with_nonmissing_estimate": sum(x["has_nonmissing_estimate"] for x in records), "companies_without_data_this_run": sum(not x["seen_in_latest_run"] for x in records)}
    return {"run_id": run_id, "membership_reference_collected_at": reference["fetched_at"], "summary": summary, "note": "No card in these estimate feeds does not establish that a company has not released results. Missing analyst coverage is not filled in.", "records": records}


def write_exports(c, root, store, report):
    reference = store.state("reference")
    rows = store.latest()
    if reference:
        current_isins = {x["ISIN Code"] for x in reference["members"]}
        rows = [x for x in rows if x.get("isin") in current_isins]
    for row in rows:
        row["seen_in_latest_run"] = row["run_id"] == report["run_id"]
    base = {"exported_at_utc": utcnow(), "source": "Moneycontrol", "scope": c["scope"], "latest_run_id": report["run_id"], "latest_run_status": report["status"], "records": rows}
    base["limitations"] = ["Collection begins with this installation; old pre-announcement snapshots are not reconstructed.", "Moneycontrol estimate-asof and original release timestamps are not supplied by these feeds.", "Values have source display precision; source percentages can differ from recomputation.", "Upcoming high/low tabs mean estimated year-over-year profit growth, not high/low analyst forecasts.", "The reference is current index membership; it cannot classify historical membership.", "Tracking every constituent does not mean analyst estimates are available for every constituent."]
    destinations = [root / "exports"]
    if c.get("publish_directory"):
        destinations.append(Path(c["publish_directory"]))
    for destination in destinations:
        atomic_json(destination / "latest_metrics.json", base)
        atomic_json(destination / "latest_run.json", report)
        atomic_json(destination / "quality_flags.json", {"exported_at_utc": base["exported_at_utc"], "records": [r for r in rows if "COMPANY_RELEASE_ACTUAL_MISMATCH" in r["quality_flags"]]})
        if reference:
            atomic_json(destination / "universe_reference.json", reference)
            atomic_json(destination / "coverage.json", build_coverage(reference, rows, report["run_id"]))


def load_config(path):
    path = Path(path).resolve()
    c = json.loads(path.read_text(encoding="utf-8"))
    if c.get("scope") not in UNIVERSES or c.get("feeds") != ["actual", "high", "low"]:
        raise PipelineError("Unsupported scope or feed configuration")
    spec = universe_info(c)
    if c.get("moneycontrol_index_id") != spec["index_id"] or c.get("member_url") != spec["member_url"]:
        raise PipelineError("Index scope, source filter and constituent URL must describe the same universe")
    if c.get("feed_index_id", spec["index_id"]) not in {spec["index_id"], "N"}:
        raise PipelineError("Feed scan must use the configured index or the public all-NSE card view")
    if c.get("interval_minutes", 0) < 30 or c.get("request_gap_seconds", 0) < 1:
        raise PipelineError("Polling must be at least 30 minutes and requests at least 1 second apart")
    if c.get("publish_directory") and not Path(c["publish_directory"]).is_absolute():
        raise PipelineError("publish_directory must be an absolute path, or null to disable")
    root = (path.parent / c["data_directory"]).resolve()
    root.mkdir(parents=True, exist_ok=True)
    return c, root


def run_once(c, root):
    store = Database(root / "moneycontrol.sqlite3")
    run_id = uuid.uuid4().hex
    report = {"run_id": run_id, "scope": c["scope"], "started_at_utc": utcnow(), "status": "running", "feeds": [], "errors": []}
    store.db.execute("INSERT INTO runs(id,started_at,status) VALUES(?,?,?)", (run_id, report["started_at_utc"], "running"))
    store.db.commit()
    client = HttpClient(c, root, store, run_id)
    try:
        reference = reference_data(c, store, client)
        report["official_members"] = len(reference["members"])
        report["reference_warnings"] = reference.get("warnings", [])
        report["mapped_calendar_identifiers"] = len(reference["identifiers"])
        report["unresolved_identifiers"] = reference["unresolved"]
        report["excluded_reference_identifiers"] = reference.get("excluded", [])
        report["members_without_calendar_identifier"] = reference.get("members_without_calendar_identifier", [])
        for feed in c["feeds"]:
            try:
                result = collect_feed(feed, c, store, client, reference)
                report["feeds"].append(result)
            except Exception as exc:
                store.db.rollback()
                report["feeds"].append({"feed": feed, "status": "error", "error": str(exc)})
                logging.exception("Feed failed: %s", feed)
        statuses = [x["status"] for x in report["feeds"]]
        report["status"] = "success" if all(s == "success" for s in statuses) else ("error" if all(s == "error" for s in statuses) else "partial")
        if all(x.get("source_cards", 0) == 0 for x in report["feeds"]):
            report["status"] = "error"
            report["errors"].append("All feeds empty or failed; this is not a healthy zero-observation run")
        report["coverage"] = build_coverage(reference, store.latest(), run_id)["summary"]
    except Exception as exc:
        report["status"] = "error"
        report["errors"].append(str(exc))
        logging.exception("Run failed")
    report["finished_at_utc"] = utcnow()
    report["stored_metric_observations"] = store.db.execute("SELECT COUNT(*) FROM observations WHERE run_id=?", (run_id,)).fetchone()[0]
    store.db.execute("UPDATE runs SET finished_at=?,status=?,report_json=? WHERE id=?", (report["finished_at_utc"], report["status"], canonical(report), run_id))
    store.db.commit()
    write_exports(c, root, store, report)
    store.db.close()
    logging.info("Run %s: %s, %s observations", run_id, report["status"], report["stored_metric_observations"])
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(Path(__file__).with_name("pipeline_config.json")))
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="Collect one complete snapshot")
    sub.add_parser("loop", help="Run continuously while this process is alive")
    sub.add_parser("status", help="Print the most recent stored run")
    ex = sub.add_parser("export", help="Export the most recently observed version of each metric")
    ex.add_argument("--as-of", help="ISO timestamp with explicit UTC offset; only captures at/before it are eligible")
    args = parser.parse_args()
    c, root = load_config(args.config)
    log = RotatingFileHandler(root / "pipeline.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    logging.basicConfig(level=logging.INFO, handlers=[log], format="%(asctime)s %(levelname)s %(message)s")
    if args.command == "status":
        db = Database(root / "moneycontrol.sqlite3")
        row = db.db.execute("SELECT report_json FROM runs ORDER BY started_at DESC LIMIT 1").fetchone()
        print(json.dumps(json.loads(row[0]) if row and row[0] else {"status": "never_run"}, indent=2))
        return 0
    if args.command == "export":
        asof = None
        if args.as_of:
            dt = datetime.fromisoformat(args.as_of)
            if dt.tzinfo is None:
                parser.error("--as-of must include a timezone offset")
            asof = dt.astimezone(UTC).isoformat(timespec="microseconds")
        db = Database(root / "moneycontrol.sqlite3")
        out = root / "exports" / ("asof_" + re.sub(r"[^0-9]", "", asof) + ".json" if asof else "latest_metrics.json")
        atomic_json(out, {"as_of_utc": asof, "records": db.latest(asof)})
        print(str(out))
        return 0
    # OS file lock is automatically released on exit, including process crashes.
    lock = open(root / "collector.lock", "a+b")
    try:
        if os.name == "nt":
            import msvcrt
            lock.seek(0)
            if not lock.read(1):
                lock.write(b"0"); lock.flush()
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, BlockingIOError):
        print(json.dumps({"status": "already_running"}))
        return 0
    while True:
        report = run_once(c, root)
        print(json.dumps(report, indent=2), flush=True)
        if args.command == "run":
            return 0 if report["status"] == "success" else 1
        time.sleep(c["interval_minutes"] * 60)


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(0)
    except Exception:
        logging.exception("Collector stopped unexpectedly")
        raise SystemExit(1)
