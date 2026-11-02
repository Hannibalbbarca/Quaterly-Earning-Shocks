import copy
import csv
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

import mc_pipeline as p


def fixture(actual=True):
    names = ["sc_id", "stockName", "quarterData", "exchange", "financialType", "mtgdate"]
    metrics = [["Net Profit", "0", "10"], ["EPS", "-", "0"]] if actual else [["Net Profit", "-5", "-10", "50"]]
    return {"success": 1, "data": {"header": [{"name": n} for n in names], "list": [["EX", "Example", metrics, "N", "Consolidated", "Nov 05, 2026"]], "tableHeader": ["Q2 FY26-27", "Sep 26", "Estimates"] if actual else ["Q2 FY26-27", "Estimates", "Sep 25", "Est Growth"]}}


def member_csv(count):
    text = io.StringIO()
    writer = csv.DictWriter(text, fieldnames=["Company Name", "Industry", "Symbol", "Series", "ISIN Code"])
    writer.writeheader()
    for index in range(count):
        writer.writerow({"Company Name": f"Company {index} Limited", "Industry": "Example", "Symbol": f"C{index}", "Series": "EQ", "ISIN Code": f"ISIN{index}"})
    return text.getvalue()


class ParsingTests(unittest.TestCase):
    def test_quarter_and_year_transition(self):
        self.assertEqual(p.quarter_period("Q4 FY25-26"), "2026-03-31")
        self.assertEqual(p.quarter_period("Q1 FY2026-2027"), "2026-06-30")
        with self.assertRaises(p.SchemaError): p.quarter_period("Q2 FY26-28")

    def test_zero_is_not_missing_and_zero_denominator_unavailable(self):
        rows, errors = p.decode_feed(fixture(), "actual", "2026-11-02T03:00:00+00:00")
        self.assertFalse(errors)
        self.assertEqual(rows[0]["actual"], "0")
        self.assertIsNone(rows[1]["actual"])
        self.assertEqual(rows[1]["estimate"], "0")
        self.assertIsNone(rows[1]["displayed_values_surprise_pct"])

    def test_expected_yoy_growth_is_not_an_analyst_high_low_range(self):
        rows, _ = p.decode_feed(fixture(False), "low", "2026-11-02T03:00:00+00:00")
        self.assertEqual(rows[0]["estimate"], "-5")
        self.assertEqual(rows[0]["prior_year_actual"], "-10")
        self.assertEqual(rows[0]["source_estimated_yoy_growth_pct"], "50")
        self.assertIsNone(rows[0]["actual"])
        self.assertTrue(rows[0]["timing_status"].startswith("BEFORE_SOURCE"))
        self.assertIsNone(rows[0]["estimate_asof"])

    def test_schema_reordering_uses_named_headers_and_changed_period_fails(self):
        doc = fixture()
        doc["data"]["header"].reverse()
        for r in doc["data"]["list"]: r.reverse()
        self.assertEqual(p.decode_feed(doc, "actual", "2026-11-02T03:00:00+00:00")[0][0]["company_id"], "EX")
        doc["data"]["tableHeader"][1] = "Jun 26"
        with self.assertRaises(p.SchemaError): p.decode_feed(doc, "actual", "2026-11-02T03:00:00+00:00")

    def test_bad_numeric_row_is_quarantined_not_zero(self):
        doc = fixture()
        doc["data"]["list"][0][2] = [["Net Profit", "broken", "10"]]
        rows, errors = p.decode_feed(doc, "actual", "2026-11-02T03:00:00+00:00")
        self.assertFalse(rows)
        self.assertEqual(len(errors), 1)


class HistoryTests(unittest.TestCase):
    def test_versions_are_retained_and_asof_does_not_see_future_revision(self):
        with tempfile.TemporaryDirectory() as temp:
            db = p.Database(Path(temp) / "test.db")
            rows, _ = p.decode_feed(fixture(), "actual", "2026-11-02T03:00:00.000000+00:00")
            payload = rows[0]
            for run, stamp, estimate in [("one", "2026-11-02T03:00:00.000000+00:00", "10"), ("two", "2026-11-02T04:00:00.000000+00:00", "10"), ("three", "2026-11-02T05:00:00.000000+00:00", "12")]:
                db.db.execute("INSERT INTO runs(id,started_at,status) VALUES(?,?,?)", (run, stamp, "success"))
                rid = db.db.execute("INSERT INTO responses(run_id,feed,url,observed_at,status,sha256,archive_path) VALUES(?,?,?,?,?,?,?)", (run, "actual", "https://example.test", stamp, 200, "x", "x")).lastrowid
                value = copy.deepcopy(payload);value["estimate"] = estimate
                db.record(value, run, rid, stamp)
            db.db.commit()
            self.assertEqual(db.db.execute("SELECT COUNT(*) FROM records").fetchone()[0], 2)
            self.assertEqual(db.db.execute("SELECT COUNT(*) FROM observations").fetchone()[0], 3)
            self.assertEqual(db.latest("2026-11-02T04:30:00.000000+00:00")[0]["estimate"], "10")
            self.assertEqual(db.latest("2026-11-02T06:00:00.000000+00:00")[0]["estimate"], "12")
            db.db.close()


class ReferenceTests(unittest.TestCase):
    def test_new_constituent_missing_from_source_calendar_is_resolved_and_cached(self):
        with tempfile.TemporaryDirectory() as temp:
            db = p.Database(Path(temp) / 'test.db')
            members = list(csv.DictReader(io.StringIO(member_csv(500))))
            reference = {'members': members, 'identifiers': {}, 'excluded': []}

            class Client:
                def __init__(self):
                    self.store = db
                    self.c = {'reference_cache_hours': 24}
                    self.requests = 0

                def fetch(self, url, feed):
                    self.requests += 1
                    return {'data': {'isinid': 'ISIN499'}}, 1, p.utcnow()

            client = Client()
            payload = {'exchange': 'N', 'company_id': 'NEW', 'company_name': 'Short label', 'nse_symbol': None}
            for _ in range(2):
                self.assertEqual(p.member_identity(payload, reference, client)['symbol'], 'C499')
            self.assertEqual(client.requests, 1)
            db.db.close()

    def test_source_symbol_finds_new_constituent_without_calendar_or_lookup(self):
        members = list(csv.DictReader(io.StringIO(member_csv(500))))
        reference = {'members': members, 'identifiers': {}, 'excluded': []}
        payload = {'exchange': 'N', 'company_id': 'NEW', 'company_name': 'Short label', 'nse_symbol': 'C499'}
        self.assertEqual(p.member_identity(payload, reference, None)['isin'], 'ISIN499')

    def test_nifty500_extra_official_member_is_retained_and_duplicate_is_rejected(self):
        spec = p.UNIVERSES["current_nifty500"]
        self.assertEqual(len(p.parse_members(member_csv(501), spec)), 501)
        with self.assertRaises(p.SchemaError):
            p.parse_members(member_csv(499), spec)
        with self.assertRaises(p.SchemaError):
            p.parse_members(member_csv(500).replace('ISIN499', 'ISIN498'), spec)

    def test_scope_change_invalidates_cached_members_and_same_scope_reuses_cache(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = p.Database(root / 'test.db')

            class Client:
                def __init__(self):
                    self.root = root
                    self.member_requests = 0

                def fetch(self, url, feed, page=None, json_response=True):
                    if feed.endswith('_members'):
                        self.member_requests += 1
                        return member_csv(500 if '500' in feed else 100), 1, p.utcnow()
                    return {'success': 1, 'data': {'list': []}}, 2, p.utcnow()

            client = Client()
            for scope in ['current_nifty100', 'current_nifty500', 'current_nifty500']:
                spec = p.UNIVERSES[scope]
                c = {'scope': scope, 'member_url': spec['member_url'], 'moneycontrol_index_id': spec['index_id'], 'reference_cache_hours': 24, 'max_pages': 5}
                ref = p.reference_data(c, db, client)
                self.assertEqual(len(ref['members']), spec['nominal_count'])
            self.assertEqual(client.member_requests, 2)
            db.db.close()

    def test_coverage_lists_missing_members_and_excludes_stale_observations(self):
        members = list(csv.DictReader(io.StringIO(member_csv(2))))
        reference = {'members': members, 'fetched_at': '2026-11-02T00:00:00+00:00'}
        common = {'observation_kind': 'upcoming_forecast', 'metric': 'net_profit', 'period_end': '2026-09-30'}
        rows = [dict(common, isin='ISIN0', run_id='old', estimate='100'), dict(common, isin='ISIN1', run_id='new', estimate=None)]
        coverage = p.build_coverage(reference, rows, 'new')
        self.assertEqual(coverage['summary']['official_members_tracked'], 2)
        self.assertEqual(coverage['summary']['companies_with_data_this_run'], 1)
        self.assertEqual(coverage['summary']['companies_with_nonmissing_estimate'], 0)
        self.assertFalse(coverage['records'][0]['seen_in_latest_run'])

    def test_nifty500_config_cannot_silently_use_nifty100_filter(self):
        with tempfile.TemporaryDirectory() as temp:
            c = {'scope': 'current_nifty500', 'feeds': ['actual', 'high', 'low'], 'moneycontrol_index_id': '28', 'member_url': p.UNIVERSES['current_nifty500']['member_url'], 'interval_minutes': 60, 'request_gap_seconds': 1.5, 'data_directory': 'data'}
            path = Path(temp) / 'config.json'
            path.write_text(json.dumps(c), encoding='utf-8')
            with self.assertRaises(p.PipelineError):
                p.load_config(path)

    def test_repeated_out_of_scope_calendar_identifier_is_looked_up_once(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = p.Database(root / "test.db")
            text = io.StringIO()
            writer = csv.DictWriter(text, fieldnames=["Company Name", "Industry", "Symbol", "Series", "ISIN Code"])
            writer.writeheader()
            for index in range(100):
                writer.writerow({"Company Name": f"Company {index} Limited", "Industry": "Example", "Symbol": f"C{index}", "Series": "EQ", "ISIN Code": f"ISIN{index}"})

            class Client:
                def __init__(self):
                    self.root = root
                    self.lookups = 0

                def fetch(self, url, feed, page=None, json_response=True):
                    if feed == "nifty100_members":
                        return text.getvalue(), 1, p.utcnow()
                    if feed == "scope_calendar":
                        row = {"scId": "OLD", "stockName": "Outside Company", "resultType": "Q2 FY26-27", "date": "30 Oct"}
                        return {"success": 1, "data": {"list": [row, row, row]}}, 2, p.utcnow()
                    self.lookups += 1
                    return {"data": {"isinid": "OUTSIDE"}}, 3, p.utcnow()

            client = Client()
            ref = p.reference_data({"member_url": "https://example.test", "reference_cache_hours": 24, "moneycontrol_index_id": "28", "max_pages": 5}, db, client)
            self.assertEqual(client.lookups, 1)
            self.assertEqual(len(ref["excluded"]), 1)
            self.assertEqual(ref["unresolved"], [])
            db.db.close()


if __name__ == "__main__":
    unittest.main()
