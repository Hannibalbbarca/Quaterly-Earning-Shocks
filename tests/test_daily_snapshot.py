import copy
import gzip
import json
from pathlib import Path
import tempfile
import unittest

import mc_pipeline as p
from scripts.snapshot_run import create_snapshot


class SnapshotTests(unittest.TestCase):
    def test_daily_history_distinguishes_new_unchanged_and_revised_and_rejects_stale_run(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            payload = {'entity_key': 'EX|N|Consolidated|2026-09-30|net_profit|reported_comparison', 'company_id': 'EX', 'metric': 'net_profit', 'estimate': '10', 'actual': '12'}
            first_capture = None
            for run, hour, estimate, kind in [('one', '03', '10', 'new'), ('two', '04', '10', 'unchanged'), ('three', '05', '11', 'revised')]:
                stamp = f'2026-11-02T{hour}:00:00.000000+00:00'
                db = p.Database(root / 'moneycontrol.sqlite3')
                report = {'run_id': run, 'started_at_utc': stamp, 'status': 'success'}
                db.db.execute('INSERT INTO runs(id,started_at,status,report_json) VALUES(?,?,?,?)', (run, stamp, 'success', json.dumps(report)))
                response = db.db.execute('INSERT INTO responses(run_id,feed,url,observed_at,status,sha256,archive_path) VALUES(?,?,?,?,?,?,?)', (run, 'actual', 'https://example.test', stamp, 200, 'x', 'x')).lastrowid
                value = dict(payload, estimate=estimate)
                db.record(value, run, response, stamp)
                db.db.commit()
                db.db.close()
                p.atomic_json(root / 'exports' / 'latest_metrics.json', {'records': [value]})
                summary = create_snapshot(root, stamp)
                self.assertEqual(summary['new_metric_versions'], int(kind == 'new'))
                self.assertEqual(summary['revised_metric_versions'], int(kind == 'revised'))
                self.assertEqual(summary['unchanged_metric_versions'], int(kind == 'unchanged'))
                if run == 'one':
                    first_capture = (root / 'snapshots' / '2026-11-02' / 'one.json.gz').read_bytes()
            self.assertEqual((root / 'snapshots' / '2026-11-02' / 'one.json.gz').read_bytes(), first_capture)
            self.assertEqual(json.loads(gzip.decompress(first_capture))['latest_metrics']['records'][0]['estimate'], '10')
            create_snapshot(root, '2026-11-02T05:00:00+00:00')
            self.assertEqual(len(list((root / 'snapshots').rglob('*.gz'))), 3)
            with self.assertRaises(p.PipelineError):
                create_snapshot(root, '2026-11-02T06:00:00+00:00')

    def test_missing_database_is_not_reported_as_success(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(p.PipelineError):
                create_snapshot(Path(temp))


if __name__ == '__main__':
    unittest.main()
