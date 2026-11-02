"""Preserve each cloud capture and distinguish new, revised and unchanged data."""
from __future__ import annotations

import gzip
import json
import os
from pathlib import Path
import sqlite3
import sys
from datetime import datetime

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
import mc_pipeline as pipeline


def create_snapshot(root, not_before=None):
    root = Path(root)
    database = root / 'moneycontrol.sqlite3'
    if not database.is_file():
        raise pipeline.PipelineError('No collector database was created')
    db = sqlite3.connect(database)
    db.row_factory = sqlite3.Row
    try:
        run = db.execute('SELECT * FROM runs ORDER BY started_at DESC LIMIT 1').fetchone()
        if not run or not run['report_json'] or run['status'] == 'running':
            raise pipeline.PipelineError('No completed collection run is available')
        if not_before and datetime.fromisoformat(run['started_at']) < datetime.fromisoformat(not_before):
            raise pipeline.PipelineError('No new collection run; refusing to relabel older data')
        report = json.loads(run['report_json'])
        changed = []
        unchanged = 0
        current = db.execute('''
            SELECT r.* FROM observations o JOIN records r ON r.id=o.record_id
            WHERE o.run_id=? ORDER BY r.entity_key,r.id
        ''', (run['id'],)).fetchall()
        for record in current:
            observed_before = db.execute('''
                SELECT 1 FROM observations WHERE record_id=? AND run_id<>? LIMIT 1
            ''', (record['id'], run['id'])).fetchone()
            if observed_before:
                unchanged += 1
                continue
            earlier_entity = db.execute('''
                SELECT 1 FROM records WHERE entity_key=? AND id<>? AND first_seen<? LIMIT 1
            ''', (record['entity_key'], record['id'], run['started_at'])).fetchone()
            changed.append(dict(json.loads(record['payload_json']), change_kind='revised' if earlier_entity else 'new'))
        summary = {
            'run_id': run['id'], 'run_status': run['status'],
            'collected_at_utc': run['started_at'],
            'new_metric_versions': sum(x['change_kind'] == 'new' for x in changed),
            'revised_metric_versions': sum(x['change_kind'] == 'revised' for x in changed),
            'unchanged_metric_versions': unchanged,
            'records': changed,
            'note': 'New means first observed by this collector. Revisions can include source metadata or classifications as well as changed values. Missing cards do not prove a company has not reported.',
        }
        pipeline.atomic_json(root / 'changes.json', summary)
        exports = {}
        for name in ['latest_metrics', 'coverage', 'quality_flags', 'universe_reference']:
            file = root / 'exports' / (name + '.json')
            if file.is_file():
                exports[name] = json.loads(file.read_text(encoding='utf-8'))
        day = datetime.fromisoformat(run['started_at']).astimezone(pipeline.IST).date().isoformat()
        destination = root / 'snapshots' / day / (run['id'] + '.json.gz')
        destination.parent.mkdir(parents=True, exist_ok=True)
        content = pipeline.canonical({'run': report, 'changes': summary, **exports}).encode('utf-8')
        if destination.exists():
            if gzip.decompress(destination.read_bytes()) != content:
                raise pipeline.PipelineError('An existing immutable capture has different contents')
        else:
            destination.write_bytes(gzip.compress(content, mtime=0))
        db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        return summary
    finally:
        db.close()


def main():
    _, root = pipeline.load_config(PROJECT / 'pipeline_config.json')
    summary = create_snapshot(root, os.environ.get('COLLECTION_STARTED_AT') or None)
    compact = {k: v for k, v in summary.items() if k not in {'records', 'note'}}
    print(json.dumps(compact, indent=2))
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a', encoding='utf-8') as output:
            output.write('### Moneycontrol capture\n\n')
            for key, value in compact.items():
                output.write(f'- {key}: {value}\n')
            output.write('\nSee `data/moneycontrol/exports/latest_run.json` for coverage and errors.\n')


if __name__ == '__main__':
    main()
