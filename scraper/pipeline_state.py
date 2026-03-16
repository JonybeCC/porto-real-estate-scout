#!/usr/bin/env python3
"""
Pipeline State Tracker — JBizz Assistant 🦞

Single source of truth for pipeline run history and step completion.
Written to data/pipeline_state.json after each step.

Usage:
  from pipeline_state import PipelineRun
  
  run = PipelineRun.start('daily_update')
  run.step_ok('scrape', listings=129, new=3)
  run.step_fail('enrich', error='ZenRows timeout')
  run.finish()

  # Query last run:
  last = PipelineRun.last()
  hours_since = last.hours_ago()
  was_ok = last.ok
"""

import json, os, time
from datetime import datetime, timezone

STATE_FILE = '/root/.openclaw/workspace/projects/real-estate/data/pipeline_state.json'
MAX_RUNS   = 30  # keep last 30 runs


class PipelineRun:
    def __init__(self, name: str, run_id: str, started_at: str):
        self.name       = name
        self.run_id     = run_id
        self.started_at = started_at
        self.steps: list[dict] = []
        self.ok         = False
        self.finished_at: str | None = None
        self.summary: dict = {}

    # ── Class methods ─────────────────────────────────────────────────────────

    @classmethod
    def start(cls, name: str) -> 'PipelineRun':
        """Start a new pipeline run and persist it immediately."""
        now = datetime.now(timezone.utc).isoformat()
        run_id = f'{name}-{int(time.time())}'
        run = cls(name, run_id, now)
        run._save()
        print(f'📋 Pipeline [{name}] started — run_id={run_id}')
        return run

    @classmethod
    def last(cls, name: str | None = None) -> 'PipelineRun | None':
        """Return the most recent completed run (optionally filtered by name)."""
        state = _load_state()
        runs = state.get('runs', [])
        if name:
            runs = [r for r in runs if r.get('name') == name]
        completed = [r for r in runs if r.get('finished_at')]
        if not completed:
            return None
        latest = max(completed, key=lambda r: r.get('finished_at', ''))
        return cls._from_dict(latest)

    @classmethod
    def last_ok(cls, name: str | None = None) -> 'PipelineRun | None':
        """Return the most recent SUCCESSFUL run."""
        state = _load_state()
        runs = state.get('runs', [])
        if name:
            runs = [r for r in runs if r.get('name') == name]
        ok_runs = [r for r in runs if r.get('ok') and r.get('finished_at')]
        if not ok_runs:
            return None
        return cls._from_dict(max(ok_runs, key=lambda r: r['finished_at']))

    @classmethod
    def _from_dict(cls, d: dict) -> 'PipelineRun':
        run = cls(d['name'], d['run_id'], d['started_at'])
        run.steps       = d.get('steps', [])
        run.ok          = d.get('ok', False)
        run.finished_at = d.get('finished_at')
        run.summary     = d.get('summary', {})
        return run

    # ── Instance methods ──────────────────────────────────────────────────────

    def step_ok(self, step: str, **kwargs):
        """Record a successful step with optional metadata."""
        entry = {
            'step':   step,
            'status': 'ok',
            'ts':     datetime.now(timezone.utc).isoformat(),
            **kwargs,
        }
        self.steps.append(entry)
        self._save()
        meta = '  '.join(f'{k}={v}' for k, v in kwargs.items())
        print(f'  ✅ [{step}] {meta}')

    def step_skip(self, step: str, reason: str = ''):
        """Record a skipped step."""
        entry = {'step': step, 'status': 'skip', 'reason': reason,
                 'ts': datetime.now(timezone.utc).isoformat()}
        self.steps.append(entry)
        self._save()
        print(f'  ⏭️  [{step}] skipped — {reason}')

    def step_fail(self, step: str, error: str = ''):
        """Record a failed step."""
        entry = {'step': step, 'status': 'fail', 'error': error,
                 'ts': datetime.now(timezone.utc).isoformat()}
        self.steps.append(entry)
        self._save()
        print(f'  ❌ [{step}] failed — {error[:120]}')

    def finish(self, ok: bool = True, **summary_kwargs):
        """Mark the run complete and persist final state."""
        self.ok          = ok
        self.finished_at = datetime.now(timezone.utc).isoformat()
        self.summary     = summary_kwargs
        self._save()
        status = '✅ OK' if ok else '❌ FAILED'
        elapsed = self._elapsed_s()
        print(f'📋 Pipeline [{self.name}] {status} — {elapsed:.0f}s elapsed')

    def hours_ago(self) -> float | None:
        """Hours since this run finished. None if not finished."""
        if not self.finished_at:
            return None
        dt = datetime.fromisoformat(self.finished_at)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600

    def step_status(self, step: str) -> str | None:
        """Return status of a named step, or None if not run."""
        for s in reversed(self.steps):
            if s['step'] == step:
                return s['status']
        return None

    # ── Internal ──────────────────────────────────────────────────────────────

    def _to_dict(self) -> dict:
        return {
            'name':        self.name,
            'run_id':      self.run_id,
            'started_at':  self.started_at,
            'finished_at': self.finished_at,
            'ok':          self.ok,
            'steps':       self.steps,
            'summary':     self.summary,
        }

    def _save(self):
        state = _load_state()
        runs  = state.get('runs', [])
        # Replace existing run with same run_id, or append
        updated = False
        for i, r in enumerate(runs):
            if r.get('run_id') == self.run_id:
                runs[i] = self._to_dict()
                updated = True
                break
        if not updated:
            runs.append(self._to_dict())
        # Trim to MAX_RUNS
        state['runs'] = runs[-MAX_RUNS:]
        state['last_updated'] = datetime.now(timezone.utc).isoformat()
        _save_state(state)

    def _elapsed_s(self) -> float:
        if not self.finished_at:
            return 0.0
        start = datetime.fromisoformat(self.started_at)
        end   = datetime.fromisoformat(self.finished_at)
        if start.tzinfo is None: start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:   end   = end.replace(tzinfo=timezone.utc)
        return (end - start).total_seconds()


# ── Module-level helpers ──────────────────────────────────────────────────────

def _load_state() -> dict:
    try:
        with open(STATE_FILE, encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {'runs': []}


def _save_state(state: dict):
    try:
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except OSError as e:
        print(f'⚠️  pipeline_state: could not save: {e}')


def print_summary(last_n: int = 10):
    """Print a human-readable table of the last N runs."""
    state = _load_state()
    runs  = state.get('runs', [])[-last_n:]
    print(f'\n📋 Pipeline history (last {len(runs)} runs)')
    print(f'  {"Run ID":<35} {"Name":<15} {"Status":<8} {"Elapsed":<8} {"Finished"}')
    print(f'  {"-"*90}')
    for r in reversed(runs):
        ok      = '✅' if r.get('ok') else '❌'
        fin     = r.get('finished_at', '—')[:16].replace('T', ' ')
        if r.get('finished_at') and r.get('started_at'):
            try:
                s  = datetime.fromisoformat(r['started_at'])
                e  = datetime.fromisoformat(r['finished_at'])
                if s.tzinfo is None: s = s.replace(tzinfo=timezone.utc)
                if e.tzinfo is None: e = e.replace(tzinfo=timezone.utc)
                elapsed = f'{(e-s).total_seconds():.0f}s'
            except (ValueError, TypeError):
                elapsed = '?'
        else:
            elapsed = '—'
        print(f'  {r.get("run_id","?"):<35} {r.get("name","?"):<15} {ok:<8} {elapsed:<8} {fin}')
    print()


if __name__ == '__main__':
    import sys
    if '--history' in sys.argv:
        print_summary()
    elif '--last' in sys.argv:
        last = PipelineRun.last()
        if last:
            print(json.dumps(last._to_dict(), indent=2))
        else:
            print('No runs found')
