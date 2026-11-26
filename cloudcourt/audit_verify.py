"""A provisional verifier that only checks whether an export has rows."""

import json

SCHEMA = '''
CREATE TABLE IF NOT EXISTS audit_checks(
    id TEXT PRIMARY KEY, slice_id TEXT, rows INTEGER, state TEXT
);
'''


def provisional(db, check_id, slice_id):
    db.executescript(SCHEMA)
    row = db.execute('SELECT document FROM audit_slices WHERE id=?',
                     (slice_id,)).fetchone()
    n = len(json.loads(row[0])) if row else 0
    state = 'verified' if n else 'empty'
    db.execute('INSERT INTO audit_checks VALUES(?,?,?,?)',
               (check_id, slice_id, n, state))
    return state
