"""A provisional erasure list excluding active incident holds."""

SCHEMA = '''
CREATE TABLE IF NOT EXISTS operator_holds(
    id TEXT PRIMARY KEY, record_id TEXT, case_id TEXT,
    authority TEXT, state TEXT
);
'''


def hold(db, hold_id, record_id, case_id, authority):
    db.executescript(SCHEMA)
    db.execute('INSERT INTO operator_holds VALUES(?,?,?,?,?)',
               (hold_id, record_id, case_id, authority, 'active'))


def provisional_erasure(db):
    db.executescript(SCHEMA)
    return [row[0] for row in db.execute('''SELECT r.id FROM operator_records r
        WHERE r.state='active' AND NOT EXISTS
        (SELECT 1 FROM operator_holds h WHERE h.record_id=r.id AND h.state='active')
        ORDER BY r.id''')]
