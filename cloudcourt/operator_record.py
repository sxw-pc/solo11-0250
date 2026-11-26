"""Operator contact facts attached to startup probe work."""

SCHEMA = '''
CREATE TABLE IF NOT EXISTS operator_records(
    id TEXT PRIMARY KEY, actor TEXT, attempt TEXT,
    contact TEXT, state TEXT
);
'''


def record(db, record_id, actor, attempt, contact):
    db.executescript(SCHEMA)
    db.execute('INSERT INTO operator_records VALUES(?,?,?,?,?)',
               (record_id, actor, attempt, contact, 'active'))
