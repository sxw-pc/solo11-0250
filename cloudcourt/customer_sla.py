"""Customer cold-start recovery promises."""

SCHEMA = '''
CREATE TABLE IF NOT EXISTS customer_slas(
    id TEXT PRIMARY KEY, customer TEXT, service TEXT,
    min_healthy INTEGER, unit_credit INTEGER, state TEXT
);
'''


def subscribe(db, sla_id, customer, service, min_healthy, unit_credit):
    db.executescript(SCHEMA)
    db.execute('INSERT INTO customer_slas VALUES(?,?,?,?,?,?)',
               (sla_id, customer, service, min_healthy, unit_credit, 'active'))
