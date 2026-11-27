"""Customer cold-start recovery promises."""

from .store import act

SCHEMA = '''
CREATE TABLE IF NOT EXISTS customer_slas(
    id TEXT PRIMARY KEY, customer TEXT, service TEXT,
    min_healthy INTEGER, unit_credit INTEGER, state TEXT
);
'''


def subscribe(db, sla_id, customer, service, min_healthy, unit_credit):
    # SCHEMA is applied by store.connect; executescript would implicitly
    # commit the surrounding IMMEDIATE event transaction.
    db.execute('INSERT INTO customer_slas VALUES(?,?,?,?,?,?)',
               (sla_id, customer, service, min_healthy, unit_credit, 'active'))


def handle(db, x):
    if x['op'] != 'sla_subscribe':
        return {'decision': 'unknown_operation'}
    old = db.execute('SELECT id FROM customer_slas WHERE id=?',
                     (x['sla_id'],)).fetchone()
    if old:
        return {'decision': 'business_identity_conflict'}
    subscribe(db, x['sla_id'], x['customer'], x['service'],
              x['min_healthy'], x['unit_credit'])
    return {'decision': 'sla_subscribed'}


def run(db, x):
    return act(db, 'sla', x, handle)
