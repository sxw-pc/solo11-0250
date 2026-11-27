"""Provisional credit by failed wave count."""

SCHEMA = '''
CREATE TABLE IF NOT EXISTS credit_drafts(
    id TEXT PRIMARY KEY, sla TEXT, failures INTEGER,
    amount INTEGER, state TEXT
);
'''


def provisional(db, draft_id, sla_id):
    # SCHEMA is applied by store.connect; executescript would implicitly
    # commit the surrounding IMMEDIATE event transaction.
    sla = db.execute('SELECT service,unit_credit FROM customer_slas WHERE id=?',
                     (sla_id,)).fetchone()
    if not sla:
        return 0
    failures = db.execute('''SELECT COUNT(*) FROM boot_waves
                             WHERE service=? AND status='failed' ''', (sla[0],)).fetchone()[0]
    amount = failures*sla[1]
    db.execute('INSERT INTO credit_drafts VALUES(?,?,?,?,?)',
               (draft_id, sla_id, failures, amount, 'draft'))
    return amount
