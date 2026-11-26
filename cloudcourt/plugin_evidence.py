"""Signed load observations and licensing documents for adapter use."""

SCHEMA = '''
CREATE TABLE IF NOT EXISTS plugin_loads(
    id TEXT PRIMARY KEY, wave TEXT, site TEXT, digest TEXT,
    observed_at INTEGER, uncertainty INTEGER, attestor TEXT
);
CREATE TABLE IF NOT EXISTS plugin_assignments(
    id TEXT PRIMARY KEY, digest TEXT, claimant TEXT,
    effective_at INTEGER, customer_consent TEXT
);
CREATE TABLE IF NOT EXISTS plugin_bank_receipts(
    id TEXT PRIMARY KEY, draft TEXT, payee TEXT,
    amount INTEGER, bank_state TEXT
);
'''


def load(db, load_id, wave, site, digest, observed_at, uncertainty, attestor):
    db.executescript(SCHEMA)
    db.execute('INSERT INTO plugin_loads VALUES(?,?,?,?,?,?,?)',
               (load_id, wave, site, digest, observed_at, uncertainty, attestor))


def assignment(db, assignment_id, digest, claimant, effective_at, customer_consent):
    db.executescript(SCHEMA)
    db.execute('INSERT INTO plugin_assignments VALUES(?,?,?,?,?)',
               (assignment_id, digest, claimant, effective_at, customer_consent))


def bank(db, receipt_id, draft, payee, amount, bank_state):
    db.executescript(SCHEMA)
    db.execute('INSERT INTO plugin_bank_receipts VALUES(?,?,?,?,?)',
               (receipt_id, draft, payee, amount, bank_state))
