"""Provisional royalty by active adapter row count."""

SCHEMA = '''
CREATE TABLE IF NOT EXISTS royalty_drafts(
    id TEXT PRIMARY KEY, publisher TEXT, count INTEGER, amount INTEGER
);
'''


def draft(db, draft_id, publisher, unit_fee):
    db.executescript(SCHEMA)
    n = db.execute('''SELECT COUNT(*) FROM plugin_activations a
                      JOIN bootstrap_plugins p ON p.id=a.plugin
                      WHERE p.publisher=? AND a.state='active' ''',
                   (publisher,)).fetchone()[0]
    db.execute('INSERT INTO royalty_drafts VALUES(?,?,?,?)',
               (draft_id, publisher, n, n*unit_fee))
    return n*unit_fee
