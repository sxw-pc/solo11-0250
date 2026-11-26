"""Third-party bootstrap adapter deployments."""

SCHEMA = '''
CREATE TABLE IF NOT EXISTS bootstrap_plugins(
    id TEXT PRIMARY KEY, digest TEXT, publisher TEXT, license_id TEXT
);
CREATE TABLE IF NOT EXISTS plugin_activations(
    id TEXT PRIMARY KEY, plugin TEXT, service TEXT, wave TEXT,
    site TEXT, state TEXT
);
'''


def catalog(db, plugin_id, digest, publisher, license_id):
    db.executescript(SCHEMA)
    db.execute('INSERT INTO bootstrap_plugins VALUES(?,?,?,?)',
               (plugin_id, digest, publisher, license_id))


def activate(db, activation_id, plugin_id, service, wave, site):
    db.executescript(SCHEMA)
    db.execute('INSERT INTO plugin_activations VALUES(?,?,?,?,?,?)',
               (activation_id, plugin_id, service, wave, site, 'active'))
