from .store import act, one

SCHEMA = '''
CREATE TABLE IF NOT EXISTS image_bundles(id TEXT PRIMARY KEY, service TEXT, generation INTEGER, digest TEXT, signer TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS build_attestations(id TEXT PRIMARY KEY, bundle TEXT, builder TEXT, manifest_digest TEXT, at INTEGER);
CREATE TABLE IF NOT EXISTS image_mounts(id TEXT PRIMARY KEY, bundle TEXT, attempt TEXT, node TEXT, state TEXT);
'''

def handle(db, x):
    if x['op'] == 'bundle':
        if not x['digest'] or not x['signer']:
            return {'decision': 'invalid_bundle'}
        db.execute('INSERT INTO image_bundles VALUES(?,?,?,?,?,?)',
                   (x['id'], x['service'], x['generation'], x['digest'], x['signer'], 'registered'))
        return {'decision': 'bundle_registered'}
    if x['op'] == 'attest':
        bundle = one(db, 'SELECT * FROM image_bundles WHERE id=?', (x['bundle'],))
        if not bundle:
            return {'decision': 'bundle_missing'}
        db.execute('INSERT INTO build_attestations VALUES(?,?,?,?,?)',
                   (x['id'], x['bundle'], x['builder'], x['manifest_digest'], x['at']))
        return {'decision': 'attestation_recorded'}
    if x['op'] == 'mount':
        bundle = one(db, 'SELECT * FROM image_bundles WHERE id=?', (x['bundle'],))
        if not bundle or bundle['state'] != 'registered':
            return {'decision': 'bundle_unavailable'}
        db.execute('INSERT INTO image_mounts VALUES(?,?,?,?,?)',
                   (x['id'], x['bundle'], x['attempt'], x['node'], 'mounted'))
        return {'decision': 'mounted'}
    return {'decision': 'unknown_operation'}

def run(db, x):
    return act(db, 'artifact', x, handle)
