from .store import act, one

SCHEMA = '''
CREATE TABLE IF NOT EXISTS cooling_zones(id TEXT PRIMARY KEY, rack TEXT, limit_c INTEGER, revision INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS thermal_samples(id TEXT PRIMARY KEY, zone TEXT, sensor TEXT, at INTEGER, celsius INTEGER);
CREATE TABLE IF NOT EXISTS node_thermal_checks(id TEXT PRIMARY KEY, zone TEXT, node TEXT, sample TEXT, state TEXT);
'''

def handle(db, x):
    if x['op'] == 'zone':
        db.execute('INSERT INTO cooling_zones VALUES(?,?,?,?,?)',
                   (x['id'], x['rack'], x['limit_c'], x['revision'], 'active'))
        return {'decision': 'zone_recorded'}
    if x['op'] == 'sample':
        zone = one(db, 'SELECT * FROM cooling_zones WHERE id=?', (x['zone'],))
        if not zone:
            return {'decision': 'zone_missing'}
        db.execute('INSERT INTO thermal_samples VALUES(?,?,?,?,?)',
                   (x['id'], x['zone'], x['sensor'], x['at'], x['celsius']))
        return {'decision': 'temperature_recorded'}
    if x['op'] == 'check':
        zone = one(db, 'SELECT * FROM cooling_zones WHERE id=?', (x['zone'],))
        sample = one(db, 'SELECT * FROM thermal_samples WHERE id=?', (x['sample'],))
        if not zone or not sample or sample['zone'] != zone['id']:
            return {'decision': 'sample_unavailable'}
        state = 'qualified' if sample['celsius'] <= zone['limit_c'] else 'over_limit'
        db.execute('INSERT INTO node_thermal_checks VALUES(?,?,?,?,?)',
                   (x['id'], zone['id'], x['node'], sample['id'], state))
        return {'decision': state}
    return {'decision': 'unknown_operation'}

def run(db, x):
    return act(db, 'thermal', x, handle)
