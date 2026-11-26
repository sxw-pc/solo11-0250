from .store import act, one

SCHEMA = '''
CREATE TABLE IF NOT EXISTS trial_plans(id TEXT PRIMARY KEY, service TEXT, generation INTEGER, clients INTEGER, until_ts INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS trial_samples(id TEXT PRIMARY KEY, plan TEXT, at INTEGER, latency_ms INTEGER, ok INTEGER);
CREATE TABLE IF NOT EXISTS trial_reports(id TEXT PRIMARY KEY, plan TEXT, measured INTEGER, state TEXT);
'''

def handle(db, x):
    if x['op'] == 'plan':
        if x['clients'] < 1:
            return {'decision': 'invalid_trial'}
        db.execute('INSERT INTO trial_plans VALUES(?,?,?,?,?,?)',
                   (x['id'], x['service'], x['generation'], x['clients'], x['until'], 'open'))
        return {'decision': 'trial_planned'}
    if x['op'] == 'sample':
        plan = one(db, 'SELECT * FROM trial_plans WHERE id=?', (x['plan'],))
        if not plan or plan['state'] != 'open' or x['at'] > plan['until_ts']:
            return {'decision': 'trial_unavailable'}
        db.execute('INSERT INTO trial_samples VALUES(?,?,?,?,?)',
                   (x['id'], x['plan'], x['at'], x['latency_ms'], int(x['ok'])))
        return {'decision': 'trial_sampled'}
    if x['op'] == 'report':
        plan = one(db, 'SELECT * FROM trial_plans WHERE id=?', (x['plan'],))
        if not plan:
            return {'decision': 'trial_missing'}
        measured = one(db, 'SELECT COUNT(*) n FROM trial_samples WHERE plan=?', (x['plan'],))['n']
        db.execute('INSERT INTO trial_reports VALUES(?,?,?,?)',
                   (x['id'], x['plan'], measured, 'draft'))
        return {'decision': 'trial_reported', 'measured': measured}
    return {'decision': 'unknown_operation'}

def run(db, x):
    return act(db, 'loadtest', x, handle)
