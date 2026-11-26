from .store import act, one

SCHEMA = '''
CREATE TABLE IF NOT EXISTS rack_feeds(id TEXT PRIMARY KEY, rack TEXT, rated_kw INTEGER, revision INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS power_reservations(id TEXT PRIMARY KEY, feed TEXT, node TEXT, kw INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS power_observations(id TEXT PRIMARY KEY, feed TEXT, at INTEGER, measured_kw INTEGER);
'''

def handle(db, x):
    if x['op'] == 'feed':
        if x['rated_kw'] < 1:
            return {'decision': 'invalid_feed'}
        db.execute('INSERT INTO rack_feeds VALUES(?,?,?,?,?)',
                   (x['id'], x['rack'], x['rated_kw'], x['revision'], 'active'))
        return {'decision': 'feed_recorded'}
    if x['op'] == 'reserve':
        feed = one(db, 'SELECT * FROM rack_feeds WHERE id=?', (x['feed'],))
        if not feed or feed['state'] != 'active' or x['kw'] < 1:
            return {'decision': 'feed_unavailable'}
        used = one(db, "SELECT COALESCE(SUM(kw),0) kw FROM power_reservations WHERE feed=? AND state='active'", (x['feed'],))['kw']
        if used + x['kw'] > feed['rated_kw']:
            return {'decision': 'power_full'}
        db.execute('INSERT INTO power_reservations VALUES(?,?,?,?,?)',
                   (x['id'], x['feed'], x['node'], x['kw'], 'active'))
        return {'decision': 'power_reserved'}
    if x['op'] == 'observe':
        feed = one(db, 'SELECT * FROM rack_feeds WHERE id=?', (x['feed'],))
        if not feed:
            return {'decision': 'feed_missing'}
        db.execute('INSERT INTO power_observations VALUES(?,?,?,?)',
                   (x['id'], x['feed'], x['at'], x['measured_kw']))
        return {'decision': 'power_observed'}
    return {'decision': 'unknown_operation'}

def run(db, x):
    return act(db, 'power', x, handle)
