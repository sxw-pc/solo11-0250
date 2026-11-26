from .store import act, one

SCHEMA = '''
CREATE TABLE IF NOT EXISTS duty_shifts(id TEXT PRIMARY KEY, actor TEXT, site TEXT, starts INTEGER, ends INTEGER, qualification TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS duty_handoffs(id TEXT PRIMARY KEY, outgoing TEXT, incoming TEXT, wave TEXT, at INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS duty_acknowledgments(id TEXT PRIMARY KEY, handoff TEXT, actor TEXT, at INTEGER);
'''

def handle(db, x):
    if x['op'] == 'shift':
        if x['ends'] <= x['starts']:
            return {'decision': 'invalid_shift'}
        db.execute('INSERT INTO duty_shifts VALUES(?,?,?,?,?,?,?)',
                   (x['id'], x['actor'], x['site'], x['starts'], x['ends'], x['qualification'], 'active'))
        return {'decision': 'shift_recorded'}
    if x['op'] == 'handoff':
        outgoing = one(db, 'SELECT * FROM duty_shifts WHERE id=?', (x['outgoing'],))
        incoming = one(db, 'SELECT * FROM duty_shifts WHERE id=?', (x['incoming'],))
        if not outgoing or not incoming or outgoing['state'] != 'active' or incoming['state'] != 'active':
            return {'decision': 'shift_unavailable'}
        db.execute('INSERT INTO duty_handoffs VALUES(?,?,?,?,?,?)',
                   (x['id'], x['outgoing'], x['incoming'], x['wave'], x['at'], 'pending'))
        return {'decision': 'handoff_opened'}
    if x['op'] == 'ack':
        handoff = one(db, 'SELECT * FROM duty_handoffs WHERE id=?', (x['handoff'],))
        if not handoff:
            return {'decision': 'handoff_missing'}
        db.execute('INSERT INTO duty_acknowledgments VALUES(?,?,?,?)',
                   (x['id'], x['handoff'], x['actor'], x['at']))
        return {'decision': 'acknowledged'}
    return {'decision': 'unknown_operation'}

def run(db, x):
    return act(db, 'duty', x, handle)
