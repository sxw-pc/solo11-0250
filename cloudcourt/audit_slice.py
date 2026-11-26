"""Provisional service-specific export from the event log."""

import json

SCHEMA = '''
CREATE TABLE IF NOT EXISTS audit_slices(
    id TEXT PRIMARY KEY, service TEXT, document TEXT, state TEXT
);
'''


def export(db, slice_id, service):
    db.executescript(SCHEMA)
    events = [dict(zip(('seq','subsystem','command','event_id','payload','result'), row))
              for row in db.execute('''SELECT seq,subsystem,command,event_id,payload,result
                                      FROM events WHERE payload LIKE ? ORDER BY seq''',
                                   ('%' + service + '%',))]
    document = json.dumps(events, sort_keys=True)
    db.execute('INSERT INTO audit_slices VALUES(?,?,?,?)',
               (slice_id, service, document, 'draft'))
    return events
