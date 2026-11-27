from .store import act,one,many
from . import duty
SCHEMA='''
CREATE TABLE IF NOT EXISTS probe_samples(id TEXT PRIMARY KEY,attempt TEXT,at INTEGER,healthy INTEGER);
CREATE TABLE IF NOT EXISTS readiness_receipts(id TEXT PRIMARY KEY,service TEXT,generation INTEGER,attempt TEXT,status TEXT,published_at INTEGER,degrade_until INTEGER);
CREATE TABLE IF NOT EXISTS rollback_cases(id TEXT PRIMARY KEY,service TEXT,cause TEXT,status TEXT);
'''

def handle(db,x):
    op=x['op']
    if op=='probe':
        denied=duty.gate(db,x)
        if denied:return denied
        a=one(db,'SELECT * FROM start_attempts WHERE id=?',(x['attempt'],))
        if not a or a['status']!='running' or not a['started']<=x['at']<=a['expires']:
            return {'decision':'probe_outside_attempt'}
        db.execute('INSERT INTO probe_samples VALUES(?,?,?,?)',(x['id'],x['attempt'],x['at'],int(x['healthy'])))
        return {'decision':'sampled'}
    if op=='publish':
        denied=duty.gate(db,x)
        if denied:return denied
        a=one(db,'SELECT * FROM start_attempts WHERE id=?',(x['attempt'],))
        w=one(db,'SELECT * FROM boot_waves WHERE id=?',(a['wave'],)) if a else None
        if not w or a['status']!='running' or a['generation']!=w['generation'] or x['at']>a['expires']:
            return {'decision':'attempt_unavailable'}
        revision=one(db,'SELECT * FROM service_revisions WHERE id=?',(w['service'],))
        if not revision or revision['generation']!=w['generation'] or revision['status']!='sealed':
            return {'decision':'stale_service_revision'}
        slot=one(db,'SELECT * FROM startup_slots WHERE id=?',(a['slot'],))
        if not slot or not slot['healthy'] or slot['generation']!=a['slot_generation']:
            return {'decision':'stale_startup_slot'}
        samples=many(db,'SELECT * FROM probe_samples WHERE attempt=? AND at<=? ORDER BY at DESC,id DESC LIMIT 2',(a['id'],x['at']))
        if len(samples)<2 or not all(s['healthy'] for s in samples):return {'decision':'health_incomplete'}
        degraded=[]
        for d in many(db,'SELECT * FROM dependency_edges WHERE service=? AND critical=1',(w['service'],)):
            parent=one(db,"SELECT * FROM readiness_receipts WHERE service=? AND status='published' AND (degrade_until IS NULL OR degrade_until>=?) ORDER BY published_at DESC LIMIT 1",(d['dependency'],x['at']))
            revision=one(db,'SELECT generation FROM service_revisions WHERE id=?',(d['dependency'],))
            if not parent or not revision or parent['generation']!=revision['generation']:
                grant=one(db,"SELECT * FROM degrade_grants WHERE service=? AND dependency=? AND service_generation=? AND status='active' AND until_ts>=? ORDER BY until_ts DESC LIMIT 1",(w['service'],d['dependency'],w['generation'],x['at']))
                if not grant:return {'decision':'parent_receipt_stale'}
                degraded.append(grant['until_ts'])
            elif parent['degrade_until'] is not None:
                degraded.append(parent['degrade_until'])
        db.execute('INSERT INTO readiness_receipts VALUES(?,?,?,?,?,?,?)',(x['id'],w['service'],w['generation'],a['id'],'published',x['at'],min(degraded) if degraded else None))
        db.execute("UPDATE start_attempts SET status='complete' WHERE id=?",(a['id'],))
        db.execute("UPDATE boot_waves SET status='complete' WHERE id=?",(w['id'],))
        return {'decision':'published'}
    if op=='revoke':
        r=one(db,'SELECT * FROM readiness_receipts WHERE id=?',(x['id'],))
        if not r or r['status']!='published':return {'decision':'not_published'}
        db.execute("UPDATE readiness_receipts SET status='revoked' WHERE id=?",(x['id'],))
        children=many(db,'''WITH RECURSIVE descendants(service) AS (SELECT service FROM dependency_edges WHERE dependency=? AND critical=1 UNION SELECT e.service FROM dependency_edges e JOIN descendants d ON e.dependency=d.service WHERE e.critical=1) SELECT service FROM descendants''',(r['service'],))
        for child in children:
            db.execute("UPDATE readiness_receipts SET status='revoked' WHERE service=? AND status='published'",(child['service'],))
            db.execute('INSERT OR IGNORE INTO rollback_cases VALUES(?,?,?,?)',(x['event_id']+'-'+child['service'],child['service'],r['service'],'pending'))
        return {'decision':'revoked','children':[x['service'] for x in children]}
    if op=='expire':
        due=many(db,"SELECT * FROM readiness_receipts WHERE status='published' AND degrade_until<?",(x['at'],))
        for receipt in due:
            db.execute("UPDATE readiness_receipts SET status='revoked' WHERE id=?",(receipt['id'],))
            children=many(db,'''WITH RECURSIVE descendants(service) AS (SELECT service FROM dependency_edges WHERE dependency=? AND critical=1 UNION SELECT e.service FROM dependency_edges e JOIN descendants d ON e.dependency=d.service WHERE e.critical=1) SELECT service FROM descendants''',(receipt['service'],))
            for child in children:
                db.execute("UPDATE readiness_receipts SET status='revoked' WHERE service=? AND status='published'",(child['service'],))
                db.execute('INSERT OR IGNORE INTO rollback_cases VALUES(?,?,?,?)',(x['event_id']+'-'+child['service'],child['service'],receipt['service'],'pending'))
        return {'decision':'expired','receipts':[r['id'] for r in due]}
    if op=='rollback':
        denied=duty.gate(db,x)
        if denied:return denied
        case=one(db,'SELECT * FROM rollback_cases WHERE id=?',(x['id'],))
        if not case or case['status']!='pending':return {'decision':'no_pending_case'}
        attempts=many(db,"SELECT a.id FROM start_attempts a JOIN boot_waves w ON w.id=a.wave WHERE w.service=? AND a.status='running'",(case['service'],))
        db.execute("UPDATE start_attempts SET status='rolled_back' WHERE wave IN (SELECT id FROM boot_waves WHERE service=?) AND status='running'",(case['service'],))
        db.execute("UPDATE boot_waves SET status='rolled_back' WHERE service=? AND status IN ('running','complete')",(case['service'],))
        db.execute("UPDATE rollback_cases SET status='complete' WHERE id=?",(x['id'],))
        return {'decision':'rolled_back','aborted_attempts':[a['id'] for a in attempts]}
    if op in ('case','open_case','rollback_case'):
        db.execute('INSERT INTO rollback_cases VALUES(?,?,?,?)',
                   (x['id'],x['service'],x.get('cause','manual_case'),'pending'))
        return {'decision':'rollback_case_opened'}
    return {'decision':'unknown_operation'}

def run(db,x):return act(db,'ready',x,handle)
