from .store import act,one,many
SCHEMA='''
CREATE TABLE IF NOT EXISTS boot_waves(id TEXT PRIMARY KEY,service TEXT,generation INTEGER,deadline INTEGER,level INTEGER,status TEXT,digest TEXT,artifact TEXT);
CREATE TABLE IF NOT EXISTS startup_slots(id TEXT PRIMARY KEY,node TEXT,capacity INTEGER,generation INTEGER,healthy INTEGER);
CREATE TABLE IF NOT EXISTS start_attempts(id TEXT PRIMARY KEY,wave TEXT,slot TEXT,generation INTEGER,slot_generation INTEGER,attempt_no INTEGER,started INTEGER,expires INTEGER,status TEXT,digest TEXT);
'''

def handle(db,x):
    op=x['op']
    if op=='slot':
        old=one(db,'SELECT * FROM startup_slots WHERE id=?',(x['id'],))
        if old and x['generation']<=old['generation']:return {'decision':'stale_slot'}
        if x['capacity']<1:return {'decision':'invalid_capacity'}
        db.execute('INSERT INTO startup_slots VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET node=excluded.node,capacity=excluded.capacity,generation=excluded.generation,healthy=excluded.healthy',(x['id'],x['node'],x['capacity'],x['generation'],int(x['healthy'])))
        return {'decision':'slot_recorded'}
    if op=='plan':
        s=one(db,'SELECT * FROM service_revisions WHERE id=?',(x['service'],))
        if not s or s['status']!='sealed' or s['generation']!=x['generation']:
            return {'decision':'contract_unavailable'}
        levels=many(db,'''WITH RECURSIVE ancestors(service,level) AS (SELECT dependency,1 FROM dependency_edges WHERE service=? UNION ALL SELECT e.dependency,a.level+1 FROM dependency_edges e JOIN ancestors a ON e.service=a.service) SELECT service,level FROM ancestors''',(x['service'],))
        if any(not one(db,"SELECT 1 FROM service_revisions WHERE id=? AND status='sealed'",(a['service'],)) for a in levels):
            return {'decision':'unsealed_dependency'}
        level=max((a['level'] for a in levels),default=0)
        # A revision sealed with an artifact pins that artifact's approved digest
        # on the wave; every launch of the wave must present exactly those bytes.
        digest=None
        artifact_id=s['artifact']
        if artifact_id:
            bundle=one(db,'SELECT * FROM image_bundles WHERE id=?',(artifact_id,))
            if not bundle:return {'decision':'artifact_unavailable'}
            digest=bundle['digest']
        db.execute('INSERT INTO boot_waves VALUES(?,?,?,?,?,?,?,?)',(x['id'],x['service'],x['generation'],x['deadline'],level,'planned',digest,artifact_id))
        return {'decision':'planned','level':level}
    if op=='launch':
        w=one(db,'SELECT * FROM boot_waves WHERE id=?',(x['wave'],))
        slot=one(db,'SELECT * FROM startup_slots WHERE id=?',(x['slot'],))
        token=one(db,'SELECT * FROM bootstrap_tokens WHERE id=?',(x['token'],))
        if not w or w['status']!='planned' or not slot or not slot['healthy'] or not token or token['service']!=w['service'] or token['generation']!=w['generation'] or token['status']!='active' or token['expires']<x['at']:
            return {'decision':'launch_denied'}
        current=one(db,'SELECT * FROM service_revisions WHERE id=?',(w['service'],))
        if not current or current['generation']!=w['generation'] or current['status']!='sealed':
            return {'decision':'stale_service_contract'}
        if w['digest'] is not None:
            # The wave pinned the approved digest at plan time: the launch must
            # name exactly those bytes and the pinned artifact must still hold a
            # live provenance certificate (its trust basis not revoked).
            if x.get('digest')!=w['digest']:return {'decision':'digest_mismatch'}
            from . import artifact
            if not artifact.valid_certificate(db,w['artifact']):return {'decision':'artifact_uncertified'}
        if x['expires']<=x['at'] or x['expires']>w['deadline']:return {'decision':'invalid_deadline'}
        occupied=one(db,"SELECT COUNT(*) n FROM start_attempts WHERE slot=? AND status='running'",(x['slot'],))['n']
        if occupied>=slot['capacity']:return {'decision':'slot_full'}
        deps=many(db,'SELECT * FROM dependency_edges WHERE service=?',(w['service'],))
        for d in deps:
            if d['critical']:
                parent=one(db,"SELECT * FROM readiness_receipts WHERE service=? AND status='published' AND (degrade_until IS NULL OR degrade_until>=?) ORDER BY published_at DESC LIMIT 1",(d['dependency'],x['at']))
                revision=one(db,'SELECT generation FROM service_revisions WHERE id=?',(d['dependency'],))
                if not parent or not revision or parent['generation']!=revision['generation']:
                    grant=one(db,"SELECT 1 FROM degrade_grants WHERE service=? AND dependency=? AND service_generation=? AND status='active' AND until_ts>=?",(w['service'],d['dependency'],w['generation'],x['at']))
                    if not grant:return {'decision':'dependency_not_ready','service':d['dependency']}
        attempt_no=one(db,'SELECT COALESCE(MAX(attempt_no),0)+1 next FROM start_attempts WHERE wave=?',(x['wave'],))['next']
        db.execute('INSERT INTO start_attempts VALUES(?,?,?,?,?,?,?,?,?,?)',(x['id'],x['wave'],x['slot'],w['generation'],slot['generation'],attempt_no,x['at'],x['expires'],'running',w['digest']))
        db.execute("UPDATE boot_waves SET status='running' WHERE id=?",(w['id'],))
        return {'decision':'launched','attempt_no':attempt_no}
    if op=='timeout':
        a=one(db,'SELECT * FROM start_attempts WHERE id=?',(x['id'],))
        if not a or a['status']!='running' or a['expires']>=x['at']:return {'decision':'not_timed_out'}
        db.execute("UPDATE start_attempts SET status='timed_out' WHERE id=?",(x['id'],))
        db.execute("UPDATE boot_waves SET status='failed' WHERE id=?",(a['wave'],))
        return {'decision':'timed_out'}
    if op=='retry':
        old=one(db,'SELECT * FROM start_attempts WHERE id=?',(x['old'],))
        if not old or old['status']!='timed_out':return {'decision':'not_retryable'}
        db.execute("UPDATE boot_waves SET status='planned' WHERE id=?",(old['wave'],))
        return {'decision':'retry_opened'}
    return {'decision':'unknown_operation'}

def run(db,x):return act(db,'wave',x,handle)
