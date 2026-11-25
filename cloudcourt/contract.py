from .store import act,one,many
SCHEMA='''
CREATE TABLE IF NOT EXISTS service_revisions(id TEXT PRIMARY KEY,generation INTEGER,status TEXT,critical INTEGER);
CREATE TABLE IF NOT EXISTS dependency_edges(service TEXT,dependency TEXT,critical INTEGER,revision INTEGER,PRIMARY KEY(service,dependency));
CREATE TABLE IF NOT EXISTS bootstrap_tokens(id TEXT PRIMARY KEY,service TEXT,generation INTEGER,expires INTEGER,status TEXT);
CREATE TABLE IF NOT EXISTS degrade_grants(id TEXT PRIMARY KEY,service TEXT,dependency TEXT,service_generation INTEGER,approver TEXT,until_ts INTEGER,status TEXT);
'''

def handle(db,x):
    op=x['op']
    if op=='revision':
        old=one(db,'SELECT * FROM service_revisions WHERE id=?',(x['id'],))
        if old and x['generation']<=old['generation']:return {'decision':'stale_revision'}
        db.execute('INSERT INTO service_revisions VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET generation=excluded.generation,status=excluded.status,critical=excluded.critical',(x['id'],x['generation'],'draft',int(x.get('critical',True))))
        db.execute("UPDATE bootstrap_tokens SET status='stale' WHERE service=? AND generation<?",(x['id'],x['generation']))
        return {'decision':'revision_recorded'}
    if op=='edge':
        a=one(db,'SELECT * FROM service_revisions WHERE id=?',(x['service'],))
        b=one(db,'SELECT * FROM service_revisions WHERE id=?',(x['dependency'],))
        if not a or not b or a['status']!='draft' or a['generation']!=x['revision']:
            return {'decision':'edge_unavailable'}
        if x['service']==x['dependency']:return {'decision':'cycle'}
        # A recursive reachability check rejects cycles before persisting the edge.
        cycle=one(db,'''WITH RECURSIVE r(id) AS (SELECT dependency FROM dependency_edges WHERE service=? UNION SELECT e.dependency FROM dependency_edges e JOIN r ON e.service=r.id) SELECT id FROM r WHERE id=? LIMIT 1''',(x['dependency'],x['service']))
        if cycle:return {'decision':'cycle'}
        db.execute('INSERT INTO dependency_edges VALUES(?,?,?,?)',(x['service'],x['dependency'],int(x['critical']),x['revision']))
        return {'decision':'edge_added'}
    if op=='seal':
        s=one(db,'SELECT * FROM service_revisions WHERE id=?',(x['id'],))
        if not s or s['status']!='draft' or s['generation']!=x['generation']:return {'decision':'not_sealable'}
        db.execute("UPDATE service_revisions SET status='sealed' WHERE id=?",(x['id'],))
        return {'decision':'sealed'}
    if op=='token':
        s=one(db,'SELECT * FROM service_revisions WHERE id=?',(x['service'],))
        if not s or s['status']!='sealed' or s['generation']!=x['generation'] or x['expires']<=x['at']:
            return {'decision':'token_denied'}
        db.execute('INSERT INTO bootstrap_tokens VALUES(?,?,?,?,?)',(x['id'],x['service'],x['generation'],x['expires'],'active'))
        return {'decision':'token_issued'}
    if op=='degrade':
        edge=one(db,'SELECT * FROM dependency_edges WHERE service=? AND dependency=?',(x['service'],x['dependency']))
        revision=one(db,'SELECT * FROM service_revisions WHERE id=?',(x['service'],))
        if not edge or not edge['critical'] or not revision or revision['status']!='sealed' or not x.get('approver') or x['until']<=x['at']:
            return {'decision':'degrade_denied'}
        db.execute('INSERT INTO degrade_grants VALUES(?,?,?,?,?,?,?)',(x['id'],x['service'],x['dependency'],revision['generation'],x['approver'],x['until'],'active'))
        return {'decision':'degrade_allowed'}
    if op=='reassess':
        s=one(db,'SELECT * FROM service_revisions WHERE id=?',(x['service'],))
        if not s:return {'decision':'unknown_service'}
        missing=[]
        for edge in many(db,'SELECT * FROM dependency_edges WHERE service=? AND critical=1',(x['service'],)):
            parent=one(db,"SELECT * FROM readiness_receipts WHERE service=? AND status='published' ORDER BY published_at DESC LIMIT 1",(edge['dependency'],))
            rev=one(db,'SELECT generation FROM service_revisions WHERE id=?',(edge['dependency'],))
            if not parent or not rev or parent['generation']!=rev['generation']:
                missing.append(edge['dependency'])
        if missing:
            db.execute("UPDATE service_revisions SET status='blocked' WHERE id=?",(x['service'],))
            return {'decision':'dependency_contract_blocked','missing':missing}
        if s['status']=='blocked':
            db.execute("UPDATE service_revisions SET status='sealed' WHERE id=?",(x['service'],))
        return {'decision':'dependency_contract_ready'}
    return {'decision':'unknown_operation'}

def run(db,x):return act(db,'contract',x,handle)
