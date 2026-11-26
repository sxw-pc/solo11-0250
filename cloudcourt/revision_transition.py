from .store import act,one,many
SCHEMA='''
CREATE TABLE IF NOT EXISTS revision_transitions(id TEXT PRIMARY KEY,service TEXT,from_generation INTEGER,to_generation INTEGER,interface_compatible INTEGER,migration_phase TEXT,rollback_until INTEGER,status TEXT,announced_at INTEGER);
CREATE TABLE IF NOT EXISTS transition_edges(transition TEXT,service TEXT,dependency TEXT,PRIMARY KEY(transition,service,dependency));
CREATE TABLE IF NOT EXISTS traffic_permits(id TEXT PRIMARY KEY,service TEXT,dependency TEXT,service_generation INTEGER,allow_read INTEGER,allow_write INTEGER,approver TEXT,until_ts INTEGER,status TEXT);
CREATE TABLE IF NOT EXISTS traffic_rulings(id TEXT PRIMARY KEY,service TEXT,transition TEXT,receipt TEXT,read_verdict TEXT,write_verdict TEXT,basis TEXT,permit TEXT,at INTEGER);
CREATE TABLE IF NOT EXISTS billing_ledger(id TEXT PRIMARY KEY,service TEXT,dependency_generation INTEGER,key_format TEXT,amount INTEGER,status TEXT,owner TEXT,confirmation_prefix TEXT,at INTEGER);
CREATE TABLE IF NOT EXISTS traffic_audit(id TEXT PRIMARY KEY,service TEXT,operation TEXT,basis TEXT,permit TEXT,at INTEGER);
'''
OPEN_TRANSITIONS=('announced','migrating','failed')

def valid_receipt(db,service):
    revision=one(db,'SELECT * FROM service_revisions WHERE id=?',(service,))
    if not revision:return None
    return one(db,"SELECT * FROM readiness_receipts WHERE service=? AND generation=? AND status='published' ORDER BY published_at DESC,id DESC LIMIT 1",(service,revision['generation']))

def reconciliation_owed(db,service):
    # A disruptive transition that completed still owes reconciliation of confirmed bills
    # before full read/write service may resume.
    return one(db,'''SELECT t.id FROM revision_transitions t JOIN transition_edges e ON e.transition=t.id
        WHERE e.service=? AND t.status='complete' AND (t.interface_compatible=0 OR t.migration_phase<>'none')
        AND EXISTS(SELECT 1 FROM billing_ledger b WHERE b.service=e.service AND b.status='confirmed') LIMIT 1''',(service,))

def affected_dependencies(db,service):
    declared=[row['dependency'] for row in many(db,"SELECT e.dependency FROM transition_edges e JOIN revision_transitions t ON t.id=e.transition WHERE e.service=? AND t.status IN ('announced','migrating','failed') ORDER BY e.dependency",(service,))]
    if declared:return declared
    return [edge['dependency'] for edge in many(db,'SELECT * FROM dependency_edges WHERE service=? AND critical=1 ORDER BY dependency',(service,)) if not valid_receipt(db,edge['dependency'])]

def evaluate(db,service,at):
    # Live ingress verdict from current evidence: own receipt, reconciliation debt, scoped permits.
    receipt=valid_receipt(db,service)
    if receipt:
        if reconciliation_owed(db,service):
            return {'read':'deny','write':'deny','basis':'reconciliation','permit':None,'receipt':receipt['id']}
        return {'read':'allow','write':'allow','basis':'current','permit':None,'receipt':receipt['id']}
    revision=one(db,'SELECT * FROM service_revisions WHERE id=?',(service,))
    generation=revision['generation'] if revision else -1
    dependencies=affected_dependencies(db,service)
    read_ok=bool(dependencies);write_ok=bool(dependencies);used=None
    for dependency in dependencies:
        permit=one(db,"SELECT * FROM traffic_permits WHERE service=? AND dependency=? AND service_generation=? AND status='active' AND until_ts>=? ORDER BY until_ts DESC,id DESC LIMIT 1",(service,dependency,generation,at))
        if not permit or not permit['allow_read']:read_ok=False
        if not permit or not permit['allow_write']:write_ok=False
        if permit and used is None:used=permit['id']
    basis='permit' if (read_ok or write_ok) else 'pause'
    return {'read':'allow' if read_ok else 'deny','write':'allow' if write_ok else 'deny','basis':basis,'permit':used,'receipt':None}

def append_ruling(db,event_id,service,at,transition=None):
    verdict=evaluate(db,service,at)
    db.execute('INSERT INTO traffic_rulings VALUES(?,?,?,?,?,?,?,?,?)',(event_id+':'+service,service,transition,verdict['receipt'],verdict['read'],verdict['write'],verdict['basis'],verdict['permit'],at))
    return verdict

def descendants(db,service):
    return many(db,'''WITH RECURSIVE d(service) AS (SELECT service FROM dependency_edges WHERE dependency=? AND critical=1 UNION SELECT e.service FROM dependency_edges e JOIN d ON e.dependency=d.service WHERE e.critical=1) SELECT service FROM d''',(service,))

def supersede_service(db,service):
    # Old receipts are superseded, never resurrected; unconfirmed writes are held with their owner.
    receipts=many(db,"SELECT * FROM readiness_receipts WHERE service=? AND status='published'",(service,))
    if not receipts:return False
    db.execute("UPDATE readiness_receipts SET status='superseded' WHERE service=? AND status='published'",(service,))
    db.execute("UPDATE billing_ledger SET status='held',owner=? WHERE service=? AND status='unconfirmed'",(service,service))
    return True

def handle(db,x):
    op=x['op']
    if op=='transition':
        current=one(db,'SELECT * FROM service_revisions WHERE id=?',(x['service'],))
        if not current:return {'decision':'unknown_service'}
        if 'interface_compatible' not in x or 'migration_phase' not in x or 'rollback_until' not in x or 'edges' not in x:
            return {'decision':'declaration_incomplete'}
        if x['to_generation']<=current['generation']:return {'decision':'stale_revision'}
        if x['from_generation']!=current['generation']:return {'decision':'revision_base_mismatch'}
        if x['migration_phase'] not in ('none','pending','migrating'):return {'decision':'invalid_migration_phase'}
        if x['rollback_until']<x['at']:return {'decision':'invalid_rollback_window'}
        for edge in x['edges']:
            if not one(db,'SELECT * FROM dependency_edges WHERE service=? AND dependency=?',(edge['service'],edge['dependency'])):
                return {'decision':'unknown_edge'}
        # Not every generation change is a failure: only interface breaks or open data
        # migration phases disrupt already-running downstream services.
        disruptive=(not x['interface_compatible']) or x['migration_phase']!='none'
        status={'none':'complete','pending':'announced','migrating':'migrating'}[x['migration_phase']]
        db.execute('UPDATE service_revisions SET generation=?,status=? WHERE id=?',(x['to_generation'],'draft',x['service']))
        db.execute("UPDATE bootstrap_tokens SET status='stale' WHERE service=? AND generation<?",(x['service'],x['to_generation']))
        db.execute('DELETE FROM dependency_edges WHERE service=? AND revision<?',(x['service'],x['to_generation']))
        db.execute('INSERT INTO revision_transitions VALUES(?,?,?,?,?,?,?,?,?)',(x['id'],x['service'],x['from_generation'],x['to_generation'],int(x['interface_compatible']),x['migration_phase'],x['rollback_until'],status,x['at']))
        for edge in x['edges']:
            db.execute('INSERT INTO transition_edges VALUES(?,?,?)',(x['id'],edge['service'],edge['dependency']))
        readjudicated=[]
        if disruptive:
            services=[]
            for edge in x['edges']:
                if edge['service'] not in services:services.append(edge['service'])
            for service in list(services):
                for child in descendants(db,service):
                    if child['service'] not in services:services.append(child['service'])
            for service in services:
                if supersede_service(db,service):
                    append_ruling(db,x['event_id'],service,x['at'],x['id'])
                    readjudicated.append(service)
        return {'decision':'transition_recorded','readjudicated':readjudicated}
    if op=='permit':
        edge=one(db,'SELECT * FROM dependency_edges WHERE service=? AND dependency=?',(x['service'],x['dependency']))
        revision=one(db,'SELECT * FROM service_revisions WHERE id=?',(x['service'],))
        operations=set(x.get('operations',[]))
        if not edge or not edge['critical'] or not revision or revision['status']!='sealed' or not x.get('approver') or x['until']<=x['at'] or not operations or not operations<={'read','write'}:
            return {'decision':'permit_denied'}
        db.execute('INSERT INTO traffic_permits VALUES(?,?,?,?,?,?,?,?,?)',(x['id'],x['service'],x['dependency'],revision['generation'],int('read' in operations),int('write' in operations),x['approver'],x['until'],'active'))
        verdict=append_ruling(db,x['event_id'],x['service'],x['at'])
        return {'decision':'permit_issued','read':verdict['read'],'write':verdict['write']}
    if op=='permit_revoke':
        permit=one(db,'SELECT * FROM traffic_permits WHERE id=?',(x['id'],))
        if not permit or permit['status']!='active':return {'decision':'permit_unavailable'}
        db.execute("UPDATE traffic_permits SET status='revoked' WHERE id=?",(x['id'],))
        append_ruling(db,x['event_id'],permit['service'],x['at'])
        return {'decision':'permit_revoked'}
    if op=='expire':
        due=many(db,"SELECT * FROM traffic_permits WHERE status='active' AND until_ts<?",(x['at'],))
        for permit in due:
            db.execute("UPDATE traffic_permits SET status='expired' WHERE id=?",(permit['id'],))
        for service in {permit['service'] for permit in due}:
            append_ruling(db,x['event_id'],service,x['at'])
        return {'decision':'expired','permits':[permit['id'] for permit in due]}
    if op=='read':
        verdict=evaluate(db,x['service'],x['at'])
        if verdict['read']=='allow':
            # Read-only continuation under a permit shows legacy-format data and owes an audit trail.
            if verdict['basis']=='permit':
                db.execute('INSERT INTO traffic_audit VALUES(?,?,?,?,?,?)',(x['event_id']+':audit',x['service'],'read',verdict['basis'],verdict['permit'],x['at']))
            return {'decision':'allowed','basis':verdict['basis']}
        return {'decision':'denied','basis':verdict['basis']}
    if op=='write':
        verdict=evaluate(db,x['service'],x['at'])
        if verdict['write']!='allow':return {'decision':'write_denied','basis':verdict['basis']}
        dependency=one(db,'SELECT dependency FROM dependency_edges WHERE service=? AND critical=1 ORDER BY dependency LIMIT 1',(x['service'],))
        generation=None
        if dependency:
            parent=one(db,'SELECT generation FROM service_revisions WHERE id=?',(dependency['dependency'],))
            generation=parent['generation'] if parent else None
        db.execute('INSERT INTO billing_ledger VALUES(?,?,?,?,?,?,?,?,?)',(x['id'],x['service'],generation,x['key_format'],x['amount'],'unconfirmed',None,None,x['at']))
        return {'decision':'write_recorded','bill':x['id']}
    if op=='confirm':
        bill=one(db,'SELECT * FROM billing_ledger WHERE id=?',(x['id'],))
        if not bill or bill['status']!='unconfirmed':return {'decision':'not_confirmable'}
        db.execute("UPDATE billing_ledger SET status='confirmed',confirmation_prefix=? WHERE id=?",(x['confirmation_prefix'],x['id']))
        return {'decision':'confirmed'}
    if op=='resolve':
        bill=one(db,'SELECT * FROM billing_ledger WHERE id=?',(x['id'],))
        if not bill or bill['status']!='held':return {'decision':'not_resolvable'}
        if x['by']!=bill['owner']:return {'decision':'not_responsible'}
        if x['resolution']=='confirm':
            db.execute("UPDATE billing_ledger SET status='confirmed',confirmation_prefix=? WHERE id=?",(x['confirmation_prefix'],x['id']))
        elif x['resolution']=='abort':
            db.execute("UPDATE billing_ledger SET status='aborted' WHERE id=?",(x['id'],))
        else:return {'decision':'not_resolvable'}
        return {'decision':'resolved','resolution':x['resolution']}
    if op=='migrate':
        transition=one(db,'SELECT * FROM revision_transitions WHERE id=?',(x['id'],))
        if not transition or transition['status'] not in ('announced','migrating'):return {'decision':'no_open_transition'}
        if x['phase'] not in ('migrating','complete','failed'):return {'decision':'invalid_phase'}
        db.execute('UPDATE revision_transitions SET status=? WHERE id=?',(x['phase'],x['id']))
        for edge in many(db,'SELECT * FROM transition_edges WHERE transition=?',(x['id'],)):
            append_ruling(db,x['event_id'],edge['service'],x['at'],x['id'])
        return {'decision':'migration_updated','phase':x['phase']}
    if op=='reconcile':
        transition=one(db,'SELECT * FROM revision_transitions WHERE id=?',(x['id'],))
        if not transition or transition['status']!='complete':return {'decision':'reconciliation_unavailable'}
        if not one(db,'SELECT * FROM transition_edges WHERE transition=? AND service=?',(x['id'],x['service'])):return {'decision':'unknown_edge'}
        pending=many(db,"SELECT * FROM billing_ledger WHERE service=? AND status IN ('unconfirmed','held')",(x['service'],))
        if pending:return {'decision':'unresolved_writes','bills':[bill['id'] for bill in pending]}
        bills=many(db,"SELECT * FROM billing_ledger WHERE service=? AND status='confirmed'",(x['service'],))
        db.execute("UPDATE billing_ledger SET status='reconciled' WHERE service=? AND status='confirmed'",(x['service'],))
        verdict=append_ruling(db,x['event_id'],x['service'],x['at'],x['id'])
        return {'decision':'reconciled','bills':[bill['id'] for bill in bills],'basis':verdict['basis']}
    if op=='readjudicate':
        if not one(db,'SELECT * FROM service_revisions WHERE id=?',(x['service'],)):return {'decision':'unknown_service'}
        verdict=append_ruling(db,x['event_id'],x['service'],x['at'])
        return {'decision':'readjudicated','read':verdict['read'],'write':verdict['write'],'basis':verdict['basis']}
    if op=='rollback':
        transition=one(db,'SELECT * FROM revision_transitions WHERE id=?',(x['id'],))
        if not transition or transition['status'] not in OPEN_TRANSITIONS:return {'decision':'rollback_unavailable'}
        if x['at']>transition['rollback_until']:return {'decision':'rollback_window_closed'}
        services=[edge['service'] for edge in many(db,'SELECT * FROM transition_edges WHERE transition=?',(x['id'],))]
        evidence=[]
        if services:
            marks=','.join('?'*len(services))
            evidence=many(db,f"SELECT * FROM billing_ledger WHERE service IN ({marks}) AND dependency_generation=? AND status IN ('confirmed','reconciled')",(*services,transition['from_generation']))
        # Rolling back requires proof of the old data format and the customer confirmation
        # prefixes recorded on confirmed bills; decrementing the generation number is not enough.
        if not evidence:return {'decision':'rollback_evidence_missing'}
        if any(bill['key_format']!=x['data_format'] or bill['confirmation_prefix']!=x['confirmation_prefix'] for bill in evidence):
            return {'decision':'rollback_evidence_invalid'}
        db.execute("UPDATE service_revisions SET generation=?,status='sealed' WHERE id=?",(transition['from_generation'],transition['service']))
        db.execute("UPDATE revision_transitions SET status='rolled_back' WHERE id=?",(x['id'],))
        for service in services:
            append_ruling(db,x['event_id'],service,x['at'],x['id'])
        return {'decision':'rolled_back','service':transition['service'],'generation':transition['from_generation']}
    return {'decision':'unknown_operation'}

def run(db,x):return act(db,'revision_transition',x,handle)
