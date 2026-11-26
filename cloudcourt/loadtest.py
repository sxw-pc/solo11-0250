import json
from math import ceil
from .store import act, one, many

SCHEMA = '''
CREATE TABLE IF NOT EXISTS trial_plans(id TEXT PRIMARY KEY, service TEXT, generation INTEGER, clients INTEGER, until_ts INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS trial_samples(id TEXT PRIMARY KEY, plan TEXT, at INTEGER, latency_ms INTEGER, ok INTEGER);
CREATE TABLE IF NOT EXISTS trial_reports(id TEXT PRIMARY KEY, plan TEXT, measured INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS load_generators(id TEXT PRIMARY KEY, status TEXT, dataset TEXT, isolated INTEGER, registered_at INTEGER);
CREATE TABLE IF NOT EXISTS capacity_plans(plan TEXT PRIMARY KEY, service TEXT, generation INTEGER, attempt TEXT, generator TEXT, dataset TEXT, window_start INTEGER, window_end INTEGER, target_clients INTEGER, slot_fingerprint TEXT, upstreams TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS capacity_classes(plan TEXT, class_id TEXT, clients INTEGER, priority TEXT, min_throughput_mrps INTEGER, max_p99_ms INTEGER, max_error_bp INTEGER, PRIMARY KEY(plan,class_id));
CREATE TABLE IF NOT EXISTS trial_arrivals(plan TEXT, seq INTEGER, at INTEGER, class_id TEXT, kind TEXT, request_id TEXT, dataset TEXT, PRIMARY KEY(plan,seq));
CREATE TABLE IF NOT EXISTS trial_requests(id TEXT PRIMARY KEY, plan TEXT, generator TEXT, class_id TEXT, kind TEXT, issued INTEGER, outcome TEXT, latency_ms INTEGER, retries INTEGER, weight INTEGER, at INTEGER, dataset TEXT);
CREATE TABLE IF NOT EXISTS trial_guards(plan TEXT, metric TEXT, observed INTEGER, limit_ms INTEGER, violated INTEGER, PRIMARY KEY(plan,metric));
CREATE TABLE IF NOT EXISTS trial_report_versions(plan TEXT, version INTEGER, state TEXT, window_start INTEGER, window_end INTEGER, intended INTEGER, responded INTEGER, success INTEGER, errors INTEGER, unsent INTEGER, empty INTEGER, retries INTEGER, throughput_mrps INTEGER, error_bp INTEGER, unfulfilled_bp INTEGER, PRIMARY KEY(plan,version));
CREATE TABLE IF NOT EXISTS trial_class_results(plan TEXT, version INTEGER, class_id TEXT, intended INTEGER, responded INTEGER, success INTEGER, errors INTEGER, unsent INTEGER, p95_ms INTEGER, p99_ms INTEGER, throughput_mrps INTEGER, error_bp INTEGER, pass INTEGER, PRIMARY KEY(plan,version,class_id));
CREATE TABLE IF NOT EXISTS capacity_certificates(id TEXT PRIMARY KEY, plan TEXT, version INTEGER, tier TEXT, capacity_clients INTEGER, unfulfilled_clients INTEGER, state TEXT, credits INTEGER, waitlist_until INTEGER, attempt TEXT, fingerprint TEXT, issued_at INTEGER, signed_at INTEGER, revoked_at INTEGER);
CREATE TABLE IF NOT EXISTS customer_onboards(id TEXT PRIMARY KEY, certificate TEXT, plan TEXT, tier TEXT, activated_at INTEGER, onboarded_clients INTEGER, waitlisted_clients INTEGER, waitlist_until INTEGER, credits INTEGER, state TEXT);
'''

BUSINESS = 'business'
EMPTY = 'empty'


def _percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    rank = ceil(pct / 100.0 * len(ordered))
    return ordered[max(rank - 1, 0)]


def _service_slots(db, service):
    rows = many(db, '''SELECT DISTINCT a.slot sid FROM start_attempts a
                       JOIN boot_waves w ON w.id=a.wave WHERE w.service=?''', (service,))
    fingerprint = []
    for row in rows:
        slot = one(db, 'SELECT * FROM startup_slots WHERE id=?', (row['sid'],))
        if slot:
            fingerprint.append([slot['id'], slot['generation'], int(slot['healthy'])])
    return json.dumps(sorted(fingerprint), sort_keys=True)


def _critical_upstreams(db, service):
    return [e['dependency'] for e in
            many(db, 'SELECT dependency FROM dependency_edges WHERE service=? AND critical=1', (service,))]


def handle(db, x):
    op = x['op']
    if op == 'plan':
        if x['clients'] < 1:
            return {'decision': 'invalid_trial'}
        db.execute('INSERT INTO trial_plans VALUES(?,?,?,?,?,?)',
                   (x['id'], x['service'], x['generation'], x['clients'], x['until'], 'open'))
        return {'decision': 'trial_planned'}
    if op == 'sample':
        plan = one(db, 'SELECT * FROM trial_plans WHERE id=?', (x['plan'],))
        if not plan or plan['state'] != 'open' or x['at'] > plan['until_ts']:
            return {'decision': 'trial_unavailable'}
        db.execute('INSERT INTO trial_samples VALUES(?,?,?,?,?)',
                   (x['id'], x['plan'], x['at'], x['latency_ms'], int(x['ok'])))
        return {'decision': 'trial_sampled'}
    if op == 'report':
        plan = one(db, 'SELECT * FROM trial_plans WHERE id=?', (x['plan'],))
        if not plan:
            return {'decision': 'trial_missing'}
        measured = one(db, 'SELECT COUNT(*) n FROM trial_samples WHERE plan=?', (x['plan'],))['n']
        db.execute('INSERT INTO trial_reports VALUES(?,?,?,?)',
                   (x['id'], x['plan'], measured, 'draft'))
        return {'decision': 'trial_reported', 'measured': measured}

    if op == 'register_generator':
        if not x.get('dataset') or not x.get('isolated'):
            return {'decision': 'dataset_not_isolated'}
        db.execute('INSERT INTO load_generators VALUES(?,?,?,?,?)',
                   (x['id'], 'active', x['dataset'], 1, x['at']))
        return {'decision': 'generator_registered', 'dataset': x['dataset']}

    if op == 'capacity_plan':
        gen = one(db, 'SELECT * FROM load_generators WHERE id=?', (x['generator'],))
        if not gen:
            return {'decision': 'generator_not_registered'}
        if gen['status'] != 'active':
            return {'decision': 'generator_retired'}
        if not gen['isolated'] or x['dataset'] != gen['dataset']:
            return {'decision': 'dataset_not_isolated'}
        attempt = one(db, 'SELECT * FROM start_attempts WHERE id=?', (x['attempt'],))
        wave = one(db, 'SELECT * FROM boot_waves WHERE id=?', (attempt['wave'],)) if attempt else None
        if not attempt or attempt['status'] != 'running' or not wave \
                or wave['service'] != x['service'] or wave['generation'] != x['generation'] \
                or attempt['generation'] != x['generation']:
            return {'decision': 'attempt_unavailable'}
        revision = one(db, "SELECT * FROM service_revisions WHERE id=? AND status='sealed' AND generation=?",
                       (x['service'], x['generation']))
        if not revision:
            return {'decision': 'stale_service_revision'}
        if x['window_start'] < attempt['started'] or x['window_end'] > attempt['expires'] \
                or x['window_start'] >= x['window_end']:
            return {'decision': 'window_outside_attempt'}
        classes = x['classes']
        if not classes or sum(c['clients'] for c in classes) != x['target_clients']:
            return {'decision': 'invalid_class_capacity'}
        arrivals = x['arrivals']
        if not arrivals:
            return {'decision': 'empty_timeline'}
        class_ids = {c['class_id'] for c in classes}
        seen = set()
        for arrival in arrivals:
            if arrival['class_id'] not in class_ids or arrival['kind'] != BUSINESS:
                return {'decision': 'timeline_class_unknown'}
            if not x['window_start'] <= arrival['at'] <= x['window_end']:
                return {'decision': 'timeline_outside_window'}
            if arrival['request_id'] in seen:
                return {'decision': 'timeline_duplicate_request'}
            seen.add(arrival['request_id'])
        upstreams = {}
        for dep in _critical_upstreams(db, x['service']):
            receipt = one(db, """SELECT * FROM readiness_receipts WHERE service=? AND status='published'
                                 AND (degrade_until IS NULL OR degrade_until>=?)
                                 ORDER BY published_at DESC LIMIT 1""", (dep, x['window_start']))
            rev = one(db, 'SELECT generation FROM service_revisions WHERE id=?', (dep,))
            if not receipt or not rev or receipt['generation'] != rev['generation']:
                return {'decision': 'upstream_not_ready', 'dependency': dep}
            upstreams[dep] = receipt['id']
        fingerprint = _service_slots(db, x['service'])
        db.execute('INSERT INTO capacity_plans VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                   (x['id'], x['service'], x['generation'], x['attempt'], x['generator'], x['dataset'],
                    x['window_start'], x['window_end'], x['target_clients'], fingerprint,
                    json.dumps(upstreams, sort_keys=True), 'open'))
        for c in classes:
            db.execute('INSERT INTO capacity_classes VALUES(?,?,?,?,?,?,?)',
                       (x['id'], c['class_id'], c['clients'], c['priority'],
                        c['min_throughput_mrps'], c['max_p99_ms'], c['max_error_bp']))
        for seq, arrival in enumerate(arrivals):
            db.execute('INSERT INTO trial_arrivals VALUES(?,?,?,?,?,?,?)',
                       (x['id'], seq, arrival['at'], arrival['class_id'], BUSINESS,
                        arrival['request_id'], x['dataset']))
        return {'decision': 'capacity_trial_planned', 'plan': x['id'],
                'arrivals': len(arrivals), 'slot_fingerprint': fingerprint,
                'upstreams': upstreams}

    if op == 'request':
        plan = one(db, 'SELECT * FROM capacity_plans WHERE plan=?', (x['plan'],))
        if not plan:
            return {'decision': 'plan_not_found'}
        if plan['state'] != 'open':
            return {'decision': 'plan_closed'}
        if x['generator'] != plan['generator']:
            return {'decision': 'generator_mismatch'}
        if x['dataset'] == 'production' or plan['dataset'] == 'production':
            return {'decision': 'production_write_blocked'}
        if x['dataset'] != plan['dataset']:
            return {'decision': 'dataset_mismatch'}
        if not plan['window_start'] <= x['at'] <= plan['window_end']:
            return {'decision': 'outside_measurement_window'}
        kind = x.get('kind', BUSINESS)
        class_id = None
        weight = int(x.get('weight', 1))
        if kind == BUSINESS:
            arrival = one(db, 'SELECT * FROM trial_arrivals WHERE plan=? AND request_id=?',
                          (x['plan'], x['id']))
            if not arrival:
                return {'decision': 'request_not_on_timeline'}
            class_id = arrival['class_id']
            if x['issued'] and arrival['dataset'] != x['dataset']:
                return {'decision': 'dataset_mismatch'}
        elif kind != EMPTY or weight < 1:
            return {'decision': 'invalid_request_kind'}
        if x['issued'] and x['outcome'] not in ('ok', 'error', 'timeout'):
            return {'decision': 'invalid_outcome'}
        if x['issued'] and x['retries'] < 0:
            return {'decision': 'invalid_outcome'}
        db.execute('INSERT INTO trial_requests VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                   (x['id'], x['plan'], x['generator'], class_id, kind, int(x['issued']),
                    x['outcome'] if x['issued'] else None,
                    x['latency_ms'] if x['issued'] else None, int(x['retries']), weight,
                    x['at'], x['dataset']))
        return {'decision': 'request_recorded', 'class_id': class_id, 'kind': kind}

    if op == 'guard':
        plan = one(db, 'SELECT * FROM capacity_plans WHERE plan=?', (x['plan'],))
        if not plan:
            return {'decision': 'plan_not_found'}
        violated = 1 if x['observed'] > x['limit'] else 0
        db.execute('INSERT INTO trial_guards VALUES(?,?,?,?,?) ON CONFLICT(plan,metric) DO UPDATE SET observed=excluded.observed,limit_ms=excluded.limit_ms,violated=excluded.violated',
                   (x['plan'], x['metric'], x['observed'], x['limit'], violated))
        return {'decision': 'guard_recorded', 'violated': bool(violated)}

    if op == 'accept':
        plan = one(db, 'SELECT * FROM capacity_plans WHERE plan=?', (x['plan'],))
        if not plan:
            return {'decision': 'plan_not_found'}
        guards = many(db, 'SELECT * FROM trial_guards WHERE plan=?', (x['plan'],))
        if not guards:
            return {'decision': 'guards_missing'}
        window_ms = plan['window_end'] - plan['window_start']
        requests = many(db, 'SELECT * FROM trial_requests WHERE plan=?', (x['plan'],))
        arrivals = many(db, 'SELECT * FROM trial_arrivals WHERE plan=? ORDER BY seq', (x['plan'],))
        req_by_id = {r['id']: r for r in requests if r['kind'] == BUSINESS}
        version = one(db, 'SELECT COALESCE(MAX(version),0)+1 v FROM trial_report_versions WHERE plan=?',
                      (x['plan'],))['v']
        class_rows = many(db, 'SELECT * FROM capacity_classes WHERE plan=?', (x['plan'],))
        covered = 0
        any_pass = False
        for c in class_rows:
            cid = c['class_id']
            intended = sum(1 for a in arrivals if a['class_id'] == cid)
            class_reqs = [req_by_id[a['request_id']] for a in arrivals
                          if a['class_id'] == cid and a['request_id'] in req_by_id]
            responded = [r for r in class_reqs if r['issued']]
            success_rows = [r for r in responded if r['outcome'] == 'ok']
            errors = sum(1 for r in responded if r['outcome'] != 'ok')
            unsent = intended - len(class_reqs)
            latencies = [r['latency_ms'] for r in success_rows]
            p95 = _percentile(latencies, 95)
            p99 = _percentile(latencies, 99)
            throughput = len(success_rows) * 1000 // window_ms
            error_bp = (errors + unsent) * 10000 // intended if intended else 0
            passed = int(p99 is not None and p99 <= c['max_p99_ms']
                         and error_bp <= c['max_error_bp']
                         and throughput >= c['min_throughput_mrps'])
            if passed:
                covered += c['clients']
                any_pass = True
            db.execute('INSERT INTO trial_class_results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (x['plan'], version, cid, intended, len(responded), len(success_rows),
                        errors, unsent, p95, p99, throughput, error_bp, passed))
        business = [r for r in requests if r['kind'] == BUSINESS]
        empty = sum(1 for r in requests if r['kind'] == EMPTY)
        responded_all = [r for r in business if r['issued']]
        success_all = [r for r in responded_all if r['outcome'] == 'ok']
        errors_all = sum(1 for r in responded_all if r['outcome'] != 'ok')
        unsent_all = len(arrivals) - len({r['id'] for r in business})
        retries_all = sum(r['retries'] for r in business)
        intended_all = len(arrivals)
        throughput_all = len(success_all) * 1000 // window_ms
        error_bp_all = (errors_all + unsent_all) * 10000 // intended_all if intended_all else 0
        unfulfilled_bp = max(plan['target_clients'] - covered, 0) * 10000 // plan['target_clients']
        guard_failed = any(g['violated'] for g in guards)
        window_ok = all(plan['window_start'] <= r['at'] <= plan['window_end'] for r in requests) \
            and all(plan['window_start'] <= a['at'] <= plan['window_end'] for a in arrivals)
        if guard_failed or not window_ok:
            state = 'failed'
        elif covered >= plan['target_clients']:
            state = 'passed'
        elif not any_pass:
            state = 'failed'
        else:
            state = 'partial'
        db.execute('INSERT INTO trial_report_versions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (x['plan'], version, state, plan['window_start'], plan['window_end'],
                    intended_all, len(responded_all), len(success_all), errors_all, unsent_all,
                    empty, retries_all, throughput_all, error_bp_all, unfulfilled_bp))
        return {'decision': 'trial_accepted', 'plan': x['plan'], 'version': version, 'state': state,
                'intended': intended_all, 'responded': len(responded_all), 'success': len(success_all),
                'errors': errors_all, 'unsent': unsent_all, 'empty': empty, 'retries': retries_all,
                'error_bp': error_bp_all, 'throughput_mrps': throughput_all,
                'unfulfilled_bp': unfulfilled_bp, 'covered_clients': covered}

    if op == 'certify':
        plan = one(db, 'SELECT * FROM capacity_plans WHERE plan=?', (x['plan'],))
        if not plan:
            return {'decision': 'plan_not_found'}
        report = one(db, 'SELECT * FROM trial_report_versions WHERE plan=? AND version=?',
                     (x['plan'], x['version']))
        if not report:
            return {'decision': 'report_not_found'}
        attempt = one(db, 'SELECT * FROM start_attempts WHERE id=?', (plan['attempt'],))
        if not attempt or attempt['status'] != 'running':
            return {'decision': 'attempt_closed'}
        revision = one(db, 'SELECT * FROM service_revisions WHERE id=?', (plan['service'],))
        if not revision or revision['generation'] != plan['generation'] or revision['status'] != 'sealed':
            return {'decision': 'stale_service_revision'}
        current_fp = _service_slots(db, plan['service'])
        if current_fp != plan['slot_fingerprint']:
            return {'decision': 'instance_drifted', 'expected': plan['slot_fingerprint'],
                    'observed': current_fp}
        for dep, receipt_id in json.loads(plan['upstreams']).items():
            receipt = one(db, "SELECT * FROM readiness_receipts WHERE id=? AND status='published'",
                          (receipt_id,))
            rev = one(db, 'SELECT generation FROM service_revisions WHERE id=?', (dep,))
            if not receipt or not rev or receipt['generation'] != rev['generation']:
                return {'decision': 'upstream_drifted', 'dependency': dep}
        if report['state'] == 'passed':
            tier, credits, waitlist_until = 'full', 0, None
        elif report['state'] == 'partial':
            if x.get('waitlist_until') is None or x.get('credits') is None \
                    or x['waitlist_until'] <= x['at'] or x['credits'] <= 0:
                return {'decision': 'partial_terms_missing'}
            tier, credits, waitlist_until = 'partial', x['credits'], x['waitlist_until']
        else:
            return {'decision': 'certificate_refused'}
        capacity = plan['target_clients'] - report['unfulfilled_bp'] * plan['target_clients'] // 10000
        unfulfilled = plan['target_clients'] - capacity
        db.execute('INSERT INTO capacity_certificates VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (x['id'], x['plan'], x['version'], tier, capacity, unfulfilled, 'issued',
                    credits, waitlist_until, plan['attempt'], plan['slot_fingerprint'], x['at'],
                    None, None))
        return {'decision': 'certificate_issued', 'certificate': x['id'], 'tier': tier,
                'capacity_clients': capacity, 'unfulfilled_clients': unfulfilled,
                'credits': credits, 'waitlist_until': waitlist_until}

    if op == 'sign':
        cert = one(db, 'SELECT * FROM capacity_certificates WHERE id=?', (x['id'],))
        if not cert:
            return {'decision': 'certificate_not_found'}
        if cert['state'] != 'issued':
            return {'decision': 'certificate_not_signable'}
        db.execute('UPDATE capacity_certificates SET state=?, signed_at=? WHERE id=?',
                   ('signed', x['at'], x['id']))
        return {'decision': 'certificate_signed', 'certificate': x['id'], 'tier': cert['tier']}

    if op == 'withdraw':
        cert = one(db, 'SELECT * FROM capacity_certificates WHERE id=?', (x['id'],))
        if not cert or cert['state'] not in ('issued', 'signed'):
            return {'decision': 'certificate_not_active'}
        db.execute('UPDATE capacity_certificates SET state=?, revoked_at=? WHERE id=?',
                   ('revoked', x['at'], x['id']))
        return {'decision': 'certificate_withdrawn', 'certificate': x['id']}

    if op == 'onboard':
        cert = one(db, 'SELECT * FROM capacity_certificates WHERE id=?', (x['certificate'],))
        if not cert:
            return {'decision': 'certificate_not_found'}
        if cert['state'] != 'signed':
            return {'decision': 'certificate_not_signed'}
        if cert['tier'] == 'full':
            waitlisted, waitlist_until, credits = 0, None, 0
        else:
            waitlisted, waitlist_until, credits = cert['unfulfilled_clients'], cert['waitlist_until'], cert['credits']
        db.execute('INSERT INTO customer_onboards VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (x['id'], x['certificate'], cert['plan'], cert['tier'], x['at'],
                    cert['capacity_clients'], waitlisted, waitlist_until, credits, 'active'))
        return {'decision': 'customers_onboarded', 'onboard': x['id'], 'tier': cert['tier'],
                'activated_at': x['at'], 'onboarded_clients': cert['capacity_clients'],
                'waitlisted_clients': waitlisted, 'waitlist_until': waitlist_until,
                'credits': credits}
    return {'decision': 'unknown_operation'}


def run(db, x):
    return act(db, 'loadtest', x, handle)
