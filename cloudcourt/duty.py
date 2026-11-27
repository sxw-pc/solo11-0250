import json

from .store import act, one, many

SCHEMA = '''
CREATE TABLE IF NOT EXISTS probe_samples(id TEXT PRIMARY KEY,attempt TEXT,at INTEGER,healthy INTEGER);
CREATE TABLE IF NOT EXISTS readiness_receipts(id TEXT PRIMARY KEY,service TEXT,generation INTEGER,attempt TEXT,status TEXT,published_at INTEGER,degrade_until INTEGER);
CREATE TABLE IF NOT EXISTS rollback_cases(id TEXT PRIMARY KEY,service TEXT,cause TEXT,status TEXT);
CREATE TABLE IF NOT EXISTS duty_shifts(id TEXT PRIMARY KEY, actor TEXT, site TEXT, starts INTEGER, ends INTEGER, qualification TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS duty_handoffs(id TEXT PRIMARY KEY, outgoing TEXT, incoming TEXT, wave TEXT, at INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS duty_acknowledgments(id TEXT PRIMARY KEY, handoff TEXT, actor TEXT, at INTEGER);
CREATE TABLE IF NOT EXISTS training_issuers(id TEXT PRIMARY KEY, name TEXT, status TEXT);
CREATE TABLE IF NOT EXISTS qualification_certificates(
  id TEXT PRIMARY KEY, actor TEXT, name TEXT, service_level TEXT,
  site TEXT, location TEXT, equipment TEXT, issuer TEXT,
  starts INTEGER, expires INTEGER, issued_at INTEGER,
  status TEXT, revoked_at INTEGER, revoke_reason TEXT);
CREATE TABLE IF NOT EXISTS designated_supervisors(
  id TEXT PRIMARY KEY, actor TEXT, site TEXT, starts INTEGER, ends INTEGER);
CREATE TABLE IF NOT EXISTS site_sessions(
  id TEXT PRIMARY KEY, actor TEXT, site TEXT, kind TEXT, independent INTEGER,
  node TEXT, permissions TEXT, started_at INTEGER, ended_at INTEGER,
  state TEXT, closed_reason TEXT);
CREATE TABLE IF NOT EXISTS shift_details(
  shift_id TEXT PRIMARY KEY, kind TEXT, home_site TEXT, cover_for TEXT,
  supervisor TEXT, note TEXT, relieved_at INTEGER);
CREATE TABLE IF NOT EXISTS shift_withdrawals(
  id TEXT PRIMARY KEY, shift_id TEXT, actor TEXT, at INTEGER,
  recorded_at INTEGER, reason TEXT);
CREATE TABLE IF NOT EXISTS shift_extensions(
  id TEXT PRIMARY KEY, shift_id TEXT, actor TEXT, site TEXT,
  starts INTEGER, ends INTEGER, at INTEGER, supervisor TEXT,
  officer_signed_at INTEGER, supervisor_signed_at INTEGER,
  received_at INTEGER, reason TEXT, bound_kind TEXT, bound_ref TEXT,
  supervised INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS emergency_authorizations(
  id TEXT PRIMARY KEY, actor TEXT, site TEXT, service_level TEXT,
  supervisor TEXT, at INTEGER, starts INTEGER, ends INTEGER,
  reason TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS equipment_registry(
  id TEXT PRIMARY KEY, node TEXT UNIQUE, site TEXT, location TEXT,
  service_level TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS service_profiles(
  service TEXT PRIMARY KEY, site TEXT, service_level TEXT);
CREATE TABLE IF NOT EXISTS customer_confirmed_ops(
  id TEXT PRIMARY KEY, service TEXT, ref_id TEXT, operation TEXT,
  customer TEXT, at INTEGER, valid_until INTEGER, site TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS operation_holds(
  id TEXT PRIMARY KEY, site TEXT, target_kind TEXT, target_id TEXT,
  reason TEXT, at INTEGER, by_actor TEXT, released_at INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS handoff_details(
  handoff_id TEXT PRIMARY KEY, site TEXT, scope TEXT, opened_at INTEGER,
  submitted_at INTEGER, completed_at INTEGER, state TEXT, freeze INTEGER,
  completed_by TEXT);
CREATE TABLE IF NOT EXISTS handoff_items(
  id TEXT PRIMARY KEY, handoff_id TEXT, kind TEXT, ref_id TEXT,
  label TEXT, detail TEXT, required INTEGER, state TEXT,
  accepted_by TEXT, accepted_at INTEGER);
CREATE TABLE IF NOT EXISTS handoff_signatures(
  id TEXT PRIMARY KEY, handoff_id TEXT, actor TEXT, role TEXT,
  signed_at INTEGER, received_at INTEGER);
CREATE TABLE IF NOT EXISTS site_confirmations(
  id TEXT PRIMARY KEY, handoff_id TEXT, session_id TEXT, actor TEXT,
  at INTEGER, received_at INTEGER, nodes TEXT, permissions TEXT,
  independent INTEGER, accepted INTEGER);
CREATE TABLE IF NOT EXISTS duty_control_policy(
  id INTEGER PRIMARY KEY CHECK(id=1), armed INTEGER, armed_at INTEGER, scope TEXT);
'''

SUCCESS_DECISIONS = {
    'slot_recorded', 'planned', 'launched', 'retry_opened',
    'sampled', 'published', 'revoked', 'expired', 'rolled_back',
}

# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------

def _names(x, *keys):
    for k in keys:
        if k in x and x[k] is not None:
            return x[k]
    return None


def _received_at(x):
    return _names(x, 'received_at', 'recorded_at', 'submitted_at') or x['at']


def _json_list(value):
    if value is None:
        return []
    if isinstance(value, str):
        return json.loads(value)
    return list(value)


def _min_not_none(*values):
    values = [v for v in values if v is not None]
    return min(values) if values else None


def issuer_recognized(db, issuer):
    if not issuer:
        return False
    rows = many(db, 'SELECT * FROM training_issuers')
    if not rows:
        return bool(str(issuer).strip())
    return any((r['id'] == issuer or r['name'] == issuer) and r['status'] == 'active'
               for r in rows)


def equipment_for_node(db, node):
    if not node:
        return None
    return one(db, 'SELECT * FROM equipment_registry WHERE node=? OR id=?', (node, node))


def resolve_site(db, x):
    """Best-effort site resolution for an operation payload."""
    site = _names(x, 'site', 'site_id')
    if site:
        return site
    eq = equipment_for_node(db, x.get('node'))
    if eq:
        return eq['site']
    slot_id = x.get('slot')
    if not slot_id:
        attempt_id = x.get('attempt') or x.get('old')
        if attempt_id:
            a = one(db, 'SELECT slot FROM start_attempts WHERE id=?', (attempt_id,))
            if a:
                slot_id = a['slot']
    if slot_id:
        s = one(db, 'SELECT node FROM startup_slots WHERE id=?', (slot_id,))
        if s:
            eq = equipment_for_node(db, s['node'])
            if eq:
                return eq['site']
    wave_id = x.get('wave')
    service = x.get('service')
    if wave_id:
        w = one(db, 'SELECT service FROM boot_waves WHERE id=?', (wave_id,))
        if w:
            service = service or w['service']
    if not service and x.get('id') and x.get('op') == 'rollback':
        case = one(db, 'SELECT service FROM rollback_cases WHERE id=?', (x['id'],))
        if case:
            service = case['service']
    if service:
        p = one(db, 'SELECT site FROM service_profiles WHERE service=?', (service,))
        if p:
            return p['site']
    return None


def resolve_level(db, x):
    level = _names(x, 'service_level', 'level', 'required_level')
    if level is not None:
        return str(level)
    service = x.get('service')
    wave_id = x.get('wave')
    if wave_id:
        w = one(db, 'SELECT service, level FROM boot_waves WHERE id=?', (wave_id,))
        if w:
            service = service or w['service']
    if service:
        p = one(db, 'SELECT service_level FROM service_profiles WHERE service=?', (service,))
        if p and p['service_level'] is not None:
            return str(p['service_level'])
    return None


def check_qualification(db, actor, at, site, level=None, node=None, warn_within=2):
    """Temporal certificate check. Revocation only invalidates from revoked_at."""
    eq = equipment_for_node(db, node) if node else None
    matching = []
    for c in many(db, 'SELECT * FROM qualification_certificates WHERE actor=? ORDER BY id', (actor,)):
        end = _min_not_none(c['expires'], c['revoked_at'])
        if not (c['starts'] <= at < end):
            continue
        if not issuer_recognized(db, c['issuer']):
            continue
        if c['site'] not in (site, '*'):
            continue
        if level is not None and c['service_level'] not in (str(level), '*'):
            continue
        if c['equipment']:
            if not (node and (c['equipment'] == node or (eq and c['equipment'] == eq['id']))):
                continue
        if c['location']:
            if not eq or eq['location'] != c['location']:
                continue
        matching.append(c)
    if not matching:
        # Produce the most precise reason possible.
        certs = many(db, 'SELECT * FROM qualification_certificates WHERE actor=? ORDER BY id', (actor,))
        if not certs:
            return {'ok': False, 'decision': 'qualification_not_certified'}
        if not any(issuer_recognized(db, c['issuer']) for c in certs):
            return {'ok': False, 'decision': 'qualification_issuer_unrecognized'}
        live = [c for c in certs if c['starts'] <= at < _min_not_none(c['expires'], c['revoked_at'])]
        if any(c['revoked_at'] is not None and at >= c['revoked_at'] for c in certs):
            if not live:
                return {'ok': False, 'decision': 'qualification_revoked'}
        if any(at >= c['expires'] for c in certs):
            if not live:
                return {'ok': False, 'decision': 'qualification_expired'}
        if site and not any(c['site'] in (site, '*') for c in live):
            return {'ok': False, 'decision': 'qualification_site_mismatch'}
        if level is not None and not any(c['service_level'] in (str(level), '*') for c in live):
            return {'ok': False, 'decision': 'qualification_level_mismatch'}
        return {'ok': False, 'decision': 'qualification_invalid'}
    chosen = matching[0]
    warnings = []
    if warn_within is not None and chosen['expires'] - at <= warn_within:
        warnings.append('qualification_expires_soon')
    return {'ok': True, 'certificate': chosen, 'warnings': warnings}


def _withdrawal_at(db, shift_id):
    row = one(db, 'SELECT MIN(at) AS at FROM shift_withdrawals WHERE shift_id=?', (shift_id,))
    return row['at'] if row and row['at'] is not None else None


def _completed_handoff_for_shift(db, shift_id, column):
    h = one(db, f'SELECT * FROM duty_handoffs WHERE {column}=? ORDER BY at DESC, id DESC LIMIT 1',
            (shift_id,))
    if not h:
        return None, None
    d = one(db, 'SELECT * FROM handoff_details WHERE handoff_id=?', (h['id'],))
    if d and d['state'] == 'completed' and d['completed_at'] is not None:
        return h, d
    return h, d


def coverage_intervals(db, site):
    """Half-open responsibility intervals for one site.

    Each interval: {start,end,actor,basis,shift,supervisor,kind}
    """
    intervals = []
    shifts = many(db, 'SELECT * FROM duty_shifts WHERE site=? ORDER BY starts, id', (site,))
    for s in shifts:
        w_at = _withdrawal_at(db, s['id'])
        end = _min_not_none(s['ends'], w_at)
        start = s['starts']
        detail_row = one(db, 'SELECT * FROM shift_details WHERE shift_id=?', (s['id'],))
        kind = detail_row['kind'] if detail_row and detail_row['kind'] else 'shift'

        h_in, d_in = _completed_handoff_for_shift(db, s['id'], 'incoming')
        open_incoming = False
        if h_in is not None:
            if d_in is not None and d_in['state'] == 'completed' and d_in['completed_at'] is not None:
                start = max(start, d_in['completed_at'])
            else:
                # Named as incoming on an unfinished handover: not responsible yet.
                open_incoming = True
        h_out, d_out = _completed_handoff_for_shift(db, s['id'], 'outgoing')
        relief = d_out['completed_at'] if d_out and d_out['state'] == 'completed' else None
        if relief is not None:
            end = _min_not_none(end, relief)
        if not open_incoming and start < end:
            intervals.append({'start': start, 'end': end, 'actor': s['actor'],
                              'basis': 'cross_site' if kind == 'cross_site' else 'shift',
                              'shift': s['id'], 'supervisor': None})

        for e in many(db, 'SELECT * FROM shift_extensions WHERE shift_id=? AND state!=? ORDER BY starts',
                      (s['id'], 'cancelled')):
            e_start = e['starts'] if e['starts'] is not None else e['at']
            e_end = _min_not_none(e['ends'], w_at, relief)
            if e_end is not None and e_start < e_end:
                intervals.append({'start': e_start, 'end': e_end, 'actor': e['actor'],
                                  'basis': 'extension', 'shift': s['id'],
                                  'supervisor': e['supervisor'], 'bound_ref': e['bound_ref']})

    for g in many(db, 'SELECT * FROM emergency_authorizations WHERE site=? AND state=?',
                  (site, 'active')):
        g_start = g['starts'] if g['starts'] is not None else g['at']
        intervals.append({'start': g_start, 'end': g['ends'], 'actor': g['actor'],
                          'basis': 'emergency', 'shift': None,
                          'supervisor': g['supervisor']})

    # A completed handover relieves the outgoing actor of EVERY interval at c.
    for h in many(db, "SELECT * FROM duty_handoffs WHERE state='completed'"):
        d = one(db, 'SELECT * FROM handoff_details WHERE handoff_id=?', (h['id'],))
        if not d or d['completed_at'] is None or d['site'] != site:
            continue
        out_shift = one(db, 'SELECT actor FROM duty_shifts WHERE id=?', (h['outgoing'],))
        if not out_shift:
            continue
        c = d['completed_at']
        for iv in intervals:
            if iv['actor'] == out_shift['actor'] and iv['start'] < c < iv['end']:
                iv['end'] = c
    intervals = [iv for iv in intervals if iv['start'] < iv['end']]
    intervals.sort(key=lambda iv: (iv['start'], iv['end'], iv['actor']))
    return intervals


def responsible_at(db, site=None, at=None, when=None):
    if when is not None:
        at = when
    # Tolerate the alternate responsible_at(db, at, site) calling order.
    if isinstance(site, (int, float)) and isinstance(at, str):
        site, at = at, site
    active = [iv for iv in coverage_intervals(db, site) if iv['start'] <= at < iv['end']]
    actors = {iv['actor'] for iv in active}
    if len(actors) > 1:
        return {'actor': None, 'status': 'conflict', 'actors': sorted(actors),
                'intervals': active}
    if len(actors) == 1:
        privileged = [iv for iv in active if iv['basis'] in ('extension', 'emergency')]
        iv = privileged[-1] if privileged else active[0]
        supervisors = sorted({x['supervisor'] for x in active if x.get('supervisor')})
        return {'actor': iv['actor'], 'status': 'responsible', 'basis': iv['basis'],
                'shift': iv['shift'], 'supervisors': supervisors, 'interval': iv}
    return {'actor': None, 'status': 'gap', 'basis': None, 'supervisors': [],
            'intervals': []}


def timeline(db, site, start, end):
    """Compressed half-open unique-responsibility segments over [start, end]."""
    bounds = {start, end}
    for iv in coverage_intervals(db, site):
        for b in (iv['start'], iv['end']):
            if start < b < end:
                bounds.add(b)
    bounds = sorted(bounds)
    raw = []
    for a, b in zip(bounds, bounds[1:]):
        if b <= start or a >= end:
            continue
        a0, b0 = max(a, start), min(b, end)
        if a0 >= b0:
            continue
        point = responsible_at(db, site, a0)
        raw.append({'start': a0, 'end': b0, 'actor': point['actor'],
                    'status': point['status'], 'basis': point.get('basis'),
                    'supervisors': point.get('supervisors', [])})
    merged = []
    for seg in raw:
        key = (seg['actor'], seg['status'], seg['basis'], tuple(seg['supervisors']))
        if merged and merged[-1]['end'] == seg['start'] and merged[-1]['_key'] == key:
            merged[-1]['end'] = seg['end']
        else:
            seg['_key'] = key
            merged.append(seg)
    for m in merged:
        m.pop('_key', None)
    return merged


def session_at(db, session_id, actor, site, at):
    if not session_id:
        return None, 'duty_session_required'
    s = one(db, 'SELECT * FROM site_sessions WHERE id=?', (session_id,))
    if not s or s['actor'] != actor or s['site'] != site:
        return None, 'duty_session_invalid'
    if s['started_at'] > at:
        return None, 'duty_session_not_started'
    if s['ended_at'] is not None and at >= s['ended_at']:
        return None, 'duty_session_closed'
    return s, None


def _control_armed(db, site):
    row = one(db, 'SELECT * FROM duty_control_policy WHERE id=1')
    if not row or not row['armed']:
        return False
    return not row['scope'] or row['scope'] in ('*', site)


def active_hold(db, x, at, site):
    refs = []
    for k in ('id', 'wave', 'attempt', 'old', 'slot', 'service', 'ref', 'target', 'case'):
        v = x.get(k)
        if isinstance(v, str):
            refs.append(v)
    if x.get('op') in ('rollback',) and isinstance(x.get('id'), str):
        case = one(db, 'SELECT service FROM rollback_cases WHERE id=?', (x['id'],))
        if case:
            refs.append(case['service'])
    for h in many(db, 'SELECT * FROM operation_holds WHERE state=?', ('active',)):
        if h['at'] > at or (h['released_at'] is not None and at >= h['released_at']):
            continue
        if h['site'] not in (None, '*', site):
            continue
        if h['target_id'] == '*' or h['target_id'] in refs:
            return h
    return None


def evaluate_operation(db, x):
    """Read-only duty gate used by wave/ready and by recomputation."""
    at = x.get('at')
    actor = x.get('actor')
    site = resolve_site(db, x)

    if at is not None and site is not None:
        hold = active_hold(db, x, at, site)
        if hold:
            return {'strict': True, 'allowed': False, 'decision': 'operation_paused',
                    'hold': hold['id']}

    if actor is None:
        if site is not None and _control_armed(db, site):
            return {'strict': True, 'allowed': False, 'decision': 'duty_identity_required',
                    'site': site}
        return {'strict': False, 'allowed': True, 'decision': None}

    if at is None:
        return {'strict': True, 'allowed': False, 'decision': 'duty_time_required'}
    if site is None:
        return {'strict': True, 'allowed': False, 'decision': 'duty_site_required'}

    point = responsible_at(db, site, at)
    if point['status'] == 'conflict':
        return {'strict': True, 'allowed': False, 'decision': 'conflicting_duty',
                'site': site, 'actors': point['actors']}
    if point['actor'] is None:
        return {'strict': True, 'allowed': False, 'decision': 'duty_gap', 'site': site}
    if point['actor'] != actor:
        return {'strict': True, 'allowed': False, 'decision': 'not_responsible_officer',
                'site': site, 'responsible': point['actor']}

    session_id = _names(x, 'session', 'session_id')
    sess, err = session_at(db, session_id, actor, site, at)
    if err:
        return {'strict': True, 'allowed': False, 'decision': err, 'site': site}

    level = resolve_level(db, x)
    node = x.get('node')
    if not node and x.get('slot'):
        s = one(db, 'SELECT node FROM startup_slots WHERE id=?', (x['slot'],))
        node = s['node'] if s else None
    q = check_qualification(db, actor, at, site, level, node)
    warnings = list(q.get('warnings', []))
    if not q['ok']:
        # A signed emergency supervisor authorization can stand in for the
        # ordinary certificate only inside its real, bounded window.
        if point.get('basis') == 'emergency':
            warnings.append('emergency_qualification_override:' + q['decision'])
        else:
            return {'strict': True, 'allowed': False, 'decision': q['decision'], 'site': site}

    # Open handover freezes production work unless a signed supervised
    # extension/emergency authorization covers this exact moment.
    frozen = one(db,
                 "SELECT * FROM handoff_details WHERE site=? AND state='open' AND freeze=1 AND opened_at<=? ORDER BY opened_at DESC LIMIT 1",
                 (site, at))
    if frozen:
        if point.get('basis') not in ('extension', 'emergency'):
            return {'strict': True, 'allowed': False, 'decision': 'handover_in_progress',
                    'handoff': frozen['handoff_id'], 'site': site}
        iv = point.get('interval') or {}
        bound = iv.get('bound_ref')
        if bound:
            try:
                allowed_refs = json.loads(bound) if str(bound).startswith('[') else [bound]
            except ValueError:
                allowed_refs = [bound]
            refs = {str(v) for v in x.values() if isinstance(v, (str, int))}
            if not any(b in refs for b in allowed_refs):
                return {'strict': True, 'allowed': False,
                        'decision': 'extension_scope_mismatch',
                        'bound_ref': bound, 'site': site}

    return {'strict': True, 'allowed': True, 'decision': None, 'site': site,
            'responsible': actor, 'basis': point.get('basis'),
            'supervisors': point.get('supervisors', []),
            'warnings': warnings}


def gate(db, x):
    """Convenience used by other subsystems. Returns a denial result or None."""
    verdict = evaluate_operation(db, x)
    if verdict.get('strict') and not verdict['allowed']:
        return {'decision': verdict['decision'], 'duty': {
            k: v for k, v in verdict.items() if k not in ('strict', 'allowed', 'decision')}}
    return None


# ---------------------------------------------------------------------------
# handover inventory
# ---------------------------------------------------------------------------

def inventory(db, at, site=None, wave_id=None, services=None, all_cases=False):
    services = {s for s in (services or []) if s}
    if wave_id and wave_id != '*':
        w = one(db, 'SELECT service FROM boot_waves WHERE id=?', (wave_id,))
        if w:
            services.add(w['service'])
    if site:
        for p in many(db, 'SELECT service FROM service_profiles WHERE site=?', (site,)):
            services.add(p['service'])
        for eq in many(db, 'SELECT * FROM equipment_registry WHERE site=?', (site,)):
            for s in many(db, 'SELECT id FROM startup_slots WHERE node=?', (eq['node'],)):
                for a in many(db, 'SELECT wave FROM start_attempts WHERE slot=?', (s['id'],)):
                    w = one(db, 'SELECT service FROM boot_waves WHERE id=?', (a['wave'],))
                    if w:
                        services.add(w['service'])

    def attempt_in_scope(a):
        if wave_id and wave_id != '*' and a['wave'] == wave_id:
            return True
        w = one(db, 'SELECT service FROM boot_waves WHERE id=?', (a['wave'],))
        if w and w['service'] in services:
            return True
        s = one(db, 'SELECT node FROM startup_slots WHERE id=?', (a['slot'],))
        if s:
            eq = equipment_for_node(db, s['node'])
            if eq and (not site or eq['site'] == site):
                return True
        return False

    items = []
    for a in many(db, "SELECT * FROM start_attempts WHERE status='running' ORDER BY id"):
        if attempt_in_scope(a):
            items.append({'kind': 'start_attempt', 'ref': a['id'],
                          'label': f"running attempt {a['id']}",
                          'detail': {'wave': a['wave'], 'expires': a['expires']},
                          'deadline': a['expires']})
    for w in many(db, "SELECT * FROM boot_waves WHERE status='running' ORDER BY id"):
        in_scope = (wave_id in (w['id'], '*')) or w['service'] in services
        if not in_scope and site:
            for a in many(db, "SELECT slot FROM start_attempts WHERE wave=? LIMIT 1", (w['id'],)):
                s = one(db, 'SELECT node FROM startup_slots WHERE id=?', (a['slot'],))
                eq = equipment_for_node(db, s['node']) if s else None
                if eq and eq['site'] == site:
                    in_scope = True
        if in_scope:
            items.append({'kind': 'running_wave', 'ref': w['id'],
                          'label': f"running wave {w['id']}",
                          'detail': {'service': w['service'], 'generation': w['generation']},
                          'deadline': w['deadline']})
    for c in many(db, "SELECT * FROM rollback_cases WHERE status='pending' ORDER BY id"):
        in_scope = all_cases or c['service'] in services
        if not in_scope and site:
            p = one(db, 'SELECT site FROM service_profiles WHERE service=?', (c['service'],))
            in_scope = bool(p and p['site'] == site)
        if in_scope:
            items.append({'kind': 'rollback_case', 'ref': c['id'],
                          'label': f"pending rollback {c['id']} ({c['service']})",
                          'detail': {'service': c['service'], 'cause': c['cause']},
                          'deadline': None})
    for c in many(db, "SELECT * FROM customer_confirmed_ops WHERE state='confirmed' ORDER BY id"):
        in_scope = all_cases or c['service'] in services or c['site'] == site
        if in_scope:
            items.append({'kind': 'customer_action', 'ref': c['id'],
                          'label': f"customer-confirmed operation {c['id']}",
                          'detail': {'operation': c['operation'], 'customer': c['customer']},
                          'deadline': c['valid_until']})
    deadlines = [i['deadline'] for i in items if i.get('deadline') is not None and i['deadline'] >= at]
    next_deadline = min(deadlines) if deadlines else None
    deadline_ref = next(({'kind': i['kind'], 'ref': i['ref'], 'at': i['deadline']}
                         for i in items if i.get('deadline') == next_deadline), None)
    return {'items': items, 'next_deadline': deadline_ref, 'services': sorted(services)}


def _scope_of(detail):
    try:
        return json.loads(detail['scope']) if detail['scope'] else {}
    except (TypeError, ValueError):
        return {}


def _completion_checks(db, hid, at=None):
    """Return (ok, problems, context). Shared by complete op and recompute."""
    h = one(db, 'SELECT * FROM duty_handoffs WHERE id=?', (hid,))
    d = one(db, 'SELECT * FROM handoff_details WHERE handoff_id=?', (hid,))
    problems = []
    if not h or not d:
        return False, ['handoff_missing'], {}
    out_shift = one(db, 'SELECT * FROM duty_shifts WHERE id=?', (h['outgoing'],))
    in_shift = one(db, 'SELECT * FROM duty_shifts WHERE id=?', (h['incoming'],))
    out_sigs = many(db, "SELECT * FROM handoff_signatures WHERE handoff_id=? AND role='outgoing'", (hid,))
    in_sigs = many(db, "SELECT * FROM handoff_signatures WHERE handoff_id=? AND role='incoming'", (hid,))
    if not out_sigs:
        problems.append('outgoing_signature_missing')
    elif out_shift and not any(s['actor'] == out_shift['actor'] for s in out_sigs):
        problems.append('outgoing_signature_actor_mismatch')
    if not in_sigs:
        problems.append('incoming_signature_missing')
    elif in_shift and not any(s['actor'] == in_shift['actor'] for s in in_sigs):
        problems.append('incoming_signature_actor_mismatch')

    conf = one(db, 'SELECT * FROM site_confirmations WHERE handoff_id=? AND accepted=1 ORDER BY at DESC LIMIT 1',
               (hid,))
    if not conf:
        problems.append('independent_site_confirmation_missing')
    else:
        sess = one(db, 'SELECT * FROM site_sessions WHERE id=?', (conf['session_id'],))
        if not sess or not sess['independent']:
            problems.append('site_confirmation_not_independent')
        if in_shift and conf['actor'] != in_shift['actor']:
            problems.append('site_confirmation_actor_mismatch')

    items = many(db, 'SELECT * FROM handoff_items WHERE handoff_id=? AND required=1', (hid,))
    unaccepted = [i['id'] for i in items if i['state'] != 'accepted']
    if unaccepted:
        problems.append('checklist_not_accepted')

    scope = _scope_of(d)
    live = inventory(db, d['opened_at'], site=d['site'], wave_id=scope.get('wave'),
                     services=scope.get('services'), all_cases=scope.get('all_cases', False))
    by_ref = {(i['kind'], i['ref_id']): i for i in items}
    missing_rollback = []
    unlisted = []
    for li in live['items']:
        key = (li['kind'], li['ref'])
        row = by_ref.get(key)
        if row is None:
            if li['kind'] == 'rollback_case':
                missing_rollback.append(li['ref'])
            else:
                unlisted.append({'kind': li['kind'], 'ref': li['ref']})
        elif row['state'] != 'accepted':
            if li['kind'] == 'rollback_case':
                missing_rollback.append(li['ref'])
    if missing_rollback:
        problems.append('rollback_case_not_handed_over')
    if unlisted:
        problems.append('inventory_changed')

    completed_at = None
    if in_sigs:
        candidates = [s['received_at'] for s in in_sigs]
        if conf:
            candidates.append(conf['received_at'])
        candidates += [i['accepted_at'] for i in items if i['accepted_at'] is not None]
        candidates = [c for c in candidates if c is not None]
        completed_at = max(candidates) if candidates else None
        if conf:
            completed_at = max(completed_at or conf['received_at'], conf['received_at'])

    if completed_at is not None and in_shift:
        q = check_qualification(db, in_shift['actor'], completed_at, d['site'],
                                scope.get('service_level'))
        if not q['ok']:
            problems.append(q['decision'])
    return not problems, problems, {'completed_at': completed_at,
                                    'missing_rollback': missing_rollback,
                                    'unlisted': unlisted,
                                    'unaccepted': unaccepted}


# ---------------------------------------------------------------------------
# command handler
# ---------------------------------------------------------------------------

def handle(db, x):
    op = x['op']

    # ---- original three operations (behaviour preserved) ----
    if op == 'shift':
        if x['ends'] <= x['starts']:
            return {'decision': 'invalid_shift'}
        db.execute('INSERT INTO duty_shifts VALUES(?,?,?,?,?,?,?)',
                   (x['id'], x['actor'], x['site'], x['starts'], x['ends'],
                    x.get('qualification'), 'active'))
        if any(k in x for k in ('kind', 'cover_for', 'home_site', 'supervisor', 'note')):
            db.execute('INSERT INTO shift_details(shift_id,kind,home_site,cover_for,supervisor,note) VALUES(?,?,?,?,?,?)',
                       (x['id'], x.get('kind', 'shift'), x.get('home_site'),
                        x.get('cover_for'), x.get('supervisor'), x.get('note')))
        return {'decision': 'shift_recorded'}

    if op == 'handoff':
        outgoing = one(db, 'SELECT * FROM duty_shifts WHERE id=?', (x['outgoing'],))
        incoming = one(db, 'SELECT * FROM duty_shifts WHERE id=?', (x['incoming'],))
        if not outgoing or not incoming or outgoing['state'] != 'active' or incoming['state'] != 'active':
            return {'decision': 'shift_unavailable'}
        db.execute('INSERT INTO duty_handoffs VALUES(?,?,?,?,?,?)',
                   (x['id'], x['outgoing'], x['incoming'], x['wave'], x['at'], 'pending'))
        return {'decision': 'handoff_opened'}

    if op == 'ack':
        handoff = one(db, 'SELECT * FROM duty_handoffs WHERE id=?', (x['handoff'],))
        if not handoff:
            return {'decision': 'handoff_missing'}
        db.execute('INSERT INTO duty_acknowledgments VALUES(?,?,?,?)',
                   (x['id'], x['handoff'], x['actor'], x['at']))
        return {'decision': 'acknowledged'}

    # ---- registries ----
    if op in ('issuer', 'register_issuer', 'training_issuer'):
        db.execute('INSERT INTO training_issuers VALUES(?,?,?)',
                   (x['id'], x.get('name', x['id']), x.get('status', 'active')))
        return {'decision': 'issuer_registered'}

    if op in ('certification', 'cert', 'issue_qualification', 'qualification',
              'qualification_register', 'register_qualification'):
        at = x.get('at', x.get('issued_at', x.get('starts', 0)))
        starts = x.get('starts', x.get('valid_from', at))
        expires = _names(x, 'expires', 'until', 'valid_until')
        if expires is None or expires <= starts:
            return {'decision': 'qualification_invalid_period'}
        db.execute('''INSERT INTO qualification_certificates
                      (id,actor,name,service_level,site,location,equipment,issuer,
                       starts,expires,issued_at,status,revoked_at,revoke_reason)
                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                   (x['id'], x['actor'], x.get('name', x.get('qualification', 'startup')),
                    str(x['service_level']) if x.get('service_level') is not None else x.get('level'),
                    x.get('site', '*'), x.get('location'), x.get('equipment'),
                    x.get('issuer'), starts, expires, x.get('issued_at', at),
                    'active', None, None))
        return {'decision': 'qualification_issued'}

    if op in ('revoke_cert', 'certification_revoke', 'revoke_qualification'):
        c = one(db, 'SELECT * FROM qualification_certificates WHERE id=?', (x['id'],))
        if not c:
            return {'decision': 'qualification_missing'}
        at = x['at']
        if c['revoked_at'] is not None:
            return {'decision': 'qualification_already_revoked'}
        db.execute("UPDATE qualification_certificates SET status='revoked', revoked_at=?, revoke_reason=? WHERE id=?",
                   (at, x.get('reason'), x['id']))
        return {'decision': 'qualification_revoked', 'revoked_at': at}

    if op in ('supervisor', 'designate_supervisor'):
        db.execute('INSERT INTO designated_supervisors VALUES(?,?,?,?,?)',
                   (x['id'], x['actor'], x['site'], x['starts'], x['ends']))
        return {'decision': 'supervisor_designated'}

    if op == 'equipment':
        db.execute('''INSERT INTO equipment_registry VALUES(?,?,?,?,?,?)
                      ON CONFLICT(id) DO UPDATE SET node=excluded.node,site=excluded.site,
                        location=excluded.location,service_level=excluded.service_level,state=excluded.state''',
                   (x['id'], x['node'], x['site'], x.get('location'),
                    str(x['service_level']) if x.get('service_level') is not None else x.get('level'),
                    x.get('state', 'active')))
        return {'decision': 'equipment_recorded'}

    if op in ('service_profile', 'profile'):
        db.execute('''INSERT INTO service_profiles VALUES(?,?,?)
                      ON CONFLICT(service) DO UPDATE SET site=excluded.site,
                        service_level=excluded.service_level''',
                   (x['service'], x.get('site'),
                    str(x['service_level']) if x.get('service_level') is not None else x.get('level')))
        return {'decision': 'service_profile_recorded'}

    if op in ('customer_confirm', 'customer_action'):
        db.execute('INSERT INTO customer_confirmed_ops VALUES(?,?,?,?,?,?,?,?,?)',
                   (x['id'], x['service'], x.get('ref'), x.get('operation'),
                    x.get('customer'), x['at'], x.get('valid_until'),
                    x.get('site'), 'confirmed'))
        return {'decision': 'customer_confirmation_recorded'}

    if op in ('arm_control', 'arm_duty_control'):
        db.execute('INSERT INTO duty_control_policy VALUES(1,?,?,?) ON CONFLICT(id) DO UPDATE SET armed=excluded.armed, armed_at=excluded.armed_at, scope=excluded.scope',
                   (int(x.get('armed', True)), x['at'], x.get('scope')))
        return {'decision': 'duty_control_armed'}

    # ---- on-site sessions ----
    if op in ('session_start', 'start_session', 'site_session'):
        at = _names(x, 'at', 'started_at')
        perms = x.get('permissions')
        db.execute('INSERT INTO site_sessions VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                   (x['id'], x['actor'], x['site'], x.get('kind', 'onsite'),
                    int(bool(x.get('independent', False))), x.get('node'),
                    json.dumps(perms or [], sort_keys=True), at, None, 'open', None))
        return {'decision': 'session_started', 'independent': bool(x.get('independent', False))}

    if op in ('session_close', 'close_session'):
        s = one(db, 'SELECT * FROM site_sessions WHERE id=?', (x['id'],))
        if not s:
            return {'decision': 'session_missing'}
        at = x['at']
        if s['ended_at'] is None:
            db.execute("UPDATE site_sessions SET ended_at=?, state='closed', closed_reason=? WHERE id=?",
                       (at, x.get('reason', 'closed'), x['id']))
        return {'decision': 'session_closed'}

    # ---- sick withdrawal ----
    if op in ('withdraw', 'sick_leave', 'withdrawal'):
        s = one(db, 'SELECT * FROM duty_shifts WHERE id=?', (x['shift'],))
        if not s:
            return {'decision': 'shift_unavailable'}
        db.execute('INSERT INTO shift_withdrawals VALUES(?,?,?,?,?,?)',
                   (x['id'], x['shift'], x.get('actor', s['actor']), x['at'],
                   _received_at(x), x.get('reason', 'sick')))
        # The officer physically leaves: open sessions end at the real time.
        db.execute("""UPDATE site_sessions SET ended_at=?, state='closed', closed_reason='withdrawal'
                      WHERE actor=? AND site=? AND ended_at IS NULL""",
                   (x['at'], s['actor'], s['site']))
        return {'decision': 'withdrawal_recorded', 'effective_from': x['at'],
                'shift_ends_unchanged': s['ends']}

    # ---- supervised extension ----
    if op in ('extension', 'grant_extension', 'shift_extension'):
        s = one(db, 'SELECT * FROM duty_shifts WHERE id=?', (x['shift'],))
        if not s:
            return {'decision': 'shift_unavailable'}
        at = x['at']
        until = _names(x, 'until', 'ends')
        if until is None or until <= at:
            return {'decision': 'invalid_extension_period'}
        sup = one(db, 'SELECT * FROM designated_supervisors WHERE actor=? AND site=? AND starts<=? AND ends>?',
                  (x['supervisor'], s['site'], at, at))
        if not sup:
            return {'decision': 'supervisor_not_authorized'}
        off_signed = x.get('officer_signed_at')
        if off_signed is None and x.get('officer_signed') is True:
            off_signed = at
        sup_signed = x.get('supervisor_signed_at')
        if sup_signed is None and x.get('supervisor_signed') is True:
            sup_signed = at
        if off_signed is None or sup_signed is None:
            return {'decision': 'extension_signatures_required'}
        starts = x.get('starts')
        bound_ref = x.get('bound_ref')
        if x.get('bound_refs'):
            bound_ref = json.dumps(list(x['bound_refs']), sort_keys=True)
        db.execute('INSERT INTO shift_extensions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (x['id'], x['shift'], s['actor'], s['site'], starts, until, at,
                    x['supervisor'], off_signed, sup_signed, _received_at(x),
                    x.get('reason'), x.get('bound_kind'), bound_ref,
                    int(x.get('supervised', True)), 'active'))
        return {'decision': 'extension_granted', 'until': until,
                'supervisor': x['supervisor'], 'supervised': True}

    # ---- emergency supervisor authorization ----
    if op in ('emergency', 'emergency_authorization'):
        at = x['at']
        starts = x.get('starts', at)
        until = _names(x, 'until', 'ends')
        if until is None or until <= starts:
            return {'decision': 'invalid_authorization_period'}
        site = x['site']
        sup = one(db, 'SELECT * FROM designated_supervisors WHERE actor=? AND site=? AND starts<=? AND ends>?',
                  (x['supervisor'], site, at, at))
        if not sup:
            return {'decision': 'supervisor_not_authorized'}
        db.execute('INSERT INTO emergency_authorizations VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (x['id'], x['actor'], site,
                    str(x['service_level']) if x.get('service_level') is not None else x.get('level'),
                    x['supervisor'], at, starts, until, x.get('reason'), 'active'))
        return {'decision': 'emergency_authorization_granted', 'starts': starts, 'until': until}

    # ---- operational pause / resume ----
    if op in ('pause', 'hold', 'pause_operation'):
        at = x['at']
        site = x.get('site')
        if not site:
            site = resolve_site(db, x)
        actor = x.get('actor')
        if actor and site:
            point = responsible_at(db, site, at)
            sup = one(db, 'SELECT 1 FROM designated_supervisors WHERE actor=? AND site=? AND starts<=? AND ends>?',
                      (actor, site, at, at))
            if point.get('actor') != actor and not sup:
                return {'decision': 'pause_not_authorized',
                        'responsible': point.get('actor')}
        db.execute('INSERT INTO operation_holds VALUES(?,?,?,?,?,?,?,?,?)',
                   (x['id'], site, x.get('target_kind', 'ref'),
                    _names(x, 'target', 'ref', 'target_id') or '*',
                    x.get('reason'), at, x.get('actor'), None, 'active'))
        return {'decision': 'operation_paused_recorded'}

    if op in ('resume', 'release', 'resume_operation'):
        h = one(db, 'SELECT * FROM operation_holds WHERE id=?', (x['id'],))
        if not h:
            return {'decision': 'hold_missing'}
        at = x['at']
        if h['state'] == 'released':
            return {'decision': 'hold_already_released'}
        actor = x.get('actor')
        point = responsible_at(db, h['site'], at) if h['site'] else None
        if not actor or not point or point['actor'] != actor:
            return {'decision': 'resume_not_authorized',
                    'responsible': point['actor'] if point else None}
        sess, err = session_at(db, _names(x, 'session', 'session_id'), actor, h['site'], at)
        if err:
            return {'decision': err}
        db.execute("UPDATE operation_holds SET state='released', released_at=? WHERE id=?", (at, x['id']))
        return {'decision': 'operation_resumed'}

    # ---- strict handover lifecycle ----
    if op in ('handoff_open', 'open_handoff', 'handoff_begin'):
        outgoing = one(db, 'SELECT * FROM duty_shifts WHERE id=?', (x['outgoing'],))
        incoming = one(db, 'SELECT * FROM duty_shifts WHERE id=?', (x['incoming'],))
        if not outgoing or not incoming:
            return {'decision': 'shift_unavailable'}
        site = x.get('site') or outgoing['site']
        at = x['at']
        wave_id = x.get('wave')
        services = x.get('services')
        all_cases = bool(x.get('all_pending_cases', x.get('all_cases', False)))
        inv = inventory(db, at, site=site, wave_id=wave_id,
                        services=services, all_cases=all_cases)
        db.execute('INSERT INTO duty_handoffs VALUES(?,?,?,?,?,?)',
                   (x['id'], x['outgoing'], x['incoming'], wave_id, at, 'open'))
        scope = json.dumps({'wave': wave_id, 'services': inv['services'],
                            'all_cases': all_cases,
                            'service_level': x.get('service_level')}, sort_keys=True)
        db.execute('INSERT INTO handoff_details VALUES(?,?,?,?,?,?,?,?,?)',
                   (x['id'], site, scope, at, x.get('submitted_at', at),
                    None, 'open', int(x.get('freeze', True)), None))
        explicit = x.get('items')
        if explicit is not None:
            listed = []
            for spec in explicit:
                if isinstance(spec, str):
                    kind, ref = spec.split(':', 1) if ':' in spec else ('ref', spec)
                else:
                    kind, ref = spec['kind'], spec['ref']
                listed.append((kind, ref))
            chosen = [i for i in inv['items'] if (i['kind'], i['ref']) in listed]
            present = {(i['kind'], i['ref']) for i in chosen}
            for kind, ref in listed:
                if (kind, ref) not in present:
                    chosen.append({'kind': kind, 'ref': ref, 'label': ref,
                                   'detail': {}, 'deadline': None})
        else:
            chosen = inv['items']
        for n, item in enumerate(chosen):
            iid = item.get('id') or f"{x['id']}-item-{n}"
            db.execute('INSERT INTO handoff_items VALUES(?,?,?,?,?,?,?,?,?,?)',
                       (iid, x['id'], item['kind'], item['ref'], item.get('label'),
                        json.dumps(item.get('detail', {}), sort_keys=True),
                        int(item.get('required', True)), 'listed', None, None))
        return {'decision': 'handoff_opened', 'items': len(chosen),
                'inventory': {'items': inv['items'], 'next_deadline': inv['next_deadline']},
                'next_deadline': inv['next_deadline']}

    if op in ('handoff_add_item', 'add_handoff_item'):
        h = one(db, 'SELECT * FROM handoff_details WHERE handoff_id=?', (x['handoff'],))
        if not h:
            return {'decision': 'handoff_missing'}
        iid = x.get('id', f"{x['handoff']}-{x['kind']}-{x['ref']}")
        db.execute('INSERT INTO handoff_items VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (iid, x['handoff'], x['kind'], x['ref'], x.get('label', x['ref']),
                    json.dumps(x.get('detail', {}), sort_keys=True),
                    int(x.get('required', True)), 'listed', None, None))
        return {'decision': 'handoff_item_added'}

    if op in ('accept_item', 'review_item', 'receive_item', 'handoff_item'):
        hid = x['handoff']
        h = one(db, 'SELECT * FROM handoff_details WHERE handoff_id=?', (hid,))
        if not h:
            return {'decision': 'handoff_missing'}
        if h['state'] != 'open':
            return {'decision': 'handoff_not_open'}
        in_shift = one(db, 'SELECT * FROM duty_shifts WHERE id=(SELECT incoming FROM duty_handoffs WHERE id=?)',
                       (hid,))
        actor = x.get('actor')
        if not in_shift or actor != in_shift['actor']:
            return {'decision': 'only_incoming_can_accept'}
        item = None
        if x.get('item'):
            item = one(db, 'SELECT * FROM handoff_items WHERE id=? AND handoff_id=?', (x['item'], hid))
        elif x.get('ref'):
            item = one(db, 'SELECT * FROM handoff_items WHERE handoff_id=? AND ref_id=? AND kind=?',
                       (hid, x['ref'], x.get('kind', 'start_attempt')))
        if not item:
            return {'decision': 'handoff_item_missing'}
        db.execute("UPDATE handoff_items SET state='accepted', accepted_by=?, accepted_at=? WHERE id=?",
                   (actor, x['at'], item['id']))
        return {'decision': 'handoff_item_accepted', 'item': item['id']}

    if op in ('site_confirm', 'confirm_site', 'onsite_confirmation'):
        hid = x['handoff']
        h = one(db, 'SELECT * FROM handoff_details WHERE handoff_id=?', (hid,))
        hh = one(db, 'SELECT * FROM duty_handoffs WHERE id=?', (hid,))
        if not h or not hh:
            return {'decision': 'handoff_missing'}
        in_shift = one(db, 'SELECT * FROM duty_shifts WHERE id=?', (hh['incoming'],))
        actor = x.get('actor')
        if not in_shift or actor != in_shift['actor']:
            return {'decision': 'only_incoming_can_confirm'}
        sess, err = session_at(db, x.get('session'), actor, h['site'], x['at'])
        if err:
            return {'decision': err}
        if not sess['independent']:
            return {'decision': 'independent_session_required'}
        nodes = _json_list(x.get('nodes', []))
        expected = [r['node'] for r in many(db, 'SELECT node FROM equipment_registry WHERE site=?', (h['site'],))
                    if r['node']]
        if x.get('all_site_nodes'):
            nodes = sorted(set(nodes) | set(expected))
        if expected and (not nodes or not set(expected).issubset(set(nodes))):
            return {'decision': 'physical_nodes_not_confirmed', 'expected': expected}
        perms = _json_list(x.get('permissions', ['launch', 'retry', 'rollback']))
        sess_perms = _json_list(sess['permissions'])
        required = set(x.get('required_permissions', ['launch', 'rollback']))
        if required and not required.issubset(set(perms)):
            return {'decision': 'permissions_not_confirmed', 'required': sorted(required)}
        if sess_perms and not required.issubset(set(sess_perms)):
            return {'decision': 'session_permissions_insufficient'}
        cid = x['id']
        db.execute('INSERT INTO site_confirmations VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (cid, hid, x['session'], actor, x['at'], _received_at(x),
                    json.dumps(sorted(nodes), sort_keys=True),
                    json.dumps(sorted(perms), sort_keys=True),
                    int(bool(sess['independent'])), 1))
        return {'decision': 'site_confirmation_recorded', 'nodes': sorted(nodes)}

    if op in ('sign', 'handoff_sign', 'sign_handoff'):
        hid = x['handoff']
        h = one(db, 'SELECT * FROM duty_handoffs WHERE id=?', (hid,))
        if not h:
            return {'decision': 'handoff_missing'}
        role = x.get('role')
        if role not in ('outgoing', 'incoming', 'supervisor'):
            return {'decision': 'signature_role_required'}
        shift_id = h['outgoing'] if role == 'outgoing' else h['incoming'] if role == 'incoming' else None
        if role in ('outgoing', 'incoming'):
            s = one(db, 'SELECT * FROM duty_shifts WHERE id=?', (shift_id,))
            if not s or x.get('actor') != s['actor']:
                return {'decision': 'signature_actor_mismatch'}
            prior = one(db, 'SELECT 1 FROM handoff_signatures WHERE handoff_id=? AND role=? AND actor=?',
                        (hid, role, x['actor']))
            if prior:
                return {'decision': 'duplicate_signature'}
        signed_at = _names(x, 'signed_at') or x['at']
        received = _received_at(x)
        if signed_at > received:
            return {'decision': 'signature_received_before_signing'}
        db.execute('INSERT INTO handoff_signatures VALUES(?,?,?,?,?,?)',
                   (x['id'], hid, x['actor'], role, signed_at, received))
        return {'decision': 'handoff_signed', 'role': role, 'effective_at': received}

    if op in ('handoff_complete', 'complete_handoff', 'finalize_handoff'):
        hid = x['handoff']
        h = one(db, 'SELECT * FROM duty_handoffs WHERE id=?', (hid,))
        d = one(db, 'SELECT * FROM handoff_details WHERE handoff_id=?', (hid,))
        if not h or not d:
            return {'decision': 'handoff_missing'}
        if d['state'] == 'completed' or h['state'] == 'completed':
            return {'decision': 'handoff_already_completed',
                    'completed_at': d['completed_at']}
        ok, problems, ctx = _completion_checks(db, hid)
        if not ok:
            return {'decision': 'handoff_incomplete', 'problems': problems, **{
                k: v for k, v in ctx.items() if v}}
        c = ctx['completed_at'] or x['at']
        db.execute("UPDATE handoff_details SET state='completed', completed_at=?, completed_by=? WHERE handoff_id=?",
                   (c, x.get('actor'), hid))
        db.execute("UPDATE duty_handoffs SET state='completed' WHERE id=?", (hid,))
        out_shift = one(db, 'SELECT * FROM duty_shifts WHERE id=?', (h['outgoing'],))
        if out_shift:
            db.execute("""UPDATE site_sessions SET ended_at=?, state='closed',
                          closed_reason='handover_completed'
                          WHERE actor=? AND site=? AND ended_at IS NULL""",
                       (c, out_shift['actor'], d['site']))
            existing = one(db, 'SELECT shift_id FROM shift_details WHERE shift_id=?', (out_shift['id'],))
            if existing:
                db.execute('UPDATE shift_details SET relieved_at=? WHERE shift_id=?', (c, out_shift['id']))
            else:
                db.execute('INSERT INTO shift_details(shift_id,relieved_at) VALUES(?,?)',
                           (out_shift['id'], c))
        return {'decision': 'handover_completed', 'completed_at': c,
                'responsible_from': c, 'incoming': h['incoming']}

    if op in ('handoff_cancel', 'cancel_handoff'):
        d = one(db, 'SELECT * FROM handoff_details WHERE handoff_id=?', (x['handoff'],))
        if not d:
            return {'decision': 'handoff_missing'}
        if d['state'] == 'completed':
            return {'decision': 'handoff_already_completed'}
        db.execute("UPDATE handoff_details SET state='cancelled' WHERE handoff_id=?", (x['handoff'],))
        db.execute("UPDATE duty_handoffs SET state='cancelled' WHERE id=?", (x['handoff'],))
        return {'decision': 'handoff_cancelled'}

    if op == 'timeline':
        seg = timeline(db, x['site'], x['start'], x['end'])
        return {'decision': 'timeline_computed', 'segments': seg}

    return {'decision': 'unknown_operation'}


def recompute(db, site=None, start=None, end=None):
    """Rebuild unique responsibility per instant from events + materialized facts.

    Cross-checks every attributed wave/ready operation event against the
    temporal duty model, so a later edit to shift.ends cannot hide a gap.
    """
    if site is None:
        site_rows = many(db, 'SELECT DISTINCT site FROM duty_shifts UNION SELECT DISTINCT site FROM site_sessions UNION SELECT DISTINCT site FROM equipment_registry')
        sites = [r['site'] for r in site_rows if r['site']]
    else:
        sites = [site]

    result_sites = {}
    violations = []
    for st in sites:
        if start is None or end is None:
            ivs = coverage_intervals(db, st)
            lo = start if start is not None else (min((i['start'] for i in ivs), default=0))
            hi = end if end is not None else (max((i['end'] for i in ivs), default=lo + 1))
        else:
            lo, hi = start, end
        seg = timeline(db, st, lo, hi)
        gaps = [s for s in seg if s['status'] == 'gap']
        conflicts = [s for s in seg if s['status'] == 'conflict']
        result_sites[st] = {'segments': seg, 'gaps': gaps, 'conflicts': conflicts,
                            'start': lo, 'end': hi}
        for c in conflicts:
            violations.append({'type': 'multiple_responsible_officers', 'site': st,
                               'at': c['start'], 'end': c['end']})

    operations = []
    events = many(db, 'SELECT * FROM events ORDER BY seq') if one(
        db, "SELECT 1 FROM sqlite_master WHERE type='table' AND name='events'") else []
    for e in events:
        if e['subsystem'] not in ('wave', 'ready'):
            continue
        try:
            p = json.loads(e['payload'])
            r = json.loads(e['result'])
        except (TypeError, ValueError):
            continue
        if p.get('actor') is None:
            continue
        at = p.get('at')
        st = resolve_site(db, p)
        verdict = evaluate_operation(db, p)
        executed = r.get('decision') in SUCCESS_DECISIONS
        row = {'event_id': e['event_id'], 'at': at, 'subsystem': e['subsystem'],
               'op': p.get('op'), 'actor': p.get('actor'), 'site': st,
               'recorded_decision': r.get('decision'), 'executed': executed,
               'gate_decision': verdict.get('decision') if not verdict.get('allowed') else 'authorized',
               'basis': verdict.get('basis')}
        if st:
            point = responsible_at(db, st, at) if at is not None else {'actor': None, 'status': 'untimed'}
            row['responsible'] = point.get('actor')
            row['responsibility_status'] = point.get('status')
        operations.append(row)
        if executed and (not verdict.get('strict') or not verdict.get('allowed')):
            violations.append({'type': 'executed_without_duty_authorization',
                               'event_id': e['event_id'], 'at': at,
                               'recorded_decision': r.get('decision'),
                               'gate': verdict.get('decision')})
        if executed and verdict.get('allowed') and row.get('responsible') not in (None, p.get('actor')):
            violations.append({'type': 'operator_not_unique_responsible',
                               'event_id': e['event_id'], 'at': at,
                               'actor': p.get('actor'),
                               'responsible': row.get('responsible')})

    # Completed handovers must still carry their evidence.
    for d in many(db, "SELECT * FROM handoff_details WHERE state='completed'"):
        ok, problems, ctx = _completion_checks(db, d['handoff_id'])
        if not ok:
            violations.append({'type': 'completed_handoff_evidence_broken',
                               'handoff': d['handoff_id'], 'problems': problems})

    return {'verified': not violations, 'sites': result_sites,
            'operations': operations, 'violations': violations}


def run(db, x):
    return act(db, 'duty', x, handle)


# Read-side aliases for acceptance/recomputation callers.
responsibility_timeline = timeline
audit = recompute
authorize_operation = evaluate_operation
