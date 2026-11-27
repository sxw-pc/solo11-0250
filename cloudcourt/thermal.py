import json
import math

from .store import act, one, many

SCHEMA = '''
CREATE TABLE IF NOT EXISTS cooling_zones(id TEXT PRIMARY KEY, rack TEXT, limit_c INTEGER, revision INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS thermal_samples(id TEXT PRIMARY KEY, zone TEXT, sensor TEXT, at INTEGER, celsius INTEGER);
CREATE TABLE IF NOT EXISTS node_thermal_checks(id TEXT PRIMARY KEY, zone TEXT, node TEXT, sample TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS thermal_profiles(zone TEXT PRIMARY KEY, reference_c REAL, ambient_c REAL, cooling_tau REAL, phase_seq TEXT, revision INTEGER);
CREATE TABLE IF NOT EXISTS thermal_nodes(id TEXT PRIMARY KEY, zone TEXT, serial TEXT, rack TEXT, registered_at INTEGER);
CREATE TABLE IF NOT EXISTS thermal_sensors(id TEXT PRIMARY KEY, node TEXT, zone TEXT, position TEXT, serial TEXT, initial_bias REAL, initial_uncertainty REAL, registered_at INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS sensor_calibrations(id TEXT PRIMARY KEY, sensor TEXT, bias_c REAL, uncertainty_c REAL, effective_at INTEGER, rationale TEXT);
CREATE TABLE IF NOT EXISTS sensor_outages(id TEXT PRIMARY KEY, sensor TEXT, disconnected_at INTEGER, reconnected_at INTEGER, reason TEXT);
CREATE TABLE IF NOT EXISTS thermal_observations(id TEXT PRIMARY KEY, sensor TEXT, at INTEGER, raw_celsius REAL, uncertainty_c REAL);
CREATE TABLE IF NOT EXISTS capacity_contracts(id TEXT PRIMARY KEY, node TEXT, committed_capacity INTEGER, effective_at INTEGER, status TEXT);
CREATE TABLE IF NOT EXISTS manufacturer_evidence(id TEXT PRIMARY KEY, node TEXT, dose_limit REAL, max_operating_c REAL, inspection TEXT, warranty_terms TEXT, recorded_at INTEGER);
CREATE TABLE IF NOT EXISTS thermal_exposure_cases(id TEXT PRIMARY KEY, node TEXT, adjudicated_at INTEGER, dose_low REAL, dose_high REAL, dose_limit REAL, verdict TEXT, point_overtemp TEXT, data_gap INTEGER, basis_event TEXT);
CREATE TABLE IF NOT EXISTS thermal_exposure_windows(id TEXT PRIMARY KEY, case_id TEXT, attempt TEXT, phase TEXT, started INTEGER, expires INTEGER);
CREATE TABLE IF NOT EXISTS device_fitness_decisions(id TEXT PRIMARY KEY, node TEXT, case_id TEXT, state TEXT, decided_at INTEGER, rationale TEXT);
CREATE TABLE IF NOT EXISTS device_fitness_reviews(id TEXT PRIMARY KEY, decision TEXT, node TEXT, state TEXT, reviewed_at INTEGER, rationale TEXT, supersedes TEXT);
CREATE TABLE IF NOT EXISTS suspect_use_periods(id TEXT PRIMARY KEY, node TEXT, case_id TEXT, from_ts INTEGER, until_ts INTEGER, status TEXT);
CREATE TABLE IF NOT EXISTS remediation_records(id TEXT PRIMARY KEY, node TEXT, decision TEXT, kind TEXT, at INTEGER, derated_capacity INTEGER, reinspect_due INTEGER, capacity_ready_at INTEGER, hardware_fee REAL, warranty_status TEXT, status TEXT);
CREATE TABLE IF NOT EXISTS warranty_records(id TEXT PRIMARY KEY, node TEXT, case_id TEXT, remediation TEXT, warranty_status TEXT, liability TEXT, at INTEGER);
CREATE TABLE IF NOT EXISTS capacity_ledger(id TEXT PRIMARY KEY, node TEXT, at INTEGER, available_capacity INTEGER, committed_capacity INTEGER, reason TEXT);
CREATE TABLE IF NOT EXISTS reinspection_records(id TEXT PRIMARY KEY, remediation TEXT, node TEXT, at INTEGER, passed INTEGER, finding TEXT, status TEXT);
CREATE TABLE IF NOT EXISTS health_probes(id TEXT PRIMARY KEY, node TEXT, at INTEGER, healthy INTEGER, note TEXT);
CREATE TABLE IF NOT EXISTS thermal_case_items(id TEXT PRIMARY KEY, case_id TEXT, kind TEXT, ref TEXT);
'''

DEFAULT_PHASES = ['startup', 'warmup', 'steady']


def handle(db, x):
    op = x['op']
    if op == 'zone':
        db.execute('INSERT INTO cooling_zones VALUES(?,?,?,?,?)',
                   (x['id'], x['rack'], x['limit_c'], x['revision'], 'active'))
        return {'decision': 'zone_recorded'}
    if op == 'sample':
        zone = one(db, 'SELECT * FROM cooling_zones WHERE id=?', (x['zone'],))
        if not zone:
            return {'decision': 'zone_missing'}
        db.execute('INSERT INTO thermal_samples VALUES(?,?,?,?,?)',
                   (x['id'], x['zone'], x['sensor'], x['at'], x['celsius']))
        return {'decision': 'temperature_recorded'}
    if op == 'check':
        zone = one(db, 'SELECT * FROM cooling_zones WHERE id=?', (x['zone'],))
        sample = one(db, 'SELECT * FROM thermal_samples WHERE id=?', (x['sample'],))
        if not zone or not sample or sample['zone'] != zone['id']:
            return {'decision': 'sample_unavailable'}
        state = 'qualified' if sample['celsius'] <= zone['limit_c'] else 'over_limit'
        db.execute('INSERT INTO node_thermal_checks VALUES(?,?,?,?,?)',
                   (x['id'], zone['id'], x['node'], sample['id'], state))
        return {'decision': state}
    if op == 'profile':
        phases = x.get('phase_seq', DEFAULT_PHASES)
        db.execute('INSERT INTO thermal_profiles VALUES(?,?,?,?,?,?)',
                   (x['zone'], x['reference_c'], x['ambient_c'], x['cooling_tau'],
                    json.dumps(phases, sort_keys=True, ensure_ascii=False), x['revision']))
        return {'decision': 'profile_recorded'}
    if op == 'node':
        db.execute('INSERT INTO thermal_nodes VALUES(?,?,?,?,?)',
                   (x['id'], x['zone'], x['serial'], x['rack'], x['at']))
        return {'decision': 'node_registered'}
    if op == 'sensor':
        node = one(db, 'SELECT * FROM thermal_nodes WHERE id=?', (x['node'],))
        if not node:
            return {'decision': 'node_missing'}
        db.execute('INSERT INTO thermal_sensors VALUES(?,?,?,?,?,?,?,?,?)',
                   (x['id'], x['node'], node['zone'], x['position'], x['serial'],
                    x.get('bias_c', 0.0), x.get('uncertainty_c', 0.0), x['at'], 'connected'))
        return {'decision': 'sensor_registered'}
    if op == 'calibrate':
        sensor = one(db, 'SELECT * FROM thermal_sensors WHERE id=?', (x['sensor'],))
        if not sensor:
            return {'decision': 'sensor_missing'}
        db.execute('INSERT INTO sensor_calibrations VALUES(?,?,?,?,?,?)',
                   (x['id'], x['sensor'], x['bias_c'], x['uncertainty_c'],
                    x['effective_at'], x.get('rationale', '')))
        return {'decision': 'calibration_recorded'}
    if op == 'disconnect':
        sensor = one(db, 'SELECT * FROM thermal_sensors WHERE id=?', (x['sensor'],))
        if not sensor:
            return {'decision': 'sensor_missing'}
        open_outage = one(db, 'SELECT * FROM sensor_outages WHERE sensor=? AND reconnected_at IS NULL',
                          (x['sensor'],))
        if open_outage:
            return {'decision': 'sensor_already_disconnected'}
        db.execute('INSERT INTO sensor_outages VALUES(?,?,?,?,?)',
                   (x['id'], x['sensor'], x['at'], None, x.get('reason', '')))
        db.execute("UPDATE thermal_sensors SET state='disconnected' WHERE id=?", (x['sensor'],))
        return {'decision': 'sensor_disconnected'}
    if op == 'reconnect':
        outage = one(db, 'SELECT * FROM sensor_outages WHERE sensor=? AND reconnected_at IS NULL',
                     (x['sensor'],))
        if not outage:
            return {'decision': 'no_open_outage'}
        if x['at'] < outage['disconnected_at']:
            return {'decision': 'reconnect_before_disconnect'}
        db.execute('UPDATE sensor_outages SET reconnected_at=? WHERE id=?', (x['at'], outage['id']))
        db.execute("UPDATE thermal_sensors SET state='connected' WHERE id=?", (x['sensor'],))
        return {'decision': 'sensor_reconnected'}
    if op == 'observe':
        sensor = one(db, 'SELECT * FROM thermal_sensors WHERE id=?', (x['sensor'],))
        if not sensor:
            return {'decision': 'sensor_missing'}
        covered = one(db, '''SELECT 1 FROM sensor_outages WHERE sensor=? AND disconnected_at<=?
                             AND (reconnected_at IS NULL OR reconnected_at>?)''',
                      (x['sensor'], x['at'], x['at']))
        if covered:
            return {'decision': 'observation_during_outage'}
        db.execute('INSERT INTO thermal_observations VALUES(?,?,?,?,?)',
                   (x['id'], x['sensor'], x['at'], x['raw_celsius'], x['uncertainty_c']))
        return {'decision': 'observation_recorded'}
    if op == 'capacity':
        db.execute('INSERT INTO capacity_contracts VALUES(?,?,?,?,?)',
                   (x['id'], x['node'], x['committed_capacity'], x['effective_at'], 'active'))
        return {'decision': 'capacity_contract_recorded'}
    if op == 'evidence':
        db.execute('INSERT INTO manufacturer_evidence VALUES(?,?,?,?,?,?,?)',
                   (x['id'], x['node'], x['dose_limit'], x['max_operating_c'],
                    x.get('inspection', ''), json.dumps(x.get('warranty_terms', {}),
                                                        sort_keys=True, ensure_ascii=False),
                    x['at']))
        return {'decision': 'manufacturer_evidence_recorded'}
    if op == 'adjudicate':
        return adjudicate(db, x)
    if op == 'remediation':
        return remediate(db, x)
    if op == 'reinspect':
        return reinspect(db, x)
    if op == 'health_probe':
        node = one(db, 'SELECT * FROM thermal_nodes WHERE id=?', (x['node'],))
        if not node:
            return {'decision': 'node_missing'}
        db.execute('INSERT INTO health_probes VALUES(?,?,?,?,?)',
                   (x['id'], x['node'], x['at'], int(x['healthy']), x.get('note', '')))
        # A fresh healthy probe is evidence only: it never lifts a fitness restriction by itself.
        return {'decision': 'health_probe_recorded', 'fitness_state': current_fitness(db, x['node'])}
    if op == 'review':
        decision = one(db, 'SELECT * FROM device_fitness_decisions WHERE id=?', (x['decision'],))
        if not decision:
            return {'decision': 'decision_missing'}
        db.execute('INSERT INTO device_fitness_reviews VALUES(?,?,?,?,?,?,?)',
                   (x['id'], x['decision'], decision['node'], x['state'], x['at'],
                    x.get('rationale', ''), x.get('supersedes', decision['id'])))
        return {'decision': 'review_appended', 'state': x['state']}
    return {'decision': 'unknown_operation'}


# ---------------------------------------------------------------------------
# Calibration, uncertainty and thermal-dose integration
# ---------------------------------------------------------------------------

def calibration_at(db, sensor_id, at):
    """Calibration effective at observation time. Late revisions append rows;
    the original raw observation is never rewritten."""
    sensor = one(db, 'SELECT * FROM thermal_sensors WHERE id=?', (sensor_id,))
    rev = one(db, '''SELECT * FROM sensor_calibrations WHERE sensor=? AND effective_at<=?
                     ORDER BY effective_at DESC, rowid DESC LIMIT 1''', (sensor_id, at))
    if rev:
        return rev['bias_c'], rev['uncertainty_c'], rev['id']
    return sensor['initial_bias'], sensor['initial_uncertainty'], None


def corrected_series(db, sensor_id):
    rows = many(db, 'SELECT * FROM thermal_observations WHERE sensor=? ORDER BY at, rowid',
                (sensor_id,))
    series = []
    for r in rows:
        bias, cal_unc, cal_id = calibration_at(db, sensor_id, r['at'])
        corrected = r['raw_celsius'] + bias
        uncertainty = math.sqrt(r['uncertainty_c'] ** 2 + cal_unc ** 2)
        series.append({'at': r['at'], 'value': corrected, 'uncertainty': uncertainty,
                       'low': corrected - uncertainty, 'high': corrected + uncertainty,
                       'calibration': cal_id, 'raw': r['raw_celsius']})
    return series


def linear_positive(y0, y1, dt, reference):
    """Integral of max(linear(y0->y1) - reference, 0) over dt."""
    if y0 <= reference and y1 <= reference:
        return 0.0
    if y0 >= reference and y1 >= reference:
        return (y0 + y1 - 2 * reference) * dt / 2
    f = (reference - y0) / (y1 - y0)
    if y1 > y0:
        return 0.5 * (1 - f) * dt * (y1 - reference)
    return 0.5 * f * dt * (y0 - reference)


def cooling_positive(y0, dt, tau, ambient, reference):
    """Integral of max(y(t)-reference,0) where y(t)=ambient+(y0-ambient)e^(-t/tau)."""
    if dt <= 0:
        return 0.0

    def full(t0, t1):
        return ((ambient - reference) * (t1 - t0)
                + (y0 - ambient) * tau * (math.exp(-t0 / tau) - math.exp(-t1 / tau)))

    if y0 <= reference:
        if ambient <= reference:
            return 0.0
        t_star = -tau * math.log((reference - ambient) / (y0 - ambient))
        return full(t_star, dt) if t_star < dt else 0.0
    if ambient >= reference:
        return full(0, dt)
    t_star = -tau * math.log((reference - ambient) / (y0 - ambient))
    return full(0, min(t_star, dt))


def sensor_exposure(db, sensor, windows, ambient, tau, reference):
    """Integrate corrected exposure over the real startup/warmup/steady timeline.

    Linear interpolation between live readings; exponential cooling inertia
    during sensor outages, before the first reading and between boot waves.
    Returns low/high dose bands and a data-gap flag. A disconnected probe can
    never produce a clean bill of health."""
    series = corrected_series(db, sensor['id'])
    outages = many(db, 'SELECT * FROM sensor_outages WHERE sensor=? ORDER BY disconnected_at',
                   (sensor['id'],))

    def outage_overlaps(t0, t1):
        return any(o['disconnected_at'] < t1 and (o['reconnected_at'] is None
                                                  or o['reconnected_at'] > t0) for o in outages)

    def cool_value(v0, dt):
        return ambient + (v0 - ambient) * math.exp(-dt / tau)

    segments = []  # (t0, t1, kind, lo0, hi0, lo1, hi1)
    last_t = last_lo = last_hi = None
    data_gap = False

    for win in windows:
        w0, w1 = win['started'], win['expires']
        inside = [p for p in series if w0 <= p['at'] <= w1]
        if last_t is None:
            if inside:
                first = inside[0]
                if first['at'] > w0:
                    data_gap = True
                    lo0 = hi0 = ambient
                    segments.append((w0, first['at'], 'cooling', lo0, hi0,
                                     cool_value(lo0, first['at'] - w0),
                                     cool_value(hi0, first['at'] - w0)))
                last_t, last_lo, last_hi = first['at'], first['low'], first['high']
            else:
                data_gap = True
                last_t, last_lo, last_hi = w0, float(ambient), float(ambient)
        if last_t < w0:
            lo1, hi1 = cool_value(last_lo, w0 - last_t), cool_value(last_hi, w0 - last_t)
            segments.append((last_t, w0, 'cooling', last_lo, last_hi, lo1, hi1))
            last_t, last_lo, last_hi = w0, lo1, hi1
        for p in inside:
            if p['at'] <= last_t:
                continue
            if outage_overlaps(last_t, p['at']):
                data_gap = True
                kind = 'cooling'
                lo1, hi1 = cool_value(last_lo, p['at'] - last_t), cool_value(last_hi, p['at'] - last_t)
            else:
                kind = 'linear'
                lo1, hi1 = p['low'], p['high']
            segments.append((last_t, p['at'], kind, last_lo, last_hi, lo1, hi1))
            last_t, last_lo, last_hi = p['at'], p['low'], p['high']
        if last_t < w1:
            if outage_overlaps(last_t, w1):
                data_gap = True
            lo1, hi1 = cool_value(last_lo, w1 - last_t), cool_value(last_hi, w1 - last_t)
            segments.append((last_t, w1, 'cooling', last_lo, last_hi, lo1, hi1))
            last_t, last_lo, last_hi = w1, lo1, hi1

    dose_low = dose_high = 0.0
    for t0, t1, kind, lo0, hi0, lo1, hi1 in segments:
        dt = t1 - t0
        if kind == 'linear':
            dose_low += linear_positive(lo0, lo1, dt, reference)
            dose_high += linear_positive(hi0, hi1, dt, reference)
        else:
            dose_low += cooling_positive(lo0, dt, tau, ambient, reference)
            dose_high += cooling_positive(hi0, dt, tau, ambient, reference)
    if any(outage_overlaps(w['started'], w['expires']) for w in windows):
        data_gap = True
    return {'sensor': sensor['id'], 'position': sensor['position'],
            'dose_low': dose_low, 'dose_high': dose_high, 'data_gap': data_gap,
            'observations': len(series)}


def current_fitness(db, node):
    review = one(db, '''SELECT r.state AS state FROM device_fitness_reviews r
                        JOIN device_fitness_decisions d ON d.id=r.decision
                        WHERE d.node=? ORDER BY r.reviewed_at DESC, r.rowid DESC LIMIT 1''', (node,))
    if review:
        return review['state']
    decision = one(db, 'SELECT state FROM device_fitness_decisions WHERE node=? ORDER BY decided_at DESC, rowid DESC LIMIT 1',
                   (node,))
    return decision['state'] if decision else None


def close_suspect_periods(db, node, at):
    db.execute("UPDATE suspect_use_periods SET until_ts=?, status='closed' WHERE node=? AND status='open'",
               (at, node))


def add_case_item(db, case_id, kind, ref):
    db.execute('INSERT INTO thermal_case_items VALUES(?,?,?,?)',
               (f'{case_id}:{kind}:{ref}', case_id, kind, ref))


def adjudicate(db, x):
    node = one(db, 'SELECT * FROM thermal_nodes WHERE id=?', (x['node'],))
    if not node:
        return {'decision': 'node_missing'}
    profile = one(db, 'SELECT * FROM thermal_profiles WHERE zone=?', (node['zone'],))
    if not profile:
        return {'decision': 'thermal_profile_missing'}
    evidence = one(db, 'SELECT * FROM manufacturer_evidence WHERE node=? ORDER BY recorded_at DESC, rowid DESC LIMIT 1',
                   (x['node'],))
    if not evidence:
        return {'decision': 'manufacturer_evidence_missing'}
    sensors = many(db, 'SELECT * FROM thermal_sensors WHERE node=?', (x['node'],))
    if not sensors:
        return {'decision': 'sensors_missing'}
    attempts = many(db, '''SELECT a.* FROM start_attempts a JOIN startup_slots s ON s.id=a.slot
                           WHERE s.node=? ORDER BY a.started, a.rowid''', (x['node'],))
    if not attempts:
        return {'decision': 'no_startup_attempts'}

    phases = json.loads(profile['phase_seq'])
    windows = [{'attempt': a['id'], 'started': a['started'], 'expires': a['expires'],
                'phase': phases[min(i, len(phases) - 1)]} for i, a in enumerate(attempts)]
    exposures = [sensor_exposure(db, s, windows, profile['ambient_c'],
                                 profile['cooling_tau'], profile['reference_c']) for s in sensors]
    # The cold inlet probe cannot offset a near-chip hotspot: take the worst
    # sensor band rather than an average.
    dose_low = max(e['dose_low'] for e in exposures)
    dose_high = max(e['dose_high'] for e in exposures)
    data_gap = int(any(e['data_gap'] for e in exposures))

    point_over = []
    for s in sensors:
        for p in corrected_series(db, s['id']):
            if p['high'] > evidence['max_operating_c']:
                point_over.append(s['id'])
                break
    limit = evidence['dose_limit']
    if dose_high <= limit:
        verdict = 'within_limit'
    elif dose_low > limit:
        verdict = 'over_limit'
    else:
        verdict = 'indeterminate'
    if data_gap and verdict == 'within_limit':
        verdict = 'indeterminate_data_gap'
    # A single near-chip excursion past the manufacturer's maximum operating
    # temperature restricts fitness even if the cumulative dose would pass.
    if point_over and verdict == 'within_limit':
        verdict = 'point_over_temperature'

    state_map = {
        'within_limit': 'fit_for_service',
        'indeterminate': 'fitness_restricted_indeterminate_dose',
        'over_limit': 'fitness_restricted_over_dose',
        'indeterminate_data_gap': 'fitness_restricted_data_gap',
        'point_over_temperature': 'fitness_restricted_point_overtemp',
    }
    fitness_state = state_map[verdict]
    rationale = ('cumulative dose band %.2f..%.2f vs limit %.2f'
                 % (dose_low, dose_high, limit))
    if point_over:
        rationale += '; point over-temperature at ' + ','.join(sorted(set(point_over)))
    if data_gap:
        rationale += '; sensor outage/gap prevents a clean clearance'

    db.execute('INSERT INTO thermal_exposure_cases VALUES(?,?,?,?,?,?,?,?,?,?)',
               (x['id'], x['node'], x['at'], dose_low, dose_high, limit, verdict,
                  ','.join(sorted(set(point_over))), data_gap, x['event_id']))
    for i, w in enumerate(windows):
        db.execute('INSERT INTO thermal_exposure_windows VALUES(?,?,?,?,?,?)',
                   (f'{x["id"]}:w{i}', x['id'], w['attempt'], w['phase'], w['started'], w['expires']))
    db.execute('INSERT INTO device_fitness_decisions VALUES(?,?,?,?,?,?)',
               (f'{x["id"]}:fitness', x['node'], x['id'], fitness_state, x['at'], rationale))
    for w in windows:
        add_case_item(db, x['id'], 'attempt', w['attempt'])
    add_case_item(db, x['id'], 'manufacturer_evidence', evidence['id'])
    contract = one(db, 'SELECT * FROM capacity_contracts WHERE node=? ORDER BY effective_at DESC, rowid DESC LIMIT 1',
                   (x['node'],))
    committed = contract['committed_capacity'] if contract else 0
    if contract:
        add_case_item(db, x['id'], 'capacity_contract', contract['id'])
    for check_id in x.get('checks', []):
        check = one(db, 'SELECT * FROM node_thermal_checks WHERE id=?', (check_id,))
        if check and check['node'] == x['node']:
            add_case_item(db, x['id'], 'check', check_id)
    if fitness_state != 'fit_for_service':
        db.execute('INSERT INTO suspect_use_periods VALUES(?,?,?,?,?,?)',
                   (f'{x["id"]}:suspect', x['node'], x['id'], windows[0]['started'], None, 'open'))
    available = committed if fitness_state == 'fit_for_service' else 0
    db.execute('INSERT INTO capacity_ledger VALUES(?,?,?,?,?,?)',
               (f'{x["id"]}:ledger', x['node'], x['at'], available, committed,
                'adjudication:' + verdict))
    return {'decision': fitness_state, 'case': x['id'], 'verdict': verdict,
            'dose_low': round(dose_low, 4), 'dose_high': round(dose_high, 4),
            'dose_limit': limit, 'band_contains_limit': bool(dose_low <= limit <= dose_high),
            'point_overtemp': sorted(set(point_over)), 'data_gap': bool(data_gap),
            'windows': [w['phase'] for w in windows],
            'sensor_bands': [{'sensor': e['sensor'], 'position': e['position'],
                              'dose_low': round(e['dose_low'], 4),
                              'dose_high': round(e['dose_high'], 4)} for e in exposures]}


def remediate(db, x):
    node = one(db, 'SELECT * FROM thermal_nodes WHERE id=?', (x['node'],))
    if not node:
        return {'decision': 'node_missing'}
    decision = one(db, 'SELECT * FROM device_fitness_decisions WHERE id=?', (x['decision'],))
    if not decision or decision['node'] != x['node']:
        return {'decision': 'decision_missing'}
    if current_fitness(db, x['node']) == 'fit_for_service':
        return {'decision': 'no_active_restriction'}
    kind = x['kind']
    if kind not in ('derated', 'reinspect_shutdown', 'replaced'):
        return {'decision': 'unknown_remediation'}
    contract = one(db, 'SELECT * FROM capacity_contracts WHERE node=? ORDER BY effective_at DESC, rowid DESC LIMIT 1',
                   (x['node'],))
    committed = contract['committed_capacity'] if contract else 0
    case_id = decision['case_id']
    warranty_status = liability = None
    available = 0
    ready_at = x['at']
    fee = x.get('hardware_fee', 0.0)
    status = 'active'

    if kind == 'derated':
        derated = x['derated_capacity']
        if derated is None or not 0 <= derated < committed:
            return {'decision': 'invalid_derated_capacity'}
        if x.get('reinspect_due') is None or x['reinspect_due'] <= x['at']:
            return {'decision': 'reinspection_due_required'}
        warranty_status = 'conditional_monitored'
        liability = 'manufacturer warranty continues only under continuous monitoring until reinspection'
        available = derated
        ready_at = x['at']
        status = 'active'
    elif kind == 'reinspect_shutdown':
        if x.get('reinspect_due') is None or x['reinspect_due'] <= x['at']:
            return {'decision': 'reinspection_due_required'}
        warranty_status = 'full_retained'
        liability = 'full manufacturer warranty retained; customer capacity suspended pending reinspection'
        available = 0
        ready_at = x['reinspect_due']
        status = 'pending_reinspection'
    else:  # replaced
        warranty_status = 'full_retained'
        liability = 'suspect component replaced at hardware fee; full warranty on replacement parts'
        available = 0
        ready_at = x.get('capacity_ready_at', x['at'])
        if ready_at < x['at']:
            return {'decision': 'invalid_capacity_ready_at'}
        status = 'replaced'
        close_suspect_periods(db, x['node'], x['at'])

    db.execute('INSERT INTO remediation_records VALUES(?,?,?,?,?,?,?,?,?,?,?)',
               (x['id'], x['node'], x['decision'], kind, x['at'],
                x.get('derated_capacity'), x.get('reinspect_due'), ready_at, fee,
                warranty_status, status))
    db.execute('INSERT INTO warranty_records VALUES(?,?,?,?,?,?,?)',
               (f'{x["id"]}:warranty', x['node'], case_id, x['id'], warranty_status, liability, x['at']))
    db.execute('INSERT INTO capacity_ledger VALUES(?,?,?,?,?,?)',
               (f'{x["id"]}:ledger', x['node'], x['at'], available, committed, 'remediation:' + kind))
    add_case_item(db, case_id, 'remediation', x['id'])
    add_case_item(db, case_id, 'warranty', f'{x["id"]}:warranty')
    return {'decision': 'remediation_recorded', 'kind': kind,
            'available_capacity': available, 'committed_capacity': committed,
            'capacity_ready_at': ready_at, 'hardware_fee': fee,
            'warranty_status': warranty_status}


def reinspect(db, x):
    record = one(db, 'SELECT * FROM remediation_records WHERE id=?', (x['remediation'],))
    if not record or record['node'] != x['node']:
        return {'decision': 'remediation_missing'}
    if record['status'] not in ('active', 'pending_reinspection'):
        return {'decision': 'remediation_not_open'}
    decision = one(db, 'SELECT * FROM device_fitness_decisions WHERE id=?', (record['decision'],))
    passed = bool(x['passed'])
    if passed:
        new_state = 'fit_for_service'
        db.execute("UPDATE remediation_records SET status='completed' WHERE id=?", (record['id'],))
        close_suspect_periods(db, x['node'], x['at'])
        contract = one(db, 'SELECT * FROM capacity_contracts WHERE node=? ORDER BY effective_at DESC, rowid DESC LIMIT 1',
                       (x['node'],))
        committed = contract['committed_capacity'] if contract else 0
        db.execute('INSERT INTO capacity_ledger VALUES(?,?,?,?,?,?)',
                   (f'{x["id"]}:ledger', x['node'], x['at'], committed, committed, 'reinspection_passed'))
        liability = 'warranty obligations from the suspect-use period remain documented; service fully restored'
    else:
        new_state = current_fitness(db, x['node']) or 'fitness_restricted_over_dose'
        db.execute("UPDATE remediation_records SET status='failed_reinspection' WHERE id=?", (record['id'],))
        liability = 'reinspection failed; component replacement required, warranty claim routed to manufacturer'
        db.execute('INSERT INTO warranty_records VALUES(?,?,?,?,?,?,?)',
                   (f'{x["id"]}:warranty', x['node'], decision['case_id'], record['id'],
                    'replacement_required', liability, x['at']))
    db.execute('INSERT INTO reinspection_records VALUES(?,?,?,?,?,?,?)',
               (x['id'], x['remediation'], x['node'], x['at'], int(passed),
                x.get('finding', ''), 'completed' if passed else 'failed'))
    # Reviews are append-only: the original fitness decision and the customer's
    # suspect-use window stay on record.
    db.execute('INSERT INTO device_fitness_reviews VALUES(?,?,?,?,?,?,?)',
               (x['id'], record['decision'], x['node'], new_state, x['at'],
                ('reinspection passed: ' if passed else 'reinspection failed: ') + x.get('finding', ''),
                record['decision']))
    return {'decision': 'reinspection_passed' if passed else 'reinspection_failed',
            'fitness_state': new_state}


def run(db, x):
    return act(db, 'thermal', x, handle)
