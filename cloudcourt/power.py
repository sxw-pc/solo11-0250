import json

from .store import act, one, many

SCHEMA = '''
CREATE TABLE IF NOT EXISTS rack_feeds(id TEXT PRIMARY KEY, rack TEXT, rated_kw INTEGER, revision INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS power_reservations(id TEXT PRIMARY KEY, feed TEXT, node TEXT, kw INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS power_observations(id TEXT PRIMARY KEY, feed TEXT, at INTEGER, measured_kw INTEGER);

-- 共享上游段（UPS / 母线 / 发电机）。两条名义馈线可以汇到同一段，冗余不能卖两份。
CREATE TABLE IF NOT EXISTS power_segments(
  id TEXT PRIMARY KEY, kind TEXT, rated_kw INTEGER, margin_kw INTEGER NOT NULL DEFAULT 0,
  sensor_generation INTEGER NOT NULL DEFAULT 1, evidence_required INTEGER NOT NULL DEFAULT 0,
  fresh_ticks INTEGER, available_from INTEGER NOT NULL DEFAULT 0,
  available_until INTEGER NOT NULL DEFAULT 1000000000, state TEXT NOT NULL DEFAULT 'active',
  revision INTEGER NOT NULL DEFAULT 1);

-- 馈线实际拓扑：上联共享段、失电时可转供的备用馈线/段、保护余量、测量代次与可用时段。
CREATE TABLE IF NOT EXISTS feed_topology(
  feed TEXT PRIMARY KEY, segment TEXT, backup_feed TEXT, backup_segment TEXT,
  margin_kw INTEGER NOT NULL DEFAULT 0, sensor_generation INTEGER NOT NULL DEFAULT 1,
  evidence_required INTEGER NOT NULL DEFAULT 0, fresh_ticks INTEGER,
  available_from INTEGER NOT NULL DEFAULT 0, available_until INTEGER NOT NULL DEFAULT 1000000000);

-- 启动用电合同：瞬时冲击 / 预热稳定 / 运行期三段曲线。
CREATE TABLE IF NOT EXISTS power_profiles(
  service TEXT PRIMARY KEY, generation INTEGER NOT NULL,
  inrush_kw INTEGER NOT NULL, inrush_ticks INTEGER NOT NULL,
  warm_kw INTEGER NOT NULL, warm_ticks INTEGER NOT NULL, run_kw INTEGER NOT NULL);

-- 已在运行的生产负载，保障优先级高于新启动。
CREATE TABLE IF NOT EXISTS power_loads(
  id TEXT PRIMARY KEY, service TEXT, node TEXT, feed TEXT, segment TEXT,
  run_kw INTEGER NOT NULL, priority INTEGER NOT NULL DEFAULT 100,
  ready INTEGER NOT NULL DEFAULT 1, state TEXT NOT NULL DEFAULT 'protected', at INTEGER);

-- 现场测量（带测量/校准代次与到达时刻）。旧 power_observations 保留兼容。
CREATE TABLE IF NOT EXISTS power_metrics(
  id TEXT PRIMARY KEY, target_kind TEXT, target TEXT, generation INTEGER NOT NULL,
  at INTEGER NOT NULL, recorded_at INTEGER NOT NULL, measured_kw INTEGER NOT NULL,
  calibrated INTEGER NOT NULL DEFAULT 1, quality TEXT NOT NULL DEFAULT 'valid');

-- 电力承诺：规划时的最坏峰值结论 + 与 wave 尝试/就绪回执的绑定 + 费用安排。
CREATE TABLE IF NOT EXISTS power_commitments(
  id TEXT PRIMARY KEY, wave TEXT, service TEXT, generation INTEGER, node TEXT,
  feed TEXT, segment TEXT, arrangement TEXT, priority INTEGER NOT NULL DEFAULT 50,
  inrush_kw INTEGER, inrush_ticks INTEGER, warm_kw INTEGER, warm_ticks INTEGER, run_kw INTEGER,
  start_at INTEGER, deadline INTEGER, state TEXT, basis TEXT, verified INTEGER,
  worst_peak_kw INTEGER, reserved_kw INTEGER, bottleneck TEXT,
  attempt TEXT, bound_at INTEGER, ready_at INTEGER,
  cost_amount INTEGER NOT NULL DEFAULT 0, cost_bearer TEXT, created_at INTEGER);

-- 清偿记录：必须同时引用有效测量与现场确认，wave.timeout 本身不放电力。
CREATE TABLE IF NOT EXISTS power_clearances(
  id TEXT PRIMARY KEY, commitment TEXT, measurement TEXT, field_ref TEXT,
  confirmed_by TEXT, at INTEGER, measured_kw INTEGER, baseline_kw INTEGER, state TEXT);

-- 故障/切换后产生的处置指令：哪些启动阻止、哪些已就绪服务降级、可转供。
CREATE TABLE IF NOT EXISTS power_directives(
  id TEXT PRIMARY KEY, kind TEXT, target TEXT, target_kind TEXT,
  cause TEXT, at INTEGER, state TEXT, detail TEXT);

CREATE TABLE IF NOT EXISTS power_costs(
  id TEXT PRIMARY KEY, arrangement TEXT, service TEXT, kind TEXT,
  amount INTEGER, party TEXT, at INTEGER, note TEXT);

-- 跨机架回退安排与实测结论，责任方必须记名。
CREATE TABLE IF NOT EXISTS power_fallbacks(
  id TEXT PRIMARY KEY, service TEXT, owner TEXT,
  from_rack TEXT, from_feed TEXT, from_segment TEXT,
  to_rack TEXT, to_feed TEXT, to_segment TEXT,
  status TEXT, tested_at INTEGER, detail TEXT);
'''

WINDOW_END = 1000000000


# ---------------------------------------------------------------------------
# 纯核算函数（wave 模块也直接调用这些函数做 launch 时复核，不走 act 事件）
# ---------------------------------------------------------------------------

def _dump(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _topology(db, feed):
    return one(db, 'SELECT * FROM feed_topology WHERE feed=?', (feed,))


def _segment(db, segment):
    return one(db, 'SELECT * FROM power_segments WHERE id=?', (segment,))


def _feed_limit(db, feed_id):
    feed = one(db, 'SELECT * FROM rack_feeds WHERE id=?', (feed_id,))
    if not feed:
        return None, None
    topo = _topology(db, feed_id)
    margin = topo['margin_kw'] if topo else 0
    return max(0, feed['rated_kw'] - margin), feed['state']


def _segment_limit(db, segment_id):
    seg = _segment(db, segment_id)
    if not seg:
        return None, None
    return max(0, seg['rated_kw'] - seg['margin_kw']), seg['state']


def _route(db, feed_id, segment_id, down_feeds, down_segments):
    """按故障集合与拓扑备用路径改接；无法改接时返回 unserved 原因。"""
    original_feed = feed_id
    if feed_id in down_feeds:
        topo = _topology(db, original_feed)
        backup = topo['backup_feed'] if topo else None
        if backup and backup not in down_feeds:
            feed_id = backup
        else:
            return feed_id, segment_id, 'feed_lost'
    if segment_id and segment_id in down_segments:
        topo = _topology(db, original_feed)
        backup_segment = topo['backup_segment'] if topo else None
        if backup_segment and backup_segment not in down_segments:
            segment_id = backup_segment
        else:
            return feed_id, segment_id, 'segment_lost'
    return feed_id, segment_id, None


def _draw(item, t):
    if item.get('constant') is not None:
        return item['constant']
    start = item['start']
    if t < start:
        return 0
    inrush_ticks = item.get('inrush_ticks') or 0
    if inrush_ticks and t < start + inrush_ticks:
        return item['inrush_kw']
    warm_ticks = item.get('warm_ticks') or 0
    if warm_ticks and t < start + inrush_ticks + warm_ticks:
        return item['warm_kw']
    return item['run_kw']


def _commitment_item(row, start_override=None):
    if row['state'] == 'energized':
        return {'cid': row['id'], 'source': 'commitment', 'source_id': row['id'],
                'service': row['service'], 'feed': row['feed'], 'segment': row['segment'],
                'priority': row['priority'], 'constant': row['run_kw']}
    if start_override is not None:
        start = start_override
    elif row['state'] == 'bound' and row['bound_at'] is not None:
        start = row['bound_at']  # 实际启动时刻优先于计划时刻
    else:
        start = row['start_at']
    return {'cid': row['id'], 'source': 'commitment', 'source_id': row['id'],
            'service': row['service'], 'feed': row['feed'], 'segment': row['segment'],
            'priority': row['priority'], 'start': start,
            'inrush_kw': row['inrush_kw'], 'inrush_ticks': row['inrush_ticks'],
            'warm_kw': row['warm_kw'], 'warm_ticks': row['warm_ticks'], 'run_kw': row['run_kw']}


def _contributors(db, exclude_commitment=None):
    items = []
    for load in many(db, "SELECT * FROM power_loads WHERE state='protected' ORDER BY id"):
        items.append({'cid': 'load:' + load['id'], 'source': 'load', 'source_id': load['id'],
                      'service': load['service'], 'feed': load['feed'], 'segment': load['segment'],
                      'priority': load['priority'], 'constant': load['run_kw']})
    active_states = ('committed', 'bound', 'energized')
    for c in many(db, "SELECT * FROM power_commitments WHERE state IN (?,?,?) ORDER BY id", active_states):
        if exclude_commitment and c['id'] == exclude_commitment:
            continue
        items.append(_commitment_item(c))
    return items


def worst_peak(db, extra=None, exclude_commitment=None, at=0,
               down_feeds=frozenset(), down_segments=frozenset()):
    """按所有曲线相位切换点核算最坏峰值（半开区间，端点相接不算重叠）。"""
    items = _contributors(db, exclude_commitment)
    if extra is not None:
        items.append(extra)
    breaks = set()
    for item in items:
        if item.get('constant') is not None:
            continue
        s = item['start']
        di = item.get('inrush_ticks') or 0
        dw = item.get('warm_ticks') or 0
        breaks.update((s, s + di, s + di + dw))
    times = sorted(breaks) if breaks else [at]
    feed_peaks = {}
    segment_peaks = {}
    peak_at = times[0]
    overall = -1
    unserved = []
    series = []
    for t in times:
        fsum = {}
        ssum = {}
        for item in items:
            kw = _draw(item, t)
            if kw <= 0:
                continue
            feed_id, segment_id, problem = _route(db, item['feed'], item['segment'],
                                                  down_feeds, down_segments)
            if problem:
                unserved.append({'cid': item['cid'], 'service': item.get('service'),
                                 'at': t, 'reason': problem})
                continue
            fsum[feed_id] = fsum.get(feed_id, 0) + kw
            if segment_id:
                ssum[segment_id] = ssum.get(segment_id, 0) + kw
        total = sum(fsum.values())  # 每条负载恰好挂一条馈线，馈线合计即总用电
        if total > overall:
            overall = total
            peak_at = t
        for fid, kw in fsum.items():
            feed_peaks[fid] = max(feed_peaks.get(fid, 0), kw)
        for sid, kw in ssum.items():
            segment_peaks[sid] = max(segment_peaks.get(sid, 0), kw)
        series.append({'at': t, 'total_kw': total, 'feeds': fsum, 'segments': ssum})
    return {'at': peak_at, 'overall_peak_kw': max(overall, 0),
            'feed_peaks': feed_peaks, 'segment_peaks': segment_peaks,
            'unserved': unserved, 'series': series}


def peak_violations(db, peak):
    violations = []
    for fid, kw in sorted(peak['feed_peaks'].items()):
        limit, state = _feed_limit(db, fid)
        if limit is None:
            violations.append({'resource': 'feed', 'id': fid, 'problem': 'topology_missing'})
        elif kw > limit:
            violations.append({'resource': 'feed', 'id': fid,
                               'peak_kw': kw, 'limit_kw': limit})
        elif state != 'active':
            violations.append({'resource': 'feed', 'id': fid, 'problem': 'feed_not_active',
                               'state': state})
    for sid, kw in sorted(peak['segment_peaks'].items()):
        limit, state = _segment_limit(db, sid)
        if limit is None:
            violations.append({'resource': 'segment', 'id': sid, 'problem': 'segment_missing'})
        elif kw > limit:
            violations.append({'resource': 'segment', 'id': sid,
                               'peak_kw': kw, 'limit_kw': limit})
        elif state != 'active':
            violations.append({'resource': 'segment', 'id': sid,
                               'problem': 'segment_not_active', 'state': state})
    return violations


def candidate_from_payload(db, x):
    """从 commit 请求或已存承诺构造候选曲线。"""
    profile = None
    if x.get('service'):
        q = 'SELECT * FROM power_profiles WHERE service=?'
        args = [x['service']]
        if x.get('generation') is not None:
            q += ' AND generation=?'
            args.append(x['generation'])
        profile = one(db, q + ' ORDER BY generation DESC LIMIT 1', tuple(args))
    inrush = x.get('inrush_kw', profile['inrush_kw'] if profile else None)
    inrush_ticks = x.get('inrush_ticks', profile['inrush_ticks'] if profile else None)
    warm = x.get('warm_kw', profile['warm_kw'] if profile else 0)
    warm_ticks = x.get('warm_ticks', profile['warm_ticks'] if profile else 0)
    run = x.get('run_kw', profile['run_kw'] if profile else None)
    if inrush is None or run is None or inrush_ticks is None:
        return None
    feed_id = x.get('feed')
    topo = _topology(db, feed_id) if feed_id else None
    segment_id = x.get('segment') or (topo['segment'] if topo else None)
    return {'cid': 'candidate:' + str(x.get('id', 'x')), 'source': 'candidate',
            'source_id': x.get('id'), 'service': x.get('service'),
            'feed': feed_id, 'segment': segment_id,
            'priority': int(x.get('priority', 50)),
            'start': x.get('start_at', x.get('at', 0)),
            'inrush_kw': int(inrush), 'inrush_ticks': int(inrush_ticks),
            'warm_kw': int(warm), 'warm_ticks': int(warm_ticks), 'run_kw': int(run)}


def evidence_status(db, target_kind, target, at):
    """返回某资源在 at 时刻可引用的最强测量证据状态。

    measured          有当前校准代次、有效、已到达且未过期的测量
    late              存在合格测量，但 at 时刻尚未到达（观测迟到）
    calibration       只有旧代次/校准失效的测量
    stale             合格测量已超过新鲜期
    missing           没有任何测量
    """
    if target_kind == 'feed':
        feed = one(db, 'SELECT * FROM rack_feeds WHERE id=?', (target,))
        if not feed:
            return {'status': 'missing', 'generation': None, 'metric': None}
        topo = _topology(db, target)
        generation = topo['sensor_generation'] if topo else 1
        fresh = topo['fresh_ticks'] if topo else None
    else:
        seg = _segment(db, target)
        if not seg:
            return {'status': 'missing', 'generation': None, 'metric': None}
        generation = seg['sensor_generation']
        fresh = seg['fresh_ticks']
    metrics = many(db, '''SELECT * FROM power_metrics
                          WHERE target_kind=? AND target=?
                          ORDER BY generation DESC, at DESC, id DESC''',
                   (target_kind, target))
    good = next((m for m in metrics
                 if m['generation'] == generation and m['calibrated'] == 1
                 and m['quality'] == 'valid'), None)
    if good is None:
        status = 'calibration' if metrics else 'missing'
        return {'status': status, 'generation': generation, 'metric': None}
    if good['at'] > at or good['recorded_at'] > at:
        return {'status': 'late', 'generation': generation, 'metric': good}
    if fresh is not None and at - good['at'] > fresh:
        return {'status': 'stale', 'generation': generation, 'metric': good}
    return {'status': 'measured', 'generation': generation, 'metric': good}


def path_evidence(db, feed_id, segment_id, at):
    resources = [('feed', feed_id)]
    if segment_id:
        resources.append(('segment', segment_id))
    report = {}
    for kind, rid in resources:
        if rid:
            ev = evidence_status(db, kind, rid, at)
            report[f'{kind}:{rid}'] = ev['status']
    verified = all(status == 'measured' for status in report.values()) and bool(report)
    return report, ('measured' if verified else 'nameplate'), verified


def _steady_consumers(db):
    consumers = []
    for load in many(db, "SELECT * FROM power_loads WHERE state='protected' ORDER BY id"):
        consumers.append({'cid': 'load:' + load['id'], 'service': load['service'],
                          'feed': load['feed'], 'segment': load['segment'],
                          'kw': load['run_kw'], 'priority': load['priority'],
                          'running': True, 'ref_kind': 'load', 'ref_id': load['id']})
    for c in many(db, "SELECT * FROM power_commitments WHERE state IN ('committed','bound','energized') ORDER BY id"):
        consumers.append({'cid': 'commitment:' + c['id'], 'service': c['service'],
                          'feed': c['feed'], 'segment': c['segment'], 'kw': c['run_kw'],
                          'priority': c['priority'], 'running': c['state'] == 'energized',
                          'state': c['state'], 'ref_kind': 'commitment', 'ref_id': c['id']})
    consumers.sort(key=_consumer_order)
    return consumers


def _consumer_order(item):
    # 数值优先级优先；同优先级时已在运/已就绪者先得容量，新候选后得。
    return (-item['priority'], 0 if item.get('running') else 1, item['cid'])


def assess_contingency(db, at=0, down_feeds=(), down_segments=(), extra_steady=None):
    """单故障稳态打包：高优先级先得容量；装不下的在运服务须降级，未启动承诺阻止。"""
    down_feeds = frozenset(down_feeds)
    down_segments = frozenset(down_segments)
    consumers = _steady_consumers(db)
    if extra_steady is not None:
        consumers.append(extra_steady)
        consumers.sort(key=_consumer_order)
    feed_used = {}
    segment_used = {}
    accepted, degrade, blocked, transfers = [], [], [], []
    for cons in consumers:
        feed_id, segment_id, problem = _route(db, cons['feed'], cons['segment'],
                                              down_feeds, down_segments)
        verdict = None
        if problem:
            verdict = problem
        else:
            flimit, _ = _feed_limit(db, feed_id)
            slimit = None
            if segment_id:
                slimit, _ = _segment_limit(db, segment_id)
            if flimit is None:
                verdict = 'feed_topology_missing'
            elif feed_used.get(feed_id, 0) + cons['kw'] > flimit:
                verdict = 'feed_capacity'
            elif slimit is not None and segment_used.get(segment_id, 0) + cons['kw'] > slimit:
                verdict = 'segment_capacity'
            elif slimit is None and segment_id and _segment(db, segment_id) is None:
                verdict = 'segment_missing'
        if verdict:
            row = {'id': cons['ref_id'], 'ref_kind': cons['ref_kind'],
                   'service': cons['service'],
                   'kw': cons['kw'], 'priority': cons['priority'], 'reason': verdict,
                   'capacity_breach': verdict in ('feed_capacity', 'segment_capacity')}
            if cons.get('running'):
                degrade.append(row)
            else:
                blocked.append(row)
            continue
        feed_used[feed_id] = feed_used.get(feed_id, 0) + cons['kw']
        if segment_id:
            segment_used[segment_id] = segment_used.get(segment_id, 0) + cons['kw']
        accepted.append({'id': cons['ref_id'], 'service': cons['service'], 'kw': cons['kw']})
        if feed_id != cons['feed'] or (segment_id and segment_id != cons['segment']):
            transfers.append({'id': cons['ref_id'], 'ref_kind': cons['ref_kind'],
                              'service': cons['service'],
                              'from_feed': cons['feed'], 'feed': feed_id,
                              'from_segment': cons['segment'], 'segment': segment_id})
    return {'at': at, 'down_feeds': sorted(down_feeds),
            'down_segments': sorted(down_segments),
            'feed_load_kw': feed_used, 'segment_load_kw': segment_used,
            'accepted': accepted, 'transfer': transfers,
            'degrade': degrade, 'blocked': blocked}


def single_fault_scenarios(db):
    feed_ids = [r['id'] for r in many(db, 'SELECT id FROM rack_feeds ORDER BY id')]
    segment_ids = [r['id'] for r in many(db, 'SELECT id FROM power_segments ORDER BY id')]
    scenarios = []
    for fid in feed_ids:
        scenarios.append({'fault': f'feed:{fid}', 'down_feeds': [fid], 'down_segments': []})
    for sid in segment_ids:
        scenarios.append({'fault': f'segment:{sid}', 'down_feeds': [], 'down_segments': [sid]})
    return scenarios


def full_assessment(db, at=0, down_feeds=(), down_segments=()):
    peak = worst_peak(db, at=at, down_feeds=frozenset(down_feeds),
                      down_segments=frozenset(down_segments))
    return {'decision': 'power_assessed', 'at': at,
            'down_feeds': sorted(down_feeds), 'down_segments': sorted(down_segments),
            'worst_peak_kw': peak['overall_peak_kw'], 'peak_at': peak['at'],
            'feed_peaks': peak['feed_peaks'], 'segment_peaks': peak['segment_peaks'],
            'violations': peak_violations(db, peak),
            'contingency': assess_contingency(db, at, down_feeds, down_segments)}


def authorize_launch(db, payload, wave_row):
    """wave.launch 在写尝试前调用。无 power_commitment 字段时保持旧行为（返回 None）。"""
    cid = payload.get('power_commitment') or payload.get('commitment') or payload.get('power')
    if cid is None:
        return None
    c = one(db, 'SELECT * FROM power_commitments WHERE id=?', (cid,))
    if not c:
        return {'decision': 'power_commitment_missing'}
    if c['state'] != 'committed':
        return {'decision': 'power_commitment_not_launchable', 'state': c['state']}
    if c['wave'] and c['wave'] != wave_row['id']:
        return {'decision': 'power_commitment_mismatch'}
    if c['service'] != wave_row['service'] or c['generation'] != wave_row['generation']:
        return {'decision': 'power_commitment_mismatch'}
    at = payload['at']
    feed = one(db, 'SELECT * FROM rack_feeds WHERE id=?', (c['feed'],))
    if not feed or feed['state'] != 'active':
        return {'decision': 'power_path_unavailable', 'feed': c['feed']}
    topo = _topology(db, c['feed'])
    if topo and not (topo['available_from'] <= at < topo['available_until']):
        return {'decision': 'outside_availability_window', 'resource': f'feed:{c["feed"]}'}
    if c['segment']:
        seg = _segment(db, c['segment'])
        if not seg or seg['state'] != 'active':
            return {'decision': 'power_path_unavailable', 'segment': c['segment']}
        if not (seg['available_from'] <= at < seg['available_until']):
            return {'decision': 'outside_availability_window', 'resource': f'segment:{c["segment"]}'}
    candidate = _commitment_item(c, start_override=at)
    peak = worst_peak(db, extra=candidate, exclude_commitment=c['id'], at=at)
    violations = peak_violations(db, peak)
    if violations or peak['unserved']:
        return {'decision': 'power_peak_exceeded',
                'worst_peak_kw': peak['overall_peak_kw'], 'peak_at': peak['at'],
                'feed_peaks': peak['feed_peaks'], 'segment_peaks': peak['segment_peaks'],
                'violations': violations, 'unserved': peak['unserved']}
    return {'decision': 'launch_authorized', 'commitment': cid,
            'worst_peak_kw': peak['overall_peak_kw'], 'peak_at': peak['at']}


def bind_launch(db, cid, attempt_id, at):
    db.execute("""UPDATE power_commitments SET state='bound', attempt=?, bound_at=?
                  WHERE id=?""", (attempt_id, at, cid))


def energize_attempt(db, attempt_id, at):
    """ready.publish 成功后调用：启动回执到位，承诺转为运行期受保护负载。"""
    db.execute("""UPDATE power_commitments SET state='energized', ready_at=?
                  WHERE attempt=? AND state='bound'""", (at, attempt_id))


# ---------------------------------------------------------------------------
# 命令处理
# ---------------------------------------------------------------------------

def handle(db, x):
    op = x['op']
    # 常见命名别名，保持入口词汇宽松。
    if op in ('ups', 'register_ups'):
        op = 'segment'
    elif op == 'startup_profile':
        op = 'profile'
    elif op == 'measure':
        op = 'measurement'
    elif op in ('ups_failure', 'ups_fault', 'ups_outage', 'segment_fault'):
        op = 'segment_outage'
    elif op in ('feed_failure', 'feed_fault', 'feed_outage_event'):
        op = 'feed_outage'
    elif op in ('switch', 'feed_switch'):
        op = 'switch_feed'
    elif op == 'release':
        op = 'clear'
    elif op in ('peak_report', 'report'):
        op = 'peak'
    elif op == 'costs':
        op = 'cost_report'

    # ---- 基础馈线（原有行为，决策字符串不得改变） ----
    if op == 'feed':
        if x['rated_kw'] < 1:
            return {'decision': 'invalid_feed'}
        db.execute('INSERT INTO rack_feeds VALUES(?,?,?,?,?)',
                   (x['id'], x['rack'], x['rated_kw'], x['revision'], 'active'))
        return {'decision': 'feed_recorded'}

    if op == 'reserve':
        feed = one(db, 'SELECT * FROM rack_feeds WHERE id=?', (x['feed'],))
        if not feed or feed['state'] != 'active' or x['kw'] < 1:
            return {'decision': 'feed_unavailable'}
        used = one(db, "SELECT COALESCE(SUM(kw),0) kw FROM power_reservations WHERE feed=? AND state='active'", (x['feed'],))['kw']
        if used + x['kw'] > feed['rated_kw']:
            return {'decision': 'power_full'}
        db.execute('INSERT INTO power_reservations VALUES(?,?,?,?,?)',
                   (x['id'], x['feed'], x['node'], x['kw'], 'active'))
        return {'decision': 'power_reserved'}

    if op == 'observe':
        feed = one(db, 'SELECT * FROM rack_feeds WHERE id=?', (x['feed'],))
        if not feed:
            return {'decision': 'feed_missing'}
        db.execute('INSERT INTO power_observations VALUES(?,?,?,?)',
                   (x['id'], x['feed'], x['at'], x['measured_kw']))
        return {'decision': 'power_observed'}

    # ---- 共享段 / UPS ----
    if op == 'segment':
        rated = x.get('rated_kw')
        if not x.get('id') or not isinstance(rated, int) or rated < 1:
            return {'decision': 'invalid_segment'}
        revision = int(x.get('revision', 1))
        old = _segment(db, x['id'])
        if old and revision <= old['revision']:
            return {'decision': 'stale_segment'}
        db.execute("""INSERT INTO power_segments(id,kind,rated_kw,margin_kw,sensor_generation,
                        evidence_required,fresh_ticks,available_from,available_until,state,revision)
                      VALUES(?,?,?,?,?,?,?,?,?,'active',?)
                      ON CONFLICT(id) DO UPDATE SET kind=excluded.kind,rated_kw=excluded.rated_kw,
                        margin_kw=excluded.margin_kw,sensor_generation=excluded.sensor_generation,
                        evidence_required=excluded.evidence_required,fresh_ticks=excluded.fresh_ticks,
                        available_from=excluded.available_from,available_until=excluded.available_until,
                        revision=excluded.revision""",
                   (x['id'], x.get('kind', 'ups'), rated, int(x.get('margin_kw', 0)),
                    int(x.get('sensor_generation', 1)), int(bool(x.get('evidence_required', False))),
                    x.get('fresh_ticks'), int(x.get('available_from', x.get('from', 0))),
                    int(x.get('available_until', x.get('until', WINDOW_END))), revision))
        return {'decision': 'segment_recorded'}

    # ---- 馈线拓扑 ----
    if op == 'topology':
        feed = one(db, 'SELECT * FROM rack_feeds WHERE id=?', (x.get('feed'),))
        if not feed:
            return {'decision': 'feed_missing'}
        segment_id = x.get('segment')
        if segment_id and not _segment(db, segment_id):
            return {'decision': 'segment_missing'}
        backup_feed = x.get('backup_feed')
        if backup_feed and not one(db, 'SELECT 1 FROM rack_feeds WHERE id=?', (backup_feed,)):
            return {'decision': 'feed_missing', 'feed': backup_feed}
        backup_segment = x.get('backup_segment')
        if backup_segment and not _segment(db, backup_segment):
            return {'decision': 'segment_missing', 'segment': backup_segment}
        db.execute("""INSERT INTO feed_topology(feed,segment,backup_feed,backup_segment,margin_kw,
                        sensor_generation,evidence_required,fresh_ticks,available_from,available_until)
                      VALUES(?,?,?,?,?,?,?,?,?,?)
                      ON CONFLICT(feed) DO UPDATE SET segment=excluded.segment,
                        backup_feed=excluded.backup_feed,backup_segment=excluded.backup_segment,
                        margin_kw=excluded.margin_kw,sensor_generation=excluded.sensor_generation,
                        evidence_required=excluded.evidence_required,fresh_ticks=excluded.fresh_ticks,
                        available_from=excluded.available_from,available_until=excluded.available_until""",
                   (x['feed'], segment_id, backup_feed, backup_segment,
                    int(x.get('margin_kw', 0)), int(x.get('sensor_generation', 1)),
                    int(bool(x.get('evidence_required', False))), x.get('fresh_ticks'),
                    int(x.get('available_from', x.get('from', 0))),
                    int(x.get('available_until', x.get('until', WINDOW_END)))))
        return {'decision': 'topology_recorded'}

    # ---- 启动用电合同 ----
    if op == 'profile':
        service = x.get('service')
        values = {k: x.get(k) for k in ('inrush_kw', 'inrush_ticks', 'warm_kw',
                                        'warm_ticks', 'run_kw', 'generation')}
        if not service or any(not isinstance(values[k], int) or values[k] < 0
                              for k in ('inrush_kw', 'inrush_ticks', 'warm_kw',
                                        'warm_ticks', 'run_kw', 'generation')):
            return {'decision': 'invalid_profile'}
        old = one(db, 'SELECT generation FROM power_profiles WHERE service=?', (service,))
        if old and values['generation'] < old['generation']:
            return {'decision': 'stale_profile'}
        db.execute("""INSERT INTO power_profiles VALUES(?,?,?,?,?,?,?)
                      ON CONFLICT(service) DO UPDATE SET generation=excluded.generation,
                        inrush_kw=excluded.inrush_kw,inrush_ticks=excluded.inrush_ticks,
                        warm_kw=excluded.warm_kw,warm_ticks=excluded.warm_ticks,run_kw=excluded.run_kw""",
                   (service, values['generation'], values['inrush_kw'], values['inrush_ticks'],
                    values['warm_kw'], values['warm_ticks'], values['run_kw']))
        return {'decision': 'profile_recorded'}

    # ---- 既有生产负载登记（优先保障） ----
    if op == 'production':
        feed = one(db, 'SELECT * FROM rack_feeds WHERE id=?', (x.get('feed'),))
        if not feed:
            return {'decision': 'feed_missing'}
        topo = _topology(db, x['feed'])
        segment_id = x.get('segment') or (topo['segment'] if topo else None)
        if segment_id and not _segment(db, segment_id):
            return {'decision': 'segment_missing'}
        if not isinstance(x.get('run_kw'), int) or x['run_kw'] < 1:
            return {'decision': 'invalid_load'}
        db.execute("""INSERT INTO power_loads(id,service,node,feed,segment,run_kw,priority,
                        ready,state,at) VALUES(?,?,?,?,?,?,?,?,'protected',?)""",
                   (x['id'], x.get('service'), x.get('node'), x['feed'], segment_id,
                    x['run_kw'], int(x.get('priority', 100)), int(x.get('ready', True)),
                    x.get('at', 0)))
        # 同步进简单馈线预留账本，让旧 reserve 的累加也能看到既有负载。
        db.execute('INSERT INTO power_reservations VALUES(?,?,?,?,?)',
                   (x['id'], x['feed'], x.get('node', x['id']), x['run_kw'], 'active'))
        return {'decision': 'production_recorded'}

    # ---- 现场测量 ----
    if op == 'measurement':
        kind = x.get('target_kind')
        target = x.get('target')
        if not kind:
            if x.get('feed') is not None:
                kind, target = 'feed', x['feed']
            elif x.get('segment') is not None:
                kind, target = 'segment', x['segment']
        if kind == 'feed':
            if not one(db, 'SELECT 1 FROM rack_feeds WHERE id=?', (target,)):
                return {'decision': 'feed_missing'}
            topo = _topology(db, target)
            generation = x.get('generation', topo['sensor_generation'] if topo else 1)
        elif kind == 'segment':
            seg = _segment(db, target)
            if not seg:
                return {'decision': 'segment_missing'}
            generation = x.get('generation', seg['sensor_generation'])
        else:
            return {'decision': 'invalid_measurement_target'}
        if not isinstance(x.get('measured_kw'), int) or x['measured_kw'] < 0 or x.get('at') is None:
            return {'decision': 'invalid_measurement'}
        calibrated = bool(x.get('calibrated', True)) and x.get('quality', 'valid') == 'valid'
        recorded_at = x.get('recorded_at', x['at'])
        db.execute("""INSERT INTO power_metrics VALUES(?,?,?,?,?,?,?,?,?)""",
                   (x['id'], kind, target, int(generation), x['at'], recorded_at,
                    x['measured_kw'], int(calibrated),
                    'valid' if calibrated else x.get('quality', 'calibration_invalid')))
        ev = evidence_status(db, kind, target, recorded_at)
        return {'decision': 'measurement_recorded', 'evidence_status': ev['status']}

    # ---- 传感器校准换代 ----
    if op == 'calibrate':
        target = x.get('target')
        kind = x.get('target_kind')
        if not kind:
            if x.get('feed') is not None:
                kind, target = 'feed', x['feed']
            else:
                kind, target = 'segment', x.get('segment')
        generation = int(x['generation'])
        if kind == 'feed':
            topo = _topology(db, target)
            if not topo:
                return {'decision': 'topology_missing'}
            db.execute('UPDATE feed_topology SET sensor_generation=? WHERE feed=?',
                       (generation, target))
        elif kind == 'segment':
            if not _segment(db, target):
                return {'decision': 'segment_missing'}
            db.execute('UPDATE power_segments SET sensor_generation=? WHERE id=?',
                       (generation, target))
        else:
            return {'decision': 'invalid_measurement_target'}
        return {'decision': 'calibration_recorded', 'sensor_generation': generation}

    # ---- 电力承诺（规划态最坏峰值 + 证据代次） ----
    if op == 'commit':
        if one(db, 'SELECT 1 FROM power_commitments WHERE id=?', (x['id'],)):
            return {'decision': 'commitment_exists'}
        feed = one(db, 'SELECT * FROM rack_feeds WHERE id=?', (x.get('feed'),))
        if not feed or feed['state'] != 'active':
            return {'decision': 'feed_unavailable'}
        candidate = candidate_from_payload(db, x)
        if candidate is None:
            return {'decision': 'startup_profile_missing'}
        topo = _topology(db, x['feed'])
        segment_id = candidate['segment']
        at = x.get('at', candidate['start'])
        start_at = candidate['start']
        # 可用时段约束的是实际启动时刻；证据新鲜度约束决策时刻 at。
        if topo and not (topo['available_from'] <= start_at < topo['available_until']):
            return {'decision': 'outside_availability_window', 'resource': f'feed:{x["feed"]}'}
        if segment_id:
            seg = _segment(db, segment_id)
            if not seg or seg['state'] != 'active':
                return {'decision': 'segment_unavailable'}
            if not (seg['available_from'] <= start_at < seg['available_until']):
                return {'decision': 'outside_availability_window',
                        'resource': f'segment:{segment_id}'}
        # 证据要求：不得把额定值当成实测容量
        evidence, basis, verified = path_evidence(db, x['feed'], segment_id, at)
        required = bool(x.get('evidence_required',
                              (topo['evidence_required'] if topo else 0)
                              or (_segment(db, segment_id)['evidence_required']
                                  if segment_id and _segment(db, segment_id) else 0)))
        if required and not verified and not x.get('allow_nameplate', False):
            bad = next((status for status in evidence.values() if status != 'measured'),
                       'missing')
            decision = {'late': 'power_measurement_late',
                        'calibration': 'sensor_calibration_stale',
                        'stale': 'power_measurement_stale',
                        'missing': 'power_measurement_missing'}.get(bad, 'power_evidence_unverified')
            return {'decision': decision, 'evidence': evidence, 'basis': 'nameplate'}
        if not verified:
            basis = 'nameplate'
        peak = worst_peak(db, extra=candidate, at=at)
        violations = peak_violations(db, peak)
        if violations or peak['unserved']:
            return {'decision': 'power_peak_exceeded',
                    'worst_peak_kw': peak['overall_peak_kw'], 'peak_at': peak['at'],
                    'feed_peaks': peak['feed_peaks'], 'segment_peaks': peak['segment_peaks'],
                    'violations': violations, 'unserved': peak['unserved']}
        # 单故障下既有生产节点必须仍可保障。候选以运行期 kw + 较低优先级参与打包：
        # 高优先级既有节点先得容量；只有在运节点因容量不足被舍弃才构成保护缺口。
        candidate_steady = {'cid': candidate['cid'], 'service': candidate['service'],
                            'feed': candidate['feed'], 'segment': candidate['segment'],
                            'kw': candidate['run_kw'], 'priority': candidate['priority'],
                            'running': False, 'ref_kind': 'candidate',
                            'ref_id': x['id']}
        n1 = {}
        protection_breach = []
        redundancy_gaps = []
        scenarios = [{'fault': f'feed:{x["feed"]}', 'down_feeds': [x['feed']], 'down_segments': []}]
        if segment_id:
            scenarios.append({'fault': f'segment:{segment_id}', 'down_feeds': [],
                              'down_segments': [segment_id]})
        topo_backup = topo['backup_feed'] if topo else None
        if topo_backup:
            scenarios.append({'fault': f'feed:{topo_backup}', 'down_feeds': [topo_backup],
                              'down_segments': []})
        for scenario in scenarios:
            verdict = assess_contingency(db, at, scenario['down_feeds'],
                                         scenario['down_segments'], candidate_steady)
            n1[scenario['fault']] = {'degrade': verdict['degrade'], 'blocked': verdict['blocked']}
            for row in verdict['degrade']:
                if row.get('capacity_breach'):
                    protection_breach.append(row)
                else:
                    redundancy_gaps.append(row)
        if protection_breach and not x.get('accept_degradation', False):
            return {'decision': 'production_protection_breach',
                    'single_fault': n1, 'existing_services_at_risk': protection_breach}
        reserved_kw = max(candidate['inrush_kw'], candidate['warm_kw'], candidate['run_kw'])
        cost_amount = int(x.get('cost_amount', 0))
        cost_bearer = x.get('cost_bearer', 'customer' if cost_amount else None)
        db.execute("""INSERT INTO power_commitments(id,wave,service,generation,node,feed,segment,
                        arrangement,priority,inrush_kw,inrush_ticks,warm_kw,warm_ticks,run_kw,
                        start_at,deadline,state,basis,verified,worst_peak_kw,reserved_kw,bottleneck,
                        attempt,bound_at,ready_at,cost_amount,cost_bearer,created_at)
                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'committed',?,?,?,?,?,NULL,NULL,NULL,?,?,?)""",
                   (x['id'], x.get('wave'), x.get('service'), x.get('generation'), x.get('node'),
                    x['feed'], segment_id, x.get('arrangement', 'standard'),
                    int(x.get('priority', 50)),
                    candidate['inrush_kw'], candidate['inrush_ticks'],
                    candidate['warm_kw'], candidate['warm_ticks'], candidate['run_kw'],
                    candidate['start'], x.get('deadline'), basis, int(verified),
                    peak['overall_peak_kw'], reserved_kw,
                    max(peak['segment_peaks'], key=peak['segment_peaks'].get, default=None),
                    cost_amount, cost_bearer, at))
        db.execute('INSERT INTO power_reservations VALUES(?,?,?,?,?)',
                   (x['id'], x['feed'], x.get('node', x['id']), reserved_kw, 'active'))
        if cost_amount:
            db.execute("""INSERT INTO power_costs(id,arrangement,service,kind,amount,party,at,note)
                          VALUES(?,?,?,?,?,?,?,?)""",
                       (x['id'] + ':cost', x.get('arrangement', 'standard'), x.get('service'),
                        x.get('cost_kind', 'facility_rental'), cost_amount, cost_bearer, at,
                        x.get('cost_note')))
        return {'decision': 'power_committed', 'commitment': x['id'],
                'basis': basis, 'verified': bool(verified), 'evidence': evidence,
                'worst_peak_kw': peak['overall_peak_kw'], 'peak_at': peak['at'],
                'feed_peaks': peak['feed_peaks'], 'segment_peaks': peak['segment_peaks'],
                'single_fault': n1, 'redundancy_gaps': redundancy_gaps,
                'reserved_kw': reserved_kw}

    # ---- 登记与实际启动尝试的绑定（通常由 wave.launch 自动完成，也可直接登记） ----
    if op in ('bind', 'bind_launch'):
        c = one(db, 'SELECT * FROM power_commitments WHERE id=?', (x.get('commitment'),))
        if not c:
            return {'decision': 'power_commitment_missing'}
        if c['state'] != 'committed':
            return {'decision': 'power_commitment_not_launchable', 'state': c['state']}
        if not x.get('attempt'):
            return {'decision': 'attempt_ref_required'}
        bind_launch(db, c['id'], x['attempt'], x.get('at', c['start_at']))
        return {'decision': 'commitment_bound', 'commitment': c['id'],
                'attempt': x['attempt'], 'state': 'bound'}

    # ---- 用实际就绪回执绑定/转运行期（也可由 ready.publish 自动触发） ----
    if op == 'receipt':
        c = one(db, 'SELECT * FROM power_commitments WHERE id=?', (x.get('commitment'),))
        if not c:
            return {'decision': 'power_commitment_missing'}
        if c['state'] != 'bound':
            return {'decision': 'commitment_not_bound', 'state': c['state']}
        receipt_id = x.get('receipt')
        if receipt_id:
            r = one(db, 'SELECT * FROM readiness_receipts WHERE id=?', (receipt_id,))
            if not r or r['status'] != 'published' or r['attempt'] != c['attempt']:
                return {'decision': 'receipt_unavailable'}
            at = x.get('at', r['published_at'])
        else:
            at = x['at']
        energize_attempt(db, c['attempt'], at)
        return {'decision': 'receipt_bound', 'state': 'energized', 'ready_at': at}

    # ---- 测量 + 现场确认后的清偿 ----
    if op == 'clear':
        c = one(db, 'SELECT * FROM power_commitments WHERE id=?', (x.get('commitment'),))
        if not c:
            return {'decision': 'power_commitment_missing'}
        if c['state'] in ('cleared',):
            return {'decision': 'already_cleared'}
        if c['state'] not in ('bound', 'degraded', 'blocked'):
            return {'decision': 'clearance_not_allowed', 'state': c['state']}
        # 绑定的启动尝试仍在 running 时不能清偿。
        if c['attempt']:
            a = one(db, 'SELECT status FROM start_attempts WHERE id=?', (c['attempt'],))
            if a and a['status'] == 'running':
                return {'decision': 'attempt_still_running', 'attempt': c['attempt']}
        measurement_id = x.get('measurement')
        if not measurement_id:
            return {'decision': 'clearance_measurement_required'}
        m = one(db, 'SELECT * FROM power_metrics WHERE id=?', (measurement_id,))
        if not m:
            return {'decision': 'clearance_measurement_required'}
        if m['target'] != c['feed'] and m['target'] != c['segment']:
            return {'decision': 'measurement_target_mismatch'}
        ev = evidence_status(db, m['target_kind'], m['target'], x['at'])
        if ev['status'] != 'measured' or ev['metric']['id'] != m['id']:
            return {'decision': {
                'late': 'power_measurement_late',
                'calibration': 'sensor_calibration_stale',
                'stale': 'power_measurement_stale'}.get(ev['status'], 'clearance_measurement_required'),
                'evidence_status': ev['status']}
        if not x.get('field_ref') or not x.get('confirmed_by'):
            return {'decision': 'field_confirmation_required'}
        # 残余负载必须回落到基线：同馈线上其余受保护负载 + 其他运行期承诺。
        baseline = 0
        for load in many(db, "SELECT * FROM power_loads WHERE feed=? AND state='protected' AND id<>?",
                         (c['feed'], c['id'])):
            baseline += load['run_kw']
        for other in many(db, "SELECT * FROM power_commitments WHERE feed=? AND state='energized' AND id<>?",
                          (c['feed'], c['id'])):
            baseline += other['run_kw']
        allowed_baseline = int(x.get('baseline_kw', baseline)) + int(x.get('tolerance_kw', 0))
        if m['measured_kw'] > allowed_baseline:
            return {'decision': 'load_not_cleared', 'measured_kw': m['measured_kw'],
                    'baseline_kw': allowed_baseline}
        db.execute("""INSERT INTO power_clearances VALUES(?,?,?,?,?,?,?,?,?)""",
                   (x['id'], c['id'], m['id'], x['field_ref'], x['confirmed_by'], x['at'],
                    m['measured_kw'], allowed_baseline, 'cleared'))
        db.execute("UPDATE power_commitments SET state='cleared' WHERE id=?", (c['id'],))
        db.execute("UPDATE power_reservations SET state='cleared' WHERE id=?", (c['id'],))
        return {'decision': 'power_cleared', 'commitment': c['id'],
                'measured_kw': m['measured_kw'], 'baseline_kw': allowed_baseline,
                'field_ref': x['field_ref']}

    # ---- 馈线切换 / 馈线故障 ----
    if op in ('feed_outage', 'switch_feed'):
        fid = x.get('feed')
        feed = one(db, 'SELECT * FROM rack_feeds WHERE id=?', (fid,))
        if not feed:
            return {'decision': 'feed_missing'}
        cause = 'feed_switch' if op == 'switch_feed' or x.get('planned') else 'feed_failure'
        new_state = 'switched' if cause == 'feed_switch' else 'failed'
        to_feed = x.get('to_feed')
        if to_feed:
            if not one(db, 'SELECT 1 FROM rack_feeds WHERE id=?', (to_feed,)):
                return {'decision': 'feed_missing', 'feed': to_feed}
            db.execute("""INSERT INTO feed_topology(feed,backup_feed) VALUES(?,?)
                          ON CONFLICT(feed) DO UPDATE SET backup_feed=excluded.backup_feed""",
                       (fid, to_feed))
        db.execute("UPDATE rack_feeds SET state=? WHERE id=?", (new_state, fid))
        verdict = assess_contingency(db, x.get('at', 0), [fid], [])
        _persist_directives(db, x['event_id'], cause, x.get('at', 0), verdict)
        return {'decision': 'feed_switched' if cause == 'feed_switch' else 'feed_failure_recorded',
                'feed': fid, 'state': new_state, **_verdict_summary(verdict)}

    # ---- 一次 UPS / 共享段失效 ----
    if op == 'segment_outage':
        sid = x.get('segment')
        if not _segment(db, sid):
            return {'decision': 'segment_missing'}
        db.execute("UPDATE power_segments SET state='failed' WHERE id=?", (sid,))
        verdict = assess_contingency(db, x.get('at', 0), [], [sid])
        _persist_directives(db, x['event_id'], 'segment_failure', x.get('at', 0), verdict)
        return {'decision': 'ups_failure_recorded', 'segment': sid,
                **_verdict_summary(verdict)}

    # ---- 恢复 ----
    if op == 'restore':
        kind = x.get('target_kind', 'feed')
        target = x.get('target') or x.get('feed') or x.get('segment')
        if kind == 'feed' or x.get('feed'):
            db.execute("UPDATE rack_feeds SET state='active' WHERE id=?", (target,))
        else:
            db.execute("UPDATE power_segments SET state='active' WHERE id=?", (target,))
        return {'decision': 'power_restored', 'target': target}

    # ---- 处置指令执行（降级 / 阻止启动） ----
    if op == 'apply_directive':
        d = one(db, 'SELECT * FROM power_directives WHERE id=?', (x.get('id'),))
        if not d:
            return {'decision': 'directive_missing'}
        if d['state'] != 'active':
            return {'decision': 'directive_not_active', 'state': d['state']}
        detail = json.loads(d['detail']) if d['detail'] else {}
        service = detail.get('service')
        if d['kind'] == 'degrade':
            db.execute("UPDATE power_loads SET state='degraded' WHERE service=? AND state='protected'",
                       (service,))
            db.execute("""UPDATE power_commitments SET state='degraded'
                          WHERE service=? AND state IN ('committed','bound','energized')""",
                       (service,))
        elif d['kind'] == 'block_start':
            cid = detail.get('ref_id') or detail.get('id')
            db.execute("UPDATE power_commitments SET state='blocked' WHERE id=? AND state='committed'",
                       (cid,))
            db.execute("UPDATE power_reservations SET state='blocked' WHERE id=? AND state='active'",
                       (cid,))
        db.execute("UPDATE power_directives SET state='applied' WHERE id=?", (d['id'],))
        return {'decision': 'directive_applied', 'directive': d['id'], 'kind': d['kind']}

    if op == 'directives':
        rows = many(db, 'SELECT * FROM power_directives ORDER BY id')
        return {'decision': 'directives_listed',
                'directives': [dict(r, detail=json.loads(r['detail']) if r['detail'] else None)
                               for r in rows]}

    # ---- 按需评估 ----
    if op == 'assess':
        return full_assessment(db, x.get('at', 0), x.get('down_feeds', ()),
                               x.get('down_segments', ()))

    if op == 'peak':
        peak = worst_peak(db, at=x.get('at', 0))
        return {'decision': 'power_peak_report',
                'worst_peak_kw': peak['overall_peak_kw'], 'peak_at': peak['at'],
                'feed_peaks': peak['feed_peaks'], 'segment_peaks': peak['segment_peaks'],
                'violations': peak_violations(db, peak)}

    if op == 'availability':
        return {'decision': 'power_availability', 'services': availability(db)}

    # ---- 费用 ----
    if op == 'cost':
        db.execute("INSERT INTO power_costs VALUES(?,?,?,?,?,?,?,?)",
                   (x['id'], x.get('arrangement', 'standard'), x.get('service'),
                    x.get('kind', 'facility_rental'), int(x['amount']),
                    x.get('party', 'customer'), x.get('at', 0), x.get('note')))
        return {'decision': 'cost_recorded', 'amount': int(x['amount']),
                'party': x.get('party', 'customer')}

    if op == 'cost_report':
        arrangement = x.get('arrangement')
        if arrangement:
            rows = many(db, 'SELECT * FROM power_costs WHERE arrangement=? ORDER BY id',
                        (arrangement,))
        else:
            rows = many(db, 'SELECT * FROM power_costs ORDER BY id')
        totals = {}
        for r in rows:
            totals[r['party']] = totals.get(r['party'], 0) + r['amount']
        return {'decision': 'cost_report', 'arrangement': arrangement,
                'costs': [dict(r) for r in rows], 'totals_by_party': totals,
                'customer_total': totals.get('customer', 0)}

    # ---- 跨机架回退安排 + 实测 ----
    if op == 'fallback':
        action = x.get('action', 'plan')
        fid = x['id']
        if action == 'plan':
            db.execute("INSERT INTO power_fallbacks VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                       (fid, x['service'], x.get('owner', 'unassigned'),
                        x.get('from_rack'), x.get('from_feed'), x.get('from_segment'),
                        x.get('to_rack'), x.get('to_feed'), x.get('to_segment'),
                        'planned', None, x.get('detail')))
            return {'decision': 'fallback_recorded', 'owner': x.get('owner', 'unassigned')}
        row = one(db, 'SELECT * FROM power_fallbacks WHERE id=?', (fid,))
        if not row:
            return {'decision': 'fallback_missing'}
        if action == 'test':
            # 实测目标路径此刻能否承载该服务的运行期用电（用登记 profile 或显式 kw）
            kw = x.get('run_kw')
            if kw is None:
                p = one(db, 'SELECT run_kw FROM power_profiles WHERE service=?', (row['service'],))
                kw = p['run_kw'] if p else None
            if kw is None:
                return {'decision': 'fallback_test_inconclusive'}
            feed_id = x.get('to_feed', row['to_feed'])
            limit, state = _feed_limit(db, feed_id)
            used = one(db, "SELECT COALESCE(SUM(run_kw),0) kw FROM power_loads WHERE feed=? AND state='protected'",
                       (feed_id,))['kw']
            used += one(db, "SELECT COALESCE(SUM(run_kw),0) kw FROM power_commitments WHERE feed=? AND state='energized'",
                        (feed_id,))['kw']
            if limit is None or state != 'active' or used + kw > limit:
                return {'decision': 'fallback_infeasible', 'feed': feed_id,
                        'used_kw': used, 'need_kw': kw, 'limit_kw': limit}
            db.execute("UPDATE power_fallbacks SET status='tested',tested_at=? WHERE id=?",
                       (x['at'], fid))
            return {'decision': 'fallback_tested', 'fallback': fid, 'feed': feed_id,
                    'headroom_kw': limit - used - kw}
        if action == 'accept':
            db.execute("UPDATE power_fallbacks SET status='accepted' WHERE id=?", (fid,))
            return {'decision': 'fallback_accepted', 'fallback': fid,
                    'owner': row['owner']}
        return {'decision': 'unknown_fallback_action'}

    return {'decision': 'unknown_operation'}


def _verdict_summary(verdict):
    return {'degrade': [r['service'] for r in verdict['degrade']],
            'blocked': [r['id'] for r in verdict['blocked']],
            'transfer': [r['service'] for r in verdict['transfer']],
            'feed_load_kw': verdict['feed_load_kw'],
            'segment_load_kw': verdict['segment_load_kw']}


def _persist_directives(db, event_id, cause, at, verdict):
    for row in verdict['degrade']:
        did = f'{event_id}:degrade:{row["id"]}'
        db.execute("INSERT OR IGNORE INTO power_directives VALUES(?,?,?,?,?,?,?,?)",
                   (did, 'degrade', row['service'], row['ref_kind'], cause, at, 'active',
                    _dump(row)))
    for row in verdict['blocked']:
        did = f'{event_id}:block:{row["id"]}'
        db.execute("INSERT OR IGNORE INTO power_directives VALUES(?,?,?,?,?,?,?,?)",
                   (did, 'block_start', row['service'], row['ref_kind'], cause, at, 'active',
                    _dump(row)))
    for row in verdict['transfer']:
        did = f'{event_id}:transfer:{row["id"]}'
        db.execute("INSERT OR IGNORE INTO power_directives VALUES(?,?,?,?,?,?,?,?)",
                   (did, 'transfer', row['service'], row['ref_kind'], cause, at, 'active',
                    _dump(row)))


def availability(db):
    """客户既有可用性：受保护负载/已就绪承诺 = available；被降级 = degraded。"""
    services = {}
    for load in many(db, 'SELECT service,state,priority FROM power_loads ORDER BY service'):
        services.setdefault(load['service'], {'service': load['service'], 'state': load['state']})
        if load['state'] == 'protected':
            services[load['service']]['state'] = 'available'
    for c in many(db, "SELECT service,state FROM power_commitments WHERE state IN ('energized','degraded') ORDER BY service"):
        current = services.get(c['service'], {'service': c['service'], 'state': 'available'})
        if c['state'] == 'degraded':
            current['state'] = 'degraded'
        services[c['service']] = current
    return sorted(services.values(), key=lambda r: r['service'])


def run(db, x):
    return act(db, 'power', x, handle)
