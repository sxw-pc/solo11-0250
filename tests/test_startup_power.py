"""冷启动波次的可证实电力承诺验收。

覆盖验收维度：
- 预留（power_commitments / power_reservations）
- 波次实际并发（start_attempts 的真实 started，而非计划槽位）
- 冲击峰值（inrush / warm / run 三段曲线按重叠时刻核算）
- 现场测量（测量代次、校准失效、观测迟到，额定值不得冒充实测）
- 馈线切换 / 馈线故障、一次 UPS 失效的不同后果
- 已有服务可用性（生产优先、降级指令）
- 两种客户安排的可用功能时刻与费用承担（错峰本地 0 费用 vs 外部独立 UPS 租用）
- 跨机架依赖撤销与记名回退责任的实测
"""

import json
import tempfile
import unittest
from pathlib import Path

from cloudcourt import MODULES, power
from cloudcourt.store import connect, many, one, replay

FIXTURE_DIR = Path(__file__).resolve().parents[1] / 'fixtures'


def run_fixture(db, name):
    """重放裸 power 命令数组；命令内可选 expect_decision 做逐步验收。"""
    commands = json.loads((FIXTURE_DIR / name).read_text(encoding='utf-8'))
    results = []
    for command in commands:
        result = power.run(db, command)
        expected = command.get('expect_decision')
        if expected is not None:
            assert result['decision'] == expected, (
                command.get('event_id'), expected, result['decision'], result)
        results.append(result)
    return results


def inrush_concurrency(db):
    """按实际 started 统计冲击窗口半开重叠的最大并发数。"""
    rows = many(db, """SELECT a.started AS started, c.inrush_ticks AS ticks
                       FROM power_commitments c JOIN start_attempts a ON a.id = c.attempt
                       WHERE c.attempt IS NOT NULL""")
    intervals = [(r['started'], r['started'] + (r['ticks'] or 0))
                 for r in rows if (r['ticks'] or 0) > 0]
    max_overlap = 0
    for tick in sorted({point for start, end in intervals for point in (start, end)}):
        max_overlap = max(max_overlap, sum(start <= tick < end for start, end in intervals))
    return max_overlap, intervals


def launch_starts(db):
    return [r['started'] for r in many(
        db, "SELECT started FROM start_attempts ORDER BY started, id")]


class PowerWorld(unittest.TestCase):
    def setUp(self):
        self.db = connect(':memory:')

    def tearDown(self):
        self.db.close()

    def runm(self, module, payload):
        return MODULES[module].run(self.db, payload)

    def build_world(self, evidence=True, feed_rated=6, ups_rated=12, ups_margin=0,
                   query=False):
        db = self.db
        for svc in ('database', 'api', 'index') + (('query',) if query else ()):
            self.runm('contract', {'event_id': f'rev-{svc}', 'op': 'revision',
                                   'id': svc, 'generation': 1})
        self.runm('contract', {'event_id': 'edge-api-db', 'op': 'edge', 'service': 'api',
                               'dependency': 'database', 'critical': True, 'revision': 1})
        self.runm('contract', {'event_id': 'edge-idx-db', 'op': 'edge', 'service': 'index',
                               'dependency': 'database', 'critical': True, 'revision': 1})
        if query:
            self.runm('contract', {'event_id': 'edge-q-api', 'op': 'edge', 'service': 'query',
                                    'dependency': 'api', 'critical': True, 'revision': 1})
            self.runm('contract', {'event_id': 'edge-q-idx', 'op': 'edge', 'service': 'query',
                                    'dependency': 'index', 'critical': True, 'revision': 1})
        for svc in ('database', 'api', 'index') + (('query',) if query else ()):
            self.runm('contract', {'event_id': f'seal-{svc}', 'op': 'seal',
                                   'id': svc, 'generation': 1})
            self.runm('contract', {'event_id': f'token-{svc}', 'op': 'token',
                                   'id': f'token-{svc}', 'service': svc, 'generation': 1,
                                   'expires': 100, 'at': 10})
            self.runm('wave', {'event_id': f'slot-{svc}', 'op': 'slot', 'id': f'slot-{svc}',
                               'node': f'node-{svc}', 'capacity': 1, 'generation': 1,
                               'healthy': True})
        power.run(db, {'event_id': 'ups', 'op': 'segment', 'id': 'ups-main',
                       'rated_kw': ups_rated, 'margin_kw': ups_margin,
                       'evidence_required': evidence})
        power.run(db, {'event_id': 'feed-a', 'op': 'feed', 'id': 'feed-a',
                       'rack': 'rack-shared', 'rated_kw': feed_rated, 'revision': 1})
        power.run(db, {'event_id': 'feed-b', 'op': 'feed', 'id': 'feed-b',
                       'rack': 'rack-shared', 'rated_kw': feed_rated, 'revision': 1})
        power.run(db, {'event_id': 'topo-a', 'op': 'topology', 'feed': 'feed-a',
                       'segment': 'ups-main', 'backup_feed': 'feed-b'})
        power.run(db, {'event_id': 'topo-b', 'op': 'topology', 'feed': 'feed-b',
                       'segment': 'ups-main', 'backup_feed': 'feed-a'})
        if evidence:
            for eid, target, kw in (('meas-a', 'feed-a', 5), ('meas-b', 'feed-b', 0),
                                    ('meas-u', 'segment:ups-main', 5)):
                kind, tid = target.split(':') if ':' in target else ('feed', target)
                payload = {'event_id': eid, 'op': 'measurement', 'id': eid,
                           'target_kind': kind, 'target': tid, 'generation': 1,
                           'at': 8, 'measured_kw': kw}
                power.run(db, payload)
        power.run(db, {'event_id': 'load-db', 'op': 'production', 'id': 'load-db',
                       'service': 'database', 'node': 'node-db', 'feed': 'feed-a',
                       'run_kw': 5, 'priority': 100, 'at': 0})
        power.run(db, {'event_id': 'profile-api', 'op': 'profile', 'service': 'api',
                       'generation': 1, 'inrush_kw': 4, 'inrush_ticks': 2, 'warm_kw': 2,
                       'warm_ticks': 2, 'run_kw': 1})
        power.run(db, {'event_id': 'profile-idx', 'op': 'profile', 'service': 'index',
                       'generation': 1, 'inrush_kw': 4, 'inrush_ticks': 2, 'warm_kw': 1,
                       'warm_ticks': 2, 'run_kw': 1})
        # 既有数据库波次已完成并发布就绪。
        self.runm('wave', {'event_id': 'plan-db', 'op': 'plan', 'id': 'wave-db',
                           'service': 'database', 'generation': 1, 'deadline': 80})
        self.runm('wave', {'event_id': 'launch-db', 'op': 'launch', 'id': 'try-db',
                           'wave': 'wave-db', 'slot': 'slot-database', 'token': 'token-database',
                           'at': 10, 'expires': 60})
        self.runm('ready', {'event_id': 'dbp1', 'op': 'probe', 'id': 'db-p1',
                            'attempt': 'try-db', 'at': 11, 'healthy': True})
        self.runm('ready', {'event_id': 'dbp2', 'op': 'probe', 'id': 'db-p2',
                            'attempt': 'try-db', 'at': 12, 'healthy': True})
        self.runm('ready', {'event_id': 'dbpub', 'op': 'publish', 'id': 'receipt-db',
                            'attempt': 'try-db', 'at': 12})
        self.runm('wave', {'event_id': 'plan-api', 'op': 'plan', 'id': 'wave-api',
                           'service': 'api', 'generation': 1, 'deadline': 90})
        self.runm('wave', {'event_id': 'plan-idx', 'op': 'plan', 'id': 'wave-idx',
                           'service': 'index', 'generation': 1, 'deadline': 90})

    def commit(self, cid, service, start, feed='feed-b', arrangement='stagger-local', **extra):
        payload = {'event_id': f'ev-{cid}', 'op': 'commit', 'id': cid,
                   'wave': f'wave-{service if service != "index" else "idx"}',
                   'service': service, 'generation': 1, 'feed': feed,
                   'arrangement': arrangement, 'start_at': start, 'at': 15}
        payload.update(extra)
        return power.run(self.db, payload)

    def launch(self, attempt, wave, slot, token, at, commitment, expires=60):
        return self.runm('wave', {'event_id': f'ev-{attempt}', 'op': 'launch', 'id': attempt,
                                  'wave': wave, 'slot': slot, 'token': token, 'at': at,
                                  'expires': expires, 'power_commitment': commitment})

    def publish(self, service, attempt, receipt, start):
        self.runm('ready', {'event_id': f'{receipt}-p1', 'op': 'probe', 'id': f'{receipt}-p1',
                            'attempt': attempt, 'at': start, 'healthy': True})
        self.runm('ready', {'event_id': f'{receipt}-p2', 'op': 'probe', 'id': f'{receipt}-p2',
                            'attempt': attempt, 'at': start + 1, 'healthy': True})
        return self.runm('ready', {'event_id': f'{receipt}-pub', 'op': 'publish',
                                   'id': receipt, 'attempt': attempt, 'at': start + 2})


class FixtureAcceptance(unittest.TestCase):
    def test_normal_fixture_replays_and_ends_energized_with_zero_fee(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / 'normal.sqlite')
            db = connect(path)
            run_fixture(db, 'startup-power-normal.json')
            self.assertEqual(one(db, "SELECT state FROM power_commitments WHERE id='commit-api'")['state'],
                             'energized')
            self.assertEqual(one(db, "SELECT state FROM power_commitments WHERE id='commit-idx'")['state'],
                             'energized')
            cost = power.run(db, {'event_id': 'z-cost', 'op': 'cost_report',
                                  'arrangement': 'stagger-local'})
            self.assertEqual(cost['customer_total'], 0)
            self.assertTrue(replay(path)['verified'])
            db.close()

    def test_failure_fixture_replays_with_all_denials(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / 'failure.sqlite')
            db = connect(path)
            run_fixture(db, 'startup-power-failure.json')
            # 共享 UPS 失效后库在降级名单；只承诺未启动的 api/index 进阻止启动名单。
            directives = power.run(db, {'event_id': 'z-dir', 'op': 'directives'})['directives']
            degraded = {d['target'] for d in directives if d['kind'] == 'degrade'}
            blocked = {d['target'] for d in directives if d['kind'] == 'block_start'}
            self.assertIn('database', degraded)
            self.assertTrue({'api', 'index'} <= blocked)
            report = power.run(db, {'event_id': 'z-cost', 'op': 'cost_report',
                                    'arrangement': 'external-ups-rental'})
            self.assertEqual(report['customer_total'], 3)
            self.assertTrue(replay(path)['verified'])
            db.close()


class PeakAndConcurrency(PowerWorld):
    def test_individual_starts_pass_but_simultaneous_inrush_exceeds_shared_ups(self):
        self.build_world()
        api = self.commit('commit-api', 'api', 20)
        self.assertEqual(api['decision'], 'power_committed')
        self.assertEqual(api['worst_peak_kw'], 9)  # 数据库 5 + API 冲击 4
        same = self.commit('commit-idx-same', 'index', 20)
        self.assertEqual(same['decision'], 'power_peak_exceeded')
        self.assertEqual(same['worst_peak_kw'], 13)  # 5 + 4 + 4 越过 12 保护界限
        resources = {(v['resource'], v['id']): v['limit_kw'] for v in same['violations']
                     if 'limit_kw' in v}
        self.assertEqual(resources[('segment', 'ups-main')], 12)
        self.assertEqual(resources[('feed', 'feed-b')], 6)  # 共用段与馈线同时越限

    def test_staggered_wave_runs_actually_staggered_with_peak_11(self):
        self.build_world()
        self.assertEqual(self.commit('commit-api', 'api', 20)['decision'], 'power_committed')
        self.assertEqual(self.commit('commit-idx', 'index', 22)['decision'], 'power_committed')
        self.assertEqual(self.launch('try-api', 'wave-api', 'slot-api', 'token-api', 20,
                                     'commit-api')['decision'], 'launched')
        # 槽有空位、承诺的是 22 分启动；21 分抢启动会按实际时刻算出 13 峰值并被拦下。
        rush = self.launch('try-rush', 'wave-idx', 'slot-index', 'token-index', 21,
                           'commit-idx')
        self.assertEqual(rush['decision'], 'power_peak_exceeded')
        self.assertEqual(rush['worst_peak_kw'], 13)
        # 按合同错峰到 22 分才真正下发。
        ok = self.launch('try-idx', 'wave-idx', 'slot-index', 'token-index', 22,
                         'commit-idx')
        self.assertEqual(ok['decision'], 'launched')
        self.assertEqual(ok['power_worst_peak_kw'], 11)  # 5 + API 预热 2 + 索引冲击 4
        self.assertEqual(launch_starts(self.db), [10, 20, 22])
        max_inrush, intervals = inrush_concurrency(self.db)
        self.assertEqual(max_inrush, 1)  # 半开区间：API 冲击 [20,22) 不与索引 22 冲击重叠
        self.assertEqual(sorted(intervals), [(20, 22), (22, 24)])
        # 承诺已绑定实际尝试与启动时刻。
        bound = one(self.db, "SELECT attempt,bound_at,state FROM power_commitments WHERE id='commit-idx'")
        self.assertEqual((bound['attempt'], bound['bound_at'], bound['state']),
                         ('try-idx', 22, 'bound'))

    def test_truly_concurrent_same_ups_would_overlap_two_inrushes(self):
        self.build_world()
        # API 已承诺 20 分启动；索引若也压到 20 分，提交核算直接看到 13 峰值并拒绝。
        self.assertEqual(self.commit('commit-api', 'api', 20)['decision'], 'power_committed')
        concurrent = self.commit('commit-idx-20', 'index', 20)
        self.assertEqual(concurrent['decision'], 'power_peak_exceeded')
        self.assertEqual(concurrent['worst_peak_kw'], 13)
        self.assertEqual(concurrent['peak_at'], 20)
        # 引擎层：把索引作为 20 分额外候选，共享 UPS 段峰值即 5+4+4。
        candidate = power.candidate_from_payload(self.db, {
            'id': 'synthetic-idx', 'service': 'index', 'generation': 1,
            'feed': 'feed-b', 'start_at': 20})
        peak = power.worst_peak(self.db, extra=candidate)
        self.assertEqual(peak['segment_peaks']['ups-main'], 13)
        self.assertEqual(peak['at'], 20)


class EvidenceRules(PowerWorld):
    def test_late_and_calibration_invalid_never_become_measured_capacity(self):
        self.build_world(evidence=False)
        # 严格资源：没有任何测量 -> 不能给出可证实承诺。
        power.run(self.db, {'event_id': 'ups-s', 'op': 'segment', 'id': 'ups-strict',
                            'rated_kw': 12, 'evidence_required': True})
        power.run(self.db, {'event_id': 'feed-s', 'op': 'feed', 'id': 'feed-s',
                            'rack': 'rack-s', 'rated_kw': 6, 'revision': 1})
        power.run(self.db, {'event_id': 'topo-s', 'op': 'topology', 'feed': 'feed-s',
                            'segment': 'ups-strict', 'evidence_required': True})
        power.run(self.db, {'event_id': 'prof-side', 'op': 'profile', 'service': 'side',
                            'generation': 1, 'inrush_kw': 2, 'inrush_ticks': 1,
                            'warm_kw': 1, 'warm_ticks': 1, 'run_kw': 1})
        missing = power.run(self.db, {'event_id': 'c0', 'op': 'commit', 'id': 'c0',
                                      'service': 'side', 'generation': 1, 'feed': 'feed-s',
                                      'start_at': 50, 'at': 50})
        self.assertEqual(missing['decision'], 'power_measurement_missing')
        # 校准换代后，旧代次测量不能充当实测。
        power.run(self.db, {'event_id': 'cal', 'op': 'calibrate', 'feed': 'feed-s',
                            'target_kind': 'feed', 'target': 'feed-s', 'generation': 2})
        power.run(self.db, {'event_id': 'm-old', 'op': 'measurement', 'id': 'm-old',
                            'feed': 'feed-s', 'generation': 1, 'at': 40, 'measured_kw': 1})
        bad_cal = power.run(self.db, {'event_id': 'c1', 'op': 'commit', 'id': 'c1',
                                      'service': 'side', 'generation': 1, 'feed': 'feed-s',
                                      'start_at': 50, 'at': 50})
        self.assertEqual(bad_cal['decision'], 'sensor_calibration_stale')
        # 合格但迟到（决策时刻尚未到达）的观测同样不能用于承诺。
        power.run(self.db, {'event_id': 'm-late', 'op': 'measurement', 'id': 'm-late',
                            'feed': 'feed-s', 'generation': 2, 'at': 40,
                            'recorded_at': 100, 'measured_kw': 1})
        late = power.run(self.db, {'event_id': 'c2', 'op': 'commit', 'id': 'c2',
                                   'service': 'side', 'generation': 1, 'feed': 'feed-s',
                                   'start_at': 50, 'at': 50})
        self.assertEqual(late['decision'], 'power_measurement_late')
        # 明确允许额定值兜底时，结论必须自报 nameplate / verified=False，不得冒充实测。
        nameplate = power.run(self.db, {'event_id': 'c3', 'op': 'commit', 'id': 'c3',
                                        'service': 'side', 'generation': 1, 'feed': 'feed-s',
                                        'start_at': 50, 'at': 50, 'allow_nameplate': True})
        self.assertEqual(nameplate['decision'], 'power_committed')
        self.assertEqual(nameplate['basis'], 'nameplate')
        self.assertFalse(nameplate['verified'])

    def test_happy_path_commitment_is_evidence_backed(self):
        self.build_world(evidence=True)
        result = self.commit('commit-api', 'api', 20)
        self.assertEqual(result['basis'], 'measured')
        self.assertTrue(result['verified'])


class ClearanceAfterFailure(PowerWorld):
    def test_wave_timeout_alone_keeps_power_until_measured_field_confirmation(self):
        self.build_world()
        # 相邻 API 已启动就绪，其运行期 1 单位构成馈线基线的一部分。
        self.commit('commit-api', 'api', 20)
        self.launch('try-api', 'wave-api', 'slot-api', 'token-api', 20, 'commit-api')
        self.publish('api', 'try-api', 'receipt-api', 20)
        self.commit('commit-idx', 'index', 22)
        self.launch('try-idx', 'wave-idx', 'slot-index', 'token-index', 22, 'commit-idx')
        # 尝试仍 running 时禁止清偿。
        early = power.run(self.db, {'event_id': 'cl-running', 'op': 'clear',
                                    'id': 'cl-running', 'commitment': 'commit-idx', 'at': 30})
        self.assertEqual(early['decision'], 'attempt_still_running')
        timed = self.runm('wave', {'event_id': 'timeout-idx', 'op': 'timeout',
                                   'id': 'try-idx', 'at': 61})
        self.assertEqual(timed['decision'], 'timed_out')
        # wave.timeout 不释放电力：承诺与预留仍占用。
        self.assertEqual(one(self.db, "SELECT state FROM power_commitments WHERE id='commit-idx'")['state'],
                         'bound')
        self.assertEqual(one(self.db, "SELECT state FROM power_reservations WHERE id='commit-idx'")['state'],
                         'active')
        no_measurement = power.run(self.db, {'event_id': 'cl0', 'op': 'clear', 'id': 'cl0',
                                             'commitment': 'commit-idx', 'at': 62})
        self.assertEqual(no_measurement['decision'], 'clearance_measurement_required')
        # 只有现场确认、没有引用测量也不行。
        no_field = power.run(self.db, {'event_id': 'm-res', 'op': 'measurement', 'id': 'm-res',
                                        'feed': 'feed-b', 'generation': 1, 'at': 62,
                                        'measured_kw': 4})
        self.assertEqual(no_field['decision'], 'measurement_recorded')
        missing_field = power.run(self.db, {'event_id': 'cl1', 'op': 'clear', 'id': 'cl1',
                                            'commitment': 'commit-idx', 'measurement': 'm-res',
                                            'at': 62})
        self.assertEqual(missing_field['decision'], 'field_confirmation_required')
        # 残余 4 高于基线（API 运行期 1），不能清偿。
        residual = power.run(self.db, {'event_id': 'cl2', 'op': 'clear', 'id': 'cl2',
                                       'commitment': 'commit-idx', 'measurement': 'm-res',
                                       'field_ref': 'FIELD-9', 'confirmed_by': 'ops-oncall',
                                       'at': 62})
        self.assertEqual(residual['decision'], 'load_not_cleared')
        self.assertEqual(residual['baseline_kw'], 1)
        # 新测量证明回落到基线并经现场确认后才清偿。
        power.run(self.db, {'event_id': 'm-ok', 'op': 'measurement', 'id': 'm-ok',
                            'feed': 'feed-b', 'generation': 1, 'at': 63, 'measured_kw': 1})
        cleared = power.run(self.db, {'event_id': 'cl3', 'op': 'clear', 'id': 'cl3',
                                      'commitment': 'commit-idx', 'measurement': 'm-ok',
                                      'field_ref': 'FIELD-10', 'confirmed_by': 'ops-oncall',
                                      'at': 63})
        self.assertEqual(cleared['decision'], 'power_cleared')
        self.assertEqual(one(self.db, "SELECT state FROM power_commitments WHERE id='commit-idx'")['state'],
                         'cleared')
        self.assertEqual(one(self.db, "SELECT state FROM power_reservations WHERE id='commit-idx'")['state'],
                         'cleared')
        # 清偿后按 wave 既有 retry 重开波次，必须凭一份新的电力承诺才能再次 launch。
        reopened = self.runm('wave', {'event_id': 'retry-idx', 'op': 'retry', 'old': 'try-idx'})
        self.assertEqual(reopened['decision'], 'retry_opened')
        second = self.commit('commit-idx-2', 'index', 64, at=64)
        self.assertEqual(second['decision'], 'power_committed')
        relaunch = self.launch('try-idx-2', 'wave-idx', 'slot-index', 'token-index',
                               64, 'commit-idx-2', expires=80)
        self.assertEqual(relaunch['decision'], 'launched')
        rebound = one(self.db, "SELECT attempt,state FROM power_commitments WHERE id='commit-idx-2'")
        self.assertEqual((rebound['attempt'], rebound['state']), ('try-idx-2', 'bound'))
        # 旧承诺不因重试而复活。
        self.assertEqual(one(self.db, "SELECT state FROM power_commitments WHERE id='commit-idx'")['state'],
                         'cleared')


class FaultsAndAvailability(PowerWorld):
    def _energize_both(self):
        self.build_world()
        self.commit('commit-api', 'api', 20)
        self.launch('try-api', 'wave-api', 'slot-api', 'token-api', 20, 'commit-api')
        self.commit('commit-idx', 'index', 22)
        self.launch('try-idx', 'wave-idx', 'slot-index', 'token-index', 22, 'commit-idx')
        self.publish('api', 'try-api', 'receipt-api', 20)
        self.publish('index', 'try-idx', 'receipt-idx', 22)

    def test_feed_switch_sheds_index_but_keeps_database_and_api(self):
        self._energize_both()
        result = power.run(self.db, {'event_id': 'feedb-out', 'op': 'switch_feed',
                                     'feed': 'feed-b', 'planned': True, 'at': 30})
        self.assertEqual(result['decision'], 'feed_switched')
        self.assertEqual(result['degrade'], ['index'])
        # API 从 feed-b 转供到 feed-a；数据库本就在 feed-a，不受这条馈线切换影响。
        self.assertIn('api', result['transfer'])
        self.assertNotIn('database', result['degrade'])
        # 馈线已断开：新的启动/预留不能再卖这条路径。
        denied = self.commit('commit-more', 'index', 40)
        self.assertEqual(denied['decision'], 'feed_unavailable')
        # 执行降级指令后，可用性视图把 index 标为 degraded，数据库/API 仍 available。
        directive = one(self.db, "SELECT id FROM power_directives WHERE kind='degrade' AND target='index'")
        applied = power.run(self.db, {'event_id': 'apply-idx', 'op': 'apply_directive',
                                       'id': directive['id']})
        self.assertEqual(applied['decision'], 'directive_applied')
        availability = {r['service']: r['state'] for r in power.availability(self.db)}
        self.assertEqual(availability['database'], 'available')
        self.assertEqual(availability['api'], 'available')
        self.assertEqual(availability['index'], 'degraded')

    def test_ups_failure_is_distinct_from_feed_switch_and_drops_shared_services(self):
        self._energize_both()
        result = power.run(self.db, {'event_id': 'ups-out', 'op': 'segment_outage',
                                     'segment': 'ups-main', 'at': 31})
        self.assertEqual(result['decision'], 'ups_failure_recorded')
        self.assertEqual(set(result['degrade']), {'database', 'api', 'index'})
        # 与馈线故障不同：没有可改接路径，segment_load 为空且全部 unserved。
        assessment = power.run(self.db, {'event_id': 'assess-ups', 'op': 'assess',
                                         'at': 31, 'down_segments': ['ups-main']})
        self.assertTrue(all(row['reason'] == 'segment_lost'
                            for row in assessment['contingency']['degrade']))

    def test_protection_margin_makes_staggered_peak_fail_when_margin_reserved(self):
        self.build_world(ups_margin=2)  # 12 额定 - 2 保护余量 => 有效 10
        api = self.commit('commit-api', 'api', 20)
        self.assertEqual(api['decision'], 'power_committed')  # 5+4=9 <= 10
        idx = self.commit('commit-idx', 'index', 22)
        self.assertEqual(idx['decision'], 'power_peak_exceeded')  # 11 > 10
        ups_violation = next(v for v in idx['violations'] if v['id'] == 'ups-main')
        self.assertEqual(ups_violation['limit_kw'], 10)

    def test_availability_window_changes_when_start_may_run(self):
        self.build_world()
        # feed-b 只在 100..200 时段可用（拓扑 upsert 保留共享段与备用馈线）。
        power.run(self.db, {'event_id': 'topo-b-window', 'op': 'topology', 'feed': 'feed-b',
                            'segment': 'ups-main', 'backup_feed': 'feed-a',
                            'available_from': 100, 'available_until': 200})
        early = self.commit('commit-early', 'api', 20)
        self.assertEqual(early['decision'], 'outside_availability_window')
        night = self.commit('commit-night', 'api', 150)
        self.assertEqual(night['decision'], 'power_committed')

    def test_new_commit_cannot_outbid_database_single_fault_margin(self):
        self.build_world()
        power.run(self.db, {'event_id': 'profile-greedy', 'op': 'profile', 'service': 'greedy',
                            'generation': 1, 'inrush_kw': 2, 'inrush_ticks': 1,
                            'warm_kw': 2, 'warm_ticks': 1, 'run_kw': 2})
        # 高优先级候选在稳态打包中插到数据库之前；任一馈线失效后 5+2 越过 6 的转供限额，
        # 会把数据库挤掉 -> 提交被以 production_protection_breach 拒绝。
        breach = power.run(self.db, {'event_id': 'c-greedy', 'op': 'commit', 'id': 'c-greedy',
                                     'service': 'greedy', 'generation': 1, 'feed': 'feed-b',
                                     'start_at': 20, 'at': 15, 'priority': 200})
        self.assertEqual(breach['decision'], 'production_protection_breach')
        self.assertTrue(any(row['service'] == 'database'
                            for row in breach['existing_services_at_risk']))
        # 同负载按普通优先级（低于生产 100）时数据库受保护，承诺可成立但故障下自身被阻止。
        safe = power.run(self.db, {'event_id': 'c-modest', 'op': 'commit', 'id': 'c-modest',
                                   'service': 'greedy', 'generation': 1, 'feed': 'feed-b',
                                   'start_at': 20, 'at': 15, 'priority': 50})
        self.assertEqual(safe['decision'], 'power_committed')
        self.assertIn('feed:feed-a', safe['single_fault'])


class TwoArrangementsComparison(PowerWorld):
    """两种客户安排面对相同拓扑/测量/启动合同都合法，但功能时刻与费用不同。"""

    def _base(self):
        self.build_world()

    def run_local_arrangement(self):
        self._base()
        self.commit('commit-api', 'api', 20)
        self.launch('try-api', 'wave-api', 'slot-api', 'token-api', 20, 'commit-api')
        self.commit('commit-idx', 'index', 22)
        self.launch('try-idx', 'wave-idx', 'slot-index', 'token-index', 22, 'commit-idx')
        peak = power.worst_peak(self.db)  # 发布前：bound 曲线按实际 started 核算
        self.publish('api', 'try-api', 'receipt-api', 20)
        self.publish('index', 'try-idx', 'receipt-idx', 22)
        query_open = one(self.db, "SELECT MAX(published_at) t FROM readiness_receipts WHERE id IN ('receipt-api','receipt-idx')")['t']
        power.run(self.db, {'event_id': 'cost0', 'op': 'cost', 'id': 'cost0',
                            'arrangement': 'stagger-local', 'amount': 0,
                            'party': 'customer', 'at': 0})
        cost = power.run(self.db, {'event_id': 'costrep', 'op': 'cost_report',
                                   'arrangement': 'stagger-local'})['customer_total']
        return {
            'name': 'stagger-local', 'starts': launch_starts(self.db),
            'max_inrush_overlap': inrush_concurrency(self.db)[0],
            'peak_ups_kw': peak['segment_peaks']['ups-main'],
            'peak_total_kw': peak['overall_peak_kw'],
            'power_violations': power.peak_violations(self.db, peak),
            'query_open_at': query_open, 'customer_cost': cost,
            'reservations_bound': many(
                self.db,
                "SELECT COUNT(*) n FROM power_commitments WHERE attempt IS NOT NULL "
                "AND state='energized'")[0]['n']}

    def run_external_arrangement(self):
        # 相同的共享 UPS 拓扑与测量仍然存在，另建一条独立 UPS 的外部机架路径。
        self._base()
        power.run(self.db, {'event_id': 'ups-ext', 'op': 'segment', 'id': 'ups-ext',
                            'rated_kw': 8})
        power.run(self.db, {'event_id': 'feed-ext', 'op': 'feed', 'id': 'feed-ext',
                            'rack': 'rack-external', 'rated_kw': 6, 'revision': 1})
        power.run(self.db, {'event_id': 'topo-ext', 'op': 'topology', 'feed': 'feed-ext',
                            'segment': 'ups-ext'})
        power.run(self.db, {'event_id': 'meas-ext', 'op': 'measurement', 'id': 'meas-ext',
                            'feed': 'feed-ext', 'generation': 1, 'at': 8, 'measured_kw': 0})
        power.run(self.db, {'event_id': 'meas-uext', 'op': 'measurement', 'id': 'meas-uext',
                            'segment': 'ups-ext', 'generation': 1, 'at': 8, 'measured_kw': 0})
        self.commit('commit-api', 'api', 20)
        self.launch('try-api', 'wave-api', 'slot-api', 'token-api', 20, 'commit-api')
        # 索引 20 分与 API 同时启动，但落在独立 UPS：承诺合法。
        idx_ext = self.commit('commit-idx-ext', 'index', 20, feed='feed-ext',
                              arrangement='external-ups-rental')
        self.assertEqual(idx_ext['decision'], 'power_committed')
        self.launch('try-idx', 'wave-idx', 'slot-index', 'token-index', 20, 'commit-idx-ext')
        peak = power.worst_peak(self.db)
        self.assertEqual(power.peak_violations(self.db, peak), [])
        self.publish('api', 'try-api', 'receipt-api', 20)
        self.publish('index', 'try-idx', 'receipt-idx', 20)
        query_open = one(self.db, "SELECT MAX(published_at) t FROM readiness_receipts WHERE id IN ('receipt-api','receipt-idx')")['t']
        power.run(self.db, {'event_id': 'rent', 'op': 'cost', 'id': 'rent',
                            'arrangement': 'external-ups-rental',
                            'kind': 'external_rack_rental', 'amount': 3,
                            'party': 'customer', 'at': 20})
        cost = power.run(self.db, {'event_id': 'rentrep', 'op': 'cost_report',
                                   'arrangement': 'external-ups-rental'})['customer_total']
        return {
            'name': 'external-ups-rental', 'starts': launch_starts(self.db),
            'max_inrush_overlap': inrush_concurrency(self.db)[0],
            'peak_shared_ups_kw': peak['segment_peaks']['ups-main'],
            'peak_external_ups_kw': peak['segment_peaks']['ups-ext'],
            'power_violations': power.peak_violations(self.db, peak),
            'query_open_at': query_open, 'customer_cost': cost,
            'reservations_bound': many(
                self.db,
                "SELECT COUNT(*) n FROM power_commitments WHERE attempt IS NOT NULL "
                "AND state='energized'")[0]['n']}

    def test_local_arrangement_is_legal_on_its_own_world(self):
        result = self.run_local_arrangement()
        self.assertEqual(result['power_violations'], [])
        self.assertEqual(result['customer_cost'], 0)

    def test_external_arrangement_is_legal_on_its_own_world(self):
        result = self.run_external_arrangement()
        self.assertEqual(result['power_violations'], [])
        self.assertEqual(result['customer_cost'], 3)

    def test_arrangements_compare_on_every_acceptance_axis(self):
        # 两个独立世界共享同一套拓扑/测量/合同基线，分别跑两种客户安排后逐维比较。
        local, external = self._isolated_arrangements()
        # 预留：两条安排都有两个已绑定实际尝试并转为 energized 的电力承诺。
        self.assertEqual(local['reservations_bound'], 2)
        self.assertEqual(external['reservations_bound'], 2)
        # 实际并发：错峰只有 1 个冲击重叠；外部安排有 2 个实际并发冲击，但分属不同 UPS。
        self.assertEqual(local['max_inrush_overlap'], 1)
        self.assertEqual(external['max_inrush_overlap'], 2)
        # 冲击峰值：本地共享 UPS 最坏 11；外部安排共享 UPS 仅 9、外部 UPS 4，均不越限。
        self.assertEqual(local['peak_ups_kw'], 11)
        self.assertEqual(external['peak_shared_ups_kw'], 9)
        self.assertEqual(external['peak_external_ups_kw'], 4)
        self.assertEqual(local['power_violations'], [])
        self.assertEqual(external['power_violations'], [])
        # 可用功能时刻：外部租用让查询早 2 个 tick 开放。
        self.assertLess(external['query_open_at'], local['query_open_at'])
        self.assertEqual((local['query_open_at'], external['query_open_at']), (24, 22))
        # 费用承担：错峰 0 额外设施费；外部机架租金 3 由客户承担。
        self.assertEqual(local['customer_cost'], 0)
        self.assertEqual(external['customer_cost'], 3)

    def _isolated_arrangements(self):
        """用两个独立内存库各跑一条安排，避免同库状态串扰。"""
        results = []
        for builder in (self._run_local_on_fresh_db, self._run_external_on_fresh_db):
            old_db = self.db
            fresh = connect(':memory:')
            try:
                self.db = fresh
                results.append(builder())  # 安排方法内部会调用 _base()->build_world()
            finally:
                self.db.close()
                self.db = old_db
        local_result, external_result = results
        return local_result, external_result

    def _run_local_on_fresh_db(self):
        return self.run_local_arrangement()

    def _run_external_on_fresh_db(self):
        return self.run_external_arrangement()


class CrossRackFallback(PowerWorld):
    def test_cross_rack_dependency_revocation_and_owned_tested_fallback(self):
        self.build_world(query=True)
        # API 在共享机架本地启动就绪。
        self.commit('commit-api', 'api', 20)
        self.launch('try-api', 'wave-api', 'slot-api', 'token-api', 20, 'commit-api')
        self.publish('api', 'try-api', 'receipt-api', 20)
        # 索引在独立 UPS 的外部机架同时启动，较早就绪。
        power.run(self.db, {'event_id': 'ups-ext', 'op': 'segment', 'id': 'ups-ext', 'rated_kw': 8})
        power.run(self.db, {'event_id': 'feed-ext', 'op': 'feed', 'id': 'feed-ext',
                            'rack': 'rack-external', 'rated_kw': 6, 'revision': 1})
        power.run(self.db, {'event_id': 'topo-ext', 'op': 'topology', 'feed': 'feed-ext',
                            'segment': 'ups-ext'})
        self.commit('commit-idx-ext', 'index', 20, feed='feed-ext',
                    arrangement='external-ups-rental')
        self.launch('try-idx', 'wave-idx', 'slot-index', 'token-index', 20, 'commit-idx-ext')
        self.publish('index', 'try-idx', 'receipt-idx', 20)
        # 查询服务依赖 api + index，拿到就绪回执后发布。
        self.runm('wave', {'event_id': 'plan-query', 'op': 'plan', 'id': 'wave-query',
                           'service': 'query', 'generation': 1, 'deadline': 90})
        self.runm('wave', {'event_id': 'launch-query', 'op': 'launch', 'id': 'try-query',
                           'wave': 'wave-query', 'slot': 'slot-query',
                           'token': 'token-query', 'at': 23, 'expires': 70})
        published = self.publish('query', 'try-query', 'receipt-query', 24)
        self.assertEqual(published['decision'], 'published')
        # 外部 UPS 失效：电力侧把 index 列入降级。
        outage = power.run(self.db, {'event_id': 'ext-out', 'op': 'segment_outage',
                                     'segment': 'ups-ext', 'at': 40})
        self.assertIn('index', outage['degrade'])
        directive = one(self.db, "SELECT id FROM power_directives WHERE kind='degrade' AND target='index'")
        power.run(self.db, {'event_id': 'apply-ext', 'op': 'apply_directive',
                            'id': directive['id']})
        # 就绪侧撤销索引回执，跨机架依赖真实传播到 query。
        revoked = self.runm('ready', {'event_id': 'revoke-idx', 'op': 'revoke',
                                      'id': 'receipt-idx'})
        self.assertEqual(revoked['decision'], 'revoked')
        self.assertIn('query', revoked['children'])
        self.assertEqual(one(self.db, "SELECT status FROM readiness_receipts WHERE id='receipt-query'")['status'],
                         'revoked')
        # 回退安排必须记名，且目标本地路径容量经实测可承载索引。
        plan = power.run(self.db, {'event_id': 'fb-plan', 'op': 'fallback', 'id': 'fb-idx',
                                   'action': 'plan', 'service': 'index', 'owner': 'ops-oncall',
                                   'from_rack': 'rack-external', 'from_feed': 'feed-ext',
                                   'from_segment': 'ups-ext', 'to_rack': 'rack-shared',
                                   'to_feed': 'feed-b', 'to_segment': 'ups-main'})
        self.assertEqual(plan['decision'], 'fallback_recorded')
        tested = power.run(self.db, {'event_id': 'fb-test', 'op': 'fallback', 'id': 'fb-idx',
                                     'action': 'test', 'at': 41})
        self.assertEqual(tested['decision'], 'fallback_tested')
        self.assertGreaterEqual(tested['headroom_kw'], 0)
        accepted = power.run(self.db, {'event_id': 'fb-accept', 'op': 'fallback', 'id': 'fb-idx',
                                       'action': 'accept'})
        self.assertEqual(accepted['decision'], 'fallback_accepted')
        self.assertEqual(accepted['owner'], 'ops-oncall')


if __name__ == '__main__':
    unittest.main()
