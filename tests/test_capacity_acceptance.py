import json
import tempfile
import unittest
from pathlib import Path

from cloudcourt import MODULES, loadtest
from cloudcourt.store import connect, replay, one, many

FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures' / 'capacity-acceptance.json'


def run_fixture(db):
    decisions = []
    for step in json.loads(FIXTURE.read_text(encoding='utf-8')):
        decisions.append(MODULES[step['subsystem']].run(db, step['payload']))
    return decisions


def bootstrap(db, service='service-b', attempt='try-b', wave='wave-b', token='token-b'):
    """Seal service + upstream, publish upstream receipt, launch the cold-start attempt."""
    contract, wave_mod, ready_mod = MODULES['contract'], MODULES['wave'], MODULES['ready']
    contract.run(db, {'event_id': f'b-{service}-r1', 'op': 'revision', 'id': 'database', 'generation': 1})
    contract.run(db, {'event_id': f'b-{service}-r2', 'op': 'revision', 'id': service, 'generation': 1})
    contract.run(db, {'event_id': f'b-{service}-e', 'op': 'edge', 'service': service,
                      'dependency': 'database', 'critical': True, 'revision': 1})
    contract.run(db, {'event_id': f'b-{service}-s1', 'op': 'seal', 'id': 'database', 'generation': 1})
    contract.run(db, {'event_id': f'b-{service}-s2', 'op': 'seal', 'id': service, 'generation': 1})
    contract.run(db, {'event_id': f'b-{service}-t1', 'op': 'token', 'id': 'token-db',
                      'service': 'database', 'generation': 1, 'expires': 200, 'at': 10})
    contract.run(db, {'event_id': f'b-{service}-t2', 'op': 'token', 'id': token,
                      'service': service, 'generation': 1, 'expires': 200, 'at': 10})
    wave_mod.run(db, {'event_id': f'b-{service}-slot', 'op': 'slot', 'id': 'slot-a',
                      'node': 'node-a', 'capacity': 4, 'generation': 1, 'healthy': True})
    wave_mod.run(db, {'event_id': f'b-{service}-w1', 'op': 'plan', 'id': 'wave-db',
                      'service': 'database', 'generation': 1, 'deadline': 180})
    wave_mod.run(db, {'event_id': f'b-{service}-l1', 'op': 'launch', 'id': 'try-db',
                      'wave': 'wave-db', 'slot': 'slot-a', 'token': 'token-db', 'at': 20, 'expires': 80})
    ready_mod.run(db, {'event_id': f'b-{service}-p1', 'op': 'probe', 'id': 'p1',
                       'attempt': 'try-db', 'at': 22, 'healthy': True})
    ready_mod.run(db, {'event_id': f'b-{service}-p2', 'op': 'probe', 'id': 'p2',
                       'attempt': 'try-db', 'at': 23, 'healthy': True})
    ready_mod.run(db, {'event_id': f'b-{service}-pub', 'op': 'publish', 'id': 'receipt-db',
                       'attempt': 'try-db', 'at': 24})
    wave_mod.run(db, {'event_id': f'b-{service}-w2', 'op': 'plan', 'id': wave,
                      'service': service, 'generation': 1, 'deadline': 180})
    launched = wave_mod.run(db, {'event_id': f'b-{service}-l2', 'op': 'launch', 'id': attempt,
                                 'wave': wave, 'slot': 'slot-a', 'token': token, 'at': 35, 'expires': 90})
    assert launched['decision'] == 'launched', launched


def make_plan(db, plan='cp', service='service-b', attempt='try-b', generator='gen',
              dataset='sandbox', window=(40, 44), target=500, classes=None, arrivals=None):
    loadtest.run(db, {'event_id': f'{plan}-g', 'op': 'register_generator', 'id': generator,
                      'dataset': dataset, 'isolated': True, 'at': 36})
    if classes is None:
        classes = [
            {'class_id': 'premium', 'clients': 300, 'priority': 'high',
             'min_throughput_mrps': 1, 'max_p99_ms': 200, 'max_error_bp': 100},
            {'class_id': 'standard', 'clients': 200, 'priority': 'normal',
             'min_throughput_mrps': 1, 'max_p99_ms': 200, 'max_error_bp': 1000},
        ]
    if arrivals is None:
        arrivals = [{'at': 40 + i, 'class_id': 'premium', 'kind': 'business',
                     'request_id': f'r-p{i}'} for i in range(4)] + \
                   [{'at': 40 + i, 'class_id': 'standard', 'kind': 'business',
                     'request_id': f'r-s{i}'} for i in range(4)]
    return loadtest.run(db, {'event_id': f'{plan}-plan', 'op': 'capacity_plan', 'id': plan,
                             'service': service, 'generation': 1, 'attempt': attempt,
                             'generator': generator, 'dataset': dataset,
                             'window_start': window[0], 'window_end': window[1],
                             'target_clients': target, 'classes': classes, 'arrivals': arrivals})


def record(db, plan, rid, at, outcome='ok', latency=50, retries=0, generator='gen',
           dataset='sandbox', **extra):
    payload = {'event_id': f'{plan}-{rid}', 'op': 'request', 'id': rid, 'plan': plan,
               'generator': generator, 'dataset': dataset, 'at': at, 'issued': True,
               'outcome': outcome, 'latency_ms': latency, 'retries': retries}
    payload.update(extra)
    return loadtest.run(db, payload)


def guards(db, plan, cpu=55, mem=60):
    loadtest.run(db, {'event_id': f'{plan}-gd1', 'op': 'guard', 'plan': plan,
                      'metric': 'cpu_pct', 'observed': cpu, 'limit': 90})
    loadtest.run(db, {'event_id': f'{plan}-gd2', 'op': 'guard', 'plan': plan,
                      'metric': 'mem_pct', 'observed': mem, 'limit': 90})


class CapacityAcceptance(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'capacity.sqlite')
        self.db = connect(self.path)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_fixture_replays_with_missing_requests_and_partial_certificate(self):
        decisions = run_fixture(self.db)
        self.assertTrue(decisions)
        # The fixture deliberately omits timeline request req-s4: it stays in the raw denominator.
        report = one(self.db, 'SELECT * FROM trial_report_versions WHERE plan=? AND version=1', ('cp-b',))
        self.assertEqual(report['intended'], 8)
        self.assertEqual(report['responded'], 7)
        self.assertEqual(report['unsent'], 1)
        self.assertEqual(report['errors'], 1)          # the timed-out request
        self.assertEqual(report['retries'], 3)         # 1 + 2 client retries are visible
        self.assertEqual(report['state'], 'partial')
        self.assertEqual(report['unfulfilled_bp'], 4000)   # 200 of 500 clients
        # Per-tenant / per-priority results are separately visible.
        by_class = {r['class_id']: r for r in
                    many(self.db, 'SELECT * FROM trial_class_results WHERE plan=? AND version=1', ('cp-b',))}
        self.assertEqual(by_class['premium']['unsent'], 0)
        self.assertEqual(by_class['premium']['pass'], 1)
        self.assertEqual(by_class['standard']['unsent'], 1)
        self.assertEqual(by_class['standard']['errors'], 1)
        self.assertEqual(by_class['standard']['pass'], 0)
        # The fast empty request is counted but cannot enter the business score.
        self.assertEqual(report['empty'], 1)
        cert = one(self.db, "SELECT * FROM capacity_certificates WHERE id='cert-b-partial'")
        self.assertEqual(cert['tier'], 'partial')
        self.assertEqual(cert['capacity_clients'], 300)
        self.assertEqual(cert['unfulfilled_clients'], 200)
        self.assertEqual(cert['state'], 'signed')
        onboard = one(self.db, "SELECT * FROM customer_onboards WHERE id='onboard-b'")
        self.assertEqual(onboard['onboarded_clients'], 300)
        self.assertEqual(onboard['waitlisted_clients'], 200)
        self.assertEqual(onboard['waitlist_until'], 200)
        self.assertEqual(onboard['credits'], 500)
        self.assertTrue(replay(self.path)['verified'])

    def test_missing_requests_count_in_raw_denominator_not_success_only(self):
        bootstrap(self.db)
        make_plan(self.db)
        for rid, at in [('r-p0', 40), ('r-p1', 41), ('r-p2', 42), ('r-p3', 43)]:
            record(self.db, 'cp', rid, at)
        for rid, at in [('r-s0', 40), ('r-s1', 41)]:      # r-s2 and r-s3 are never sent
            record(self.db, 'cp', rid, at)
        guards(self.db, 'cp')
        result = loadtest.run(self.db, {'event_id': 'cp-accept', 'op': 'accept', 'plan': 'cp'})
        self.assertEqual(result['intended'], 8)
        self.assertEqual(result['unsent'], 2)
        # Every recorded response succeeded, yet the class fails: success-only scoring would pass it.
        self.assertEqual(result['errors'], 0)
        standard = one(self.db, "SELECT * FROM trial_class_results WHERE plan='cp' AND class_id='standard'")
        self.assertEqual(standard['intended'], 4)
        self.assertEqual(standard['unsent'], 2)
        self.assertEqual(standard['error_bp'], 5000)
        self.assertEqual(standard['pass'], 0)
        self.assertEqual(result['state'], 'partial')
        self.assertTrue(replay(self.path)['verified'])

    def test_tail_latency_and_timeout_fail_independently_of_average(self):
        bootstrap(self.db)
        make_plan(self.db, plan='tail', target=300,
                  classes=[{'class_id': 'premium', 'clients': 300, 'priority': 'high',
                            'min_throughput_mrps': 1, 'max_p99_ms': 200, 'max_error_bp': 1000}],
                  arrivals=[{'at': 40 + i, 'class_id': 'premium', 'kind': 'business',
                             'request_id': f't{i}'} for i in range(4)])
        record(self.db, 'tail', 't0', 40, latency=10)
        record(self.db, 'tail', 't1', 41, latency=10)
        record(self.db, 'tail', 't2', 42, latency=12)
        record(self.db, 'tail', 't3', 43, outcome='timeout', latency=None, retries=2)
        guards(self.db, 'tail')
        result = loadtest.run(self.db, {'event_id': 'tail-accept', 'op': 'accept', 'plan': 'tail'})
        row = one(self.db, "SELECT * FROM trial_class_results WHERE plan='tail'")
        # Average latency of the three successes is ~11ms; the long tail and timeout still fail the class.
        self.assertEqual(row['p99_ms'], 12)
        self.assertEqual(row['errors'], 1)
        self.assertEqual(row['pass'], 0)
        self.assertEqual(result['state'], 'failed')
        self.assertEqual(result['retries'], 2)

    def test_timeline_replay_and_window_are_enforced(self):
        bootstrap(self.db)
        made = make_plan(self.db, plan='strict')
        self.assertEqual(made['decision'], 'capacity_trial_planned')
        # A business request that was never on the generator timeline cannot be scored.
        self.assertEqual(record(self.db, 'strict', 'rogue', 40)['decision'], 'request_not_on_timeline')
        # A timeline request arriving outside the contracted measurement window is rejected.
        self.assertEqual(record(self.db, 'strict', 'r-p0', 99)['decision'], 'outside_measurement_window')
        # Generator identity is bound to the plan.
        self.assertEqual(record(self.db, 'strict', 'r-p1', 41, generator='other-gen')['decision'],
                         'generator_mismatch')
        # Duplicate request ids on the timeline are rejected at planning time.
        dup = make_plan(self.db, plan='dup',
                        arrivals=[{'at': 40, 'class_id': 'premium', 'kind': 'business', 'request_id': 'x'},
                                  {'at': 41, 'class_id': 'premium', 'kind': 'business', 'request_id': 'x'}])
        self.assertEqual(dup['decision'], 'timeline_duplicate_request')
        # An unknown class cannot appear on the timeline.
        bad_class = make_plan(self.db, plan='badclass',
                              arrivals=[{'at': 40, 'class_id': 'ghost', 'kind': 'business',
                                         'request_id': 'g1'}])
        self.assertEqual(bad_class['decision'], 'timeline_class_unknown')
        # Windows must sit inside the start attempt's lifetime.
        outside = make_plan(self.db, plan='outside', window=(5, 99))
        self.assertEqual(outside['decision'], 'window_outside_attempt')

    def test_trial_traffic_is_isolated_from_production_writes(self):
        bootstrap(self.db)
        # A generator without an isolated dataset cannot be registered.
        denied = loadtest.run(self.db, {'event_id': 'g-prod', 'op': 'register_generator',
                                        'id': 'g-prod', 'dataset': 'production', 'isolated': False, 'at': 36})
        self.assertEqual(denied['decision'], 'dataset_not_isolated')
        made = make_plan(self.db, plan='iso')
        self.assertEqual(made['decision'], 'capacity_trial_planned')
        # Test identities must not be used to touch production data.
        self.assertEqual(record(self.db, 'iso', 'r-p0', 40, dataset='production')['decision'],
                         'production_write_blocked')
        # A foreign dataset cannot be mixed into the plan's isolated dataset.
        self.assertEqual(record(self.db, 'iso', 'r-p1', 41, dataset='other-sandbox')['decision'],
                         'dataset_mismatch')
        # Planning against a dataset different from the generator's registered one is rejected.
        other = make_plan(self.db, plan='iso2', generator='gen2', dataset='sandbox',
                          arrivals=None)
        self.assertEqual(other['decision'], 'capacity_trial_planned')
        switched = loadtest.run(self.db, {'event_id': 'iso3-plan', 'op': 'capacity_plan', 'id': 'iso3',
                                          'service': 'service-b', 'generation': 1, 'attempt': 'try-b',
                                          'generator': 'gen2', 'dataset': 'elsewhere',
                                          'window_start': 40, 'window_end': 44, 'target_clients': 500,
                                          'classes': [{'class_id': 'premium', 'clients': 500,
                                                       'priority': 'high', 'min_throughput_mrps': 1,
                                                       'max_p99_ms': 200, 'max_error_bp': 100}],
                                          'arrivals': [{'at': 40, 'class_id': 'premium',
                                                        'kind': 'business', 'request_id': 'z1'}]})
        self.assertEqual(switched['decision'], 'dataset_not_isolated')

    def test_empty_requests_cannot_dilute_throughput_or_tail(self):
        bootstrap(self.db)
        make_plan(self.db, plan='dilute', target=300,
                  classes=[{'class_id': 'premium', 'clients': 300, 'priority': 'high',
                            'min_throughput_mrps': 1, 'max_p99_ms': 200, 'max_error_bp': 100}],
                  arrivals=[{'at': 40 + i, 'class_id': 'premium', 'kind': 'business',
                             'request_id': f'd{i}'} for i in range(4)])
        for i in range(4):
            record(self.db, 'dilute', f'd{i}', 40 + i, latency=100 + i)
        # A flood of instant empty requests would dominate any success-only aggregate.
        for i in range(200):
            record(self.db, 'dilute', f'empty-{i}', 44, latency=1, kind='empty', weight=100)
        guards(self.db, 'dilute')
        result = loadtest.run(self.db, {'event_id': 'dilute-accept', 'op': 'accept', 'plan': 'dilute'})
        self.assertEqual(result['empty'], 200)
        self.assertEqual(result['intended'], 4)
        row = one(self.db, "SELECT * FROM trial_class_results WHERE plan='dilute'")
        self.assertEqual(row['p99_ms'], 103)       # business traffic only
        self.assertEqual(row['intended'], 4)
        self.assertEqual(row['pass'], 1)

    def test_resource_guard_violation_fails_even_with_clean_traffic(self):
        bootstrap(self.db)
        make_plan(self.db, plan='guard', target=300,
                  classes=[{'class_id': 'premium', 'clients': 300, 'priority': 'high',
                            'min_throughput_mrps': 1, 'max_p99_ms': 200, 'max_error_bp': 0}],
                  arrivals=[{'at': 40 + i, 'class_id': 'premium', 'kind': 'business',
                             'request_id': f'g{i}'} for i in range(4)])
        for i in range(4):
            record(self.db, 'guard', f'g{i}', 40 + i, latency=20)
        loadtest.run(self.db, {'event_id': 'guard-bad', 'op': 'guard', 'plan': 'guard',
                               'metric': 'cpu_pct', 'observed': 97, 'limit': 90})
        result = loadtest.run(self.db, {'event_id': 'guard-accept', 'op': 'accept', 'plan': 'guard'})
        self.assertEqual(result['state'], 'failed')
        # Acceptance requires resource evidence to exist at all.
        make_plan(self.db, plan='noguard', target=300,
                  classes=[{'class_id': 'premium', 'clients': 300, 'priority': 'high',
                            'min_throughput_mrps': 1, 'max_p99_ms': 200, 'max_error_bp': 100}],
                  arrivals=[{'at': 40 + i, 'class_id': 'premium', 'kind': 'business',
                             'request_id': f'ng{i}'} for i in range(4)])
        self.assertEqual(loadtest.run(self.db, {'event_id': 'no-guard-accept', 'op': 'accept',
                                                'plan': 'noguard'})['decision'], 'guards_missing')

    def test_node_expansion_fences_old_report_and_requires_a_new_trial(self):
        bootstrap(self.db)
        make_plan(self.db, plan='drift')
        for i in range(4):
            record(self.db, 'drift', f'r-p{i}', 40 + i)
        for i in range(4):
            record(self.db, 'drift', f'r-s{i}', 40 + i)
        guards(self.db, 'drift')
        first = loadtest.run(self.db, {'event_id': 'drift-accept1', 'op': 'accept', 'plan': 'drift'})
        self.assertEqual(first['state'], 'passed')
        # The platform adds capacity: the node slot advances a generation.
        MODULES['wave'].run(self.db, {'event_id': 'drift-slot2', 'op': 'slot', 'id': 'slot-a',
                                      'node': 'node-a', 'capacity': 8, 'generation': 2, 'healthy': True})
        refused = loadtest.run(self.db, {'event_id': 'drift-cert-old', 'op': 'certify',
                                         'id': 'cert-old', 'plan': 'drift', 'version': 1, 'at': 60})
        self.assertEqual(refused['decision'], 'instance_drifted')
        # A fresh attempt on the expanded instance carries a new fingerprint and can be trialed anew.
        MODULES['wave'].run(self.db, {'event_id': 'drift-w2', 'op': 'plan', 'id': 'wave-b2',
                                      'service': 'service-b', 'generation': 1, 'deadline': 180})
        self.assertEqual(MODULES['wave'].run(self.db, {
            'event_id': 'drift-l2', 'op': 'launch', 'id': 'try-b2', 'wave': 'wave-b2',
            'slot': 'slot-a', 'token': 'token-b', 'at': 36, 'expires': 89})['decision'], 'launched')
        made = make_plan(self.db, plan='drift2', attempt='try-b2',
                         arrivals=[{'at': 40 + i, 'class_id': 'premium', 'kind': 'business',
                                    'request_id': f'n{i}'} for i in range(4)],
                         target=300, classes=[
                             {'class_id': 'premium', 'clients': 300, 'priority': 'high',
                              'min_throughput_mrps': 1, 'max_p99_ms': 200, 'max_error_bp': 100}])
        self.assertNotEqual(made['slot_fingerprint'],
                            one(self.db, "SELECT slot_fingerprint FROM capacity_plans WHERE plan='drift'")['slot_fingerprint'])
        for i in range(4):
            record(self.db, 'drift2', f'n{i}', 40 + i)
        guards(self.db, 'drift2')
        loadtest.run(self.db, {'event_id': 'drift2-accept', 'op': 'accept', 'plan': 'drift2'})
        issued = loadtest.run(self.db, {'event_id': 'drift2-cert', 'op': 'certify',
                                        'id': 'cert-new', 'plan': 'drift2', 'version': 1, 'at': 61})
        self.assertEqual(issued['decision'], 'certificate_issued')
        # The old report still cannot certify the expanded instance.
        self.assertEqual(loadtest.run(self.db, {'event_id': 'drift-cert-retry', 'op': 'certify',
                                                'id': 'cert-old2', 'plan': 'drift', 'version': 1,
                                                'at': 62})['decision'], 'instance_drifted')

    def test_upstream_revocation_and_revision_change_fence_certification(self):
        bootstrap(self.db)
        make_plan(self.db, plan='fence')
        for i in range(4):
            record(self.db, 'fence', f'r-p{i}', 40 + i)
        for i in range(4):
            record(self.db, 'fence', f'r-s{i}', 40 + i)
        guards(self.db, 'fence')
        loadtest.run(self.db, {'event_id': 'fence-accept', 'op': 'accept', 'plan': 'fence'})
        # Upstream degradation after the trial invalidates the pinned upstream state.
        MODULES['ready'].run(self.db, {'event_id': 'fence-revoke', 'op': 'revoke', 'id': 'receipt-db'})
        self.assertEqual(loadtest.run(self.db, {'event_id': 'fence-cert-up', 'op': 'certify',
                                                'id': 'cert-up', 'plan': 'fence', 'version': 1,
                                                'at': 60})['decision'], 'upstream_drifted')
        # A new service revision generation likewise leaves the old report without a sealed matching revision.
        MODULES['contract'].run(self.db, {'event_id': 'fence-rev2', 'op': 'revision',
                                          'id': 'service-b', 'generation': 2})
        self.assertEqual(loadtest.run(self.db, {'event_id': 'fence-cert-rev', 'op': 'certify',
                                                'id': 'cert-rev', 'plan': 'fence', 'version': 1,
                                                'at': 61})['decision'], 'stale_service_revision')

    def test_reruns_append_versions_without_erasing_the_first_failure(self):
        bootstrap(self.db)
        make_plan(self.db, plan='versioned', target=300,
                  classes=[{'class_id': 'premium', 'clients': 300, 'priority': 'high',
                            'min_throughput_mrps': 1, 'max_p99_ms': 200, 'max_error_bp': 100}],
                  arrivals=[{'at': 40 + i, 'class_id': 'premium', 'kind': 'business',
                             'request_id': f'v{i}'} for i in range(4)])
        # First run: generator only manages to send one request — acceptance fails.
        record(self.db, 'versioned', 'v0', 40)
        guards(self.db, 'versioned')
        first = loadtest.run(self.db, {'event_id': 'v-accept1', 'op': 'accept', 'plan': 'versioned'})
        self.assertEqual(first['version'], 1)
        self.assertEqual(first['state'], 'failed')
        # Replay the remaining timeline entries and rerun: a new version is appended, row one stays failed.
        for i in range(1, 4):
            record(self.db, 'versioned', f'v{i}', 40 + i)
        second = loadtest.run(self.db, {'event_id': 'v-accept2', 'op': 'accept', 'plan': 'versioned'})
        self.assertEqual(second['version'], 2)
        self.assertEqual(second['state'], 'passed')
        versions = many(self.db, 'SELECT version,state FROM trial_report_versions WHERE plan=? ORDER BY version',
                        ('versioned',))
        self.assertEqual([(v['version'], v['state']) for v in versions], [(1, 'failed'), (2, 'passed')])
        # Certification cannot shop the failed first version to a customer.
        self.assertEqual(loadtest.run(self.db, {'event_id': 'v-cert1', 'op': 'certify',
                                                'id': 'cert-v1', 'plan': 'versioned', 'version': 1,
                                                'at': 60})['decision'], 'certificate_refused')
        issued = loadtest.run(self.db, {'event_id': 'v-cert2', 'op': 'certify', 'id': 'cert-v2',
                                        'plan': 'versioned', 'version': 2, 'at': 61})
        self.assertEqual(issued['decision'], 'certificate_issued')
        self.assertEqual(issued['tier'], 'full')
        self.assertEqual(issued['capacity_clients'], 300)
        self.assertEqual(issued['unfulfilled_clients'], 0)

    def test_certificate_lifecycle_sign_withdraw_and_onboarding_terms(self):
        bootstrap(self.db)
        make_plan(self.db, plan='life', target=300,
                  classes=[{'class_id': 'premium', 'clients': 300, 'priority': 'high',
                            'min_throughput_mrps': 1, 'max_p99_ms': 200, 'max_error_bp': 100}],
                  arrivals=[{'at': 40 + i, 'class_id': 'premium', 'kind': 'business',
                             'request_id': f'l{i}'} for i in range(4)])
        for i in range(4):
            record(self.db, 'life', f'l{i}', 40 + i)
        guards(self.db, 'life')
        loadtest.run(self.db, {'event_id': 'life-accept', 'op': 'accept', 'plan': 'life'})
        loadtest.run(self.db, {'event_id': 'life-cert', 'op': 'certify', 'id': 'cert-life',
                               'plan': 'life', 'version': 1, 'at': 60})
        # Customers cannot be onboarded against an unsigned certificate.
        self.assertEqual(loadtest.run(self.db, {'event_id': 'life-onboard-early', 'op': 'onboard',
                                                'id': 'on-early', 'certificate': 'cert-life',
                                                'at': 61})['decision'], 'certificate_not_signed')
        self.assertEqual(loadtest.run(self.db, {'event_id': 'life-sign', 'op': 'sign',
                                                'id': 'cert-life', 'at': 62})['decision'],
                         'certificate_signed')
        onboard = loadtest.run(self.db, {'event_id': 'life-onboard', 'op': 'onboard', 'id': 'on-life',
                                         'certificate': 'cert-life', 'at': 63})
        self.assertEqual(onboard['onboarded_clients'], 300)
        self.assertEqual(onboard['waitlisted_clients'], 0)
        # Withdrawal is its own fact and immediately blocks further use of the certificate.
        self.assertEqual(loadtest.run(self.db, {'event_id': 'life-withdraw', 'op': 'withdraw',
                                                'id': 'cert-life', 'at': 70})['decision'],
                         'certificate_withdrawn')
        self.assertEqual(loadtest.run(self.db, {'event_id': 'life-onboard2', 'op': 'onboard',
                                                'id': 'on-life2', 'certificate': 'cert-life',
                                                'at': 71})['decision'], 'certificate_not_signed')
        cert = one(self.db, "SELECT * FROM capacity_certificates WHERE id='cert-life'")
        self.assertEqual(cert['state'], 'revoked')
        self.assertEqual(cert['signed_at'], 62)
        self.assertEqual(cert['revoked_at'], 70)

    def test_partial_certificate_requires_waitlist_deadline_and_credits(self):
        bootstrap(self.db)
        make_plan(self.db)
        for rid, at in [('r-p0', 40), ('r-p1', 41), ('r-p2', 42), ('r-p3', 43)]:
            record(self.db, 'cp', rid, at)
        for rid, at in [('r-s0', 40), ('r-s1', 41)]:
            record(self.db, 'cp', rid, at)
        guards(self.db, 'cp')
        loadtest.run(self.db, {'event_id': 'cp-accept', 'op': 'accept', 'plan': 'cp'})
        # Partial acceptance cannot be certified without the customer-facing remedy terms.
        self.assertEqual(loadtest.run(self.db, {'event_id': 'cp-cert-no-terms', 'op': 'certify',
                                                'id': 'cert-no-terms', 'plan': 'cp', 'version': 1,
                                                'at': 60})['decision'], 'partial_terms_missing')
        issued = loadtest.run(self.db, {'event_id': 'cp-cert', 'op': 'certify', 'id': 'cert-partial',
                                        'plan': 'cp', 'version': 1, 'at': 60,
                                        'waitlist_until': 200, 'credits': 500})
        self.assertEqual(issued['tier'], 'partial')
        self.assertEqual(issued['capacity_clients'], 300)
        self.assertEqual(issued['unfulfilled_clients'], 200)
        self.assertTrue(replay(self.path)['verified'])


if __name__ == '__main__':
    unittest.main()
