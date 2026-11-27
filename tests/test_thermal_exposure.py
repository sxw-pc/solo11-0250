import json
import tempfile
import unittest
from pathlib import Path

from cloudcourt import contract, wave, thermal
from cloudcourt.store import connect, replay, one, many

FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures' / 'thermal-exposure.json'


def load_events():
    return json.loads(FIXTURE.read_text(encoding='utf-8'))


class ThermalExposureBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'thermal.sqlite')
        self.db = connect(self.path)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def run_event(self, event):
        from cloudcourt import MODULES
        return MODULES[event['subsystem']].run(self.db, event['payload'])

    def play_fixture(self):
        results = {}
        for event in load_events():
            r = self.run_event(event)
            results[(event['subsystem'], event['payload'].get('id'),
                     event['payload']['op'])] = r
        return results

    def thermal(self, **payload):
        return thermal.run(self.db, payload)


class DualProbeColdStart(ThermalExposureBase):
    """Node N: inlet 36 C vs near-chip 46 C with a documented calibration bias;
    the manufacturer dose limit falls inside the correction uncertainty band."""

    def setUp(self):
        super().setUp()
        self.play_fixture()

    def test_two_original_samples_keep_raw_and_calibrated_values(self):
        inlet = one(self.db, "SELECT * FROM thermal_observations WHERE id='ob-n-in-0'")
        chip = one(self.db, "SELECT * FROM thermal_observations WHERE id='ob-n-ch-0'")
        # Raw observations are never rewritten by the later calibration.
        self.assertEqual(inlet['raw_celsius'], 36.0)
        self.assertEqual(chip['raw_celsius'], 46.0)
        cal = one(self.db, "SELECT * FROM sensor_calibrations WHERE id='cal-n-chip-1'")
        self.assertEqual(cal['bias_c'], -1.5)
        self.assertEqual(cal['uncertainty_c'], 1.2)
        chip_sensor = one(self.db, "SELECT * FROM thermal_sensors WHERE id='sensor-n-chip'")
        self.assertEqual(chip_sensor['position'], 'near_chip')
        self.assertEqual(one(self.db, "SELECT position FROM thermal_sensors WHERE id='sensor-n-inlet'")['position'],
                         'inlet')
        node = one(self.db, "SELECT * FROM thermal_nodes WHERE id='node-n'")
        self.assertEqual(node['serial'], 'SN-N-0001')
        # Corrected near-chip value: 46 - 1.5 = 44.5, uncertainty sqrt(0.5^2+1.2^2)=1.3
        series = thermal.corrected_series(self.db, 'sensor-n-chip')
        first = series[0]
        self.assertAlmostEqual(first['value'], 44.5, places=6)
        self.assertAlmostEqual(first['uncertainty'], 1.3, places=6)
        self.assertEqual(first['calibration'], 'cal-n-chip-1')
        self.assertEqual(first['raw'], 46.0)

    def test_cold_inlet_probe_cannot_offset_hotspot_dose(self):
        case = one(self.db, "SELECT * FROM thermal_exposure_cases WHERE id='case-n-1'")
        self.assertEqual(case['verdict'], 'indeterminate')
        self.assertTrue(case['dose_low'] <= case['dose_limit'] <= case['dose_high'])
        # The inlet (36 C < 40 C reference) accumulates no dose; the near-chip
        # band alone governs the node-level verdict.
        inlet_band = one(self.db, '''SELECT * FROM thermal_exposure_windows WHERE case_id='case-n-1' ''')
        self.assertIsNotNone(inlet_band)
        decision = one(self.db, "SELECT * FROM device_fitness_decisions WHERE node='node-n'")
        self.assertEqual(decision['state'], 'fitness_restricted_indeterminate_dose')
        self.assertIn('cumulative dose band', decision['rationale'])

    def test_indeterminate_band_is_disclosed_to_customer(self):
        case = one(self.db, "SELECT * FROM thermal_exposure_cases WHERE id='case-n-1'")
        self.assertAlmostEqual(case['dose_low'], 1272.5, places=3)
        self.assertAlmostEqual(case['dose_high'], 2052.5, places=3)
        self.assertAlmostEqual(case['dose_limit'], 1662.5, places=3)
        # The undecidable range (low..high around the limit) is the information
        # the customer must receive rather than a binary pass/fail.
        self.assertGreater(case['dose_high'] - case['dose_low'], 0.0)

    def test_startup_attempt_time_window_drives_phase_integration(self):
        windows = many(self.db, "SELECT * FROM thermal_exposure_windows WHERE case_id='case-n-1' ORDER BY rowid")
        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0]['attempt'], 'attempt-node-n-slot-n')
        self.assertEqual(windows[0]['phase'], 'startup')
        self.assertEqual((windows[0]['started'], windows[0]['expires']), (100, 400))
        attempt = one(self.db, "SELECT * FROM start_attempts WHERE id='attempt-node-n-slot-n'")
        self.assertEqual(attempt['started'], windows[0]['started'])

    def test_case_items_link_checks_attempts_and_capacity_one_by_one(self):
        items = many(self.db, "SELECT * FROM thermal_case_items WHERE case_id='case-n-1' ORDER BY rowid")
        kinds = sorted(i['kind'] for i in items)
        self.assertEqual(kinds, sorted(['attempt', 'manufacturer_evidence',
                                        'capacity_contract', 'check',
                                        'remediation', 'warranty']))
        check_item = next(i for i in items if i['kind'] == 'check')
        self.assertEqual(check_item['ref'], 'check-n-1')
        # The legacy single-point check remains on record, even though it cannot
        # by itself justify clearance.
        self.assertEqual(one(self.db, "SELECT state FROM node_thermal_checks WHERE id='check-n-1'")['state'],
                         'qualified')

    def test_derated_path_capacity_and_conditional_warranty(self):
        rem = one(self.db, "SELECT * FROM remediation_records WHERE id='rem-n-derate'")
        self.assertEqual(rem['kind'], 'derated')
        self.assertEqual(rem['derated_capacity'], 40)
        self.assertEqual(rem['warranty_status'], 'conditional_monitored')
        self.assertEqual(rem['status'], 'active')
        ledger = many(self.db, "SELECT * FROM capacity_ledger WHERE node='node-n' ORDER BY rowid")
        self.assertEqual(ledger[0]['available_capacity'], 0)       # restricted at adjudication
        self.assertEqual(ledger[0]['committed_capacity'], 100)
        self.assertEqual(ledger[1]['available_capacity'], 40)      # derated service
        warranty = one(self.db, "SELECT * FROM warranty_records WHERE remediation='rem-n-derate'")
        self.assertEqual(warranty['warranty_status'], 'conditional_monitored')
        self.assertIn('monitoring', warranty['liability'])

    def test_healthy_probe_does_not_clear_restriction(self):
        before = one(self.db, "SELECT state FROM device_fitness_decisions WHERE node='node-n'")['state']
        probe = one(self.db, "SELECT * FROM health_probes WHERE id='probe-n-1'")
        self.assertTrue(probe['healthy'])
        # The probe response echoes the still-restricted fitness state.
        self.assertEqual(thermal.run(self.db, {'event_id': 'probe-again', 'op': 'health_probe',
                                               'id': 'probe-n-2', 'node': 'node-n', 'at': 510,
                                               'healthy': True})['fitness_state'], before)
        # No new clearance decision or full-capacity ledger row was created.
        self.assertEqual(one(self.db, "SELECT COUNT(*) n FROM device_fitness_decisions WHERE node='node-n'")['n'], 1)
        latest_ledger = one(self.db, "SELECT * FROM capacity_ledger WHERE node='node-n' ORDER BY rowid DESC LIMIT 1")
        self.assertEqual(latest_ledger['available_capacity'], 40)

    def test_suspect_use_period_is_preserved(self):
        period = one(self.db, "SELECT * FROM suspect_use_periods WHERE node='node-n'")
        self.assertEqual(period['status'], 'open')
        self.assertEqual(period['from_ts'], 100)
        self.assertIsNone(period['until_ts'])


class ShutdownReinspectReplace(ThermalExposureBase):
    """Node P: stop-and-reinspect path, failed reinspection, then part replacement."""

    def setUp(self):
        super().setUp()
        self.play_fixture()

    def test_shutdown_keeps_full_warranty_but_zero_capacity(self):
        rem = one(self.db, "SELECT * FROM remediation_records WHERE id='rem-p-stop'")
        # The failed reinspection later flips this record; the warranty decision
        # recorded at shutdown time remains on its own row.
        self.assertEqual(rem['kind'], 'reinspect_shutdown')
        stop_warranty = one(self.db, "SELECT * FROM warranty_records WHERE id='rem-p-stop:warranty'")
        self.assertEqual(stop_warranty['warranty_status'], 'full_retained')
        ledger = one(self.db, "SELECT * FROM capacity_ledger WHERE reason LIKE 'remediation:reinspect%' LIMIT 1")
        self.assertEqual(ledger['available_capacity'], 0)
        self.assertEqual(ledger['committed_capacity'], 100)

    def test_failed_reinspection_keeps_restriction_and_routes_warranty(self):
        ri = one(self.db, "SELECT * FROM reinspection_records WHERE id='ri-p-fail'")
        self.assertEqual(ri['status'], 'failed')
        self.assertFalse(ri['passed'])
        rem = one(self.db, "SELECT * FROM remediation_records WHERE id='rem-p-stop'")
        self.assertEqual(rem['status'], 'failed_reinspection')
        # An append-only review keeps the original restricted decision visible.
        reviews = many(self.db, "SELECT * FROM device_fitness_reviews WHERE node='node-p'")
        self.assertTrue(any(r['state'].startswith('fitness_restricted') for r in reviews))
        decision = one(self.db, "SELECT * FROM device_fitness_decisions WHERE node='node-p'")
        self.assertEqual(decision['state'], 'fitness_restricted_indeterminate_dose')
        warranty = one(self.db, "SELECT * FROM warranty_records WHERE id='ri-p-fail:warranty'")
        self.assertEqual(warranty['warranty_status'], 'replacement_required')

    def test_replacement_restores_later_capacity_at_hardware_fee_full_warranty(self):
        rem = one(self.db, "SELECT * FROM remediation_records WHERE id='rem-p-replace'")
        self.assertEqual(rem['kind'], 'replaced')
        self.assertEqual(rem['hardware_fee'], 1200.0)
        self.assertEqual(rem['capacity_ready_at'], 2600)
        self.assertEqual(rem['warranty_status'], 'full_retained')
        # The suspect-use window is closed but its history remains.
        period = one(self.db, "SELECT * FROM suspect_use_periods WHERE node='node-p'")
        self.assertEqual(period['status'], 'closed')
        self.assertEqual((period['from_ts'], period['until_ts']), (100, 2100))

    def test_two_paths_same_trajectory_different_capacity_and_warranty(self):
        n = one(self.db, "SELECT * FROM remediation_records WHERE id='rem-n-derate'")
        p = one(self.db, "SELECT * FROM remediation_records WHERE id='rem-p-stop'")
        self.assertEqual(n['derated_capacity'], 40)          # earlier, lower capacity
        self.assertEqual(n['warranty_status'], 'conditional_monitored')
        self.assertEqual(p['warranty_status'], 'full_retained')
        # Same underlying band shape (both indeterminate on the same trajectory).
        nc = one(self.db, "SELECT verdict FROM thermal_exposure_cases WHERE id='case-n-1'")
        pc = one(self.db, "SELECT verdict FROM thermal_exposure_cases WHERE id='case-p-1'")
        self.assertEqual(nc['verdict'], pc['verdict'])


class SensorOutageAdjacentWavesLateCalibration(ThermalExposureBase):
    def setUp(self):
        super().setUp()
        self.play_fixture()

    def test_sensor_outage_blocks_clearance_and_preserves_original_readings(self):
        outage = one(self.db, "SELECT * FROM sensor_outages WHERE id='out-q-1'")
        # The same outage row is updated in place with the reconnect time; no row is erased.
        self.assertEqual((outage['disconnected_at'], outage['reconnected_at']), (140, 220))
        case = one(self.db, "SELECT * FROM thermal_exposure_cases WHERE id='case-q-1'")
        self.assertEqual(case['verdict'], 'indeterminate_data_gap')
        self.assertTrue(case['data_gap'])
        # Even though the high limit is far above the observed band, the outage
        # prevents a clean clearance.
        self.assertGreater(case['dose_limit'], case['dose_high'])
        decision = one(self.db, "SELECT * FROM device_fitness_decisions WHERE node='node-q'")
        self.assertEqual(decision['state'], 'fitness_restricted_data_gap')
        # Raw readings before and after the outage are all retained.
        self.assertEqual(
            one(self.db, "SELECT COUNT(*) n FROM thermal_observations WHERE sensor='sensor-q-chip'")['n'], 4)

    def test_observation_during_outage_is_rejected(self):
        r = self.thermal(event_id='q-bad-obs', op='observe', id='ob-q-bad',
                         sensor='sensor-q-chip', at=180, raw_celsius=55.0, uncertainty_c=0.3)
        self.assertEqual(r['decision'], 'observation_during_outage')
        # Reopening after reconnect works, and duplicate disconnect is fenced.
        self.assertEqual(self.thermal(event_id='q-disc-again', op='disconnect',
                                      id='out-q-2', sensor='sensor-q-chip', at=250)['decision'],
                         'sensor_disconnected')
        self.assertEqual(self.thermal(event_id='q-disc-dup', op='disconnect',
                                      id='out-q-3', sensor='sensor-q-chip', at=260)['decision'],
                         'sensor_already_disconnected')

    def test_adjacent_waves_accumulate_across_cooling_gap(self):
        windows = many(self.db, "SELECT * FROM thermal_exposure_windows WHERE case_id='case-r-1' ORDER BY rowid")
        self.assertEqual([w['phase'] for w in windows], ['startup', 'warmup'])
        attempts = [w['attempt'] for w in windows]
        self.assertEqual(attempts, ['attempt-node-r-slot-r1', 'attempt-node-r-slot-r2'])
        # Two real time windows are integrated into one cumulative node dose.
        case = one(self.db, "SELECT * FROM thermal_exposure_cases WHERE id='case-r-1'")
        self.assertEqual(case['verdict'], 'indeterminate')
        self.assertTrue(case['dose_low'] <= case['dose_limit'] <= case['dose_high'])
        items = many(self.db, "SELECT kind FROM thermal_case_items WHERE case_id='case-r-1'")
        self.assertEqual(sorted(i['kind'] for i in items),
                         sorted(['attempt', 'attempt', 'manufacturer_evidence', 'capacity_contract']))

    def test_late_calibration_revises_dose_without_erasing_originals(self):
        cal = one(self.db, "SELECT * FROM sensor_calibrations WHERE id='cal-r-1'")
        self.assertEqual(cal['rationale'], 'correction issued after first wave; originals retained')
        raw = one(self.db, "SELECT * FROM thermal_observations WHERE id='ob-r1-ch-0'")
        self.assertEqual(raw['raw_celsius'], 44.0)  # original untouched
        # Every reading is corrected under the late calibration (44 - 1 = 43).
        series = thermal.corrected_series(self.db, 'sensor-r-chip')
        self.assertTrue(all(p['calibration'] == 'cal-r-1' for p in series))
        self.assertAlmostEqual(series[0]['value'], 43.0, places=6)

    def test_reviews_append_only_never_overwrite_prior_decision(self):
        original = one(self.db, "SELECT * FROM device_fitness_decisions WHERE node='node-p'")
        r = self.thermal(event_id='p-manual-review', op='review', id='review-p-1',
                         decision=original['id'], state='fitness_restricted_indeterminate_dose',
                         at=2200, rationale='customer kept using node during suspect window')
        self.assertEqual(r['decision'], 'review_appended')
        reviews = many(self.db, "SELECT * FROM device_fitness_reviews WHERE decision=? ORDER BY rowid",
                       (original['id'],))
        self.assertGreaterEqual(len(reviews), 2)
        # The original decision row is immutable.
        self.assertEqual(one(self.db, "SELECT state FROM device_fitness_decisions WHERE id=?",
                             (original['id'],))['state'], 'fitness_restricted_indeterminate_dose')


class PointOverTemperatureAndReplay(ThermalExposureBase):
    def _near_chip_overtemp_node(self, node='node-hot'):
        # Inlet stays cold while the near-chip corrected reading breaches the
        # manufacturer's maximum operating temperature.
        contract.run(self.db, {'event_id': 'hot-rev', 'op': 'revision', 'id': 'svc-h', 'generation': 1})
        contract.run(self.db, {'event_id': 'hot-seal', 'op': 'seal', 'id': 'svc-h', 'generation': 1})
        contract.run(self.db, {'event_id': 'hot-token', 'op': 'token', 'id': 'token-h',
                               'service': 'svc-h', 'generation': 1, 'expires': 1000, 'at': 0})
        wave.run(self.db, {'event_id': 'hot-slot', 'op': 'slot', 'id': 'slot-hot', 'node': node,
                           'capacity': 10, 'generation': 1, 'healthy': True})
        wave.run(self.db, {'event_id': 'hot-plan', 'op': 'plan', 'id': 'wave-h', 'service': 'svc-h',
                           'generation': 1, 'deadline': 1000})
        wave.run(self.db, {'event_id': 'hot-launch', 'op': 'launch', 'id': 'attempt-hot',
                           'wave': 'wave-h', 'slot': 'slot-hot', 'token': 'token-h',
                           'at': 0, 'expires': 100})
        self.thermal(event_id='hot-zone', op='zone', id='z-hot', rack='rack-h', limit_c=40, revision=1)
        self.thermal(event_id='hot-profile', op='profile', zone='z-hot', reference_c=30.0,
                     ambient_c=20.0, cooling_tau=30.0, revision=1)
        self.thermal(event_id='hot-node', op='node', id=node, zone='z-hot', serial='SN-HOT',
                     rack='rack-h', at=0)
        self.thermal(event_id='hot-inlet', op='sensor', id='s-hot-in', node=node,
                     position='inlet', serial='IN', bias_c=0.0, uncertainty_c=0.2, at=0)
        self.thermal(event_id='hot-chip', op='sensor', id='s-hot-chip', node=node,
                     position='near_chip', serial='CHIP', bias_c=0.0, uncertainty_c=0.2, at=0)
        self.thermal(event_id='hot-ob-in0', op='observe', id='o-hot-in-0', sensor='s-hot-in',
                     at=0, raw_celsius=26.0, uncertainty_c=0.2)
        self.thermal(event_id='hot-ob-chip0', op='observe', id='o-hot-chip-0', sensor='s-hot-chip',
                     at=0, raw_celsius=48.0, uncertainty_c=0.2)
        self.thermal(event_id='hot-ob-in', op='observe', id='o-hot-in', sensor='s-hot-in',
                     at=50, raw_celsius=28.0, uncertainty_c=0.2)
        self.thermal(event_id='hot-ob-chip', op='observe', id='o-hot-chip', sensor='s-hot-chip',
                     at=50, raw_celsius=52.0, uncertainty_c=0.2)
        self.thermal(event_id='hot-ob-in1', op='observe', id='o-hot-in-1', sensor='s-hot-in',
                     at=100, raw_celsius=27.0, uncertainty_c=0.2)
        self.thermal(event_id='hot-ob-chip1', op='observe', id='o-hot-chip-1', sensor='s-hot-chip',
                     at=100, raw_celsius=51.0, uncertainty_c=0.2)
        self.thermal(event_id='hot-cap', op='capacity', id='cap-hot', node=node,
                     committed_capacity=10, effective_at=0)
        self.thermal(event_id='hot-ev', op='evidence', id='ev-hot', node=node,
                     dose_limit=100000.0, max_operating_c=51.0,
                     inspection='', warranty_terms={}, at=101)
        return self.thermal(event_id='hot-adj', op='adjudicate', id='case-hot', node=node, at=102)

    def test_near_chip_breach_is_flagged_despite_cold_inlet_and_huge_dose_limit(self):
        r = self._near_chip_overtemp_node()
        self.assertEqual(r['point_overtemp'], ['s-hot-chip'])
        self.assertEqual(r['verdict'], 'point_over_temperature')
        case = one(self.db, "SELECT * FROM thermal_exposure_cases WHERE id='case-hot'")
        self.assertEqual(case['point_overtemp'], 's-hot-chip')
        # Dose alone would pass; the point breach still restricts the node.
        self.assertEqual(r['decision'], 'fitness_restricted_point_overtemp')
        self.assertIn('point over-temperature',
                      one(self.db, "SELECT rationale FROM device_fitness_decisions WHERE id='case-hot:fitness'")['rationale'])

    def test_invalid_remediation_inputs_are_rejected(self):
        self.play_fixture()
        # Derated capacity must stay below the committed 100 and cannot be missing.
        bad = self.thermal(event_id='n-bad-derate', op='remediation', id='rem-bad',
                           node='node-n', decision='case-n-1:fitness', kind='derated',
                           derated_capacity=150, reinspect_due=3000, at=440)
        self.assertEqual(bad['decision'], 'invalid_derated_capacity')
        missing_due = self.thermal(event_id='n-no-due', op='remediation', id='rem-nodue',
                                   node='node-n', decision='case-n-1:fitness', kind='derated',
                                   derated_capacity=40, at=440)
        self.assertEqual(missing_due['decision'], 'reinspection_due_required')

    def test_reinspection_passed_restores_full_capacity_and_closes_suspect_period(self):
        self.play_fixture()
        # Build a second derated node path that passes its reinspection.
        contract.run(self.db, {'event_id': 'ok-rev', 'op': 'revision', 'id': 'svc-ok', 'generation': 1})
        contract.run(self.db, {'event_id': 'ok-seal', 'op': 'seal', 'id': 'svc-ok', 'generation': 1})
        contract.run(self.db, {'event_id': 'ok-token', 'op': 'token', 'id': 'token-ok',
                               'service': 'svc-ok', 'generation': 1, 'expires': 1000, 'at': 0})
        wave.run(self.db, {'event_id': 'ok-slot', 'op': 'slot', 'id': 'slot-ok', 'node': 'node-ok',
                           'capacity': 50, 'generation': 1, 'healthy': True})
        wave.run(self.db, {'event_id': 'ok-plan', 'op': 'plan', 'id': 'wave-ok', 'service': 'svc-ok',
                           'generation': 1, 'deadline': 1000})
        wave.run(self.db, {'event_id': 'ok-launch', 'op': 'launch', 'id': 'attempt-ok',
                           'wave': 'wave-ok', 'slot': 'slot-ok', 'token': 'token-ok',
                           'at': 0, 'expires': 100})
        self.thermal(event_id='ok-zone', op='zone', id='z-ok', rack='rack-o', limit_c=40, revision=1)
        self.thermal(event_id='ok-profile', op='profile', zone='z-ok', reference_c=30.0,
                     ambient_c=20.0, cooling_tau=30.0, revision=1)
        self.thermal(event_id='ok-node', op='node', id='node-ok', zone='z-ok', serial='SN-OK',
                     rack='rack-o', at=0)
        self.thermal(event_id='ok-sensor', op='sensor', id='s-ok', node='node-ok',
                     position='near_chip', serial='S', bias_c=0.0, uncertainty_c=0.2, at=0)
        self.thermal(event_id='ok-ob0', op='observe', id='o-ok-0', sensor='s-ok', at=0,
                     raw_celsius=33.0, uncertainty_c=0.2)
        self.thermal(event_id='ok-ob', op='observe', id='o-ok', sensor='s-ok', at=50,
                     raw_celsius=35.0, uncertainty_c=0.2)
        self.thermal(event_id='ok-ob1', op='observe', id='o-ok-1', sensor='s-ok', at=100,
                     raw_celsius=34.0, uncertainty_c=0.2)
        self.thermal(event_id='ok-cap', op='capacity', id='cap-ok', node='node-ok',
                     committed_capacity=50, effective_at=0)
        self.thermal(event_id='ok-ev', op='evidence', id='ev-ok', node='node-ok',
                     dose_limit=100.0, max_operating_c=50.0, inspection='',
                     warranty_terms={}, at=101)
        adj = self.thermal(event_id='ok-adj', op='adjudicate', id='case-ok', node='node-ok', at=102)
        self.assertEqual(adj['decision'], 'fitness_restricted_over_dose')
        rem = self.thermal(event_id='ok-rem', op='remediation', id='rem-ok', node='node-ok',
                           decision='case-ok:fitness', kind='derated', derated_capacity=20,
                           reinspect_due=500, at=103)
        self.assertEqual(rem['available_capacity'], 20)
        passed = self.thermal(event_id='ok-ri', op='reinspect', id='ri-ok', remediation='rem-ok',
                              node='node-ok', at=500, passed=True, finding='all margins nominal')
        self.assertEqual(passed['decision'], 'reinspection_passed')
        self.assertEqual(passed['fitness_state'], 'fit_for_service')
        ledger = one(self.db, "SELECT * FROM capacity_ledger WHERE node='node-ok' ORDER BY rowid DESC LIMIT 1")
        self.assertEqual(ledger['available_capacity'], 50)
        self.assertEqual(one(self.db, "SELECT status FROM remediation_records WHERE id='rem-ok'")['status'],
                         'completed')
        period = one(self.db, "SELECT * FROM suspect_use_periods WHERE node='node-ok'")
        self.assertEqual(period['status'], 'closed')

    def test_full_fixture_replays_to_identical_state(self):
        self.play_fixture()
        report = replay(self.path)
        self.assertTrue(report['verified'], report['mismatch'])
        self.assertGreater(report['decisions'], 90)


if __name__ == '__main__':
    unittest.main()
