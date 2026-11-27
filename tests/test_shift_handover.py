import itertools
import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from cloudcourt import MODULES, contract, duty, ready, wave
from cloudcourt.store import connect, one, replay

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / 'fixtures' / 'shift-handover-cross-shift.json'
COUNTER = itertools.count()


def load_envelope():
    return json.loads(FIXTURE.read_text(encoding='utf-8'))


class HandoverTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'shift.sqlite')
        self.db = connect(self.path)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def run_mod(self, module, **payload):
        payload.setdefault('event_id', f'ev-{next(COUNTER)}')
        return module.run(self.db, payload)

    def D(self, **payload):
        return self.run_mod(duty, **payload)

    def W(self, **payload):
        return self.run_mod(wave, **payload)

    def R(self, **payload):
        return self.run_mod(ready, **payload)

    def C(self, **payload):
        return self.run_mod(contract, **payload)

    def seed_issuer(self, issuer='cta'):
        return self.D(op='issuer', id='cta', name='cta', event_id='issuer')

    def seed_cert(self, cid='cert-a', actor='alice', level='L2', site='site-1',
                  issuer='cta', starts=0, expires=100, **extra):
        self.seed_issuer()
        return self.D(op='certification', id=cid, actor=actor, service_level=level,
                      site=site, issuer=issuer, starts=starts, expires=expires, at=starts,
                      event_id=cid, **extra)

    def seed_shift_pair(self, a_end=10, b_start=10, b_end=22, site='site-1'):
        self.D(op='shift', id='shift-a', actor='alice', site=site,
               starts=0, ends=a_end, qualification='startup', event_id='shift-a')
        self.D(op='shift', id='shift-b', actor='bob', site=site,
               starts=b_start, ends=b_end, qualification='startup', event_id='shift-b')

    def seed_sessions(self, site='site-1', b_independent=True, b_start=8):
        self.D(op='session_start', id='session-a', actor='alice', site=site, at=0,
               permissions=['launch', 'retry', 'rollback', 'publish'], event_id='sess-a')
        self.D(op='session_start', id='session-b', actor='bob', site=site, at=b_start,
               independent=b_independent,
               permissions=['launch', 'retry', 'rollback', 'publish'], event_id='sess-b')

    def seed_basic_duty(self, **kw):
        site = kw.get('site', 'site-1')
        self.seed_cert()
        self.seed_cert(cid='cert-b', actor='bob')
        self.D(op='supervisor', id='sup-z', actor='zhao', site=site, starts=0, ends=100,
               event_id='sup-z')
        self.seed_shift_pair(**{k: v for k, v in kw.items()
                                if k in ('a_end', 'b_start', 'b_end', 'site')})
        self.seed_sessions(site=site,
                           b_independent=kw.get('b_independent', True),
                           b_start=kw.get('b_start', 8))

    def seed_wave(self, service='svc-b', site='site-1', node='node-b', slot='slot-b',
                  capacity=1, deadline=200, level='L2'):
        self.C(op='revision', id=service, generation=1, event_id='rev-' + service)
        self.C(op='seal', id=service, generation=1, event_id='seal-' + service)
        self.C(op='token', id='token-' + service, service=service, generation=1,
               expires=300, at=0, event_id='token-' + service)
        self.D(op='equipment', id='eq-' + node, node=node, site=site, event_id='eq-' + node)
        if level:
            self.D(op='service_profile', service=service, site=site, service_level=level,
                   event_id='profile-' + service)
        self.W(op='slot', id=slot, node=node, capacity=capacity, generation=1, healthy=True,
               event_id='slot-' + slot)
        self.W(op='plan', id='wave-' + service, service=service, generation=1, deadline=deadline,
               event_id='plan-' + service)

    def launch(self, event_id='launch-1', attempt='try-1', wave='wave-svc-b', slot='slot-b',
               token='token-svc-b', at=5, actor='alice', session='session-a', site='site-1',
               **extra):
        return self.W(event_id=event_id, op='launch', id=attempt, wave=wave, slot=slot,
                      token=token, at=at, expires=90, actor=actor, session=session,
                      site=site, **extra)

    def complete_standard_handoff(self, hid='ho-1', at=10, nodes=None, sign_at=None):
        sign_at = sign_at or at
        nodes = nodes or ['node-b']
        items = [r['id'] for r in self.db.execute(
            "SELECT id FROM handoff_items WHERE handoff_id=?", (hid,)).fetchall()]
        for iid in items:
            self.D(op='accept_item', handoff=hid, item=iid, actor='bob', at=at)
        self.D(op='site_confirm', id='confirm-1', handoff=hid, session='session-b',
               actor='bob', at=at, nodes=nodes, event_id='confirm-1')
        self.D(op='sign', id='sig-out', handoff=hid, actor='alice', role='outgoing',
               at=sign_at, event_id='sig-out')
        self.D(op='sign', id='sig-in', handoff=hid, actor='bob', role='incoming',
               at=sign_at, event_id='sig-in')
        return self.D(op='handoff_complete', handoff=hid, actor='bob', at=at,
                      event_id='complete-1')


class QualificationBindingTests(HandoverTestBase):
    def test_shift_name_string_is_not_a_certificate(self):
        # The legacy shift row only carries the display string "startup".
        self.D(op='shift', id='shift-a', actor='alice', site='site-1',
               starts=0, ends=10, qualification='startup')
        self.D(op='session_start', id='sess-a', actor='alice', site='site-1', at=0)
        verdict = duty.evaluate_operation(self.db, {
            'op': 'launch', 'at': 5, 'actor': 'alice', 'session': 'sess-a',
            'site': 'site-1', 'service_level': 'L2'})
        self.assertFalse(verdict['allowed'])
        self.assertEqual(verdict['decision'], 'qualification_not_certified')

    def test_level_site_issuer_and_expiry_are_all_bound(self):
        self.seed_issuer()
        self.D(op='shift', id='sa', actor='alice', site='site-1', starts=0, ends=30)
        self.D(op='certification', id='c1', actor='alice', service_level='L2',
               site='site-1', issuer='cta', starts=0, expires=12, at=0)
        self.D(op='session_start', id='s1', actor='alice', site='site-1', at=0)

        def verdict(**over):
            p = {'op': 'launch', 'at': 5, 'actor': 'alice', 'session': 's1',
                 'site': 'site-1', 'service_level': 'L2'}
            p.update(over)
            return duty.evaluate_operation(self.db, p)

        self.assertTrue(verdict()['allowed'])
        self.assertEqual(verdict(service_level='L3')['decision'],
                         'qualification_level_mismatch')
        self.assertEqual(
            duty.check_qualification(self.db, 'alice', 5, 'site-2', 'L2')['decision'],
            'qualification_site_mismatch')
        self.assertEqual(verdict(at=12)['decision'], 'qualification_expired')
        # A same-named certificate from an unrecognised issuer is not valid.
        self.D(op='certification', id='c-fake', actor='eve', service_level='L2',
               site='site-1', issuer='mail-order-mill', starts=0, expires=99, at=0)
        fake = duty.check_qualification(self.db, 'eve', 5, 'site-1', 'L2')
        self.assertEqual(fake['decision'], 'qualification_issuer_unrecognized')

    def test_equipment_location_scope_is_enforced(self):
        self.seed_cert(cid='c-node', equipment='node-other')
        self.D(op='shift', id='sa', actor='alice', site='site-1', starts=0, ends=10)
        self.D(op='session_start', id='sess-a', actor='alice', site='site-1', at=0)
        self.D(op='equipment', id='eq-b', node='node-b', site='site-1')
        v = duty.evaluate_operation(self.db, {
            'op': 'launch', 'at': 5, 'actor': 'alice', 'session': 'sess-a',
            'slot': 'slot-b', 'site': 'site-1'})
        # slot-b -> node-b, but certificate is bound to node-other.
        self.assertEqual(v['decision'], 'qualification_invalid')

    def test_near_expiry_warns_but_real_expiry_bites(self):
        self.seed_cert(expires=11)
        self.D(op='shift', id='sa', actor='alice', site='site-1', starts=0, ends=20)
        self.D(op='session_start', id='sess-a', actor='alice', site='site-1', at=0)
        v10 = duty.evaluate_operation(self.db, {
            'op': 'launch', 'at': 10, 'actor': 'alice', 'session': 'sess-a', 'site': 'site-1'})
        self.assertTrue(v10['allowed'])
        self.assertIn('qualification_expires_soon', v10['warnings'])
        v11 = duty.evaluate_operation(self.db, {
            'op': 'launch', 'at': 11, 'actor': 'alice', 'session': 'sess-a', 'site': 'site-1'})
        self.assertEqual(v11['decision'], 'qualification_expired')

    def test_valid_certificate_allows_real_launch(self):
        self.seed_basic_duty()
        self.seed_wave()
        result = self.launch()
        self.assertEqual(result['decision'], 'launched')
        self.assertTrue(replay(self.path)['verified'])


class LateAcknowledgmentTests(HandoverTestBase):
    def _world_open_handoff(self):
        self.seed_basic_duty()
        opened = self.D(op='handoff_open', id='ho-1', outgoing='shift-a',
                        incoming='shift-b', at=9, site='site-1')
        self.assertEqual(opened['decision'], 'handoff_opened')

    def test_late_incoming_signature_leaves_visible_gap(self):
        self._world_open_handoff()
        self.D(op='sign', id='sig-out', handoff='ho-1', actor='alice',
               role='outgoing', at=9)
        # Bob's evidence only arrives at t=12, two minutes after Alice's end.
        self.D(op='site_confirm', id='conf-1', handoff='ho-1', session='session-b',
               actor='bob', at=12, nodes=[])
        self.D(op='sign', id='sig-in', handoff='ho-1', actor='bob',
               role='incoming', at=12)
        done = self.D(op='handoff_complete', handoff='ho-1', at=12)
        self.assertEqual(done['decision'], 'handover_completed')
        self.assertEqual(done['completed_at'], 12)

        self.assertEqual(duty.responsible_at(self.db, 'site-1', 9)['actor'], 'alice')
        for t in (10, 11, 11.9 if False else 11):
            point = duty.responsible_at(self.db, 'site-1', t)
            self.assertIsNone(point['actor'], t)
            self.assertEqual(point['status'], 'gap')
        self.assertEqual(duty.responsible_at(self.db, 'site-1', 12)['actor'], 'bob')
        seg = duty.timeline(self.db, 'site-1', 0, 22)
        gaps = [s for s in seg if s['status'] == 'gap']
        self.assertEqual([(g['start'], g['end']) for g in gaps], [(10, 12)])

    def test_backdated_signature_cannot_fill_gap(self):
        self._world_open_handoff()
        self.D(op='sign', id='sig-out', handoff='ho-1', actor='alice',
               role='outgoing', at=9)
        # Claims to have signed at 9 but is physically received only at 12.
        signed = self.D(op='sign', id='sig-in', handoff='ho-1', actor='bob',
                        role='incoming', at=9, received_at=12)
        self.assertEqual(signed['effective_at'], 12)
        self.D(op='site_confirm', id='conf-1', handoff='ho-1', session='session-b',
               actor='bob', at=12, received_at=12, nodes=[])
        done = self.D(op='handoff_complete', handoff='ho-1', at=12)
        self.assertEqual(done['decision'], 'handover_completed')
        self.assertEqual(done['completed_at'], 12)
        self.assertIsNone(duty.responsible_at(self.db, 'site-1', 11)['actor'])

    def test_production_launch_in_gap_is_denied(self):
        self._world_open_handoff()
        for actor, session in (('bob', 'session-b'), ('alice', 'session-a')):
            denied = self.W(event_id='gap-launch-' + actor, op='launch',
                            id='try-gap-' + actor, wave='wave-x', slot='slot-x',
                            token='tok-x', at=11, actor=actor, session=session,
                            site='site-1')
            self.assertEqual(denied['decision'], 'duty_gap', actor)

    def test_shift_end_rewrite_is_caught_by_event_replay(self):
        self.test_late_incoming_signature_leaves_visible_gap()
        # Someone tries to erase the unattended interval by editing shift.ends.
        self.db.execute("UPDATE duty_shifts SET ends=12 WHERE id='shift-a'")
        self.assertIsNotNone(duty.responsible_at(self.db, 'site-1', 11)['actor'])
        report = replay(self.path)
        self.assertFalse(report['verified'])
        self.assertTrue(any(m.get('table') == 'duty_shifts' for m in report['mismatch']))


class SickWithdrawalTests(HandoverTestBase):
    def test_sick_withdrawal_clips_duty_and_emergency_cover_restores_line(self):
        self.seed_basic_duty(b_end=22)
        self.D(op='withdraw', id='wd-1', shift='shift-a', at=7, reason='sudden illness')
        # shift.ends is never rewritten.
        self.assertEqual(one(self.db, 'SELECT ends FROM duty_shifts WHERE id=?',
                             ('shift-a',))['ends'], 10)
        self.assertEqual(duty.responsible_at(self.db, 'site-1', 6)['actor'], 'alice')
        self.assertEqual(duty.responsible_at(self.db, 'site-1', 7)['status'], 'gap')
        # Her on-site session is physically closed at the withdrawal time.
        denied = self.launch(event_id='sick-launch', at=7)
        self.assertEqual(denied['decision'], 'duty_gap')

        # Supervisor grants Bob bounded emergency cover starting at 7.
        em = self.D(op='emergency', id='em-1', actor='bob', site='site-1',
                    supervisor='zhao', at=7, starts=7, until=12, reason='sick cover')
        self.assertEqual(em['decision'], 'emergency_authorization_granted')
        p7 = duty.responsible_at(self.db, 'site-1', 8)
        self.assertEqual(p7['actor'], 'bob')
        self.assertEqual(p7['basis'], 'emergency')
        self.assertEqual(p7['supervisors'], ['zhao'])

        # Completing handover at 8 gives Bob the ordinary shift from then on.
        opened = self.D(op='handoff_open', id='ho-1', outgoing='shift-a',
                        incoming='shift-b', at=7, site='site-1')
        self.assertEqual(opened['decision'], 'handoff_opened')
        done = self.complete_standard_handoff(at=8)
        self.assertEqual(done['decision'], 'handover_completed')
        seg = duty.timeline(self.db, 'site-1', 0, 22)
        self.assertFalse([s for s in seg if s['status'] == 'gap' and s['start'] >= 8])
        rec = duty.recompute(self.db)
        self.assertTrue(rec['verified'], rec['violations'])
        self.assertTrue(replay(self.path)['verified'])


class CrossSiteCoverTests(HandoverTestBase):
    def test_home_site_certificate_does_not_cover_other_site(self):
        self.seed_issuer()
        self.D(op='certification', id='c-carol-2', actor='carol', service_level='L2',
               site='site-2', issuer='cta', starts=0, expires=100, at=0)
        self.D(op='shift', id='shift-c', actor='carol', site='site-1',
               starts=0, ends=8, qualification='startup', kind='cross_site',
               home_site='site-2', cover_for='dwight')
        self.D(op='session_start', id='sess-c', actor='carol', site='site-1', at=0,
               independent=True)
        bad = duty.evaluate_operation(self.db, {
            'op': 'launch', 'at': 4, 'actor': 'carol', 'session': 'sess-c',
            'site': 'site-1', 'service_level': 'L2'})
        self.assertEqual(bad['decision'], 'qualification_site_mismatch')

        # A real site-1 endorsement makes the cross-site cover lawful and the
        # timeline records it as cross_site rather than a normal shift.
        self.D(op='certification', id='c-carol-1', actor='carol', service_level='L2',
               site='site-1', issuer='cta', starts=0, expires=100, at=0)
        good = duty.evaluate_operation(self.db, {
            'op': 'launch', 'at': 4, 'actor': 'carol', 'session': 'sess-c',
            'site': 'site-1', 'service_level': 'L2'})
        self.assertTrue(good['allowed'], good)
        point = duty.responsible_at(self.db, 'site-1', 4)
        self.assertEqual(point['actor'], 'carol')
        self.assertEqual(point['basis'], 'cross_site')


class QualificationRevocationTests(HandoverTestBase):
    def test_revocation_is_temporal_and_keeps_record(self):
        self.seed_cert(expires=100)
        self.D(op='shift', id='sa', actor='alice', site='site-1', starts=0, ends=20)
        self.D(op='session_start', id='sess-a', actor='alice', site='site-1', at=0)
        before = duty.check_qualification(self.db, 'alice', 7, 'site-1')
        self.assertTrue(before['ok'])
        revoked = self.D(op='revoke_cert', id='cert-a', at=8, reason='training lapse')
        self.assertEqual(revoked['decision'], 'qualification_revoked')
        row = one(self.db, 'SELECT * FROM qualification_certificates WHERE id=?', ('cert-a',))
        self.assertEqual(row['status'], 'revoked')
        self.assertEqual(row['revoked_at'], 8)
        after = duty.check_qualification(self.db, 'alice', 9, 'site-1')
        self.assertFalse(after['ok'])
        self.assertEqual(after['decision'], 'qualification_revoked')
        denied = self.launch(event_id='launch-revoked', at=9, session='sess-a')
        self.assertEqual(denied['decision'], 'qualification_revoked')
        # Operations before the revocation remain attributable and valid.
        allowed = duty.evaluate_operation(self.db, {
            'op': 'launch', 'at': 7, 'actor': 'alice', 'session': 'sess-a',
            'site': 'site-1'})
        self.assertTrue(allowed['allowed'])


class HandoverChecklistTests(HandoverTestBase):
    def _open_with_pending_case(self, explicit_items=None, services=('svc-b',)):
        self.seed_basic_duty()
        self.R(event_id='case-1', op='case', id='rc-1', service='svc-b',
               cause='customer visible')
        return self.D(op='handoff_open', id='ho-1', outgoing='shift-a',
                      incoming='shift-b', at=9, site='site-1',
                      services=list(services), items=explicit_items)

    def test_omitted_rollback_case_blocks_completion_until_received(self):
        # Submitted checklist deliberately lists no items and omits rc-1.
        opened = self._open_with_pending_case(explicit_items=[])
        self.assertEqual(opened['decision'], 'handoff_opened')
        self.D(op='site_confirm', id='confirm-1', handoff='ho-1', session='session-b',
               actor='bob', at=10, nodes=[])
        self.D(op='sign', id='sig-out', handoff='ho-1', actor='alice',
               role='outgoing', at=10)
        self.D(op='sign', id='sig-in', handoff='ho-1', actor='bob',
               role='incoming', at=10)
        blocked = self.D(op='handoff_complete', handoff='ho-1', at=10)
        self.assertEqual(blocked['decision'], 'handoff_incomplete')
        self.assertIn('rollback_case_not_handed_over', blocked['problems'])
        self.assertEqual(blocked.get('missing_rollback'), ['rc-1'])

        self.D(op='handoff_add_item', id='item-rc', handoff='ho-1',
               kind='rollback_case', ref='rc-1')
        acc = self.D(op='accept_item', handoff='ho-1', kind='rollback_case',
                     ref='rc-1', actor='bob', at=10)
        self.assertEqual(acc['decision'], 'handoff_item_accepted')
        done = self.D(event_id='complete-real', op='handoff_complete',
                      handoff='ho-1', at=10)
        self.assertEqual(done['decision'], 'handover_completed')

    def test_rollback_case_appearing_after_snapshot_cannot_be_hidden(self):
        self.seed_basic_duty()
        self.D(op='handoff_open', id='ho-1', outgoing='shift-a', incoming='shift-b',
               at=9, site='site-1', services=['svc-b'])
        # A new customer-visible case lands while the handover is open.
        self.R(event_id='case-late', op='case', id='rc-late', service='svc-b')
        self.D(op='site_confirm', id='confirm-late', handoff='ho-1', session='session-b',
               actor='bob', at=10, nodes=[])
        self.D(op='sign', id='sig-out-late', handoff='ho-1', actor='alice',
               role='outgoing', at=10)
        self.D(op='sign', id='sig-in-late', handoff='ho-1', actor='bob',
               role='incoming', at=10)
        blocked = self.D(event_id='complete-blocked', op='handoff_complete',
                         handoff='ho-1', at=10)
        self.assertEqual(blocked['decision'], 'handoff_incomplete')
        self.assertEqual(blocked.get('missing_rollback'), ['rc-late'])
        self.D(op='handoff_add_item', id='item-rc-late', handoff='ho-1',
               kind='rollback_case', ref='rc-late')
        self.D(op='accept_item', handoff='ho-1', kind='rollback_case', ref='rc-late',
               actor='bob', at=10)
        done = self.D(event_id='complete-ok', op='handoff_complete',
                      handoff='ho-1', at=10)
        self.assertEqual(done['decision'], 'handover_completed')

    def test_duplicate_completion_cannot_create_two_duty_officers(self):
        self.seed_basic_duty()
        self.D(op='handoff_open', id='ho-1', outgoing='shift-a', incoming='shift-b', at=9)
        first = self.complete_standard_handoff(at=10)
        self.assertEqual(first['decision'], 'handover_completed')
        second = self.D(event_id='complete-2', op='handoff_complete',
                        handoff='ho-1', at=10)
        self.assertEqual(second['decision'], 'handoff_already_completed')
        # Replaying the exact first command returns its recorded result.
        again = duty.run(self.db, {'event_id': 'complete-1', 'op': 'handoff_complete',
                                   'handoff': 'ho-1', 'actor': 'bob', 'at': 10})
        self.assertEqual(again['decision'], 'handover_completed')
        at10 = duty.responsible_at(self.db, 'site-1', 10)
        self.assertEqual(at10['actor'], 'bob')

    def test_site_confirmation_needs_independent_session(self):
        self.seed_basic_duty(b_independent=False)
        self.D(op='handoff_open', id='ho-1', outgoing='shift-a', incoming='shift-b', at=9)
        result = self.D(op='site_confirm', id='confirm-bad', handoff='ho-1',
                        session='session-b', actor='bob', at=9, nodes=[])
        self.assertEqual(result['decision'], 'independent_session_required')

    def test_completion_invalidates_outgoing_session(self):
        self.seed_basic_duty()
        self.D(op='handoff_open', id='ho-1', outgoing='shift-a', incoming='shift-b', at=9)
        self.complete_standard_handoff(at=10)
        row = one(self.db, 'SELECT state, ended_at FROM site_sessions WHERE id=?',
                  ('session-a',))
        self.assertEqual(row['state'], 'closed')
        self.assertEqual(row['ended_at'], 10)
        _, err = duty.session_at(self.db, 'session-a', 'alice', 'site-1', 11)
        self.assertEqual(err, 'duty_session_closed')

    def test_legacy_loose_ack_keeps_original_contract(self):
        self.D(op='shift', id='a', actor='alice', site='s', starts=1, ends=5,
               qualification='startup')
        self.D(op='shift', id='b', actor='bob', site='s', starts=5, ends=10,
               qualification='startup')
        self.assertEqual(self.D(op='handoff', id='h', outgoing='a', incoming='b',
                                wave='w', at=5)['decision'], 'handoff_opened')
        self.assertEqual(self.D(op='ack', id='ack', handoff='h', actor='anyone',
                                at=5)['decision'], 'acknowledged')

    def test_armed_control_rejects_anonymous_operations(self):
        self.D(op='arm_control', at=0, scope='site-1')
        verdict = duty.evaluate_operation(self.db, {
            'op': 'launch', 'at': 5, 'site': 'site-1'})
        self.assertEqual(verdict['decision'], 'duty_identity_required')


class EnforcementAcrossSystemsTests(HandoverTestBase):
    def test_launch_retry_and_rollback_check_duty(self):
        self.seed_basic_duty()
        self.seed_wave()
        self.assertEqual(self.launch(at=5)['decision'], 'launched')
        # After Alice's end with no completed handover, every gated production
        # entry point is refused even with otherwise-valid business state.
        self.D(op='handoff_open', id='ho-1', outgoing='shift-a', incoming='shift-b', at=9)
        gap_launch = self.launch(event_id='l2', attempt='try-2', at=11, actor='bob')
        self.assertEqual(gap_launch['decision'], 'duty_gap')
        gap_retry = self.W(event_id='retry-gap', op='retry', old='try-1', at=11,
                           actor='bob', session='session-b', site='site-1')
        self.assertEqual(gap_retry['decision'], 'duty_gap')
        gap_rb = self.R(event_id='rb-gap', op='rollback', id='rc-1', at=11,
                        actor='bob', session='session-b', site='site-1')
        # rc-1 does not need to exist: the duty gate fires first in the open
        # unattended interval after Alice's end.
        self.assertEqual(gap_rb['decision'], 'duty_gap')

    def test_anonymous_legacy_payloads_remain_compatible(self):
        # No actor/session: the original 12-test contract is preserved.
        self.seed_wave()
        result = self.W(event_id='legacy-launch', op='launch', id='try-l',
                        wave='wave-svc-b', slot='slot-b', token='token-svc-b',
                        at=5, expires=90)
        self.assertEqual(result['decision'], 'launched')


class CrossShiftFixtureTests(HandoverTestBase):
    def load_fixture(self):
        results = []
        for step in load_envelope():
            results.append(MODULES[step['subsystem']].run(self.db, step['payload']))
        return results

    def test_fixture_inventory_locks_all_open_work(self):
        results = self.load_fixture()
        self.assertEqual(results[-1]['decision'], 'handoff_opened')
        refs = {i['ref'] for i in results[-1]['inventory']['items']}
        self.assertEqual(refs, {'try-db', 'try-b', 'wave-db', 'wave-b',
                                'rollback-b', 'customer-action-1'})
        self.assertEqual(results[-1]['next_deadline']['ref'], 'try-b')
        self.assertEqual(results[-1]['next_deadline']['at'], 19)
        self.assertTrue(replay(self.path)['verified'])

    def _accept_all(self, at):
        ids = [r['id'] for r in self.db.execute(
            "SELECT id FROM handoff_items WHERE handoff_id='handoff-1'").fetchall()]
        for iid in ids:
            r = self.D(op='accept_item', handoff='handoff-1', item=iid,
                       actor='bob', at=at)
            self.assertEqual(r['decision'], 'handoff_item_accepted', iid)

    def test_path_a_supervised_extension_alice_recovers_early(self):
        self.load_fixture()
        ext = self.D(op='extension', id='ext-1', shift='shift-a', supervisor='zhao',
                     at=9, until=14, officer_signed_at=9, supervisor_signed_at=9,
                     reason='finish upstream database verification',
                     bound_refs=['try-b', 'try-db', 'wave-b', 'wave-db'])
        self.assertEqual(ext['decision'], 'extension_granted')

        # Bob cannot act before he has accepted the handover.
        early = self.R(event_id='pa-bob-early', op='probe', id='pa-be',
                       attempt='try-b', at=10, healthy=True,
                       actor='bob', session='session-b')
        self.assertEqual(early['decision'], 'not_responsible_officer')

        # Alice, inside the signed supervised extension, closes the database
        # verification and then the B startup.
        for pid, at in (('pa-dbp1', 10), ('pa-dbp2', 11)):
            self.assertEqual(self.R(event_id=pid, op='probe', id=pid,
                                    attempt='try-db', at=at, healthy=True,
                                    actor='alice', session='session-a')['decision'], 'sampled')
        self.assertEqual(self.R(event_id='pa-dbpub', op='publish', id='receipt-db-2',
                                attempt='try-db', at=11,
                                actor='alice', session='session-a')['decision'], 'published')
        for pid, at in (('pa-bp1', 11), ('pa-bp2', 12)):
            self.assertEqual(self.R(event_id=pid, op='probe', id=pid,
                                    attempt='try-b', at=at, healthy=True,
                                    actor='alice', session='session-a')['decision'], 'sampled')
        pub_b = self.R(event_id='pa-bpub', op='publish', id='receipt-b-2',
                       attempt='try-b', at=12, actor='alice', session='session-a')
        self.assertEqual(pub_b['decision'], 'published')
        recovery_at = one(self.db,
                          "SELECT published_at FROM readiness_receipts WHERE id='receipt-b-2'")['published_at']
        self.assertEqual(recovery_at, 12)

        # The pending customer rollback is still alive and is handed over, not hidden.
        self.assertEqual(one(self.db, "SELECT status FROM rollback_cases WHERE id='rollback-b'")['status'],
                         'pending')
        self._accept_all(13)
        self.assertEqual(self.D(op='site_confirm', id='pa-conf', handoff='handoff-1',
                                session='session-b', actor='bob', at=13,
                                nodes=['node-db', 'node-b'])['decision'],
                         'site_confirmation_recorded')
        self.D(op='sign', id='pa-sig-a', handoff='handoff-1', actor='alice',
               role='outgoing', at=13)
        self.D(op='sign', id='pa-sig-b', handoff='handoff-1', actor='bob',
               role='incoming', at=13)
        done = self.D(event_id='pa-complete', op='handoff_complete',
                      handoff='handoff-1', at=13)
        self.assertEqual(done['decision'], 'handover_completed')

        tl = duty.timeline(self.db, 'site-1', 0, 24)
        self.assertFalse([s for s in tl if s['status'] != 'responsible'])
        self.assertEqual(duty.responsible_at(self.db, 'site-1', 11)['basis'], 'extension')
        self.assertEqual(duty.responsible_at(self.db, 'site-1', 11)['supervisors'], ['zhao'])
        self.assertEqual(duty.responsible_at(self.db, 'site-1', 12)['actor'], 'alice')
        self.assertEqual(duty.responsible_at(self.db, 'site-1', 14)['actor'], 'bob')

        rec = duty.recompute(self.db)
        self.assertTrue(rec['verified'], rec['violations'])
        ops_b = [o for o in rec['operations'] if o.get('event_id') == 'pa-bpub']
        self.assertEqual(ops_b[0]['actor'], 'alice')
        self.assertEqual(ops_b[0]['responsible'], 'alice')
        self.assertTrue(replay(self.path)['verified'])

    def test_path_b_pause_itemised_handover_bob_recovers_later(self):
        self.load_fixture()
        paused = self.D(op='pause', id='hold-b', at=9, actor='zhao', site='site-1',
                        target='try-b', reason='supervisor pause for itemised takeover',
                        event_id='pb-pause')
        self.assertEqual(paused['decision'], 'operation_paused_recorded')
        blocked = self.R(event_id='pb-probe-blocked', op='probe', id='pb-blocked',
                         attempt='try-b', at=10, healthy=True,
                         actor='alice', session='session-a')
        self.assertEqual(blocked['decision'], 'operation_paused')

        # Bob receives every listed item one by one, confirms physical nodes
        # from his independent session, and both sign.
        self._accept_all(10)
        self.assertEqual(self.D(op='site_confirm', id='pb-conf', handoff='handoff-1',
                                session='session-b', actor='bob', at=10,
                                nodes=['node-db', 'node-b'])['decision'],
                         'site_confirmation_recorded')
        self.D(op='sign', id='pb-sig-a', handoff='handoff-1', actor='alice',
               role='outgoing', at=10)
        self.D(op='sign', id='pb-sig-b', handoff='handoff-1', actor='bob',
               role='incoming', at=10)
        done = self.D(event_id='pb-complete', op='handoff_complete',
                      handoff='handoff-1', at=10)
        self.assertEqual(done['decision'], 'handover_completed')

        resume = self.D(op='resume', id='hold-b', at=10, actor='bob',
                        session='session-b', event_id='pb-resume')
        self.assertEqual(resume['decision'], 'operation_resumed')

        for pid, at in (('pb-dbp1', 11), ('pb-dbp2', 12)):
            self.R(event_id=pid, op='probe', id=pid, attempt='try-db', at=at,
                   healthy=True, actor='bob', session='session-b')
        self.R(event_id='pb-dbpub', op='publish', id='pb-receipt-db',
               attempt='try-db', at=12, actor='bob', session='session-b')
        for pid, at in (('pb-bp1', 13), ('pb-bp2', 14)):
            self.R(event_id=pid, op='probe', id=pid, attempt='try-b', at=at,
                   healthy=True, actor='bob', session='session-b')
        pub_b = self.R(event_id='pb-bpub', op='publish', id='pb-receipt-b',
                       attempt='try-b', at=15, actor='bob', session='session-b')
        self.assertEqual(pub_b['decision'], 'published')
        recovery_at = one(self.db,
                          "SELECT published_at FROM readiness_receipts WHERE id='pb-receipt-b'")['published_at']
        self.assertEqual(recovery_at, 15)

        tl = duty.timeline(self.db, 'site-1', 0, 24)
        self.assertFalse([s for s in tl if s['status'] != 'responsible'], tl)
        self.assertEqual(duty.responsible_at(self.db, 'site-1', 9)['actor'], 'alice')
        self.assertEqual(duty.responsible_at(self.db, 'site-1', 10)['actor'], 'bob')

        rec = duty.recompute(self.db)
        self.assertTrue(rec['verified'], rec['violations'])
        op = [o for o in rec['operations'] if o.get('event_id') == 'pb-bpub'][0]
        self.assertEqual(op['actor'], 'bob')
        self.assertEqual(op['responsible'], 'bob')
        self.assertTrue(replay(self.path)['verified'])


class ExtensionGuardTests(HandoverTestBase):
    def test_extension_requires_both_signatures_and_authorised_supervisor(self):
        self.seed_basic_duty()
        self.D(op='handoff_open', id='ho-1', outgoing='shift-a', incoming='shift-b', at=9)
        unsigned = self.D(event_id='ext-unsigned', op='extension', id='ext-u',
                          shift='shift-a', supervisor='zhao', at=9, until=14)
        self.assertEqual(unsigned['decision'], 'extension_signatures_required')
        stranger = self.D(event_id='ext-stranger', op='extension', id='ext-s',
                          shift='shift-a', supervisor='mallory', at=9, until=14,
                          officer_signed_at=9, supervisor_signed_at=9)
        self.assertEqual(stranger['decision'], 'supervisor_not_authorized')
        valid = self.D(op='extension', id='ext-ok', shift='shift-a', supervisor='zhao',
                       at=9, until=14, officer_signed=True, supervisor_signed=True)
        self.assertEqual(valid['decision'], 'extension_granted')
        # The unsigned proposal created no coverage interval.
        self.assertEqual(duty.responsible_at(self.db, 'site-1', 11)['basis'], 'extension')
        ext_rows = self.db.execute(
            "SELECT id FROM shift_extensions WHERE state='active'").fetchall()
        self.assertEqual([r[0] for r in ext_rows], ['ext-ok'])

    def test_emergency_requires_designated_supervisor(self):
        self.seed_cert()
        self.D(op='shift', id='sa', actor='alice', site='site-1', starts=0, ends=10)
        denied = self.D(op='emergency', id='em-x', actor='bob', site='site-1',
                        supervisor='mallory', at=9, until=12)
        self.assertEqual(denied['decision'], 'supervisor_not_authorized')


class ConcurrentLaunchTests(HandoverTestBase):
    def test_concurrent_launches_never_exceed_slot_capacity(self):
        self.seed_basic_duty(a_end=120, b_start=130, b_end=200)
        service = 'svc-c'
        self.C(op='revision', id=service, generation=1)
        self.C(op='seal', id=service, generation=1)
        self.C(op='token', id='token-c', service=service, generation=1,
               expires=300, at=0)
        self.D(op='equipment', id='eq-c', node='node-c', site='site-1')
        self.W(op='slot', id='slot-c', node='node-c', capacity=1,
               generation=1, healthy=True)
        # Eight distinct planned waves racing for the same capacity-1 slot.
        for i in range(8):
            self.W(op='plan', id=f'wave-c-{i}', service=service,
                   generation=1, deadline=200)
        barrier = threading.Barrier(8)

        def worker(i):
            db = connect(self.path)
            try:
                barrier.wait(timeout=10)
                r = wave.run(db, {
                    'event_id': f'conc-event-{i}', 'op': 'launch',
                    'id': f'try-conc-{i}', 'wave': f'wave-c-{i}', 'slot': 'slot-c',
                    'token': 'token-c', 'at': 50, 'expires': 90,
                    'actor': 'alice', 'session': 'session-a', 'site': 'site-1'})
                return r['decision']
            finally:
                db.close()

        with ThreadPoolExecutor(max_workers=8) as pool:
            decisions = list(pool.map(worker, range(8)))
        self.assertEqual(decisions.count('launched'), 1, decisions)
        self.assertEqual(decisions.count('slot_full'), 7, decisions)
        running = one(self.db,
                     "SELECT COUNT(*) n FROM start_attempts WHERE slot='slot-c' AND status='running'")['n']
        self.assertEqual(running, 1)
        self.assertTrue(replay(self.path)['verified'])

    def test_duplicate_concurrent_completion_installs_one_officer(self):
        self.seed_basic_duty()
        self.D(op='handoff_open', id='ho-1', outgoing='shift-a', incoming='shift-b', at=9)
        self.D(op='site_confirm', id='confirm-race', handoff='ho-1', session='session-b',
               actor='bob', at=10, nodes=[])
        self.D(op='sign', id='sig-race-a', handoff='ho-1', actor='alice',
               role='outgoing', at=10)
        self.D(op='sign', id='sig-race-b', handoff='ho-1', actor='bob',
               role='incoming', at=10)
        barrier = threading.Barrier(2)

        def complete(event_id):
            db = connect(self.path)
            try:
                barrier.wait(timeout=10)
                return duty.run(db, {'event_id': event_id, 'op': 'handoff_complete',
                                     'handoff': 'ho-1', 'actor': 'bob', 'at': 10})['decision']
            finally:
                db.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            decisions = list(pool.map(complete, ('comp-race-1', 'comp-race-2')))
        self.assertEqual(decisions.count('handover_completed'), 1, decisions)
        self.assertEqual(decisions.count('handoff_already_completed'), 1, decisions)
        self.assertEqual(duty.responsible_at(self.db, 'site-1', 10)['actor'], 'bob')
        conflicts = [s for s in duty.timeline(self.db, 'site-1', 0, 22)
                     if s['status'] == 'conflict']
        self.assertEqual(conflicts, [])
        self.assertTrue(replay(self.path)['verified'])


class RecomputeUniquenessTests(HandoverTestBase):
    def test_every_instant_has_at_most_one_responsible_officer(self):
        self.seed_basic_duty(b_start=8)
        self.D(op='handoff_open', id='ho-1', outgoing='shift-a', incoming='shift-b', at=5)
        self.complete_standard_handoff(at=9)
        segments = duty.timeline(self.db, 'site-1', 0, 22)
        covered = 0
        for seg in segments:
            self.assertIn(seg['status'], ('responsible', 'gap'))
            if seg['status'] == 'responsible':
                self.assertTrue(seg['actor'])
                covered += seg['end'] - seg['start']
        # Completion at 9 relieves Alice early; Bob is unique from t=9.
        self.assertEqual(duty.responsible_at(self.db, 'site-1', 8)['actor'], 'alice')
        self.assertEqual(duty.responsible_at(self.db, 'site-1', 9)['actor'], 'bob')
        self.assertGreater(covered, 0)
        rec = duty.recompute(self.db)
        self.assertTrue(rec['verified'], rec['violations'])


if __name__ == '__main__':
    unittest.main()
