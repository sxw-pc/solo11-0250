"""Regression tests for verifiable operator erasure.

Scenario: operators R1 and R2 both worked the cold-start probe anomaly on
attempt A1 (two probes, one readiness receipt).  R1's records are under a
court incident hold; R2 asked to be erased.  R2's contact details live in
operator_records, in shared event payloads and in one offline backup.
"""

import json
import tempfile
import unittest
from pathlib import Path

from cloudcourt import MODULES, contract, ready, wave
from cloudcourt import operator_erasure as erasure
from cloudcourt.operator_record import record
from cloudcourt.operator_retention import provisional_erasure
from cloudcourt.store import connect, many, one
from cloudcourt.store import replay as store_replay

PHONE_A = 'phone-A-555'
PHONE_B = 'phone-B-777'
BADGE_B = 'EMP-B-42'


def build_attempt(db):
    """Attempt A1: two probes signed by two operators, then a receipt."""
    contract.run(db, {'event_id': 'c1', 'op': 'revision', 'id': 'svc',
                      'generation': 1})
    contract.run(db, {'event_id': 'c2', 'op': 'seal', 'id': 'svc',
                      'generation': 1})
    contract.run(db, {'event_id': 'c3', 'op': 'token', 'id': 'tok',
                      'service': 'svc', 'generation': 1, 'expires': 500,
                      'at': 10})
    wave.run(db, {'event_id': 'w1', 'op': 'slot', 'id': 'slot-1',
                  'node': 'node-1', 'capacity': 2, 'generation': 1,
                  'healthy': True})
    wave.run(db, {'event_id': 'w2', 'op': 'plan', 'id': 'wave-1',
                  'service': 'svc', 'generation': 1, 'deadline': 400})
    wave.run(db, {'event_id': 'w3', 'op': 'launch', 'id': 'A1',
                  'wave': 'wave-1', 'slot': 'slot-1', 'token': 'tok',
                  'at': 100, 'expires': 300})
    ready.run(db, {'event_id': 'e1', 'op': 'probe', 'id': 'p1',
                   'attempt': 'A1', 'at': 110, 'healthy': True,
                   'actor': 'operator-A', 'contact': PHONE_A})
    ready.run(db, {'event_id': 'e2', 'op': 'probe', 'id': 'p2',
                   'attempt': 'A1', 'at': 120, 'healthy': True,
                   'actor': 'operator-B', 'contact': PHONE_B,
                   'badge': BADGE_B})
    ready.run(db, {'event_id': 'e3', 'op': 'publish', 'id': 'rcpt-1',
                   'attempt': 'A1', 'at': 130, 'actor': 'operator-A',
                   'contact': PHONE_A})


def build_records(db):
    record(db, 'R1', 'operator-A', 'A1', PHONE_A)
    record(db, 'R2', 'operator-B', 'A1', PHONE_B)
    erasure.file_incident_ticket(db, 'T1', 'A1', 'operator-A', PHONE_A,
                                 'cold start anomaly triage')
    erasure.file_incident_ticket(db, 'T2', 'A1', 'operator-B', PHONE_B,
                                 'cold start anomaly triage')
    erasure.incident_hold(db, 'H1', 'R1', 'case-7', 'court-9', 100, 200,
                          9999)


def all_text(db):
    parts = []
    for (table,) in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name "
            "NOT LIKE 'sqlite_%'"):
        for row in db.execute(f'SELECT * FROM {table}'):
            parts.append(repr(tuple(row)))
    return '\n'.join(parts)


class ErasureTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'ops.sqlite')

    def tearDown(self):
        self.tmp.cleanup()

    # -- hold scoping ------------------------------------------------------

    def test_hold_scoped_to_case_operator_and_probe_interval(self):
        db = connect(':memory:')
        build_attempt(db)
        build_records(db)
        hold = one(db, "SELECT * FROM operator_holds WHERE id='H1'")
        self.assertEqual(hold['record_id'], 'R1')
        self.assertEqual(hold['case_id'], 'case-7')
        self.assertEqual(hold['authority'], 'court-9')
        self.assertEqual((hold['probe_from'], hold['probe_until']),
                         (100, 200))
        self.assertEqual(hold['expires'], 9999)
        # Working the same attempt does not freeze R2's data.
        self.assertEqual(provisional_erasure(db), ['R2'])
        self.assertEqual(erasure.erasure_plan(db), ['R2'])
        db.close()

    def test_held_operator_cannot_be_erased(self):
        db = connect(':memory:')
        build_attempt(db)
        build_records(db)
        refused = erasure.request_erasure(db, 'req-x', 'R1', 'case-8',
                                          'court-9', 'escrow')
        self.assertEqual(refused['decision'], 'held_by_incident')
        db.close()

    # -- escrow arrangement -------------------------------------------------

    def test_escrow_erasure_full_lifecycle(self):
        db = connect(self.path)
        build_attempt(db)
        build_records(db)
        erasure.register_backup(db, 'bk-1', 'A1', 'ops',
                                json.dumps({'contact': PHONE_B,
                                            'badge': BADGE_B}))
        self.assertEqual(erasure.erasure_plan(db), ['R2'])
        result = erasure.erase_operator(db, 'req-1', 'R2', 'case-8',
                                        'court-9', 'escrow', 'escrow-agent',
                                        at=500)
        self.assertEqual(result['decision'], 'erased')
        self.assertEqual(result['events_sealed'], 3)
        self.assertEqual(result['backups_destroyed'], 1)

        # R2's online rows are irreversible digests; R1's held row is intact.
        r2 = one(db, "SELECT * FROM operator_records WHERE id='R2'")
        self.assertEqual(r2['state'], 'erased')
        self.assertTrue(r2['contact'].startswith('digest-'))
        self.assertTrue(r2['actor'].startswith('erased-'))
        r1 = one(db, "SELECT * FROM operator_records WHERE id='R1'")
        self.assertEqual(r1['contact'], PHONE_A)
        self.assertEqual(r1['state'], 'active')
        t2 = one(db, "SELECT * FROM incident_tickets WHERE id='T2'")
        self.assertTrue(t2['contact'].startswith('digest-'))
        t1 = one(db, "SELECT * FROM incident_tickets WHERE id='T1'")
        self.assertEqual(t1['contact'], PHONE_A)

        # No phone, employee id or reversible trace of R2 anywhere,
        # including event payloads and the destroyed backup.
        dump = all_text(db)
        self.assertNotIn(PHONE_B, dump)
        self.assertNotIn(BADGE_B, dump)
        e2 = one(db, "SELECT payload FROM events WHERE event_id='e2'")
        self.assertNotIn('operator-B', e2['payload'])
        self.assertEqual(json.loads(e2['payload'])['quarantined'], 'escrow')
        self.assertEqual(
            one(db, "SELECT state, content FROM backup_copies WHERE "
                    "id='bk-1'"),
            {'state': 'destroyed', 'content': None})

        # Startup business facts and proofs survive the erasure.
        self.assertEqual(
            one(db, "SELECT status FROM readiness_receipts WHERE "
                    "id='rcpt-1'")['status'], 'published')
        self.assertEqual(
            one(db, "SELECT status FROM start_attempts WHERE id='A1'")
            ['status'], 'complete')
        self.assertEqual(
            len(many(db, 'SELECT * FROM probe_samples WHERE attempt=?',
                     ('A1',))), 2)
        proof = erasure.attempt_order_proof(db, 'A1')
        self.assertEqual(proof['probe_order'], ['p1', 'p2'])
        db.close()

        # The operational store still replays every startup verdict.
        rep = erasure.replay(self.path)
        self.assertTrue(rep['verified'], rep)
        self.assertEqual(rep['sealed'], 3)

        # The court reviews R1 through the escrow custodian, in order.
        db = connect(self.path)
        review = erasure.court_review(db, 'case-7', 'escrow-agent')
        self.assertEqual(review['decision'], 'court_review')
        self.assertEqual(review['mode'], 'escrow')
        opened = {e['event_id']: e for e in review['events']}
        self.assertEqual(opened['e1']['payload']['contact'], PHONE_A)
        self.assertEqual(opened['e1']['operator'], 'operator-A')
        self.assertIsNone(opened['e2']['payload'])
        db.close()
        seq = erasure.court_replay(self.path, 'case-7', 'escrow-agent')
        self.assertEqual(seq['sequence'], ['e1', 'e3'])
        self.assertEqual(seq['operators'], ['operator-A'])

    # -- sealed arrangement -------------------------------------------------

    def test_sealed_mode_separates_identity_keys(self):
        db = connect(self.path)
        build_attempt(db)
        build_records(db)
        result = erasure.erase_operator(db, 'req-1', 'R2', 'case-8',
                                        'court-9', 'sealed', 'court', at=500)
        self.assertEqual(result['decision'], 'erased')

        keys = {k['key_id']: k
                for k in many(db, 'SELECT * FROM key_custody')}
        case_key = keys['case-case-7-A1']
        self.assertEqual(case_key['holder'], 'court')
        self.assertEqual(case_key['state'], 'active')
        self.assertIsNotNone(case_key['material'])
        erased_key = keys['erased-A1']
        self.assertEqual(erased_key['state'], 'destroyed')
        self.assertIsNone(erased_key['material'])

        quarantine = {r['event_id']: r
                      for r in many(db, 'SELECT * FROM event_quarantine')}
        self.assertEqual(quarantine['e1']['key_id'], 'case-case-7-A1')
        self.assertEqual(quarantine['e2']['key_id'], 'erased-A1')
        self.assertEqual(quarantine['e2']['erased_operator'], 'operator-B')

        # The court opens R1's originals with the case key it controls;
        # R2's wrapping key is gone, so those bytes stay sealed.
        review = erasure.court_review(db, 'case-7', 'court')
        self.assertEqual(review['decision'], 'court_review')
        opened = {e['event_id']: e for e in review['events']}
        self.assertEqual(opened['e1']['payload']['actor'], 'operator-A')
        self.assertIsNone(opened['e2']['payload'])
        # Nobody else holds the case key.
        denied = erasure.court_review(db, 'case-7', 'escrow-agent')
        self.assertEqual(denied['decision'], 'key_unavailable')
        # The platform keeps only the non-identifying order proof.
        proof = erasure.attempt_order_proof(db, 'A1')
        self.assertEqual(proof['probe_order'], ['p1', 'p2'])
        self.assertNotIn(PHONE_B, all_text(db))
        db.close()
        self.assertTrue(erasure.replay(self.path)['verified'])

    def test_modes_are_not_interchangeable(self):
        db = connect(':memory:')
        build_attempt(db)
        build_records(db)
        record(db, 'R3', 'operator-C', 'A1', 'phone-C-999')
        first = erasure.erase_operator(db, 'req-1', 'R2', 'case-8',
                                       'court-9', 'escrow', 'escrow-agent')
        self.assertEqual(first['decision'], 'erased')
        conflict = erasure.erase_operator(db, 'req-2', 'R3', 'case-8',
                                          'court-9', 'sealed', 'court')
        self.assertEqual(conflict['decision'], 'mode_conflict')
        self.assertEqual(conflict['committed'], 'escrow')
        self.assertEqual(conflict['requested'], 'sealed')
        r3 = one(db, "SELECT * FROM operator_records WHERE id='R3'")
        self.assertEqual(r3['state'], 'active')
        db.close()

    # -- custody verification ------------------------------------------------

    def test_receipt_waits_for_key_and_backup_custody(self):
        db = connect(':memory:')
        build_attempt(db)
        build_records(db)
        erasure.register_key(db, 'key-ext-1', 'A1', 'operator-B',
                             'identity-wrap-copy', 'partner', 'deadbeef')
        erasure.register_backup(db, 'bk-ext', 'A1', 'partner',
                                json.dumps({'contact': PHONE_B}))
        erasure.request_erasure(db, 'req-1', 'R2', 'case-8', 'court-9',
                                'escrow')
        blocked = erasure.execute_erasure(db, 'req-1', 'escrow-agent')
        self.assertEqual(blocked['decision'], 'custody_not_verified')
        self.assertEqual([k['key_id'] for k in blocked['pending']['keys']],
                         ['key-ext-1'])
        self.assertEqual([b['id'] for b in blocked['pending']['backups']],
                         ['bk-ext'])
        # No receipt, and R2's contact is not scrubbed while copies survive.
        self.assertIsNone(one(db, 'SELECT * FROM erasure_receipts'))
        self.assertEqual(
            one(db, "SELECT contact FROM operator_records WHERE id='R2'")
            ['contact'], PHONE_B)

        erasure.destroy_key(db, 'key-ext-1')
        erasure.destroy_backup(db, 'bk-ext')
        done = erasure.execute_erasure(db, 'req-1', 'escrow-agent')
        self.assertEqual(done['decision'], 'erased')
        self.assertIsNotNone(
            one(db, "SELECT * FROM erasure_receipts WHERE "
                    "id='receipt-req-1'"))
        self.assertNotIn(PHONE_B, all_text(db))
        db.close()

    # -- post-erasure guards --------------------------------------------------

    def test_backup_restore_never_reidentifies(self):
        db = connect(self.path)
        build_attempt(db)
        build_records(db)
        erasure.register_backup(db, 'bk-1', 'A1', 'ops',
                                json.dumps({'contact': PHONE_B}))
        erasure.erase_operator(db, 'req-1', 'R2', 'case-8', 'court-9',
                               'escrow', 'escrow-agent')
        # The destroyed copy is gone for good.
        self.assertEqual(erasure.restore_backup(db, 'bk-1')['decision'],
                         'backup_destroyed')
        # A pre-erasure copy resurfacing from an offline drive is
        # quarantined, not merged back into operational queries.
        erasure.register_backup(db, 'bk-2', 'A1', 'ops',
                                json.dumps({'contact': PHONE_B}))
        self.assertEqual(erasure.restore_backup(db, 'bk-2')['decision'],
                         'quarantined_not_restored')
        self.assertNotIn(PHONE_B, all_text(db))
        db.close()
        self.assertTrue(erasure.replay(self.path)['verified'])

    def test_late_probe_after_erasure_stays_proof_only(self):
        db = connect(self.path)
        build_attempt(db)
        build_records(db)
        erasure.erase_operator(db, 'req-1', 'R2', 'case-8', 'court-9',
                               'escrow', 'escrow-agent')
        res = erasure.record_late_probe(db, 'p3', 'A1', 140, True)
        self.assertEqual(res['decision'], 'recorded_as_proof')
        self.assertIsNone(one(db, "SELECT * FROM probe_samples WHERE "
                                  "id='p3'"))
        proof = one(db, "SELECT * FROM probe_fact_proofs WHERE "
                        "probe_id='p3'")
        self.assertIsNotNone(proof)
        self.assertEqual(proof['basis'], 'late')
        db.close()
        self.assertTrue(erasure.replay(self.path)['verified'])

        # Without an erasure the same helper records an ordinary sample.
        db = connect(':memory:')
        build_attempt(db)
        self.assertEqual(
            erasure.record_late_probe(db, 'p9', 'A1', 140, True)['decision'],
            'sampled')
        self.assertIsNotNone(one(db, "SELECT * FROM probe_samples WHERE "
                                     "id='p9'"))
        db.close()

    def test_repeated_erasure_cannot_resurrect(self):
        db = connect(':memory:')
        build_attempt(db)
        build_records(db)
        first = erasure.erase_operator(db, 'req-1', 'R2', 'case-8',
                                       'court-9', 'escrow', 'escrow-agent')
        self.assertEqual(first['decision'], 'erased')
        again = erasure.request_erasure(db, 'req-2', 'R2', 'case-8',
                                        'court-9', 'escrow')
        self.assertEqual(again['decision'], 'already_erased')
        rerun = erasure.execute_erasure(db, 'req-1', 'escrow-agent')
        self.assertEqual(rerun['decision'], 'request_unavailable')
        self.assertEqual(len(many(db, 'SELECT * FROM erasure_receipts')), 1)
        r2 = one(db, "SELECT * FROM operator_records WHERE id='R2'")
        self.assertTrue(r2['contact'].startswith('digest-'))
        self.assertEqual(erasure.erasure_plan(db), [])
        db.close()

    def test_narrowed_hold_keeps_scope_and_review(self):
        db = connect(self.path)
        build_attempt(db)
        build_records(db)
        narrowed = erasure.narrow_hold(db, 'H1', 105, 115)
        self.assertEqual(narrowed['decision'], 'hold_narrowed')
        hold = one(db, "SELECT * FROM operator_holds WHERE id='H1'")
        self.assertEqual((hold['probe_from'], hold['probe_until']),
                         (105, 115))
        self.assertEqual(hold['case_id'], 'case-7')
        self.assertEqual(hold['authority'], 'court-9')
        self.assertEqual(erasure.narrow_hold(db, 'H1', 200, 100)['decision'],
                         'invalid_interval')
        self.assertEqual(erasure.narrow_hold(db, 'H-missing', 1, 2)
                         ['decision'], 'hold_unavailable')
        # The narrowed hold still shields R1 and only R1.
        self.assertEqual(erasure.erasure_plan(db), ['R2'])
        self.assertEqual(
            erasure.request_erasure(db, 'req-x', 'R1', 'case-8', 'court-9',
                                    'escrow')['decision'],
            'held_by_incident')
        # Erasing R2 under the narrowed hold still works, and the court
        # keeps its review of R1's signing order.
        done = erasure.erase_operator(db, 'req-1', 'R2', 'case-8',
                                      'court-9', 'escrow', 'escrow-agent')
        self.assertEqual(done['decision'], 'erased')
        review = erasure.court_review(db, 'case-7', 'escrow-agent')
        self.assertEqual(review['decision'], 'court_review')
        db.close()
        self.assertTrue(erasure.replay(self.path)['verified'])

    # -- replay fences ---------------------------------------------------------

    def test_legacy_replay_cannot_cross_quarantine(self):
        db = connect(self.path)
        build_attempt(db)
        build_records(db)
        erasure.erase_operator(db, 'req-1', 'R2', 'case-8', 'court-9',
                               'escrow', 'escrow-agent')
        db.close()
        fenced = erasure.legacy_replay(self.path)
        self.assertEqual(fenced['decision'], 'legacy_replay_fenced')
        self.assertFalse(fenced['verified'])
        # The store-level replay entry point routes through the
        # erasure-aware path instead of re-executing raw payloads.
        rep = store_replay(self.path)
        self.assertTrue(rep['verified'], rep)
        # A hand-rolled raw replay cannot resurrect R2 either: the stored
        # payloads no longer carry the identity.
        db = connect(self.path)
        fresh = connect(':memory:')
        for event in many(db, 'SELECT * FROM events ORDER BY seq'):
            MODULES[event['subsystem']].run(fresh,
                                            json.loads(event['payload']))
        dump = all_text(fresh)
        self.assertNotIn(PHONE_B, dump)
        self.assertNotIn(BADGE_B, dump)
        self.assertNotIn('operator-B', dump)
        fresh.close()
        db.close()

    def test_business_facts_and_audit_survive_erasure(self):
        db = connect(self.path)
        build_attempt(db)
        build_records(db)
        erasure.erase_operator(db, 'req-1', 'R2', 'case-8', 'court-9',
                               'escrow', 'escrow-agent')
        # The customer-facing readiness fact is still queryable.
        receipt = one(db, "SELECT * FROM readiness_receipts WHERE "
                          "id='rcpt-1'")
        self.assertEqual(receipt['status'], 'published')
        self.assertEqual(receipt['service'], 'svc')
        # Health samples and completed audit rows are kept.
        samples = many(db, 'SELECT * FROM probe_samples WHERE attempt=? '
                       'ORDER BY at', ('A1',))
        self.assertEqual([s['id'] for s in samples], ['p1', 'p2'])
        self.assertTrue(all(s['healthy'] for s in samples))
        self.assertEqual(
            one(db, "SELECT status FROM boot_waves WHERE id='wave-1'")
            ['status'], 'complete')
        db.close()
        rep = erasure.replay(self.path)
        self.assertTrue(rep['verified'], rep)
        self.assertEqual(rep['decisions'], 9)


if __name__ == '__main__':
    unittest.main()
