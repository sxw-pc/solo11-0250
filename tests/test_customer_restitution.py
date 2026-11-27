import unittest

from cloudcourt.store import connect, replay, one, many
from cloudcourt import customer_sla, customer_restitution as R
from cloudcourt.credit_draft import provisional


def wave(db, wave_id, service, status):
    """Event-sourced incident wave so replay rebuilds the loss window."""
    return R.run(db, {'event_id': 'wave-' + wave_id, 'op': 'incident_wave',
                      'id': wave_id, 'service': service, 'status': status})


class RestitutionCase:
    """Builder feeding the five evidence sources through idempotent events."""

    def __init__(self, db, sla_id, customer, service, min_healthy=1,
                 unit_credit=10):
        self.db = db
        self.sla = sla_id
        customer_sla.run(db, {'event_id': 'sla-' + sla_id,
                              'op': 'sla_subscribe', 'sla_id': sla_id,
                              'customer': customer, 'service': service,
                              'min_healthy': min_healthy,
                              'unit_credit': unit_credit})
        self.service = service
        self.n = 0

    def _id(self, prefix):
        self.n += 1
        return f'{prefix}-{self.n}'

    def instance(self, instance_id, node, accepted_at=1, revoked_at=None):
        rid = self._id('inst')
        self._run({'op': 'instance_accept', 'id': rid, 'sla': self.sla,
                   'instance_id': instance_id, 'node': node,
                   'accepted_at': accepted_at})
        if revoked_at is not None:
            self._run({'op': 'instance_revoke', 'id': rid,
                       'at': revoked_at})

    def recovery(self, at):
        self._run({'op': 'recovery_mark', 'id': self._id('rec'),
                   'sla': self.sla, 'service': self.service, 'at': at})

    def entry(self, order_key, client_request_id, amount=100,
              service_fee=10, received_at=10):
        return self._run({'op': 'entry_receipt', 'id': self._id('entry'),
                          'sla': self.sla, 'order_key': order_key,
                          'client_request_id': client_request_id,
                          'amount': amount, 'service_fee': service_fee,
                          'received_at': received_at})

    def confirm(self, order_key, signed_at=11):
        return self._run({'op': 'customer_confirmation',
                          'id': self._id('conf'), 'sla': self.sla,
                          'order_key': order_key, 'signed_at': signed_at})

    def order(self, order_key, seq, processed_at=12):
        return self._run({'op': 'platform_order', 'id': self._id('pord'),
                          'sla': self.sla, 'order_key': order_key,
                          'seq': seq, 'processed_at': processed_at})

    def charge(self, order_key, amount=100, charged_at=13):
        return self._run({'op': 'payer_charge', 'id': self._id('chg'),
                          'sla': self.sla, 'order_key': order_key,
                          'amount': amount, 'charged_at': charged_at})

    def adjudicate(self, case_id, wave_id):
        return self._run({'op': 'adjudicate', 'id': self._id('adj'),
                          'case_id': case_id, 'sla': self.sla,
                          'wave': wave_id})

    def _run(self, payload):
        payload['event_id'] = self._id('ev')
        return R.run(self.db, payload)


class CustomerRestitutionTest(unittest.TestCase):
    def setUp(self):
        self.db = connect(':memory:')

    def tearDown(self):
        self.db.close()

    # --- 甲：无损失败 -------------------------------------------------
    def test_a_lossless_failed_wave_pays_nothing_and_voids_draft(self):
        c = RestitutionCase(self.db, 'S-A', 'customer-A', 'billing',
                            min_healthy=2, unit_credit=50)
        wave(self.db, 'W1', 'billing', 'failed')
        # Two independently accepted instances stay healthy through recovery.
        c.instance('i-1', 'node-1', accepted_at=1)
        c.instance('i-2', 'node-2', accepted_at=1)
        # Orders keep completing across the failed wave.
        for seq, key in enumerate(('o1', 'o2'), start=1):
            c.entry(key, f'req-{key}', received_at=5 + seq)
            c.confirm(key)
            c.order(key, seq)
        c.recovery(at=50)
        self.assertEqual(50, provisional(self.db, 'D-A', 'S-A'))

        result = c.adjudicate('case-A', 'W1')
        self.assertEqual('no_loss', result['loss_state'])
        self.assertEqual(2, result['healthy_instances'])
        self.assertEqual(0, result['service_fee_due'])
        self.assertEqual(0, result['escrow_due'])
        draft = one(self.db, "SELECT state FROM credit_drafts WHERE id='D-A'")
        self.assertEqual('voided', draft['state'])
        self.assertEqual({'decision': 'settled_no_loss'},
                         R.run(self.db, {'event_id': 'e-settle-a',
                                         'op': 'settle', 'case_id': 'case-A'}))

    def test_replica_below_floor_with_lost_order_is_loss_confirmed(self):
        c = RestitutionCase(self.db, 'S-A2', 'customer-A2', 'billing',
                            min_healthy=2, unit_credit=50)
        wave(self.db, 'W1b', 'billing', 'failed')
        c.instance('i-1', 'node-1')          # only one of two contracted
        c.entry('o1', 'req-o1')
        c.confirm('o1')
        c.recovery(at=50)
        result = c.adjudicate('case-A2', 'W1b')
        self.assertEqual('loss_confirmed', result['loss_state'])
        self.assertEqual(1, result['healthy_instances'])

    # --- 乙：有损完成 -------------------------------------------------
    def test_b_complete_wave_41_disputed_42_never_arrived(self):
        c = RestitutionCase(self.db, 'S-B', 'customer-B', 'orders',
                            min_healthy=1, unit_credit=10)
        wave(self.db, 'W2', 'orders', 'complete')
        c.instance('i-9', 'node-9')
        # Running instances only processed through sequence 40.
        for seq in range(1, 41):
            key = f'ord-{seq}'
            c.entry(key, f'req-{seq}')
            c.confirm(key)
            c.order(key, seq)
        # 41: customer holds a signed success receipt, platform has no order,
        # the payer already charged it.  No entry receipt exists, so the
        # local tables cannot resolve the executed-vs-precharged fork.
        c.confirm('ord-41')
        c.charge('ord-41', amount=100)
        # 42: never entered the platform at all (customer-side paperwork only).
        c.confirm('ord-42')
        c.recovery(at=80)
        # W2 is 'complete': wave-count bookkeeping drafts zero credit even
        # though real customer work is damaged — the flaw this module fixes.
        self.assertEqual(0, provisional(self.db, 'D-B', 'S-B'))

        result = c.adjudicate('case-B', 'W2')
        self.assertEqual('loss_confirmed', result['loss_state'])
        self.assertEqual('disputed',
                         one(self.db, '''SELECT classification FROM
                                         restitution_findings
                                         WHERE order_key='ord-41' ''')['classification'])
        self.assertEqual('never_arrived',
                         one(self.db, '''SELECT classification FROM
                                         restitution_findings
                                         WHERE order_key='ord-42' ''')['classification'])
        # 42 must never become platform debt; no money line references it.
        self.assertIsNone(one(self.db, '''SELECT 1 FROM bank_payments
                                          WHERE order_key='ord-42' '''))
        # 41's money is escrowed, not refunded outright: principal + fee.
        self.assertEqual(110, result['escrow_due'])
        self.assertEqual(0, result['principal_due'])
        self.assertEqual(0, result['service_fee_due'])
        draft = one(self.db, "SELECT state FROM credit_drafts WHERE id='D-B'")
        self.assertEqual('superseded', draft['state'])

    def test_duplicate_client_requests_collapse_to_one_order(self):
        c = RestitutionCase(self.db, 'S-D', 'customer-D', 'orders',
                            min_healthy=1, unit_credit=10)
        wave(self.db, 'W3', 'orders', 'failed')
        c.instance('i-d', 'node-d')
        first = c.entry('ord-7', 'request-same', received_at=10)
        self.assertEqual('entry_recorded', first['decision'])
        # Client retries the same request id under a new receipt id.
        retry = c.entry('ord-7-retry', 'request-same', received_at=11)
        self.assertEqual('duplicate_client_request', retry['decision'])
        self.assertEqual('ord-7', retry['order_key'])
        # A late server ack then completes the single business order.
        c.confirm('ord-7')
        c.order('ord-7', 7, processed_at=20)
        c.recovery(at=50)
        result = c.adjudicate('case-D', 'W3')
        self.assertEqual('no_loss', result['loss_state'])
        self.assertEqual(1, result['findings'].get('completed', 0))

    def test_late_platform_order_reclassifies_receipt_loss_not_missing_work(self):
        c = RestitutionCase(self.db, 'S-L', 'customer-L', 'orders',
                            min_healthy=1, unit_credit=10)
        wave(self.db, 'W4', 'orders', 'complete')
        c.instance('i-l', 'node-l')
        # The entry exists, but the server ack (and its order record) is late:
        # the customer never received a confirmation.
        c.entry('ord-8', 'req-8')
        c.recovery(at=50)
        result = c.adjudicate('case-L', 'W4')
        self.assertEqual('provably_unfinished',
                         one(self.db, '''SELECT classification FROM
                                         restitution_findings
                                         WHERE order_key='ord-8' ''')['classification'])
        # The platform order lands afterwards; re-adjudication must see the
        # work as executed with a lost receipt, never as a missing order.
        c.order('ord-8', 8, processed_at=60)
        result = R.run(self.db, {'event_id': 're-adj', 'op': 'adjudicate',
                                 'case_id': 'case-L', 'sla': 'S-L',
                                 'wave': 'W4'})
        self.assertEqual('processed_receipt_lost',
                         one(self.db, '''SELECT classification FROM
                                         restitution_findings
                                         WHERE order_key='ord-8' ''')['classification'])
        self.assertEqual(10, result['service_fee_due'])
        self.assertEqual(0, result['principal_due'])

    # --- 现金轨道：银行清算与退票 ------------------------------------
    def test_cash_track_refund_requires_bank_clearing_and_handles_bounce(self):
        c = RestitutionCase(self.db, 'S-C', 'customer-C', 'orders',
                            min_healthy=1, unit_credit=10)
        wave(self.db, 'W5', 'orders', 'complete')
        c.instance('i-c', 'node-c')
        c.entry('ord-31', 'req-31', amount=200, service_fee=20)
        c.confirm('ord-31')
        c.charge('ord-31', amount=200)
        c.recovery(at=70)
        c.adjudicate('case-C', 'W5')
        self.assertEqual('provably_unfinished',
                         one(self.db, '''SELECT classification FROM
                                         restitution_findings
                                         WHERE order_key='ord-31' ''')['classification'])

        # The three money buckets cannot be merged into one failure count.
        self.assertEqual({'decision': 'amount_mismatch'},
                         R.run(self.db, {'event_id': 'p-bad', 'op': 'bank_pay',
                                         'id': 'pay-bad', 'case_id': 'case-C',
                                         'order_key': 'ord-31',
                                         'kind': 'service_fee_refund',
                                         'amount': 220}))
        fee_pay = R.run(self.db, {'event_id': 'p-fee', 'op': 'bank_pay',
                                  'id': 'pay-fee', 'case_id': 'case-C',
                                  'order_key': 'ord-31',
                                  'kind': 'service_fee_refund', 'amount': 20})
        self.assertEqual('payment_initiated', fee_pay['decision'])
        principal_pay = R.run(self.db, {'event_id': 'p-prin', 'op': 'bank_pay',
                                        'id': 'pay-prin', 'case_id': 'case-C',
                                        'order_key': 'ord-31',
                                        'kind': 'principal_refund',
                                        'amount': 200})
        self.assertEqual('payment_initiated', principal_pay['decision'])
        R.run(self.db, {'event_id': 'w-fee', 'op': 'fee_waiver',
                        'id': 'w-31', 'case_id': 'case-C',
                        'order_key': 'ord-31', 'amount': 20})

        # Pending bank payments are not performance: settlement is blocked.
        blocked = R.run(self.db, {'event_id': 's-1', 'op': 'settle',
                                  'case_id': 'case-C'})
        self.assertEqual('settlement_blocked', blocked['decision'])

        # First the fee payment bounces; the duty stays open.
        self.assertEqual('bank_bounced',
                         R.run(self.db, {'event_id': 'cl-fee-bad',
                                         'op': 'bank_clear', 'id': 'pay-fee',
                                         'outcome': 'bounced'})['decision'])
        retry_fee = R.run(self.db, {'event_id': 'p-fee-2', 'op': 'bank_pay',
                                    'id': 'pay-fee-2', 'case_id': 'case-C',
                                    'order_key': 'ord-31',
                                    'kind': 'service_fee_refund',
                                    'amount': 20})
        self.assertEqual('payment_initiated', retry_fee['decision'])
        R.run(self.db, {'event_id': 'cl-fee-2', 'op': 'bank_clear',
                        'id': 'pay-fee-2', 'outcome': 'cleared'})
        R.run(self.db, {'event_id': 'cl-prin', 'op': 'bank_clear',
                        'id': 'pay-prin', 'outcome': 'cleared'})
        settled = R.run(self.db, {'event_id': 's-2', 'op': 'settle',
                                  'case_id': 'case-C'})
        self.assertEqual('settled', settled['decision'])

    # --- 争议款托管与释放 ---------------------------------------------
    def test_escrow_held_then_released_after_payer_original_check(self):
        c = RestitutionCase(self.db, 'S-E', 'customer-E', 'orders',
                            min_healthy=1, unit_credit=10)
        wave(self.db, 'W6', 'orders', 'complete')
        c.instance('i-e', 'node-e')
        c.confirm('ord-41')
        c.charge('ord-41', amount=100)
        c.recovery(at=70)
        c.adjudicate('case-E', 'W6')
        hold = R.run(self.db, {'event_id': 'h1', 'op': 'escrow_hold',
                               'id': 'hold-41', 'case_id': 'case-E',
                               'order_key': 'ord-41', 'amount': 110})
        self.assertEqual('escrow_held', hold['decision'])
        # Replaying the same hold event is idempotent, not an integrity error.
        self.assertEqual('escrow_already_held',
                         R.run(self.db, {'event_id': 'h1-dup',
                                         'op': 'escrow_hold', 'id': 'hold-41',
                                         'case_id': 'case-E',
                                         'order_key': 'ord-41',
                                         'amount': 110})['decision'])
        # A direct cash refund against escrowed money must be refused.
        self.assertEqual('payment_not_owed',
                         R.run(self.db, {'event_id': 'no-cash',
                                         'op': 'bank_pay', 'id': 'pay-x',
                                         'case_id': 'case-E',
                                         'order_key': 'ord-41',
                                         'kind': 'principal_refund',
                                         'amount': 100})['decision'])

        # Payer originals prove execution: the hold is released to the
        # platform and no customer payout is created.
        released = R.run(self.db, {'event_id': 'res-ok', 'op': 'escrow_resolve',
                                   'id': 'hold-41',
                                   'resolution': 'payer_confirms_executed'})
        self.assertEqual('escrow_released_to_platform', released['decision'])
        self.assertEqual('settled',
                         R.run(self.db, {'event_id': 's-e', 'op': 'settle',
                                         'case_id': 'case-E'})['decision'])

    def test_escrow_pays_customer_when_payer_proves_non_execution(self):
        c = RestitutionCase(self.db, 'S-E2', 'customer-E2', 'orders',
                            min_healthy=1, unit_credit=10)
        wave(self.db, 'W7', 'orders', 'complete')
        c.instance('i-e2', 'node-e2')
        c.confirm('ord-51')
        c.charge('ord-51', amount=100)
        c.recovery(at=70)
        c.adjudicate('case-E2', 'W7')
        R.run(self.db, {'event_id': 'h2', 'op': 'escrow_hold',
                        'id': 'hold-51', 'case_id': 'case-E2',
                        'order_key': 'ord-51', 'amount': 110})
        payable = R.run(self.db, {'event_id': 'res-bad',
                                  'op': 'escrow_resolve', 'id': 'hold-51',
                                  'resolution': 'payer_confirms_not_executed',
                                  'payment_id': 'payout-51'})
        self.assertEqual('escrow_payable_to_customer', payable['decision'])
        # Still not performance until the payout clears.
        blocked = R.run(self.db, {'event_id': 's-block', 'op': 'settle',
                                  'case_id': 'case-E2'})
        self.assertEqual('settlement_blocked', blocked['decision'])
        R.run(self.db, {'event_id': 'cl-payout', 'op': 'bank_clear',
                        'id': 'payout-51', 'outcome': 'cleared'})
        self.assertEqual('settled',
                         R.run(self.db, {'event_id': 's-e2', 'op': 'settle',
                                         'case_id': 'case-E2'})['decision'])

    # --- 第三方先扣款：从未提交的请求 --------------------------------
    def test_precharged_request_refunded_through_payer_reconciliation(self):
        c = RestitutionCase(self.db, 'S-P', 'customer-P', 'orders',
                            min_healthy=1, unit_credit=10)
        wave(self.db, 'W8', 'orders', 'complete')
        c.instance('i-p', 'node-p')
        # Only a payer charge exists: no entry receipt, no confirmation.
        c.charge('ord-61', amount=80, charged_at=9)
        c.recovery(at=70)
        result = c.adjudicate('case-P', 'W8')
        self.assertEqual(1, result['findings'].get('precharged'))
        refund = R.run(self.db, {'event_id': 'pr1', 'op': 'payer_refund',
                                 'id': 'refund-61', 'case_id': 'case-P',
                                 'order_key': 'ord-61', 'amount': 80})
        self.assertEqual('payer_refund_initiated', refund['decision'])
        # A platform cash/service refund is not owed for a never-submitted job.
        self.assertEqual('payment_not_owed',
                         R.run(self.db, {'event_id': 'no-svc',
                                         'op': 'bank_pay', 'id': 'pay-61',
                                         'case_id': 'case-P',
                                         'order_key': 'ord-61',
                                         'kind': 'service_fee_refund',
                                         'amount': 10})['decision'])
        blocked = R.run(self.db, {'event_id': 's-p1', 'op': 'settle',
                                  'case_id': 'case-P'})
        self.assertEqual('settlement_blocked', blocked['decision'])
        R.run(self.db, {'event_id': 'pr-cl', 'op': 'payer_refund_clear',
                        'id': 'refund-61', 'outcome': 'cleared'})
        self.assertEqual('settled',
                         R.run(self.db, {'event_id': 's-p2', 'op': 'settle',
                                         'case_id': 'case-P'})['decision'])

    # --- 替代轨道：容量验收、防重复、拒签、平台承担算力 --------------
    def test_substitute_batch_needs_capacity_and_signoff_and_blocks_redo(self):
        c = RestitutionCase(self.db, 'S-S', 'customer-S', 'orders',
                            min_healthy=1, unit_credit=10)
        wave(self.db, 'W9', 'orders', 'complete')
        c.instance('i-s', 'node-s')
        c.entry('ord-71', 'req-71', amount=150, service_fee=15)
        c.confirm('ord-71')
        c.charge('ord-71', amount=150)
        c.recovery(at=70)
        c.adjudicate('case-S', 'W9')

        plan = R.run(self.db, {'event_id': 'sp1', 'op': 'substitute_plan',
                               'batch_id': 'batch-71', 'case_id': 'case-S',
                               'order_key': 'ord-71'})
        self.assertEqual('substitute_planned', plan['decision'])
        # The original charged order is now locked against re-charge.
        self.assertEqual('locked',
                         one(self.db, '''SELECT status FROM recharge_locks
                                         WHERE order_key='ord-71' ''')['status'])
        # Cash and substitute tracks are mutually exclusive: once a batch
        # exists for the order, a cash payment must be refused as such.
        self.assertEqual('track_conflict_substitute',
                         R.run(self.db, {'event_id': 'no-cash2',
                                         'op': 'bank_pay', 'id': 'pay-71',
                                         'case_id': 'case-S',
                                         'order_key': 'ord-71',
                                         'kind': 'principal_refund',
                                         'amount': 150})['decision'])
        # Execution before independent capacity acceptance is fenced.
        self.assertEqual('capacity_not_accepted',
                         R.run(self.db, {'event_id': 'ex0',
                                         'op': 'substitute_execute',
                                         'batch_id': 'batch-71'})['decision'])
        self.assertEqual('capacity_rejected',
                         R.run(self.db, {'event_id': 'cap0',
                                         'op': 'substitute_capacity',
                                         'batch_id': 'batch-71',
                                         'accepted': False,
                                         'independent_instances': 0})['decision'])
        self.assertEqual('capacity_accepted',
                         R.run(self.db, {'event_id': 'cap1',
                                         'op': 'substitute_capacity',
                                         'batch_id': 'batch-71',
                                         'accepted': True,
                                         'independent_instances': 2})['decision'])
        self.assertEqual('substitute_executed',
                         R.run(self.db, {'event_id': 'ex1',
                                         'op': 'substitute_execute',
                                         'batch_id': 'batch-71'})['decision'])
        # Repeated execution must not create a duplicate order.
        self.assertEqual('substitute_already_executed',
                         R.run(self.db, {'event_id': 'ex2',
                                         'op': 'substitute_execute',
                                         'batch_id': 'batch-71'})['decision'])

        # Customer refuses the first delivery: the batch cannot settle.
        self.assertEqual('customer_refused',
                         R.run(self.db, {'event_id': 'sign0',
                                         'op': 'substitute_signoff',
                                         'batch_id': 'batch-71',
                                         'accepted': False})['decision'])
        blocked = R.run(self.db, {'event_id': 's-s1', 'op': 'settle',
                                  'case_id': 'case-S'})
        self.assertEqual('settlement_blocked', blocked['decision'])
        # A reworked batch replaces the refused one and is finally signed for.
        plan2 = R.run(self.db, {'event_id': 'sp2', 'op': 'substitute_plan',
                                'batch_id': 'batch-71-r2', 'case_id': 'case-S',
                                'order_key': 'ord-71'})
        self.assertEqual('substitute_planned', plan2['decision'])
        R.run(self.db, {'event_id': 'cap2', 'op': 'substitute_capacity',
                        'batch_id': 'batch-71-r2', 'accepted': True,
                        'independent_instances': 2})
        R.run(self.db, {'event_id': 'ex3', 'op': 'substitute_execute',
                        'batch_id': 'batch-71-r2'})
        self.assertEqual('customer_signed_off',
                         R.run(self.db, {'event_id': 'sign1',
                                         'op': 'substitute_signoff',
                                         'batch_id': 'batch-71-r2',
                                         'accepted': True})['decision'])
        # Platform bears the substitute compute fee; the service fee is also
        # waived, and no principal refund moves because the work got done.
        self.assertEqual(15, one(self.db, '''SELECT compute_fee FROM
                                              substitute_batches
                                              WHERE id='batch-71-r2' ''')['compute_fee'])
        R.run(self.db, {'event_id': 'w71', 'op': 'fee_waiver', 'id': 'w-71',
                        'case_id': 'case-S', 'order_key': 'ord-71',
                        'amount': 15})
        self.assertEqual('settled',
                         R.run(self.db, {'event_id': 's-s2', 'op': 'settle',
                                         'case_id': 'case-S'})['decision'])

    def test_recharge_lock_blocks_redo_of_confirmed_charge(self):
        c = RestitutionCase(self.db, 'S-R', 'customer-R', 'orders',
                            min_healthy=1, unit_credit=10)
        wave(self.db, 'W10', 'orders', 'complete')
        c.instance('i-r', 'node-r')
        c.entry('ord-81', 'req-81')
        c.confirm('ord-81')
        c.charge('ord-81', amount=100)
        c.recovery(at=70)
        c.adjudicate('case-R', 'W10')
        self.assertEqual('recharge_locked',
                         R.run(self.db, {'event_id': 'lock1',
                                         'op': 'recharge_lock', 'id': 'lk-81',
                                         'case_id': 'case-R',
                                         'order_key': 'ord-81'})['decision'])
        # An order with no charge cannot be locked (nothing to re-charge);
        # re-adjudication extends the same case for this SLA and wave.
        c.entry('ord-82', 'req-82')
        c.confirm('ord-82')
        c.adjudicate('case-R', 'W10')
        self.assertEqual('no_charge_to_lock',
                         R.run(self.db, {'event_id': 'lock2',
                                         'op': 'recharge_lock', 'id': 'lk-82',
                                         'case_id': 'case-R',
                                         'order_key': 'ord-82'})['decision'])

    # --- 可恢复提交交接：完整事件流可重放 -----------------------------
    def test_full_ledger_is_replayable(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / 'restitution.sqlite')
            db = connect(path)
            c = RestitutionCase(db, 'S-Z', 'customer-Z', 'orders',
                                min_healthy=1, unit_credit=10)
            wave(db, 'W11', 'orders', 'complete')
            c.instance('i-z', 'node-z')
            c.entry('ord-91', 'req-91', amount=120, service_fee=12)
            c.confirm('ord-91')
            c.charge('ord-91', amount=120)
            c.confirm('ord-92')           # never arrived
            c.recovery(at=70)
            c.adjudicate('case-Z', 'W11')
            R.run(db, {'event_id': 'bz1', 'op': 'bank_pay', 'id': 'pay-z-fee',
                       'case_id': 'case-Z', 'order_key': 'ord-91',
                       'kind': 'service_fee_refund', 'amount': 12})
            R.run(db, {'event_id': 'bz2', 'op': 'bank_pay', 'id': 'pay-z-p',
                       'case_id': 'case-Z', 'order_key': 'ord-91',
                       'kind': 'principal_refund', 'amount': 120})
            R.run(db, {'event_id': 'bz3', 'op': 'bank_clear',
                       'id': 'pay-z-fee', 'outcome': 'cleared'})
            R.run(db, {'event_id': 'bz4', 'op': 'bank_clear',
                       'id': 'pay-z-p', 'outcome': 'cleared'})
            R.run(db, {'event_id': 'bz5', 'op': 'fee_waiver', 'id': 'w-z',
                       'case_id': 'case-Z', 'order_key': 'ord-91',
                       'amount': 12})
            # Replaying an identical event must return the same decision.
            again = R.run(db, {'event_id': 'bz5', 'op': 'fee_waiver',
                               'id': 'w-z', 'case_id': 'case-Z',
                               'order_key': 'ord-91', 'amount': 12})
            self.assertEqual('fee_waived', again['decision'])
            self.assertEqual('settled',
                             R.run(db, {'event_id': 'sz', 'op': 'settle',
                                        'case_id': 'case-Z'})['decision'])
            db.close()
            report = replay(path)
            self.assertTrue(report['verified'], report['mismatch'])


if __name__ == '__main__':
    unittest.main()
