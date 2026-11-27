"""Customer restitution after cold-start incidents.

A failed boot wave is not a customer outage and a ``complete`` wave is not
proof that real orders executed.  Every customer loss window is reconstructed
from five independent evidence sources:

* contracted replica floor (``customer_slas``) versus actually accepted
  independent instances covering the window;
* request-entry receipts, carrying the client idempotency key so retried
  client requests and late server acks collapse onto one business order;
* customer signed order confirmations;
* platform order records (orders the running instances really executed);
* third-party payer charges;
and the recovery instant that closes the window.

Orders are classified per business order key, never per failed wave:

==========================  ================================================
finding                     meaning
==========================  ================================================
completed                   platform executed, entry+confirmation present
processed_receipt_lost      platform executed but its entry/ack evidence is
                            missing; only the service fee is waived
provably_unfinished         entered the platform and was never executed;
                            platform owes service-fee refund (cash track) or
                            a substitute batch (substitute track)
disputed                    customer signature + payer charge but no entry
                            and no platform order; money is escrowed until
                            the payer's original record resolves the fork
never_arrived               customer-side evidence only; never reached the
                            platform, so it is not platform debt
precharged                  payer charged without any business submission;
                            the platform owes reconciliation/refund to the
                            payer, not service compensation
==========================  ================================================

Principal, service-fee relief and substitute-compute cost are tracked as
separate money movements.  Cash refunds only count once the bank clears them
(a bounced payment reopens the duty); substitute work only counts once
independently capacity-accepted and finally signed off by the customer.
Provisional ``credit_drafts`` rows are bookkeeping: a lossless customer gets
the draft ``voided`` and no money, a lossy customer gets it ``superseded`` by
the adjudicated order-level case.  All writes are idempotent events, so the
whole ledger is replayable through ``store.replay``.
"""

from .store import act, one, many
from . import customer_sla, credit_draft
import json

SCHEMA = customer_sla.SCHEMA + credit_draft.SCHEMA + '''
CREATE TABLE IF NOT EXISTS instance_acceptances(
    id TEXT PRIMARY KEY, sla TEXT, instance_id TEXT, node TEXT,
    accepted_at INTEGER, revoked_at INTEGER
);
CREATE TABLE IF NOT EXISTS recovery_marks(
    id TEXT PRIMARY KEY, sla TEXT, service TEXT, recovered_at INTEGER
);
CREATE TABLE IF NOT EXISTS entry_receipts(
    id TEXT PRIMARY KEY, sla TEXT, order_key TEXT, client_request_id TEXT,
    amount INTEGER, service_fee INTEGER, received_at INTEGER
);
CREATE TABLE IF NOT EXISTS customer_confirmations(
    id TEXT PRIMARY KEY, sla TEXT, order_key TEXT, signed_at INTEGER
);
CREATE TABLE IF NOT EXISTS platform_orders(
    id TEXT PRIMARY KEY, sla TEXT, order_key TEXT, seq INTEGER,
    processed_at INTEGER
);
CREATE TABLE IF NOT EXISTS payer_charges(
    id TEXT PRIMARY KEY, sla TEXT, order_key TEXT, amount INTEGER,
    charged_at INTEGER
);
CREATE TABLE IF NOT EXISTS restitution_findings(
    id TEXT PRIMARY KEY, case_id TEXT, sla TEXT, order_key TEXT,
    classification TEXT, principal INTEGER, service_fee INTEGER,
    substitute_fee INTEGER, evidence TEXT
);
CREATE TABLE IF NOT EXISTS restitution_cases(
    id TEXT PRIMARY KEY, sla TEXT, service TEXT, wave TEXT,
    recovered_at INTEGER, loss_state TEXT, state TEXT,
    principal_due INTEGER, service_fee_due INTEGER, substitute_fee_due INTEGER,
    escrow_due INTEGER, summary TEXT
);
CREATE TABLE IF NOT EXISTS bank_payments(
    id TEXT PRIMARY KEY, case_id TEXT, order_key TEXT, kind TEXT,
    amount INTEGER, status TEXT
);
CREATE TABLE IF NOT EXISTS service_fee_waivers(
    id TEXT PRIMARY KEY, case_id TEXT, order_key TEXT, amount INTEGER
);
CREATE TABLE IF NOT EXISTS escrow_holds(
    id TEXT PRIMARY KEY, case_id TEXT, order_key TEXT, amount INTEGER,
    status TEXT, resolution TEXT
);
CREATE TABLE IF NOT EXISTS payer_refunds(
    id TEXT PRIMARY KEY, case_id TEXT, order_key TEXT, amount INTEGER,
    status TEXT
);
CREATE TABLE IF NOT EXISTS substitute_batches(
    id TEXT PRIMARY KEY, case_id TEXT, order_key TEXT,
    capacity_accepted INTEGER, executed INTEGER, customer_signed INTEGER,
    compute_fee INTEGER, status TEXT
);
CREATE TABLE IF NOT EXISTS recharge_locks(
    id TEXT PRIMARY KEY, case_id TEXT, order_key TEXT, status TEXT
);
'''


def _sla(db, sla_id):
    return one(db, 'SELECT * FROM customer_slas WHERE id=?', (sla_id,))


def _active_instances(db, sla_id, at):
    """Accepted independent instances whose acceptance covers ``at``."""
    return many(db, '''SELECT * FROM instance_acceptances
                       WHERE sla=? AND accepted_at<=?
                         AND (revoked_at IS NULL OR revoked_at>?)''',
                (sla_id, at, at))


def _classify(has_entry, has_confirmation, has_order, has_charge):
    """Pure evidence rule for one business order key."""
    if has_order:
        # The platform really executed it.  Missing evidence means the
        # server ack was lost, not that the work is missing.
        if has_entry and has_confirmation:
            return 'completed'
        return 'processed_receipt_lost'
    if has_confirmation and has_charge and not has_entry:
        # Signature + external charge, no entry and no platform record:
        # the local tables cannot tell a lost post-execution receipt from
        # a payer pre-charge, so the money is held until payer originals.
        return 'disputed'
    if has_entry:
        # The request demonstrably entered the platform and never ran.
        return 'provably_unfinished'
    if has_charge:
        # Money moved before any business submission reached the platform.
        return 'precharged'
    # Customer-side paperwork only; the request never arrived.
    return 'never_arrived'


def _gather_evidence(db, sla_id):
    rows = {}

    def bucket(order_key):
        return rows.setdefault(order_key, {
            'entry': None, 'confirmations': [], 'orders': [], 'charges': []})

    for r in many(db, 'SELECT * FROM entry_receipts WHERE sla=?', (sla_id,)):
        b = bucket(r['order_key'])
        # Repeated client requests / duplicate entry ids are the same order.
        if b['entry'] is None or r['received_at'] < b['entry']['received_at']:
            b['entry'] = r
    for r in many(db, 'SELECT * FROM customer_confirmations WHERE sla=?', (sla_id,)):
        bucket(r['order_key'])['confirmations'].append(r)
    for r in many(db, 'SELECT * FROM platform_orders WHERE sla=?', (sla_id,)):
        bucket(r['order_key'])['orders'].append(r)
    for r in many(db, 'SELECT * FROM payer_charges WHERE sla=?', (sla_id,)):
        bucket(r['order_key'])['charges'].append(r)
    return rows


def _adjudicate(db, x):
    sla = _sla(db, x['sla'])
    if not sla:
        return {'decision': 'sla_unknown'}
    wave = one(db, 'SELECT * FROM boot_waves WHERE id=?', (x['wave'],))
    if not wave:
        return {'decision': 'wave_unknown'}
    recovery = one(db, 'SELECT * FROM recovery_marks WHERE sla=?',
                   (x['sla'],))
    recovered_at = recovery['recovered_at'] if recovery else x.get('at', 0)

    case = one(db, 'SELECT * FROM restitution_cases WHERE sla=? AND wave=?',
               (x['sla'], x['wave']))
    if case and case['state'] not in ('adjudicated',):
        return {'decision': 'case_locked', 'state': case['state']}

    # Replica cross-check: contracted floor vs accepted independent
    # instances continuously covering the loss window.
    instances = _active_instances(db, x['sla'], recovered_at)
    floor_met = len({i['instance_id'] for i in instances}) >= sla['min_healthy']

    evidence = _gather_evidence(db, x['sla'])
    findings = []
    for order_key in sorted(evidence):
        e = evidence[order_key]
        entry, orders, charges = e['entry'], e['orders'], e['charges']
        classification = _classify(
            entry is not None, bool(e['confirmations']),
            bool(orders), bool(charges))
        principal = charges[0]['amount'] if charges else (
            entry['amount'] if entry else 0)
        service_fee = entry['service_fee'] if entry and entry['service_fee'] is not None else sla['unit_credit']
        substitute_fee = service_fee if classification == 'provably_unfinished' else 0
        findings.append({
            'order_key': order_key,
            'classification': classification,
            'principal': principal,
            'service_fee': service_fee,
            'substitute_fee': substitute_fee,
            'evidence': {
                'entry': entry['id'] if entry else None,
                'confirmations': [c['id'] for c in e['confirmations']],
                'platform_orders': [o['id'] for o in orders],
                'payer_charges': [c['id'] for c in charges],
            },
        })

    impactful = [f for f in findings if f['classification'] not in
                 ('completed', 'never_arrived')]
    # A failed wave with enough accepted replicas and every entered order
    # executed is a lossless failure: the customer never lost service.
    loss_state = 'no_loss' if floor_met and not impactful else 'loss_confirmed'

    principal_due = sum(f['principal'] for f in findings
                        if f['classification'] in ('provably_unfinished',))
    service_fee_due = sum(f['service_fee'] for f in findings
                          if f['classification'] in
                          ('provably_unfinished', 'processed_receipt_lost'))
    escrow_due = sum(f['principal'] + f['service_fee'] for f in findings
                     if f['classification'] == 'disputed')
    substitute_fee_due = sum(f['substitute_fee'] for f in findings)

    if case is None:
        case_id = x['case_id']
        db.execute('''INSERT INTO restitution_cases
                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',
                   (case_id, x['sla'], sla['service'], x['wave'],
                    recovered_at, loss_state, 'adjudicated',
                    principal_due, service_fee_due, substitute_fee_due,
                    escrow_due, ''))
    else:
        case_id = case['id']
        db.execute('''UPDATE restitution_cases SET
                        recovered_at=?, loss_state=?, state='adjudicated',
                        principal_due=?, service_fee_due=?,
                        substitute_fee_due=?, escrow_due=?
                      WHERE id=?''',
                   (recovered_at, loss_state, principal_due, service_fee_due,
                    substitute_fee_due, escrow_due, case_id))
        db.execute('DELETE FROM restitution_findings WHERE case_id=?',
                   (case_id,))

    summary = {}
    for f in findings:
        summary[f['classification']] = summary.get(f['classification'], 0) + 1
        db.execute('''INSERT INTO restitution_findings
                      VALUES(?,?,?,?,?,?,?,?,?)''',
                   (case_id + ':' + f['order_key'], case_id, x['sla'],
                    f['order_key'], f['classification'], f['principal'],
                    f['service_fee'], f['substitute_fee'],
                    json.dumps(f['evidence'], sort_keys=True)))
    db.execute('UPDATE restitution_cases SET summary=? WHERE id=?',
               (json.dumps(summary, sort_keys=True), case_id))

    # Provisional drafts never settle anything: they are either voided by a
    # lossless cross-check or superseded by the order-level adjudication.
    drafts = many(db, 'SELECT * FROM credit_drafts WHERE sla=?', (x['sla'],))
    for d in drafts:
        if loss_state == 'no_loss':
            db.execute("UPDATE credit_drafts SET state='voided' WHERE id=?",
                       (d['id'],))
        elif d['state'] == 'draft':
            db.execute("UPDATE credit_drafts SET state='superseded' WHERE id=?",
                       (d['id'],))

    return {
        'decision': 'adjudicated',
        'case_id': case_id,
        'loss_state': loss_state,
        'healthy_instances': len({i['instance_id'] for i in instances}),
        'min_healthy': sla['min_healthy'],
        'findings': summary,
        'principal_due': principal_due,
        'service_fee_due': service_fee_due,
        'substitute_fee_due': substitute_fee_due,
        'escrow_due': escrow_due,
    }


def _finding(db, case_id, order_key):
    return one(db, '''SELECT * FROM restitution_findings
                      WHERE case_id=? AND order_key=?''',
               (case_id, order_key))


def _bank_pay(db, x):
    case = one(db, 'SELECT * FROM restitution_cases WHERE id=?', (x['case_id'],))
    if not case:
        return {'decision': 'case_unknown'}
    if case['state'] == 'settled':
        return {'decision': 'case_locked', 'state': 'settled'}
    if one(db, '''SELECT 1 FROM substitute_batches
                  WHERE case_id=? AND order_key=?''',
           (x['case_id'], x['order_key'])):
        return {'decision': 'track_conflict_substitute'}
    f = _finding(db, x['case_id'], x['order_key'])
    if not f:
        return {'decision': 'order_unknown'}
    if x['kind'] == 'service_fee_refund':
        if f['classification'] not in ('provably_unfinished',
                                       'processed_receipt_lost'):
            return {'decision': 'payment_not_owed',
                    'classification': f['classification']}
        if x['amount'] != f['service_fee']:
            return {'decision': 'amount_mismatch'}
    elif x['kind'] == 'principal_refund':
        # Principal is only refunded in cash for entered-but-unrun orders;
        # disputed principal is escrowed, precharged money goes via payer.
        if f['classification'] != 'provably_unfinished':
            return {'decision': 'payment_not_owed',
                    'classification': f['classification']}
        if x['amount'] != f['principal']:
            return {'decision': 'amount_mismatch'}
    else:
        return {'decision': 'unknown_payment_kind'}
    existing = one(db, 'SELECT * FROM bank_payments WHERE id=?', (x['id'],))
    if existing:
        return {'decision': 'payment_already_initiated',
                'status': existing['status']}
    db.execute('INSERT INTO bank_payments VALUES(?,?,?,?,?,?)',
               (x['id'], x['case_id'], x['order_key'], x['kind'],
                x['amount'], 'pending'))
    return {'decision': 'payment_initiated'}


def _bank_clear(db, x):
    p = one(db, 'SELECT * FROM bank_payments WHERE id=?', (x['id'],))
    if not p:
        return {'decision': 'payment_unknown'}
    if p['status'] in ('cleared', 'bounced'):
        return {'decision': 'payment_already_cleared', 'status': p['status']}
    status = 'cleared' if x['outcome'] == 'cleared' else 'bounced'
    db.execute('UPDATE bank_payments SET status=? WHERE id=?', (status, p['id']))
    return {'decision': 'bank_cleared' if status == 'cleared' else 'bank_bounced',
            'payment_id': p['id']}


def _fee_waiver(db, x):
    case = one(db, 'SELECT * FROM restitution_cases WHERE id=?', (x['case_id'],))
    if not case:
        return {'decision': 'case_unknown'}
    f = _finding(db, x['case_id'], x['order_key'])
    if not f or f['classification'] not in ('provably_unfinished',
                                            'processed_receipt_lost'):
        return {'decision': 'waiver_not_owed'}
    if x['amount'] != f['service_fee']:
        return {'decision': 'amount_mismatch'}
    db.execute('INSERT OR IGNORE INTO service_fee_waivers VALUES(?,?,?,?)',
               (x['id'], x['case_id'], x['order_key'], x['amount']))
    return {'decision': 'fee_waived'}


def _escrow_hold(db, x):
    case = one(db, 'SELECT * FROM restitution_cases WHERE id=?', (x['case_id'],))
    if not case:
        return {'decision': 'case_unknown'}
    existing = one(db, 'SELECT * FROM escrow_holds WHERE id=?', (x['id'],))
    if existing:
        return {'decision': 'escrow_already_held', 'status': existing['status']}
    f = _finding(db, x['case_id'], x['order_key'])
    if not f or f['classification'] != 'disputed':
        return {'decision': 'escrow_not_owed'}
    amount = f['principal'] + f['service_fee']
    if x['amount'] != amount:
        return {'decision': 'amount_mismatch'}
    db.execute('INSERT INTO escrow_holds VALUES(?,?,?,?,?,?)',
               (x['id'], x['case_id'], x['order_key'], amount, 'held', None))
    return {'decision': 'escrow_held', 'amount': amount}


def _escrow_resolve(db, x):
    h = one(db, 'SELECT * FROM escrow_holds WHERE id=?', (x['id'],))
    if not h:
        return {'decision': 'escrow_unknown'}
    if h['status'] != 'held':
        return {'decision': 'escrow_locked', 'status': h['status']}
    if x['resolution'] not in ('payer_confirms_executed',
                               'payer_confirms_not_executed'):
        return {'decision': 'unknown_resolution'}
    if x['resolution'] == 'payer_confirms_executed':
        # Payer originals prove the platform ran the order: release the hold
        # back to the platform; the customer owes nothing more.
        db.execute("UPDATE escrow_holds SET status='released', resolution=? WHERE id=?",
                   (x['resolution'], h['id']))
        return {'decision': 'escrow_released_to_platform'}
    # Payer originals prove the business never executed: the held money is
    # paid out to the customer through a real bank payment.
    db.execute("UPDATE escrow_holds SET status='payable', resolution=? WHERE id=?",
               (x['resolution'], h['id']))
    if 'payment_id' in x:
        db.execute('INSERT INTO bank_payments VALUES(?,?,?,?,?,?)',
                   (x['payment_id'], h['case_id'], h['order_key'],
                    'escrow_payout', h['amount'], 'pending'))
    return {'decision': 'escrow_payable_to_customer', 'amount': h['amount']}


def _payer_refund(db, x):
    case = one(db, 'SELECT * FROM restitution_cases WHERE id=?', (x['case_id'],))
    if not case:
        return {'decision': 'case_unknown'}
    f = _finding(db, x['case_id'], x['order_key'])
    if not f or f['classification'] not in ('precharged', 'provably_unfinished'):
        return {'decision': 'payer_refund_not_owed'}
    charge = one(db, '''SELECT * FROM payer_charges
                        WHERE sla=? AND order_key=?''', (case['sla'], x['order_key']))
    if not charge or x['amount'] != charge['amount']:
        return {'decision': 'amount_mismatch'}
    existing = one(db, 'SELECT * FROM payer_refunds WHERE id=?', (x['id'],))
    if existing:
        return {'decision': 'payer_refund_already_initiated',
                'status': existing['status']}
    db.execute('INSERT INTO payer_refunds VALUES(?,?,?,?,?)',
               (x['id'], x['case_id'], x['order_key'], x['amount'], 'pending'))
    return {'decision': 'payer_refund_initiated'}


def _payer_refund_clear(db, x):
    r = one(db, 'SELECT * FROM payer_refunds WHERE id=?', (x['id'],))
    if not r:
        return {'decision': 'refund_unknown'}
    if r['status'] in ('cleared', 'bounced'):
        return {'decision': 'refund_already_cleared', 'status': r['status']}
    status = 'cleared' if x['outcome'] == 'cleared' else 'bounced'
    db.execute('UPDATE payer_refunds SET status=? WHERE id=?', (status, r['id']))
    return {'decision': 'payer_refund_cleared' if status == 'cleared'
            else 'payer_refund_bounced', 'refund_id': r['id']}


def _recharge_lock(db, x):
    case = one(db, 'SELECT * FROM restitution_cases WHERE id=?', (x['case_id'],))
    if not case:
        return {'decision': 'case_unknown'}
    f = _finding(db, x['case_id'], x['order_key'])
    if not f:
        return {'decision': 'order_unknown'}
    # A confirmed charge must never be re-charged because of an incident
    # report; the lock fences retries on every order carrying a charge.
    charge = one(db, '''SELECT 1 FROM payer_charges
                        WHERE sla=? AND order_key=?''',
                 (case['sla'], x['order_key']))
    if not charge:
        return {'decision': 'no_charge_to_lock'}
    db.execute('INSERT OR IGNORE INTO recharge_locks VALUES(?,?,?,?)',
               (x['id'], x['case_id'], x['order_key'], 'locked'))
    return {'decision': 'recharge_locked'}


def _substitute_plan(db, x):
    case = one(db, 'SELECT * FROM restitution_cases WHERE id=?', (x['case_id'],))
    if not case:
        return {'decision': 'case_unknown'}
    if case['state'] == 'settled':
        return {'decision': 'case_locked', 'state': 'settled'}
    f = _finding(db, x['case_id'], x['order_key'])
    if not f or f['classification'] != 'provably_unfinished':
        return {'decision': 'substitute_not_owed'}
    if one(db, '''SELECT 1 FROM bank_payments
                  WHERE case_id=? AND order_key=?''',
           (x['case_id'], x['order_key'])):
        return {'decision': 'track_conflict_cash'}
    if one(db, 'SELECT 1 FROM substitute_batches WHERE id=?', (x['batch_id'],)):
        return {'decision': 'business_identity_conflict'}
    # The original order may not be re-charged under the substitute track.
    db.execute('INSERT OR IGNORE INTO recharge_locks VALUES(?,?,?,?)',
               (x['batch_id'] + ':lock', x['case_id'], x['order_key'], 'locked'))
    db.execute('INSERT INTO substitute_batches VALUES(?,?,?,?,?,?,?,?)',
               (x['batch_id'], x['case_id'], x['order_key'], 0, 0, 0,
                f['substitute_fee'], 'planned'))
    return {'decision': 'substitute_planned'}


def _substitute_capacity(db, x):
    b = one(db, 'SELECT * FROM substitute_batches WHERE id=?', (x['batch_id'],))
    if not b:
        return {'decision': 'batch_unknown'}
    if b['status'] not in ('planned', 'capacity_rejected'):
        return {'decision': 'batch_locked', 'status': b['status']}
    if not x['accepted']:
        db.execute("UPDATE substitute_batches SET status='capacity_rejected' WHERE id=?",
                   (b['id'],))
        return {'decision': 'capacity_rejected'}
    # Independent acceptance must prove real spare capacity before work runs.
    if x['independent_instances'] < 1:
        return {'decision': 'capacity_evidence_missing'}
    db.execute('UPDATE substitute_batches SET capacity_accepted=1, status=? WHERE id=?',
               ('capacity_accepted', b['id']))
    return {'decision': 'capacity_accepted'}


def _substitute_execute(db, x):
    b = one(db, 'SELECT * FROM substitute_batches WHERE id=?', (x['batch_id'],))
    if not b:
        return {'decision': 'batch_unknown'}
    if b['status'] == 'executed':
        # Repeated delivery must not create a second order.
        return {'decision': 'substitute_already_executed'}
    if not b['capacity_accepted']:
        return {'decision': 'capacity_not_accepted'}
    db.execute('UPDATE substitute_batches SET executed=1, status=? WHERE id=?',
               ('executed', b['id']))
    return {'decision': 'substitute_executed'}


def _substitute_signoff(db, x):
    b = one(db, 'SELECT * FROM substitute_batches WHERE id=?', (x['batch_id'],))
    if not b:
        return {'decision': 'batch_unknown'}
    if not b['executed']:
        return {'decision': 'not_executed'}
    if b['customer_signed']:
        return {'decision': 'already_signed_off'}
    if not x['accepted']:
        db.execute("UPDATE substitute_batches SET status='customer_refused' WHERE id=?",
                   (b['id'],))
        return {'decision': 'customer_refused'}
    db.execute('UPDATE substitute_batches SET customer_signed=1, status=? WHERE id=?',
               ('signed_off', b['id']))
    return {'decision': 'customer_signed_off'}


def _settle(db, x):
    case = one(db, 'SELECT * FROM restitution_cases WHERE id=?', (x['case_id'],))
    if not case:
        return {'decision': 'case_unknown'}
    if case['state'] == 'settled':
        return {'decision': 'case_already_settled'}
    if case['loss_state'] == 'no_loss':
        # Lossless failure: no money is owed; the voided draft is the record.
        db.execute("UPDATE restitution_cases SET state='settled' WHERE id=?",
                   (case['id'],))
        return {'decision': 'settled_no_loss'}

    findings = many(db, 'SELECT * FROM restitution_findings WHERE case_id=?',
                    (case['id'],))
    pending = []
    for f in findings:
        c = f['classification']
        if c in ('provably_unfinished', 'processed_receipt_lost'):
            waived = one(db, '''SELECT 1 FROM service_fee_waivers
                                WHERE case_id=? AND order_key=?''',
                         (case['id'], f['order_key']))
            if not waived:
                pending.append({'order_key': f['order_key'], 'missing': 'service_fee_waiver'})
        if c == 'provably_unfinished':
            cash = many(db, '''SELECT * FROM bank_payments
                               WHERE case_id=? AND order_key=?''',
                        (case['id'], f['order_key']))
            batches = many(db, '''SELECT * FROM substitute_batches
                                  WHERE case_id=? AND order_key=?''',
                           (case['id'], f['order_key']))
            if cash:
                # A bounced payment stays on file as history; the duty is
                # only satisfied once at least one payment actually clears.
                if not any(p['status'] == 'cleared' for p in cash):
                    pending.append({'order_key': f['order_key'],
                                    'missing': 'bank_clearing'})
                # Principal and service fee must each clear separately.
                cleared_kinds = {p['kind'] for p in cash
                                 if p['status'] == 'cleared'}
                if 'principal_refund' not in cleared_kinds:
                    pending.append({'order_key': f['order_key'],
                                    'missing': 'principal_clearing'})
                if 'service_fee_refund' not in cleared_kinds:
                    pending.append({'order_key': f['order_key'],
                                    'missing': 'service_fee_clearing'})
            elif batches:
                # A refused batch stays on file; a reworked batch that the
                # customer finally signs off discharges the substitute duty.
                if not any(b['customer_signed'] for b in batches):
                    pending.append({'order_key': f['order_key'],
                                    'missing': 'customer_signoff'})
            else:
                pending.append({'order_key': f['order_key'],
                                'missing': 'cash_or_substitute_track'})
        if c == 'disputed':
            h = one(db, '''SELECT * FROM escrow_holds
                           WHERE case_id=? AND order_key=?''',
                    (case['id'], f['order_key']))
            if not h or h['status'] == 'held':
                pending.append({'order_key': f['order_key'],
                                'missing': 'escrow_resolution'})
            elif h['status'] == 'payable':
                payout = one(db, '''SELECT * FROM bank_payments
                                    WHERE case_id=? AND order_key=?
                                      AND kind='escrow_payout' ''',
                             (case['id'], f['order_key']))
                if not payout or payout['status'] != 'cleared':
                    pending.append({'order_key': f['order_key'],
                                    'missing': 'escrow_payout_clearing'})
        if c == 'precharged':
            r = one(db, '''SELECT * FROM payer_refunds
                           WHERE case_id=? AND order_key=?''',
                    (case['id'], f['order_key']))
            if not r or r['status'] != 'cleared':
                pending.append({'order_key': f['order_key'],
                                'missing': 'payer_refund_clearing'})
    if pending:
        return {'decision': 'settlement_blocked', 'pending': pending}
    db.execute("UPDATE restitution_cases SET state='settled' WHERE id=?",
               (case['id'],))
    return {'decision': 'settled'}


def _recovery_mark(db, x):
    db.execute('INSERT OR IGNORE INTO recovery_marks VALUES(?,?,?,?)',
               (x['id'], x['sla'], x['service'], x['at']))
    return {'decision': 'recovery_marked'}


def _incident_wave(db, x):
    """Event-sourced boot-wave record so the loss window replays fully."""
    old = one(db, 'SELECT * FROM boot_waves WHERE id=?', (x['id'],))
    if old:
        same = (old['service'] == x['service']
                and old['status'] == x['status']
                and old['generation'] == x.get('generation', 1)
                and old['deadline'] == x.get('deadline', 0))
        if not same:
            return {'decision': 'business_identity_conflict'}
        return {'decision': 'wave_already_recorded'}
    db.execute('INSERT INTO boot_waves VALUES(?,?,?,?,?,?)',
               (x['id'], x['service'], x.get('generation', 1),
                x.get('deadline', 0), x.get('level', 0), x['status']))
    return {'decision': 'incident_wave_recorded'}


def _instance(db, x):
    if x['op'] == 'instance_revoke':
        old = one(db, 'SELECT * FROM instance_acceptances WHERE id=?', (x['id'],))
        if not old:
            return {'decision': 'instance_unknown'}
        if old['revoked_at'] is not None:
            return {'decision': 'instance_already_revoked'}
        db.execute('UPDATE instance_acceptances SET revoked_at=? WHERE id=?',
                   (x['at'], old['id']))
        return {'decision': 'instance_revoked'}
    old = one(db, 'SELECT * FROM instance_acceptances WHERE id=?', (x['id'],))
    if old:
        return {'decision': 'business_identity_conflict'}
    db.execute('INSERT INTO instance_acceptances VALUES(?,?,?,?,?,?)',
               (x['id'], x['sla'], x['instance_id'], x['node'],
                x['accepted_at'], None))
    return {'decision': 'instance_accepted'}


def handle(db, x):
    op = x['op']
    if op in ('instance_accept', 'instance_revoke'):
        return _instance(db, x)
    if op == 'recovery_mark':
        return _recovery_mark(db, x)
    if op == 'incident_wave':
        return _incident_wave(db, x)
    if op == 'entry_receipt':
        old = one(db, 'SELECT * FROM entry_receipts WHERE id=?', (x['id'],))
        if old:
            return {'decision': 'business_identity_conflict'}
        # Same client request id retried: collapse onto the first business
        # order key instead of booking a second order.
        dup = one(db, '''SELECT * FROM entry_receipts
                         WHERE sla=? AND client_request_id=?''',
                  (x['sla'], x['client_request_id']))
        if dup:
            return {'decision': 'duplicate_client_request',
                    'order_key': dup['order_key']}
        db.execute('INSERT INTO entry_receipts VALUES(?,?,?,?,?,?,?)',
                   (x['id'], x['sla'], x['order_key'], x['client_request_id'],
                    x['amount'], x.get('service_fee'), x['received_at']))
        return {'decision': 'entry_recorded'}
    if op == 'customer_confirmation':
        db.execute('INSERT OR IGNORE INTO customer_confirmations VALUES(?,?,?,?)',
                   (x['id'], x['sla'], x['order_key'], x['signed_at']))
        return {'decision': 'confirmation_recorded'}
    if op == 'platform_order':
        db.execute('INSERT OR IGNORE INTO platform_orders VALUES(?,?,?,?,?)',
                   (x['id'], x['sla'], x['order_key'], x['seq'],
                   x['processed_at']))
        return {'decision': 'platform_order_recorded'}
    if op == 'payer_charge':
        db.execute('INSERT OR IGNORE INTO payer_charges VALUES(?,?,?,?,?)',
                   (x['id'], x['sla'], x['order_key'], x['amount'],
                    x['charged_at']))
        return {'decision': 'payer_charge_recorded'}
    if op == 'adjudicate':
        return _adjudicate(db, x)
    if op == 'bank_pay':
        return _bank_pay(db, x)
    if op == 'bank_clear':
        return _bank_clear(db, x)
    if op == 'fee_waiver':
        return _fee_waiver(db, x)
    if op == 'escrow_hold':
        return _escrow_hold(db, x)
    if op == 'escrow_resolve':
        return _escrow_resolve(db, x)
    if op == 'payer_refund':
        return _payer_refund(db, x)
    if op == 'payer_refund_clear':
        return _payer_refund_clear(db, x)
    if op == 'recharge_lock':
        return _recharge_lock(db, x)
    if op == 'substitute_plan':
        return _substitute_plan(db, x)
    if op == 'substitute_capacity':
        return _substitute_capacity(db, x)
    if op == 'substitute_execute':
        return _substitute_execute(db, x)
    if op == 'substitute_signoff':
        return _substitute_signoff(db, x)
    if op == 'settle':
        return _settle(db, x)
    return {'decision': 'unknown_operation'}


def run(db, x):
    return act(db, 'restitution', x, handle)
