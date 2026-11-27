"""Verifiable operator erasure with incident holds and replayable startup facts.

Two operators handled one cold-start probe anomaly on shared attempt A1.  One
operator's records are under a court ordered incident hold; the other asked to
be erased.  The plain event store keeps every command payload forever, so a
naive row delete is undone by the next replay.  This module performs erasure
that survives replay, backup restore and late probes while keeping the
incident hold reviewable and the startup business verdicts reproducible.

Two court approved arrangements are supported, and they are deliberately not
one deletion mode:

* ``escrow`` -- the shared original payloads are sealed by an independent
  judicial escrow custodian.  The operational store keeps only irreversible
  probe fact proofs and destroys the requester's identifiable bytes and
  backup copies.  The court reviews the held operator through the custodian.
* ``sealed`` -- the court controls the shared events as quarantined
  ciphertext.  Each operator's identity is wrapped under a per-operator key;
  the erased operator's decryption capability is destroyed while the held
  operator's case key and a non-identifying attempt-order proof remain.

An erasure receipt is issued only after every key copy and backup holder has
been accounted for, and the legacy replay path is fenced so it cannot
regenerate the erased operator across the quarantine.
"""

import hashlib
import hmac
import json
import secrets

from . import operator_record, operator_retention, ready
from .store import connect, many, one, tx

SCHEMA = '''
CREATE TABLE IF NOT EXISTS erasure_requests(
    id TEXT PRIMARY KEY, record_id TEXT, attempt TEXT, case_id TEXT,
    authority TEXT, mode TEXT, state TEXT, requested_at INTEGER
);
CREATE TABLE IF NOT EXISTS erasure_receipts(
    id TEXT PRIMARY KEY, request_id TEXT, record_id TEXT, attempt TEXT,
    case_id TEXT, mode TEXT, tombstone TEXT, events_sealed INTEGER,
    backups_destroyed INTEGER, keys_destroyed INTEGER, completed_at INTEGER
);
CREATE TABLE IF NOT EXISTS erasure_tombstones(
    record_id TEXT PRIMARY KEY, attempt TEXT, receipt_id TEXT, mode TEXT
);
CREATE TABLE IF NOT EXISTS event_quarantine(
    event_id TEXT PRIMARY KEY, attempt TEXT, mode TEXT, custodian TEXT,
    case_id TEXT, ciphertext TEXT, key_id TEXT, held_operator TEXT,
    erased_operator TEXT, attempt_order INTEGER
);
CREATE TABLE IF NOT EXISTS key_custody(
    key_id TEXT PRIMARY KEY, attempt TEXT, operator TEXT, purpose TEXT,
    holder TEXT, state TEXT, material TEXT
);
CREATE TABLE IF NOT EXISTS backup_copies(
    id TEXT PRIMARY KEY, attempt TEXT, holder TEXT, state TEXT, content TEXT
);
CREATE TABLE IF NOT EXISTS probe_fact_proofs(
    id TEXT PRIMARY KEY, attempt TEXT, probe_id TEXT, at INTEGER,
    healthy INTEGER, digest TEXT, basis TEXT
);
CREATE TABLE IF NOT EXISTS attempt_order_proofs(
    attempt TEXT PRIMARY KEY, probe_order TEXT, basis TEXT
);
CREATE TABLE IF NOT EXISTS incident_tickets(
    id TEXT PRIMARY KEY, attempt TEXT, actor TEXT, contact TEXT, note TEXT
);
'''

MODES = ('escrow', 'sealed')
IDENTITY_FIELDS = ('actor', 'operator', 'contact', 'badge', 'phone', 'email')
PLATFORM_HOLDERS = ('ops', 'platform')
HOLD_COLUMNS = ('probe_from', 'probe_until', 'expires')
LEGACY_REPLAY_MESSAGE = (
    'legacy replay is fenced: quarantined events for this attempt require '
    'operator_erasure.replay (post-erasure) or court_replay (held case)')


def _hash(*parts):
    digest = hashlib.sha256()
    for part in parts:
        digest.update(str(part).encode('utf-8'))
        digest.update(b'\x00')
    return digest.hexdigest()


def _key():
    return secrets.token_hex(16)


def _seal(key, plaintext):
    stream = hashlib.sha256(('stream:' + key).encode('utf-8')).digest()
    data = plaintext.encode('utf-8')
    body = bytes(b ^ stream[i % len(stream)] for i, b in enumerate(data))
    tag = hmac.new(key.encode('utf-8'), body, hashlib.sha256).hexdigest()
    return tag + ':' + body.hex()


def _unseal(key, ciphertext):
    tag, body_hex = ciphertext.split(':', 1)
    body = bytes.fromhex(body_hex)
    expected = hmac.new(key.encode('utf-8'), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(tag, expected):
        raise ValueError('sealed payload integrity check failed')
    stream = hashlib.sha256(('stream:' + key).encode('utf-8')).digest()
    data = bytes(b ^ stream[i % len(stream)] for i, b in enumerate(body))
    return data.decode('utf-8')


def _script(db, sql):
    # executescript() implicitly commits any pending transaction, which
    # would break execute_erasure's tx(); run the statements one by one.
    for statement in sql.split(';'):
        if statement.strip():
            db.execute(statement)


def _ensure_schema(db):
    _script(db, SCHEMA)
    _script(db, operator_record.SCHEMA)
    _script(db, operator_retention.SCHEMA)
    _script(db, ready.SCHEMA)
    _ensure_hold_columns(db)


def _ensure_hold_columns(db):
    cols = {row[1] for row in db.execute('PRAGMA table_info(operator_holds)')}
    for col in HOLD_COLUMNS:
        if col not in cols:
            db.execute(f'ALTER TABLE operator_holds ADD COLUMN {col} INTEGER')


def _attempt_events(db, attempt):
    return many(db, 'SELECT * FROM events WHERE payload LIKE ? ORDER BY seq',
                ('%"attempt": "' + attempt + '"%',))


def _redacted_payload(payload, mode):
    clean = {k: v for k, v in payload.items() if k not in IDENTITY_FIELDS}
    clean['quarantined'] = mode
    return json.dumps(clean, sort_keys=True, ensure_ascii=False)


def _proofs_for_attempt(db, attempt, basis):
    samples = many(
        db, 'SELECT * FROM probe_samples WHERE attempt=? ORDER BY at, id',
        (attempt,))
    for sample in samples:
        digest = _hash(sample['id'], sample['attempt'], sample['at'],
                       sample['healthy'])
        db.execute(
            'INSERT OR REPLACE INTO probe_fact_proofs VALUES(?,?,?,?,?,?,?)',
            ('proof-' + sample['id'], attempt, sample['id'], sample['at'],
             sample['healthy'], digest, basis))
    order = ','.join(sample['id'] for sample in samples)
    db.execute('INSERT OR REPLACE INTO attempt_order_proofs VALUES(?,?,?)',
               (attempt, order, basis))


def _quarantine_events(db, events, attempt, mode, custodian, case_id,
                       held_actor, erased_actor, escrow_key, held_key,
                       erased_key):
    """Move shared originals out of the operational store.

    The operational ``events`` row keeps only a redacted payload (business
    fields intact, identity fields removed) so replay can still reproduce
    startup facts without ever regenerating an operator identity.  The
    original goes to the quarantine under the arrangement's key:

    * ``escrow`` -- every original sealed under the custodian's key.
    * ``sealed`` -- the held operator's originals under the case key, the
      erased operator's originals under a wrapping key that is destroyed.
    """
    for order, event in enumerate(events):
        payload = json.loads(event['payload'])
        actor = payload.get('actor')
        if mode == 'escrow':
            ciphertext = _seal(escrow_key, event['payload'])
            key_id = 'escrow-' + attempt
        elif actor == erased_actor:
            ciphertext = _seal(erased_key, event['payload'])
            key_id = 'erased-' + attempt
        else:
            ciphertext = _seal(held_key, event['payload'])
            key_id = 'case-' + case_id + '-' + attempt
        db.execute(
            'INSERT OR REPLACE INTO event_quarantine VALUES(?,?,?,?,?,?,?,'
            '?,?,?)',
            (event['event_id'], attempt, mode, custodian, case_id, ciphertext,
             key_id, held_actor if actor == held_actor else None,
             erased_actor if actor == erased_actor else None, order))
        db.execute('UPDATE events SET payload=? WHERE event_id=?',
                   (_redacted_payload(payload, mode), event['event_id']))


def _scrub_business_rows(db, attempt, erased_actor):
    """Replace the requester's online rows with irreversible digests.

    Health samples, readiness receipts and attempt outcomes stay in place as
    business facts; only the identifying columns are reduced to one-way
    hashes so no phone number, employee id or reversible summary remains.
    """
    for row in many(db, 'SELECT * FROM operator_records WHERE attempt=?',
                    (attempt,)):
        if row['actor'] == erased_actor:
            db.execute(
                "UPDATE operator_records SET actor=?, contact=?, "
                "state='erased' WHERE id=?",
                ('erased-' + _hash(row['actor'])[:12],
                 'digest-' + _hash(row['contact']), row['id']))
    for row in many(db, 'SELECT * FROM incident_tickets WHERE attempt=?',
                    (attempt,)):
        if row['actor'] == erased_actor:
            db.execute(
                'UPDATE incident_tickets SET actor=?, contact=? WHERE id=?',
                ('erased-' + _hash(row['actor'])[:12],
                 'digest-' + _hash(row['contact']), row['id']))


def _destroy_platform_copies(db, attempt, erased_actor):
    """Destroy copies the platform itself holds; external holders must be
    attested destroyed before the receipt is issued."""
    backups = many(
        db, "SELECT * FROM backup_copies WHERE attempt=? AND state='held'",
        (attempt,))
    for backup in backups:
        db.execute(
            "UPDATE backup_copies SET state='destroyed', content=NULL "
            'WHERE id=?', (backup['id'],))
    keys = many(
        db, "SELECT * FROM key_custody WHERE attempt=? AND operator=? AND "
        "state='active'", (attempt, erased_actor))
    for key in keys:
        db.execute(
            "UPDATE key_custody SET state='destroyed', material=NULL "
            'WHERE key_id=?', (key['key_id'],))
    return backups, keys


def _pending_custody(db, attempt, erased_actor):
    """Copies held outside platform control that block the receipt."""
    report = custody_report(db, attempt)
    keys = [k for k in report['keys']
            if k['state'] == 'active' and k['operator'] == erased_actor
            and k['holder'] not in PLATFORM_HOLDERS]
    backups = [b for b in report['backups']
               if b['state'] == 'held'
               and b['holder'] not in PLATFORM_HOLDERS]
    return {'keys': keys, 'backups': backups}


def _active_hold(db, record_id):
    holds = many(
        db, "SELECT * FROM operator_holds WHERE record_id=? AND "
        "state='active'", (record_id,))
    return holds[0] if holds else None


# ---------------------------------------------------------------------------
# Custody registry


def register_backup(db, backup_id, attempt, holder, content):
    """Register an offline backup copy that must be accounted for."""
    _ensure_schema(db)
    db.execute('INSERT INTO backup_copies VALUES(?,?,?,?,?)',
               (backup_id, attempt, holder, 'held', content))


def register_key(db, key_id, attempt, operator, purpose, holder, material):
    """Register a key copy whose custody must be verified before erasure."""
    _ensure_schema(db)
    db.execute('INSERT INTO key_custody VALUES(?,?,?,?,?,?,?)',
               (key_id, attempt, operator, purpose, holder, 'active',
                material))


def destroy_backup(db, backup_id):
    """Attest destruction of one backup copy and drop its content."""
    _ensure_schema(db)
    db.execute(
        "UPDATE backup_copies SET state='destroyed', content=NULL WHERE id=?",
        (backup_id,))
    return {'decision': 'backup_destroyed', 'backup': backup_id}


def destroy_key(db, key_id):
    """Attest destruction of one key copy and drop its material."""
    _ensure_schema(db)
    db.execute(
        "UPDATE key_custody SET state='destroyed', material=NULL "
        'WHERE key_id=?', (key_id,))
    return {'decision': 'key_destroyed', 'key': key_id}


def custody_report(db, attempt):
    """List every key and backup holder that must be verified first."""
    _ensure_schema(db)
    return {
        'keys': many(db, 'SELECT * FROM key_custody WHERE attempt=?',
                     (attempt,)),
        'backups': many(db, 'SELECT * FROM backup_copies WHERE attempt=?',
                        (attempt,)),
    }


def custody_clear(db, attempt, erased_actor):
    pending = _pending_custody(db, attempt, erased_actor)
    return not pending['keys'] and not pending['backups']


# ---------------------------------------------------------------------------
# Incident holds


def incident_hold(db, hold_id, record_id, case_id, authority, probe_from,
                  probe_until, expires):
    """Place a court hold scoped to one operator, case and probe interval.

    The hold binds the case, the issuing authority, the specific operator
    record and the probe range with its term, so two operators working on
    the same attempt never freezes the other operator's data.
    """
    _ensure_schema(db)
    db.execute(
        'INSERT INTO operator_holds(id, record_id, case_id, authority, '
        'state, probe_from, probe_until, expires) VALUES(?,?,?,?,?,?,?,?)',
        (hold_id, record_id, case_id, authority, 'active', probe_from,
         probe_until, expires))
    return {'decision': 'hold_placed', 'hold': hold_id}


def narrow_hold(db, hold_id, probe_from, probe_until):
    """Narrow a hold to a smaller probe interval ordered by the court."""
    _ensure_schema(db)
    hold_row = one(db, 'SELECT * FROM operator_holds WHERE id=?', (hold_id,))
    if not hold_row or hold_row['state'] != 'active':
        return {'decision': 'hold_unavailable'}
    if probe_from > probe_until:
        return {'decision': 'invalid_interval'}
    db.execute(
        'UPDATE operator_holds SET probe_from=?, probe_until=? WHERE id=?',
        (probe_from, probe_until, hold_id))
    return {'decision': 'hold_narrowed', 'hold': hold_id,
            'probe_from': probe_from, 'probe_until': probe_until}


def erasure_plan(db):
    """Provisional erasure list: active records without an active hold and
    without a tombstone from an earlier erasure."""
    _ensure_schema(db)
    return [rid for rid in operator_retention.provisional_erasure(db)
            if not one(db, 'SELECT 1 FROM erasure_tombstones WHERE '
                       'record_id=?', (rid,))]


# ---------------------------------------------------------------------------
# Erasure


def request_erasure(db, request_id, record_id, case_id, authority, mode,
                    at=0):
    """Register an erasure request for one operator record.

    Refused while an active incident hold covers the record, and refused for
    an already erased record so a repeated erasure can never resurrect
    contact details.
    """
    _ensure_schema(db)
    if mode not in MODES:
        return {'decision': 'unknown_mode'}
    record_row = one(db, 'SELECT * FROM operator_records WHERE id=?',
                     (record_id,))
    if not record_row:
        return {'decision': 'unknown_record'}
    if _active_hold(db, record_id):
        return {'decision': 'held_by_incident', 'record_id': record_id}
    if one(db, 'SELECT 1 FROM erasure_tombstones WHERE record_id=?',
           (record_id,)):
        return {'decision': 'already_erased', 'record_id': record_id}
    old = one(db, 'SELECT * FROM erasure_requests WHERE id=?', (request_id,))
    if old:
        return {'decision': 'duplicate_request', 'record_id': record_id}
    db.execute(
        'INSERT INTO erasure_requests VALUES(?,?,?,?,?,?,?,?)',
        (request_id, record_id, record_row['attempt'], case_id, authority,
         mode, 'pending', at))
    return {'decision': 'erasure_requested', 'record_id': record_id,
            'attempt': record_row['attempt'], 'mode': mode}


def execute_erasure(db, request_id, custodian, at=0):
    """Carry out a requested erasure and issue the receipt.

    The receipt is issued only after every externally held key copy and
    backup of the requester has been attested destroyed, so the receipt is
    never contradicted by a surviving copy.
    """
    _ensure_schema(db)
    request = one(db, 'SELECT * FROM erasure_requests WHERE id=?',
                  (request_id,))
    if not request or request['state'] != 'pending':
        return {'decision': 'request_unavailable'}
    record_row = one(db, 'SELECT * FROM operator_records WHERE id=?',
                     (request['record_id'],))
    if not record_row:
        return {'decision': 'unknown_record'}
    if _active_hold(db, request['record_id']):
        return {'decision': 'held_by_incident',
                'record_id': request['record_id']}
    attempt = request['attempt']
    erased_actor = record_row['actor']
    mode = request['mode']

    # The two arrangements are not interchangeable: one attempt commits to
    # exactly one mode, or a restored store would hand the same event an
    # identity that contradicts the receipt.
    tomb = one(db, 'SELECT * FROM erasure_tombstones WHERE attempt=?',
               (attempt,))
    if tomb and tomb['mode'] != mode:
        return {'decision': 'mode_conflict', 'attempt': attempt,
                'committed': tomb['mode'], 'requested': mode}

    # Verify every key copy and backup holder before the receipt.
    pending = _pending_custody(db, attempt, erased_actor)
    if pending['keys'] or pending['backups']:
        return {'decision': 'custody_not_verified', 'pending': pending}

    held = many(
        db, 'SELECT r.* FROM operator_records r WHERE r.attempt=? AND '
        'r.id != ? AND EXISTS (SELECT 1 FROM operator_holds h WHERE '
        "h.record_id=r.id AND h.state='active')",
        (attempt, request['record_id']))
    held_actor = held[0]['actor'] if held else None
    held_case = one(
        db, "SELECT case_id FROM operator_holds WHERE record_id=? AND "
        "state='active'", (held[0]['id'],))['case_id'] if held \
        else request['case_id']

    events = _attempt_events(db, attempt)
    with tx(db):
        # 1. Irreversible probe fact proofs keep the startup verdicts
        #    replayable without any operator identity.
        _proofs_for_attempt(db, attempt, basis=mode + ':' + request_id)
        # 2. Quarantine the shared originals for the court, per arrangement.
        if mode == 'escrow':
            escrow_key = _key()
            _quarantine_events(db, events, attempt, mode, custodian,
                               held_case, held_actor, erased_actor,
                               escrow_key, None, None)
            db.execute(
                'INSERT OR REPLACE INTO key_custody VALUES(?,?,?,?,?,?,?)',
                ('escrow-' + attempt, attempt, None, 'escrow-originals',
                 custodian, 'active', escrow_key))
        else:
            held_key, erased_key = _key(), _key()
            _quarantine_events(db, events, attempt, mode, custodian,
                               held_case, held_actor, erased_actor,
                               None, held_key, erased_key)
            db.execute(
                'INSERT OR REPLACE INTO key_custody VALUES(?,?,?,?,?,?,?)',
                ('case-' + held_case + '-' + attempt, attempt, held_actor,
                 'case-review', custodian, 'active', held_key))
            db.execute(
                'INSERT OR REPLACE INTO key_custody VALUES(?,?,?,?,?,?,?)',
                ('erased-' + attempt, attempt, erased_actor,
                 'identity-wrap', custodian, 'destroyed', None))
        # 3. Destroy the requester's identifiable bytes everywhere online.
        _scrub_business_rows(db, attempt, erased_actor)
        backups, keys = _destroy_platform_copies(db, attempt, erased_actor)
        # 4. The receipt is issued only with a clean custody sheet.
        if not custody_clear(db, attempt, erased_actor):
            raise ValueError('custody not clear at receipt time')
        tombstone = _hash(request_id, erased_actor, attempt, at)
        receipt_id = 'receipt-' + request_id
        db.execute(
            'INSERT INTO erasure_receipts VALUES(?,?,?,?,?,?,?,?,?,?,?)',
            (receipt_id, request_id, request['record_id'], attempt,
             request['case_id'], mode, tombstone, len(events), len(backups),
             len(keys), at))
        db.execute('INSERT INTO erasure_tombstones VALUES(?,?,?,?)',
                   (request['record_id'], attempt, receipt_id, mode))
        db.execute("UPDATE erasure_requests SET state='executed' WHERE id=?",
                   (request_id,))
    return {'decision': 'erased', 'receipt': receipt_id, 'mode': mode,
            'events_sealed': len(events), 'backups_destroyed': len(backups),
            'keys_destroyed': len(keys), 'tombstone': tombstone}


def erase_operator(db, request_id, record_id, case_id, authority, mode,
                   custodian, at=0):
    """Convenience: request and execute one erasure in a single step."""
    requested = request_erasure(db, request_id, record_id, case_id,
                                authority, mode, at)
    if requested['decision'] != 'erasure_requested':
        return requested
    return execute_erasure(db, request_id, custodian, at)


def file_incident_ticket(db, ticket_id, attempt, actor, contact, note):
    """Record an incident collaboration ticket for one attempt."""
    _ensure_schema(db)
    db.execute('INSERT INTO incident_tickets VALUES(?,?,?,?,?)',
               (ticket_id, attempt, actor, contact, note))


# ---------------------------------------------------------------------------
# Post-erasure guards


def record_late_probe(db, probe_id, attempt, at, healthy):
    """Record a probe that arrives after erasure without re-identifying.

    The sample is stored as an irreversible fact proof only; no operator
    identity can flow back into the operational tables through a late probe.
    """
    _ensure_schema(db)
    if one(db, 'SELECT 1 FROM erasure_tombstones WHERE attempt=?',
           (attempt,)):
        digest = _hash(probe_id, attempt, at, int(healthy))
        db.execute(
            'INSERT OR REPLACE INTO probe_fact_proofs VALUES(?,?,?,?,?,?,?)',
            ('proof-' + probe_id, attempt, probe_id, at, int(healthy),
             digest, 'late'))
        return {'decision': 'recorded_as_proof', 'probe': probe_id}
    db.execute('INSERT INTO probe_samples VALUES(?,?,?,?)',
               (probe_id, attempt, at, int(healthy)))
    return {'decision': 'sampled', 'probe': probe_id}


def restore_backup(db, backup_id):
    """Restore an offline backup; erased content stays unidentifiable.

    A backup destroyed at erasure time has no content left to restore, and
    any pre-erasure copy that resurfaces afterwards is quarantined instead
    of merged, so the requester's contact details never re-enter ordinary
    operational queries.
    """
    _ensure_schema(db)
    backup = one(db, 'SELECT * FROM backup_copies WHERE id=?', (backup_id,))
    if not backup:
        return {'decision': 'unknown_backup'}
    if backup['state'] == 'destroyed' or backup['content'] is None:
        return {'decision': 'backup_destroyed', 'backup': backup_id}
    if one(db, 'SELECT 1 FROM erasure_tombstones WHERE attempt=?',
           (backup['attempt'],)):
        db.execute(
            "UPDATE backup_copies SET state='quarantined', content=NULL "
            'WHERE id=?', (backup_id,))
        return {'decision': 'quarantined_not_restored', 'backup': backup_id}
    return {'decision': 'restored', 'backup': backup_id}


# ---------------------------------------------------------------------------
# Court access and replay


def court_review(db, case_id, custodian, key_id=None):
    """Open the held operator's originals for court review.

    The court reconstructs the probe signing order and the incident sequence
    for the held operator only, either from the escrow custodian or from the
    case key it controls.  The erased operator's bytes are never returned.
    """
    _ensure_schema(db)
    sealed_rows = many(
        db, 'SELECT * FROM event_quarantine WHERE case_id=? ORDER BY '
        'attempt_order', (case_id,))
    if not sealed_rows:
        return {'decision': 'no_sealed_events', 'case_id': case_id}
    mode = sealed_rows[0]['mode']
    opened = []
    for row in sealed_rows:
        if row['held_operator'] is None:
            opened.append({'event_id': row['event_id'],
                           'attempt_order': row['attempt_order'],
                           'operator': None, 'payload': None})
            continue
        key_row = one(
            db, 'SELECT * FROM key_custody WHERE key_id=? AND holder=?',
            (key_id or row['key_id'], custodian))
        if not key_row or key_row['state'] != 'active' or \
                key_row['material'] is None:
            return {'decision': 'key_unavailable', 'case_id': case_id}
        payload = json.loads(_unseal(key_row['material'], row['ciphertext']))
        opened.append({'event_id': row['event_id'],
                       'attempt_order': row['attempt_order'],
                       'operator': row['held_operator'],
                       'payload': payload})
    return {'decision': 'court_review', 'case_id': case_id, 'mode': mode,
            'events': opened}


def attempt_order_proof(db, attempt):
    """Non-identifying proof of the probe order for one attempt."""
    _ensure_schema(db)
    proof = one(db, 'SELECT * FROM attempt_order_proofs WHERE attempt=?',
                (attempt,))
    if not proof:
        return {'decision': 'no_proof', 'attempt': attempt}
    order = proof['probe_order'].split(',') if proof['probe_order'] else []
    return {'decision': 'attempt_order', 'attempt': attempt,
            'probe_order': order, 'basis': proof['basis']}


def replay(path):
    """Replay the operational store after erasure.

    Every event is re-executed from its stored payload; quarantined events
    carry redacted payloads whose business fields reproduce the recorded
    startup facts.  The erased operator's identity is never regenerated.
    """
    source = connect(path)
    _ensure_schema(source)
    events = many(source, 'SELECT * FROM events ORDER BY seq')
    sealed = one(source, 'SELECT COUNT(*) n FROM event_quarantine')['n']
    target = connect(':memory:')
    _ensure_schema(target)
    mismatch = []
    from cloudcourt import MODULES
    for event in events:
        payload = json.loads(event['payload'])
        observed = MODULES[event['subsystem']].run(target, payload)
        if observed != json.loads(event['result']):
            mismatch.append({'event_id': event['event_id'],
                             'reason': 'decision'})
    business = ('probe_samples', 'readiness_receipts', 'start_attempts',
                'boot_waves', 'rollback_cases', 'service_revisions',
                'dependency_edges', 'bootstrap_tokens', 'degrade_grants',
                'startup_slots')
    for (table,) in source.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
            "('probe_samples','readiness_receipts','start_attempts',"
            "'boot_waves','rollback_cases','service_revisions',"
            "'dependency_edges','bootstrap_tokens','degrade_grants',"
            "'startup_slots') ORDER BY name"):
        a = [tuple(r) for r in source.execute(
            f'SELECT * FROM {table} ORDER BY rowid')]
        b = [tuple(r) for r in target.execute(
            f'SELECT * FROM {table} ORDER BY rowid')]
        if a != b:
            mismatch.append({'table': table, 'reason': 'state'})
    source.close()
    target.close()
    return {'verified': not mismatch, 'decisions': len(events),
            'sealed': sealed, 'mismatch': mismatch}


def legacy_replay(path):
    """Fence for the pre-erasure replay path.

    The old replay re-executes raw payloads and would regenerate the erased
    operator across the quarantine, so it is refused once any event is
    quarantined.  Use :func:`replay` for the operational store or
    :func:`court_replay` for a held case.
    """
    source = connect(path)
    _ensure_schema(source)
    sealed = one(source, 'SELECT COUNT(*) n FROM event_quarantine')['n']
    source.close()
    if sealed:
        return {'verified': False, 'decision': 'legacy_replay_fenced',
                'reason': LEGACY_REPLAY_MESSAGE}
    from .store import replay as _replay
    return _replay(path)


def court_replay(path, case_id, custodian, key_id=None):
    """Court-side replay of a held case from the custodian's originals."""
    source = connect(path)
    _ensure_schema(source)
    review = court_review(source, case_id, custodian, key_id)
    source.close()
    if review['decision'] != 'court_review':
        return review
    ordered = sorted((e for e in review['events'] if e['payload']),
                     key=lambda e: e['attempt_order'])
    return {'decision': 'court_replay', 'case_id': case_id,
            'mode': review['mode'],
            'sequence': [e['event_id'] for e in ordered],
            'operators': sorted({e['operator'] for e in ordered})}
