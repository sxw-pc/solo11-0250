import json

from .store import act, one, many

SCHEMA = '''
CREATE TABLE IF NOT EXISTS image_bundles(id TEXT PRIMARY KEY, service TEXT, generation INTEGER, digest TEXT, signer TEXT, tag TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS build_attestations(id TEXT PRIMARY KEY, bundle TEXT, builder TEXT, manifest_digest TEXT, at INTEGER);
CREATE TABLE IF NOT EXISTS image_mounts(id TEXT PRIMARY KEY, bundle TEXT, attempt TEXT, node TEXT, state TEXT);
CREATE TABLE IF NOT EXISTS build_evidence(id TEXT PRIMARY KEY, bundle TEXT, source_commit TEXT, lockfile_digest TEXT, build_env TEXT, toolchain TEXT, signer TEXT, builder TEXT, artifact_digest TEXT, service TEXT, service_generation INTEGER, trust_boundary TEXT, at INTEGER);
CREATE TABLE IF NOT EXISTS artifact_observations(id TEXT PRIMARY KEY, bundle TEXT, kind TEXT, node TEXT, digest TEXT, at INTEGER);
CREATE TABLE IF NOT EXISTS reproductions(id TEXT PRIMARY KEY, bundle TEXT, builder TEXT, source_commit TEXT, lockfile_digest TEXT, build_env TEXT, toolchain TEXT, result_digest TEXT, differences TEXT, conclusion TEXT, at INTEGER);
CREATE TABLE IF NOT EXISTS artifact_licenses(id TEXT PRIMARY KEY, bundle TEXT, lockfile_digest TEXT, verdict TEXT, at INTEGER);
CREATE TABLE IF NOT EXISTS provenance_certificates(id TEXT PRIMARY KEY, bundle TEXT, digest TEXT, service TEXT, service_generation INTEGER, evidence TEXT, reproduction TEXT, basis TEXT, issued_at INTEGER, status TEXT);
CREATE TABLE IF NOT EXISTS revocations(id TEXT PRIMARY KEY, subject_kind TEXT, subject TEXT, disposition TEXT, scope TEXT, at INTEGER);
CREATE TABLE IF NOT EXISTS feature_scopes(id TEXT PRIMARY KEY, bundle TEXT, feature TEXT, state TEXT, reason TEXT, at INTEGER);
'''

EVIDENCE_LAYERS = ('source_commit', 'lockfile_digest', 'build_env', 'toolchain', 'signer', 'builder')
REPRODUCTION_LAYERS = ('source_commit', 'lockfile_digest', 'build_env', 'toolchain', 'builder', 'result_digest')
SUBJECT_KINDS = ('signer', 'builder', 'toolchain', 'bundle')
OBSERVATION_KINDS = ('registry', 'cache', 'mount')


def latest_evidence(db, bundle):
    return one(db, 'SELECT * FROM build_evidence WHERE bundle=? ORDER BY at DESC, rowid DESC LIMIT 1', (bundle,))


def latest_observation(db, bundle, kind, node=None):
    if node is None:
        return one(db, 'SELECT * FROM artifact_observations WHERE bundle=? AND kind=? ORDER BY at DESC, rowid DESC LIMIT 1', (bundle, kind))
    return one(db, 'SELECT * FROM artifact_observations WHERE bundle=? AND kind=? AND node=? ORDER BY at DESC, rowid DESC LIMIT 1', (bundle, kind, node))


def revoked(db, kind, subject):
    return one(db, 'SELECT 1 AS hit FROM revocations WHERE subject_kind=? AND subject=?', (kind, subject)) is not None


def valid_certificate(db, bundle):
    # A certificate authorizes delivery only while its recorded trust basis is
    # outside every revocation; historical rows are never rewritten.
    cert = one(db, "SELECT * FROM provenance_certificates WHERE bundle=? AND status='issued' ORDER BY issued_at DESC, rowid DESC LIMIT 1", (bundle,))
    if not cert:
        return None
    if revoked(db, 'bundle', bundle):
        return None
    for element in json.loads(cert['basis']):
        kind, _, subject = element.partition(':')
        if revoked(db, kind, subject):
            return None
    return cert


def handle(db, x):
    op = x['op']
    if op == 'bundle':
        if not x['digest'] or not x['signer']:
            return {'decision': 'invalid_bundle'}
        db.execute('INSERT INTO image_bundles VALUES(?,?,?,?,?,?,?)',
                   (x['id'], x['service'], x['generation'], x['digest'], x['signer'], x.get('tag'), 'registered'))
        return {'decision': 'bundle_registered'}
    if op == 'attest':
        bundle = one(db, 'SELECT * FROM image_bundles WHERE id=?', (x['bundle'],))
        if not bundle:
            return {'decision': 'bundle_missing'}
        if not x.get('builder') or not x.get('manifest_digest'):
            return {'decision': 'attestation_rejected'}
        db.execute('INSERT INTO build_attestations VALUES(?,?,?,?,?)',
                   (x['id'], x['bundle'], x['builder'], x['manifest_digest'], x['at']))
        return {'decision': 'attestation_recorded'}
    if op == 'evidence':
        # Layer-bound build evidence: source commit, dependency lockfile, build
        # environment, toolchain, signer identity, artifact digest and the
        # customer-approved (sealed) service revision must all line up.
        bundle = one(db, 'SELECT * FROM image_bundles WHERE id=?', (x['bundle'],))
        if not bundle:
            return {'decision': 'evidence_rejected', 'reason': 'bundle_missing'}
        if any(not x.get(layer) for layer in EVIDENCE_LAYERS):
            return {'decision': 'evidence_rejected', 'reason': 'layer_missing'}
        if x.get('artifact_digest') != bundle['digest']:
            return {'decision': 'evidence_rejected', 'reason': 'digest_mismatch'}
        if x.get('service') != bundle['service'] or x.get('service_generation') != bundle['generation']:
            return {'decision': 'evidence_rejected', 'reason': 'service_mismatch'}
        revision = one(db, 'SELECT * FROM service_revisions WHERE id=?', (bundle['service'],))
        if not revision or revision['status'] != 'sealed' or revision['generation'] != bundle['generation']:
            return {'decision': 'evidence_rejected', 'reason': 'revision_unapproved'}
        db.execute('INSERT INTO build_evidence VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (x['id'], x['bundle'], x['source_commit'], x['lockfile_digest'], x['build_env'],
                    x['toolchain'], x['signer'], x['builder'], x['artifact_digest'],
                    x['service'], x['service_generation'], x.get('trust_boundary', ''), x.get('at', 0)))
        return {'decision': 'evidence_recorded'}
    if op == 'observe':
        # Registry image, node cache and mounted bytes are measured separately;
        # the newest measurement per location is the one delivery checks use.
        bundle = one(db, 'SELECT * FROM image_bundles WHERE id=?', (x['bundle'],))
        if not bundle:
            return {'decision': 'observation_rejected', 'reason': 'bundle_missing'}
        if x.get('kind') not in OBSERVATION_KINDS:
            return {'decision': 'observation_rejected', 'reason': 'kind_unknown'}
        if not x.get('digest'):
            return {'decision': 'observation_rejected', 'reason': 'digest_missing'}
        db.execute('INSERT INTO artifact_observations VALUES(?,?,?,?,?,?)',
                   (x['id'], x['bundle'], x['kind'], x.get('node'), x['digest'], x.get('at', 0)))
        return {'decision': 'observation_recorded'}
    if op == 'reproduce':
        # An independent rebuild publishes comparable inputs, the differences it
        # found and a reproducibility conclusion; unexplained divergence stays on
        # record and blocks certification.
        bundle = one(db, 'SELECT * FROM image_bundles WHERE id=?', (x['bundle'],))
        if not bundle:
            return {'decision': 'reproduction_rejected', 'reason': 'bundle_missing'}
        evidence = latest_evidence(db, x['bundle'])
        if not evidence:
            return {'decision': 'reproduction_rejected', 'reason': 'evidence_missing'}
        if any(not x.get(layer) for layer in REPRODUCTION_LAYERS):
            return {'decision': 'reproduction_rejected', 'reason': 'layer_missing'}
        same_inputs = (x['source_commit'] == evidence['source_commit']
                       and x['lockfile_digest'] == evidence['lockfile_digest'])
        differences = list(x.get('differences', []))
        if not same_inputs:
            conclusion = 'inputs_mismatched'
        elif x['result_digest'] == bundle['digest']:
            conclusion = 'reproducible'
        elif differences:
            conclusion = 'diverged_explained'
        else:
            conclusion = 'diverged_unexplained'
        db.execute('INSERT INTO reproductions VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                   (x['id'], x['bundle'], x['builder'], x['source_commit'], x['lockfile_digest'],
                    x['build_env'], x['toolchain'], x['result_digest'],
                    json.dumps(differences), conclusion, x.get('at', 0)))
        return {'decision': 'reproduction_recorded', 'conclusion': conclusion}
    if op == 'license':
        bundle = one(db, 'SELECT * FROM image_bundles WHERE id=?', (x['bundle'],))
        if not bundle:
            return {'decision': 'license_rejected', 'reason': 'bundle_missing'}
        if not x.get('lockfile_digest'):
            return {'decision': 'license_rejected', 'reason': 'lockfile_missing'}
        if x.get('verdict') not in ('approved', 'denied'):
            return {'decision': 'license_rejected', 'reason': 'verdict_unknown'}
        db.execute('INSERT INTO artifact_licenses VALUES(?,?,?,?,?)',
                   (x['id'], x['bundle'], x['lockfile_digest'], x['verdict'], x.get('at', 0)))
        return {'decision': 'license_recorded'}
    if op == 'certify':
        bundle = one(db, 'SELECT * FROM image_bundles WHERE id=?', (x['bundle'],))
        if not bundle:
            return {'decision': 'certification_denied', 'reason': 'bundle_missing'}
        evidence = latest_evidence(db, x['bundle'])
        if not evidence:
            return {'decision': 'certification_denied', 'reason': 'evidence_missing'}
        license_ = one(db, 'SELECT * FROM artifact_licenses WHERE bundle=? AND lockfile_digest=? ORDER BY at DESC, rowid DESC LIMIT 1',
                       (x['bundle'], evidence['lockfile_digest']))
        if not license_ or license_['verdict'] != 'approved':
            return {'decision': 'certification_denied', 'reason': 'license_unresolved'}
        untrusted = any(revoked(db, kind, evidence[kind]) for kind in ('signer', 'builder', 'toolchain'))
        reproduction_id = None
        if untrusted:
            # A leaked key ends the evidential value of the old signature: only an
            # independent builder reproducing the same bytes from the same source
            # and lockfile can re-establish provenance.
            reproduction = one(db, 'SELECT * FROM reproductions WHERE bundle=? ORDER BY at DESC, rowid DESC LIMIT 1', (x['bundle'],))
            if not reproduction:
                return {'decision': 'certification_denied', 'reason': 'signature_untrusted'}
            if reproduction['conclusion'] != 'reproducible':
                return {'decision': 'certification_denied', 'reason': 'reproduction_diverged'}
            if reproduction['builder'] == evidence['builder']:
                return {'decision': 'certification_denied', 'reason': 'builder_not_independent'}
            if revoked(db, 'builder', reproduction['builder']) or revoked(db, 'toolchain', reproduction['toolchain']):
                return {'decision': 'certification_denied', 'reason': 'reproduction_untrusted'}
            basis = ['builder:' + reproduction['builder'], 'toolchain:' + reproduction['toolchain']]
            reproduction_id = reproduction['id']
        else:
            basis = ['signer:' + evidence['signer'], 'builder:' + evidence['builder'], 'toolchain:' + evidence['toolchain']]
        db.execute('INSERT INTO provenance_certificates VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (x['id'], x['bundle'], bundle['digest'], evidence['service'], evidence['service_generation'],
                    evidence['id'], reproduction_id, json.dumps(basis), x.get('at', 0), 'issued'))
        return {'decision': 'certified', 'basis': basis}
    if op == 'revoke':
        # Append-only: the revocation adds affected scope and disposition records
        # without touching historical evidence, certificates or mounts.
        if x.get('subject_kind') not in SUBJECT_KINDS or not x.get('subject'):
            return {'decision': 'revocation_rejected', 'reason': 'subject_unknown'}
        affected_bundles = set()
        if x['subject_kind'] == 'bundle':
            affected_bundles.add(x['subject'])
        for row in many(db, 'SELECT * FROM build_evidence'):
            if x['subject_kind'] != 'bundle' and row[x['subject_kind']] == x['subject']:
                affected_bundles.add(row['bundle'])
        affected_certificates = []
        for cert in many(db, "SELECT * FROM provenance_certificates WHERE status='issued'"):
            covered = (x['subject_kind'] == 'bundle' and cert['bundle'] == x['subject']) or \
                      (x['subject_kind'] != 'bundle' and x['subject_kind'] + ':' + x['subject'] in json.loads(cert['basis']))
            if covered:
                affected_certificates.append(cert['id'])
                affected_bundles.add(cert['bundle'])
        scope = {'note': x.get('scope', ''), 'bundles': sorted(affected_bundles),
                 'certificates': sorted(affected_certificates)}
        db.execute('INSERT INTO revocations VALUES(?,?,?,?,?,?)',
                   (x['id'], x['subject_kind'], x['subject'], x.get('disposition', ''),
                    json.dumps(scope, sort_keys=True), x.get('at', 0)))
        return {'decision': 'revocation_recorded', 'affected': sorted(affected_bundles),
                'certificates': sorted(affected_certificates)}
    if op == 'feature':
        bundle = one(db, 'SELECT * FROM image_bundles WHERE id=?', (x['bundle'],))
        if not bundle:
            return {'decision': 'feature_rejected', 'reason': 'bundle_missing'}
        if not x.get('feature') or x.get('state') not in ('available', 'paused'):
            return {'decision': 'feature_rejected', 'reason': 'state_unknown'}
        db.execute('INSERT INTO feature_scopes VALUES(?,?,?,?,?,?)',
                   (x['id'], x['bundle'], x['feature'], x['state'], x.get('reason', ''), x.get('at', 0)))
        return {'decision': 'feature_recorded'}
    if op == 'mount':
        bundle = one(db, 'SELECT * FROM image_bundles WHERE id=?', (x['bundle'],))
        if not bundle or bundle['state'] != 'registered':
            return {'decision': 'bundle_unavailable'}
        if 'digest' not in x:
            db.execute('INSERT INTO image_mounts VALUES(?,?,?,?,?)',
                       (x['id'], x['bundle'], x['attempt'], x['node'], 'mounted'))
            return {'decision': 'mounted'}
        # Verified mount: measured bytes, the digest pinned by the start attempt,
        # a live provenance certificate and per-location measurements must all
        # agree; the mutable tag is never consulted.
        if x['digest'] != bundle['digest']:
            return {'decision': 'digest_mismatch'}
        attempt = one(db, 'SELECT * FROM start_attempts WHERE id=?', (x['attempt'],))
        if attempt and attempt['digest'] is not None and attempt['digest'] != bundle['digest']:
            return {'decision': 'digest_mismatch'}
        if not valid_certificate(db, x['bundle']):
            issued = one(db, "SELECT 1 AS hit FROM provenance_certificates WHERE bundle=? AND status='issued'", (x['bundle'],))
            return {'decision': 'provenance_revoked' if issued else 'provenance_missing'}
        registry = latest_observation(db, x['bundle'], 'registry')
        if not registry or registry['digest'] != bundle['digest']:
            return {'decision': 'registry_unverified'}
        cache = latest_observation(db, x['bundle'], 'cache', x['node'])
        if not cache or cache['digest'] != bundle['digest']:
            return {'decision': 'cache_unverified'}
        db.execute('INSERT INTO artifact_observations VALUES(?,?,?,?,?,?)',
                   (x['id'] + ':mount', x['bundle'], 'mount', x['node'], x['digest'], x.get('at', 0)))
        db.execute('INSERT INTO image_mounts VALUES(?,?,?,?,?)',
                   (x['id'], x['bundle'], x['attempt'], x['node'], 'mounted'))
        return {'decision': 'mounted'}
    return {'decision': 'unknown_operation'}


def run(db, x):
    return act(db, 'artifact', x, handle)
