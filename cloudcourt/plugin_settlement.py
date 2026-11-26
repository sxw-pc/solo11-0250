"""Settlement of real bootstrap-adapter use, continuity rights and bank funds.

The provisional modules deliberately count console rows.  This module closes the
loop from evidence to legal/accounting state:

* usage is pinned to component digest, wave, site and an actual attempt/run
  interval; retries and duplicate activations collapse to one wave/site use;
* load measurements with overlapping clock intervals remain conflicting proof;
* licenses, assignments and price attachments are checked by signature,
  effective time, digest and site scope;
* escrow, bank clearance/dishonour, duplicate payment and customer-signed
  replacement are separate facts and none can be impersonated by another.

The module exposes both an event-ish :func:`run` operation interface and small
direct helpers.  All helpers accept an ordinary :class:`sqlite3.Connection`.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager

from .bootstrap_plugin import SCHEMA as BOOTSTRAP_SCHEMA
from .plugin_evidence import SCHEMA as EVIDENCE_SCHEMA
from .plugin_royalty import SCHEMA as ROYALTY_SCHEMA

SCHEMA = """
CREATE TABLE IF NOT EXISTS plugin_agreements(
    id TEXT PRIMARY KEY,
    kind TEXT,
    customer TEXT,
    supplier TEXT,
    claimant TEXT,
    digest TEXT,
    site TEXT,
    wave TEXT,
    model TEXT,
    unit_fee INTEGER,
    signed_at INTEGER,
    effective_from INTEGER,
    effective_to INTEGER,
    signatures TEXT,
    consent TEXT,
    non_interruptible INTEGER,
    escrow_account TEXT,
    payload TEXT,
    status TEXT
);
CREATE TABLE IF NOT EXISTS plugin_wave_results(
    id TEXT PRIMARY KEY,
    wave TEXT,
    site TEXT,
    status TEXT,
    attempt TEXT,
    started_at INTEGER,
    success_at INTEGER,
    ended_at INTEGER,
    source TEXT,
    payload TEXT,
    UNIQUE(wave, site)
);
CREATE TABLE IF NOT EXISTS plugin_rulings(
    id TEXT PRIMARY KEY,
    wave TEXT,
    site TEXT,
    digest TEXT,
    evidence TEXT,
    decided_by TEXT,
    at INTEGER,
    reason TEXT
);
CREATE TABLE IF NOT EXISTS plugin_obligations(
    id TEXT PRIMARY KEY,
    usage_key TEXT,
    wave TEXT,
    site TEXT,
    plugin TEXT,
    digest TEXT,
    started_at INTEGER,
    success_at INTEGER,
    ended_at INTEGER,
    basis TEXT,
    creditor TEXT,
    payee TEXT,
    quantity INTEGER,
    unit_fee INTEGER,
    amount INTEGER,
    status TEXT,
    reason TEXT,
    license_id TEXT,
    paid_amount INTEGER NOT NULL DEFAULT 0,
    discharge_payment TEXT,
    created_at INTEGER,
    updated_at INTEGER
);
CREATE TABLE IF NOT EXISTS plugin_license_certs(
    id TEXT PRIMARY KEY,
    customer TEXT,
    kind TEXT,
    scope TEXT,
    starts_at INTEGER,
    expires_at INTEGER,
    signatures TEXT,
    guarantee_id TEXT,
    escrow_id TEXT,
    status TEXT,
    issued_at INTEGER
);
CREATE TABLE IF NOT EXISTS plugin_escrows(
    id TEXT PRIMARY KEY,
    obligation TEXT,
    usage_key TEXT,
    account TEXT,
    amount INTEGER,
    at INTEGER,
    status TEXT,
    released_at INTEGER,
    released_to TEXT,
    proof TEXT
);
CREATE TABLE IF NOT EXISTS plugin_guarantees(
    id TEXT PRIMARY KEY,
    bank TEXT,
    number TEXT,
    supplier TEXT,
    customer TEXT,
    site TEXT,
    wave TEXT,
    digest TEXT,
    amount INTEGER,
    starts_at INTEGER,
    expires_at INTEGER,
    status TEXT,
    payload TEXT
);
CREATE TABLE IF NOT EXISTS plugin_payments(
    id TEXT PRIMARY KEY,
    obligation TEXT,
    usage_key TEXT,
    receipt_id TEXT UNIQUE,
    payer TEXT,
    payee TEXT,
    amount INTEGER,
    at INTEGER,
    state TEXT,
    channel TEXT,
    idem_key TEXT UNIQUE,
    detail TEXT
);
CREATE TABLE IF NOT EXISTS plugin_replacements(
    id TEXT PRIMARY KEY,
    wave TEXT,
    site TEXT,
    from_digest TEXT,
    to_digest TEXT,
    corrected_digest TEXT,
    vendor TEXT,
    window_start INTEGER,
    window_end INTEGER,
    installed_at INTEGER,
    compatible INTEGER,
    healthy INTEGER,
    status TEXT,
    customer_signature TEXT,
    signed_at INTEGER,
    note TEXT
);
CREATE TABLE IF NOT EXISTS plugin_corrections(
    id TEXT PRIMARY KEY,
    target_kind TEXT,
    target_id TEXT,
    kind TEXT,
    payload TEXT,
    at INTEGER,
    reason TEXT
);
CREATE TABLE IF NOT EXISTS plugin_ledger(
    id TEXT PRIMARY KEY,
    op TEXT,
    kind TEXT,
    account TEXT,
    obligation TEXT,
    amount INTEGER,
    at INTEGER
);
CREATE TABLE IF NOT EXISTS plugin_draft_links(
    draft TEXT PRIMARY KEY,
    obligation TEXT,
    usage_key TEXT
);
"""

_ALL_SCHEMA = "\n".join((BOOTSTRAP_SCHEMA, ROYALTY_SCHEMA, EVIDENCE_SCHEMA, SCHEMA))
# cloudcourt.store.connect executes module.SCHEMA; expose the complete
# seed+settlement DDL so event replay databases have every referenced table.
SCHEMA = _ALL_SCHEMA

PUBLISHED_STATES = {"published", "complete", "completed", "ready", "success", "successful"}
FAILED_STATES = {"failed", "probe_failed", "failure", "timed_out", "timeout", "rolled_back", "denied"}
RUNNING_STATES = {"running", "launching", "planned"}
GOOD_BANK_STATES = {"cleared", "settled", "paid", "received", "collected", "reconciled", "已清算", "已到账", "已收款"}
PENDING_BANK_STATES = {"pending", "processing", "in_transit", "debited", "sent", "待清算", "处理中", "在途", "已扣款"}
RETURNED_BANK_STATES = {"returned", "dishonored", "bounced", "chargeback", "reversed", "failed", "退票", "拒付", "退回"}
SITE_MODELS = {"published_site", "successful_site", "site", "per_site", "ready_site", "按成功站点", "按站点"}
WAVE_MODELS = {"first_loaded_wave", "loaded_wave", "wave", "per_wave", "first_load_wave", "按首次装载波次", "按波次"}


# ---------------------------------------------------------------------------
# Small DB helpers: unlike store.one these also work on a raw sqlite3 connect.
# ---------------------------------------------------------------------------

def one(db, sql, args=()):
    cur = db.execute(sql, args)
    row = cur.fetchone()
    if row is None:
        return None
    if isinstance(row, sqlite3.Row):
        return dict(row)
    columns = [item[0] for item in cur.description]
    return dict(zip(columns, row))


def many(db, sql, args=()):
    cur = db.execute(sql, args)
    columns = [item[0] for item in cur.description]
    return [dict(zip(columns, row)) if not isinstance(row, sqlite3.Row) else dict(row)
            for row in cur.fetchall()]


def ensure_schema(db):
    """Create seed and settlement tables. Safe to call repeatedly."""
    if one(db, "SELECT 1 AS x FROM sqlite_master WHERE type='table' AND name='plugin_agreements'"):
        return db
    if getattr(db, "row_factory", None) is None:
        db.row_factory = sqlite3.Row
    db.executescript(_ALL_SCHEMA)
    return db


@contextmanager
def _tx(db):
    nested = db.in_transaction
    db.execute("SAVEPOINT plugin_work" if nested else "BEGIN IMMEDIATE")
    try:
        yield
        db.execute("RELEASE SAVEPOINT plugin_work" if nested else "COMMIT")
    except Exception:
        if nested:
            db.execute("ROLLBACK TO SAVEPOINT plugin_work")
            db.execute("RELEASE SAVEPOINT plugin_work")
        else:
            db.execute("ROLLBACK")
        raise


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    if isinstance(value, dict):
        return [value]
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):
            try:
                parsed = json.loads(text)
                return parsed if isinstance(parsed, list) else [parsed]
            except json.JSONDecodeError:
                pass
        if "," in text:
            return [part.strip() for part in text.split(",") if part.strip()]
        return [text]
    return [value]


def _scope_values(value):
    values = []
    for item in _as_list(value):
        if isinstance(item, str) and item[:1] in ("[", '"'):
            try:
                item = json.loads(item)
            except json.JSONDecodeError:
                pass
        if isinstance(item, dict):
            for key in ("sites", "digests", "waves", "site", "digest", "wave", "id", "value"):
                if key in item:
                    values.extend(_scope_values(item[key]))
        elif isinstance(item, (list, tuple)):
            values.extend(_scope_values(item))
        else:
            values.append(item)
    return values


def scope_contains(stored_scope, actual):
    if actual is None:
        return True
    values = [str(v) for v in _scope_values(stored_scope)]
    if not values:
        return True
    if any(v in ("*", "%", "all", "any", "ALL") for v in values):
        return True
    actual = str(actual)
    for value in values:
        if value == actual:
            return True
        # Seed evidence uses customer_consent='site-1-only'.
        if value.endswith("-only") and value[:-5] == actual:
            return True
        if value.endswith("_only") and value[:-5] == actual:
            return True
    return False


def _table_exists(db, name):
    return one(db, "SELECT 1 AS x FROM sqlite_master WHERE type='table' AND name=?", (name,)) is not None


def _norm_bank_state(state):
    text = str(state or "").strip().lower()
    if text in GOOD_BANK_STATES:
        return "cleared"
    if text in RETURNED_BANK_STATES:
        return "returned"
    if text in PENDING_BANK_STATES:
        return "pending"
    return text or "unknown"


# ---------------------------------------------------------------------------
# Evidence interpretation
# ---------------------------------------------------------------------------

def load_verdict(db, wave, site):
    """Return the digest proof verdict for one wave/site.

    Two distinct-digest measurements whose uncertainty intervals overlap can
    never be ordered by their central observed timestamps alone.
    """
    ensure_schema(db)
    rows = many(db, "SELECT * FROM plugin_loads WHERE wave=? AND site=? ORDER BY rowid", (wave, site))
    measurements = []
    for row in rows:
        at = row["observed_at"]
        uncertainty = row["uncertainty"] or 0
        measurements.append({
            "id": row["id"],
            "digest": row["digest"],
            "observed_at": at,
            "uncertainty": uncertainty,
            "lower": at - uncertainty,
            "upper": at + uncertainty,
            "attestor": row["attestor"],
            "signed": bool(row["attestor"]),
        })

    ruling = one(db, "SELECT * FROM plugin_rulings WHERE wave=? AND site=? ORDER BY at DESC,rowid DESC LIMIT 1",
                 (wave, site))
    result = {
        "wave": wave,
        "site": site,
        "measurement_count": len(measurements),
        "signed_count": sum(1 for m in measurements if m["signed"]),
        "all_signed": bool(measurements) and all(m["signed"] for m in measurements),
        "measurements": measurements,
        "raw_status": "missing",
        "digest": None,
        "candidate_digests": sorted({m["digest"] for m in measurements if m["digest"]}),
        "overlap": False,
        "ruling": ruling["id"] if ruling else None,
        "status": "missing",
    }
    if not measurements:
        return result
    if not result["all_signed"]:
        result["raw_status"] = result["status"] = "signature_missing"
        return result

    digests = result["candidate_digests"]
    if len(digests) == 1:
        result["raw_status"] = "confirmed"
        result["status"] = "confirmed"
        result["digest"] = digests[0]
    else:
        groups = {d: [m for m in measurements if m["digest"] == d] for d in digests}
        bounds = {
            d: (min(m["lower"] for m in ms), max(m["upper"] for m in ms))
            for d, ms in groups.items()
        }
        # A digest is uniquely provable as the first execution only when its
        # whole uncertainty envelope is before every other digest's envelope.
        earliest = []
        for candidate in digests:
            candidate_lo, candidate_hi = bounds[candidate]
            if all(candidate_hi < bounds[other][0] for other in digests if other != candidate):
                earliest.append(candidate)
        # Record whether any distinct-digest pair overlaps for the report.
        for i, left in enumerate(digests):
            for right in digests[i + 1:]:
                left_lo, left_hi = bounds[left]
                right_lo, right_hi = bounds[right]
                if not (left_hi < right_lo or right_hi < left_lo):
                    result["overlap"] = True
        if len(earliest) == 1:
            result["raw_status"] = "ordered"
            result["status"] = "ordered"
            result["digest"] = earliest[0]
        else:
            result["raw_status"] = "overlap_conflict"
            result["status"] = "overlap_conflict"
            result["digest"] = None

    if ruling:
        result["final_digest"] = ruling["digest"]
        result["status"] = "resolved"
        result["digest"] = ruling["digest"]
    return result


def assignment_verdict(db, digest=None, site=None, wave=None, at=None):
    """Check seeded plugin_assignments and richer plugin_agreements."""
    ensure_schema(db)
    reasons = []
    matches = []

    for row in many(db, "SELECT * FROM plugin_assignments ORDER BY rowid"):
        ok = True
        why = []
        if digest is not None and not scope_contains(row["digest"], digest):
            ok = False
            why.append("digest_out_of_scope")
        if site is not None and not scope_contains(row["customer_consent"], site):
            ok = False
            why.append("site_consent_out_of_scope")
        if at is not None and row["effective_at"] is not None and row["effective_at"] > at:
            ok = False
            why.append("not_yet_effective")
        if ok:
            matches.append({"source": "plugin_assignments", "id": row["id"], "claimant": row["claimant"]})
        else:
            reasons.extend(why)

    rows = many(db, "SELECT * FROM plugin_agreements WHERE kind IN ('assignment','assign')")
    for row in rows:
        ok = True
        if digest is not None and not scope_contains(row["digest"], digest):
            ok = False
            reasons.append("digest_out_of_scope")
        if site is not None and not scope_contains(row["site"], site) and not scope_contains(row["consent"], site):
            ok = False
            reasons.append("site_consent_out_of_scope")
        if wave is not None and not scope_contains(row["wave"], wave):
            ok = False
            reasons.append("wave_out_of_scope")
        if at is not None:
            if row["effective_from"] is not None and row["effective_from"] > at:
                ok = False
                reasons.append("not_yet_effective")
            if row["effective_to"] is not None and row["effective_to"] < at:
                ok = False
                reasons.append("expired")
        signature_values = _scope_values(row["signatures"]) if row["signatures"] else []
        if not signature_values:
            ok = False
            reasons.append("unsigned")
        if ok:
            matches.append({"source": "plugin_agreements", "id": row["id"],
                            "claimant": row["claimant"] or row["supplier"]})

    covered = bool(matches)
    return {"covered": covered, "matches": matches,
            "claimant": matches[0]["claimant"] if matches else None,
            "reasons": sorted(set(reasons))}


def assignment_covers(db, digest=None, site=None, *args, **kwargs):
    # Tolerate both assignment_covers(db,d,site,at) and (db,d,site,wave,at).
    wave = kwargs.pop("wave", None)
    at = kwargs.pop("at", None)
    for value in args:
        if isinstance(value, (int, float)):
            at = value
        elif isinstance(value, str) and wave is None:
            wave = value
    return assignment_verdict(db, digest, site, wave=wave, at=at)["covered"]


# ---------------------------------------------------------------------------
# Real usage scan
# ---------------------------------------------------------------------------

def _outcome_from_text(status):
    text = str(status or "").strip().lower()
    if text in PUBLISHED_STATES:
        return "published"
    if text in FAILED_STATES:
        return "failed"
    if text in RUNNING_STATES:
        return "running"
    return "unknown"


def _wave_facts(db, wave, site, service=None):
    facts = {
        "outcome": "unstarted",
        "attempt_count": 0,
        "attempt_ids": [],
        "retry_count": 0,
        "successful_attempt": None,
        "successful_interval": None,
        "failed_intervals": [],
        "wave_status": None,
        "result_source": None,
    }
    if not wave:
        return facts

    explicit = one(db,
                   """SELECT * FROM plugin_wave_results
                      WHERE wave=? AND (site=? OR site='*' OR site IS NULL)
                      ORDER BY CASE WHEN site=? THEN 0 ELSE 1 END, rowid LIMIT 1""",
                   (wave, site, site))
    if explicit:
        facts["outcome"] = _outcome_from_text(explicit["status"])
        facts["result_source"] = "plugin_wave_results:" + explicit["id"]
        if explicit["attempt"]:
            facts["attempt_ids"].append(explicit["attempt"])
        if explicit["started_at"] is not None or explicit["success_at"] is not None:
            facts["successful_interval"] = {
                "attempt": explicit["attempt"],
                "started_at": explicit["started_at"],
                "success_at": explicit["success_at"],
                "ended_at": explicit["ended_at"],
            }

    if _table_exists(db, "boot_waves"):
        wave_row = one(db, "SELECT * FROM boot_waves WHERE id=?", (wave,))
        if wave_row:
            facts["wave_status"] = wave_row["status"]
            if facts["outcome"] == "unstarted":
                facts["outcome"] = _outcome_from_text(wave_row["status"])

    attempts = []
    if _table_exists(db, "start_attempts"):
        sql = "SELECT * FROM start_attempts WHERE wave=? ORDER BY attempt_no,rowid"
        attempts = many(db, sql, (wave,))
        facts["attempt_count"] = len(attempts)
        facts["attempt_ids"] = [a["id"] for a in attempts]
        facts["retry_count"] = max(0, len(attempts) - 1)

    published_receipt = None
    if attempts and _table_exists(db, "readiness_receipts"):
        ids = [a["id"] for a in attempts]
        marks = ",".join("?" * len(ids))
        receipts = many(db,
                        f"SELECT * FROM readiness_receipts WHERE attempt IN ({marks}) ORDER BY published_at,rowid",
                        ids)
        if receipts:
            published_receipt = receipts[-1]

    success_attempt = None
    if published_receipt:
        success_attempt = one(db, "SELECT * FROM start_attempts WHERE id=?",
                              (published_receipt["attempt"],)) if _table_exists(db, "start_attempts") else None
        facts["outcome"] = "published"
        facts["result_source"] = facts["result_source"] or "readiness_receipts:" + published_receipt["id"]
    else:
        for attempt in attempts:
            if attempt["status"] == "complete":
                success_attempt = attempt
                if facts["outcome"] != "published":
                    facts["outcome"] = "published"
                facts["result_source"] = facts["result_source"] or "start_attempts:complete"
                break

    if success_attempt:
        facts["successful_attempt"] = success_attempt["id"]
        success_at = published_receipt["published_at"] if published_receipt else None
        facts["successful_interval"] = {
            "attempt": success_attempt["id"],
            "started_at": success_attempt["started"],
            "success_at": success_at,
            "ended_at": success_at if success_at is not None else success_attempt["expires"],
        }

    # Probe failure gives a concrete end to an unsuccessful actual run.
    if _table_exists(db, "probe_samples"):
        for attempt in attempts:
            if attempt["id"] == facts["successful_attempt"]:
                continue
            latest = one(db,
                         "SELECT * FROM probe_samples WHERE attempt=? ORDER BY at DESC,rowid DESC LIMIT 1",
                         (attempt["id"],))
            first_bad = one(db,
                            "SELECT MIN(at) AS at FROM probe_samples WHERE attempt=? AND healthy=0",
                            (attempt["id"],))
            if latest and not latest["healthy"]:
                if facts["outcome"] in ("unstarted", "running", "unknown"):
                    facts["outcome"] = "failed"
                facts["failed_intervals"].append({
                    "attempt": attempt["id"],
                    "started_at": attempt["started"],
                    "failed_at": first_bad["at"] if first_bad and first_bad["at"] is not None else latest["at"],
                    "ended_at": first_bad["at"] if first_bad and first_bad["at"] is not None else attempt["expires"],
                })
            elif attempt["status"] in ("timed_out", "failed", "rolled_back"):
                if facts["outcome"] in ("unstarted", "running", "unknown"):
                    facts["outcome"] = "failed"
                facts["failed_intervals"].append({
                    "attempt": attempt["id"],
                    "started_at": attempt["started"],
                    "failed_at": None,
                    "ended_at": attempt["expires"],
                })

    if not attempts and explicit and explicit["started_at"] is not None:
        facts["attempt_count"] = max(facts["attempt_count"], 1)
    return facts


def actual_usage(db, at=None):
    """Return one deduplicated usage episode per plugin/service/wave/site."""
    ensure_schema(db)
    activations = many(db, """
        SELECT a.*, p.digest AS catalog_digest, p.publisher, p.license_id
        FROM plugin_activations a
        LEFT JOIN bootstrap_plugins p ON p.id=a.plugin
        ORDER BY a.rowid
    """)
    groups = {}
    order = []
    for row in activations:
        key = (row["plugin"], row["service"], row["wave"], row["site"])
        if key not in groups:
            order.append(key)
            groups[key] = {
                "key": "|".join("" if part is None else str(part) for part in key),
                "plugin": row["plugin"],
                "service": row["service"],
                "wave": row["wave"],
                "site": row["site"],
                "activation_ids": [],
                "activation_states": [],
                "duplicate_activation_count": 0,
            }
        group = groups[key]
        group["activation_ids"].append(row["id"])
        group["activation_states"].append(row["state"])
    for group in groups.values():
        group["duplicate_activation_count"] = max(0, len(group["activation_ids"]) - 1)

    episodes = []
    for key in order:
        episode = dict(groups[key])
        facts = _wave_facts(db, episode["wave"], episode["site"], episode["service"])
        episode.update(facts)
        proof = load_verdict(db, episode["wave"], episode["site"]) if episode["wave"] and episode["site"] else None
        episode["load_proof"] = proof
        episode["load_count"] = proof["measurement_count"] if proof else 0
        episode["digest"] = proof["digest"] if proof else None
        episode["raw_digest_status"] = proof["raw_status"] if proof else "missing"

        replacement = one(db,
                          """SELECT * FROM plugin_replacements
                             WHERE wave=? AND site=? AND status='accepted'
                             ORDER BY signed_at DESC,rowid DESC LIMIT 1""",
                          (episode["wave"], episode["site"]))
        episode["accepted_replacement"] = replacement["id"] if replacement else None
        if replacement:
            episode["current_digest"] = replacement["corrected_digest"] or replacement["to_digest"]
            episode["customer_signed_at"] = replacement["signed_at"]
        else:
            episode["current_digest"] = episode["digest"]

        has_start = episode["attempt_count"] > 0 or bool(episode["successful_interval"]) or \
            bool(episode["failed_intervals"])
        episode["actually_started"] = has_start
        if episode["outcome"] == "published":
            episode["usage_class"] = "successful"
            episode["billable"] = True
        elif has_start and episode["load_count"]:
            episode["usage_class"] = "attempted_loaded"
            episode["billable"] = True
        elif has_start:
            episode["usage_class"] = "attempted_without_load"
            episode["billable"] = False
        elif episode["load_count"]:
            episode["usage_class"] = "loaded_without_start"
            episode["billable"] = False
        else:
            episode["usage_class"] = "unstarted"
            episode["billable"] = False
        if at is not None:
            # `at` is an as-of filter for future-dated facts; current scan still
            # preserves all historical episodes.
            episode["as_of"] = at
        episodes.append(episode)
    return episodes


def usage_report(db, at=None):
    ensure_schema(db)
    episodes = actual_usage(db, at)
    standby = many(db, """
        SELECT p.* FROM bootstrap_plugins p
        WHERE NOT EXISTS (SELECT 1 FROM plugin_activations a WHERE a.plugin=p.id)
          AND NOT EXISTS (SELECT 1 FROM plugin_loads l WHERE l.digest=p.digest)
        ORDER BY p.id
    """) if _table_exists(db, "bootstrap_plugins") else []
    return {
        "at": at,
        "activation_rows": one(db, "SELECT COUNT(*) AS n FROM plugin_activations")["n"],
        "episode_count": len(episodes),
        "episodes": episodes,
        "standby_components": standby,
        "published_sites": sorted({e["site"] for e in episodes if e["outcome"] == "published"}),
        "successful_waves": sorted({e["wave"] for e in episodes if e["outcome"] == "published"}),
        "loaded_waves": sorted({e["wave"] for e in episodes if e["load_count"] and e["actually_started"]}),
        "indeterminate": [
            {"wave": e["wave"], "site": e["site"], "reason": e["load_proof"]["status"]}
            for e in episodes
            if e.get("load_proof") and e["load_proof"]["status"] == "overlap_conflict"
        ],
        "duplicate_activation_groups": [e["key"] for e in episodes if e["duplicate_activation_count"]],
        "retried_waves": [e["wave"] for e in episodes if e["retry_count"]],
    }


# ---------------------------------------------------------------------------
# Agreements / licenses
# ---------------------------------------------------------------------------

def _record_agreement(db, x, kind):
    aid = x.get("id")
    if not aid:
        return {"decision": "agreement_id_required"}
    old = one(db, "SELECT * FROM plugin_agreements WHERE id=?", (aid,))
    if old:
        return {"decision": "reused", "id": aid}

    suppliers = _as_list(x.get("suppliers") or x.get("parties") or x.get("supplier") or x.get("claimant"))
    signatures = x.get("signatures", x.get("signed_by"))
    signature_list = _as_list(signatures)
    customer = x.get("customer")
    escrow_account = x.get("escrow_account") or (x.get("escrow") if isinstance(x.get("escrow"), str) else None)
    scope = x.get("scope") if isinstance(x.get("scope"), dict) else {}
    digest = x.get("digests", x.get("digest", scope.get("digests", scope.get("digest", "*"))))
    site = x.get("sites", x.get("site", scope.get("sites", scope.get("site", "*"))))
    wave = x.get("waves", x.get("wave", scope.get("waves", scope.get("wave", "*"))))
    model = x.get("pricing_model", x.get("model", x.get("basis")))
    fee = x.get("unit_fee", x.get("fee", x.get("amount")))
    signed_at = x.get("signed_at", x.get("at"))
    effective_from = x.get("effective_from", x.get("effective_at", signed_at))
    effective_to = x.get("effective_to", x.get("until"))
    db.execute("""INSERT INTO plugin_agreements VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
               (aid, kind, customer, x.get("supplier") or (suppliers[0] if suppliers else None),
                x.get("claimant"), _json(digest), _json(site), _json(wave), model, fee,
                signed_at, effective_from, effective_to, _json(signature_list),
                x.get("customer_consent", x.get("consent")), int(bool(x.get("non_interruptible", False))),
                escrow_account, _json(x), x.get("status", "active")))
    return {"decision": "agreement_recorded", "id": aid, "kind": kind}


def _applicable_terms(db, episode, at=None):
    terms = many(db, "SELECT * FROM plugin_agreements WHERE model IS NOT NULL AND status!='withdrawn' ORDER BY rowid")
    # Authorization is pinned to the historical run start, not the later
    # reconciliation/as-of timestamp.  `at` remains available to callers but
    # must not make a late-signed term retroactive.
    start_at = (
        episode["successful_interval"]["started_at"] if episode.get("successful_interval") else None)
    if start_at is None and episode.get("failed_intervals"):
        start_at = episode["failed_intervals"][0]["started_at"]
    if start_at is None:
        start_at = 0
    applicable = []
    catalog_digest = None
    # The activation's catalog digest is only a nameplate, not load proof.
    if _table_exists(db, "bootstrap_plugins"):
        plugin = one(db, "SELECT digest,publisher FROM bootstrap_plugins WHERE id=?", (episode["plugin"],))
        if plugin:
            catalog_digest = plugin["digest"]
    for term in terms:
        if term["effective_from"] is not None and term["effective_from"] > start_at:
            continue
        if term["effective_to"] is not None and term["effective_to"] < start_at:
            continue
        digest_scope = json.loads(term["digest"]) if term["digest"] and term["digest"].startswith("[") else term["digest"]
        site_scope = json.loads(term["site"]) if term["site"] and term["site"].startswith("[") else term["site"]
        wave_scope = json.loads(term["wave"]) if term["wave"] and term["wave"].startswith("[") else term["wave"]
        digest_to_check = episode["digest"] or catalog_digest or "*"
        if not scope_contains(digest_scope, digest_to_check) and not scope_contains(digest_scope, episode["digest"]):
            continue
        if not scope_contains(site_scope, episode["site"]):
            continue
        if not scope_contains(wave_scope, episode["wave"]):
            continue
        model_class = "site" if term["model"] in SITE_MODELS else "wave" if term["model"] in WAVE_MODELS else "other"
        eligible = True
        reasons = []
        if model_class == "site" and episode["outcome"] != "published":
            eligible = False
            reasons.append("wave_not_published")
        if model_class == "wave":
            if not episode["actually_started"] or not episode["load_count"]:
                eligible = False
                reasons.append("no_actual_loaded_start")
            proof_status = episode["load_proof"]["status"] if episode.get("load_proof") else "missing"
            if proof_status in ("overlap_conflict", "missing", "signature_missing"):
                eligible = False
                reasons.append("digest_" + proof_status)
        creditor = term["claimant"] or term["supplier"]
        publisher = None
        if _table_exists(db, "bootstrap_plugins"):
            plugin = one(db, "SELECT publisher FROM bootstrap_plugins WHERE id=?", (episode["plugin"],))
            publisher = plugin["publisher"] if plugin else None
        if creditor and publisher and creditor != publisher:
            verdict = assignment_verdict(db, episode["digest"], episode["site"],
                                         wave=episode["wave"], at=start_at)
            if not verdict["covered"]:
                eligible = False
                reasons.append("assignment_out_of_scope")
        if not _scope_values(term["signatures"]):
            eligible = False
            reasons.append("unsigned_price_attachment")
        applicable.append({"term": term, "model_class": model_class, "eligible": eligible,
                           "reasons": reasons, "start_at": start_at, "creditor": creditor})
    return applicable


def reconcile(db, x=None, **kwargs):
    """Materialize idempotent obligation/claim rows from current evidence."""
    ensure_schema(db)
    payload = dict(x) if isinstance(x, dict) else {}
    payload.update(kwargs)
    payload.setdefault("op", "reconcile")
    if payload.get("event_id") and x is None:
        return run(db, payload)
    if payload.get("event_id"):
        from .store import act
        return act(db, "plugin_settlement", payload, handle)
    with _tx(db):
        return _reconcile(db, payload)


def _reconcile(db, x):
    at = x.get("at")
    report = usage_report(db, at)
    created_or_updated = []
    for episode in report["episodes"]:
        for assessment in _applicable_terms(db, episode, at):
            term = assessment["term"]
            basis = term["model"]
            creditor = assessment["creditor"] or "unknown"
            qty = 1
            fee = term["unit_fee"] if term["unit_fee"] is not None else 0
            amount = qty * fee
            oid = x.get("obligation_id") or "O|{}|{}|{}|{}|{}".format(
                episode["wave"], episode["site"], basis, creditor, term["id"])
            old = one(db, "SELECT * FROM plugin_obligations WHERE id=?", (oid,))
            status = "confirmed" if assessment["eligible"] else "blocked"
            reason = ",".join(assessment["reasons"]) or None
            now = x.get("at")
            interval = episode.get("successful_interval") or {}
            failed_interval = (episode.get("failed_intervals") or [{}])[0]
            started_at = interval.get("started_at", failed_interval.get("started_at"))
            success_at = interval.get("success_at")
            ended_at = interval.get("ended_at", failed_interval.get("ended_at"))
            if old and old["status"] in ("paid", "settled", "escrowed", "dishonored", "discharged", "withdrawn"):
                # Financial/claim facts are not rewritten by a later evidence
                # scan.  Corrections must go through the explicit path.
                status = old["status"]
                reason = old["reason"]
            if old:
                db.execute("""UPDATE plugin_obligations SET usage_key=?,wave=?,site=?,plugin=?,digest=?,
                    started_at=?,success_at=?,ended_at=?,basis=?,creditor=?,quantity=?,unit_fee=?,amount=?,
                    status=?,reason=?,updated_at=? WHERE id=?""",
                           (episode["key"], episode["wave"], episode["site"], episode["plugin"], episode["digest"],
                            started_at, success_at, ended_at, basis, creditor, qty, fee, amount,
                            status, reason, now, oid))
            else:
                db.execute("""INSERT INTO plugin_obligations
                    (id,usage_key,wave,site,plugin,digest,started_at,success_at,ended_at,basis,creditor,payee,
                     quantity,unit_fee,amount,status,reason,paid_amount,created_at,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?)""",
                           (oid, episode["key"], episode["wave"], episode["site"], episode["plugin"],
                            episode["digest"], started_at, success_at, ended_at, basis, creditor, creditor,
                            qty, fee, amount, status, reason, now, now))
            created_or_updated.append(oid)

    drafts = []
    if _table_exists(db, "royalty_drafts"):
        for draft in many(db, "SELECT * FROM royalty_drafts ORDER BY id"):
            publisher = draft["publisher"]
            active_n = one(db, """SELECT COUNT(*) AS n FROM plugin_activations a
                                  JOIN bootstrap_plugins p ON p.id=a.plugin
                                  WHERE p.publisher=? AND a.state='active'""", (publisher,))["n"]
            episodes = [e for e in report["episodes"] if e.get("catalog_digest") or True]
            drafts.append({
                "id": draft["id"], "publisher": publisher, "provisional_count": draft["count"],
                "active_rows": active_n, "provisional_amount": draft["amount"],
                "row_count_is_console_fact": draft["count"] == active_n,
                "warning": "active_rows_are_not_billable_usage" if draft["count"] else None,
            })

    obligations = many(db, "SELECT * FROM plugin_obligations ORDER BY id")
    return {
        "decision": "reconciled",
        "at": at,
        "usage": report,
        "obligation_ids": created_or_updated,
        "obligations": obligations,
        "drafts": drafts,
        "confirmed_amount": sum(o["amount"] for o in obligations if o["status"] == "confirmed"),
        "blocked_amount": sum(o["amount"] for o in obligations if o["status"] == "blocked"),
        "escrowed_amount": sum(o["amount"] for o in obligations if o["status"] == "escrowed"),
        "settled_amount": sum(o["paid_amount"] for o in obligations),
    }


# ---------------------------------------------------------------------------
# Licences and guarantees
# ---------------------------------------------------------------------------

def issue_license(db, x):
    lid = x.get("id")
    if not lid:
        return {"decision": "license_id_required"}
    old = one(db, "SELECT * FROM plugin_license_certs WHERE id=?", (lid,))
    if old:
        return {"decision": "reused", "id": lid}
    kind = x.get("kind", "continuation")
    signatures = _as_list(x.get("signatures", x.get("signed_by")))
    suppliers = _as_list(x.get("suppliers") or x.get("parties"))
    if kind in ("joint", "joint_continuation"):
        triparty = None
        if x.get("agreement"):
            triparty = one(db, "SELECT * FROM plugin_agreements WHERE id=? AND kind='triparty'", (x["agreement"],))
        if not triparty and (len(set(map(str, suppliers))) < 2 or len(signatures) < 2):
            return {"decision": "joint_signatures_required"}
    guarantee_id = x.get("guarantee_id", x.get("guarantee"))
    if kind in ("bank_guaranteed", "bank_guaranteed_alternative", "guaranteed_alternative"):
        guarantee = one(db, "SELECT * FROM plugin_guarantees WHERE id=?", (guarantee_id,)) if guarantee_id else None
        if not guarantee or guarantee["status"] != "active":
            return {"decision": "bank_guarantee_required"}
    starts = x.get("starts_at", x.get("at"))
    expires = x.get("expires_at", x.get("until"))
    if expires is None or starts is None or expires < starts:
        return {"decision": "invalid_license_window"}
    scope = x.get("scope") if isinstance(x.get("scope"), dict) else {
        "sites": _as_list(x.get("sites", "*")), "waves": _as_list(x.get("waves", "*")),
        "digests": _as_list(x.get("digests", "*")),
    }
    db.execute("INSERT INTO plugin_license_certs VALUES(?,?,?,?,?,?,?,?,?,?,?)",
               (lid, x.get("customer"), kind, _json(scope), starts, expires, _json(signatures),
                guarantee_id, x.get("escrow"), x.get("status", "active"), x.get("at", starts)))
    return {"decision": "license_issued", "id": lid, "kind": kind, "status": "active",
            "payment_received": False, "replacement_accepted": False}


def license_status(db, license_id, at=None):
    ensure_schema(db)
    row = one(db, "SELECT * FROM plugin_license_certs WHERE id=?", (license_id,))
    if not row:
        return None
    result = dict(row)
    if at is not None and row["expires_at"] is not None and at > row["expires_at"]:
        result["current_status"] = "expired"
    else:
        result["current_status"] = row["status"]
    # A licence certificate never proves bank receipt or a replacement.
    result["payment_received"] = False
    result["replacement_accepted"] = False
    if row["guarantee_id"]:
        result["guarantee"] = one(db, "SELECT * FROM plugin_guarantees WHERE id=?", (row["guarantee_id"],))
    return result


def has_continuing_rights(db, customer=None, site=None, wave=None, digest=None, at=None):
    ensure_schema(db)
    for cert in many(db, "SELECT * FROM plugin_license_certs WHERE status='active'"):
        if customer and cert["customer"] and cert["customer"] != customer:
            continue
        if at is not None:
            if cert["starts_at"] is not None and at < cert["starts_at"]:
                continue
            if cert["expires_at"] is not None and at > cert["expires_at"]:
                continue
        try:
            scope = json.loads(cert["scope"]) if cert["scope"] else {}
        except json.JSONDecodeError:
            scope = {}
        if site is not None and not scope_contains(scope.get("sites", "*"), site):
            continue
        if wave is not None and not scope_contains(scope.get("waves", "*"), wave):
            continue
        if digest is not None and not scope_contains(scope.get("digests", "*"), digest):
            continue
        return True
    for agreement in many(db, "SELECT * FROM plugin_agreements WHERE non_interruptible=1 AND status='active'"):
        if customer and agreement["customer"] and agreement["customer"] != customer:
            continue
        if at is not None:
            if agreement["effective_from"] is not None and at < agreement["effective_from"]:
                continue
            if agreement["effective_to"] is not None and at > agreement["effective_to"]:
                continue
        if site is not None and not scope_contains(agreement["site"], site):
            continue
        if wave is not None and not scope_contains(agreement["wave"], wave):
            continue
        if digest is not None and not scope_contains(agreement["digest"], digest):
            continue
        return True
    return False


def bank_guarantee(db, x):
    gid = x.get("id")
    if not gid or not x.get("bank") or x.get("amount") is None:
        return {"decision": "guarantee_details_required"}
    if one(db, "SELECT 1 FROM plugin_guarantees WHERE id=?", (gid,)):
        return {"decision": "reused", "id": gid}
    db.execute("INSERT INTO plugin_guarantees VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
               (gid, x["bank"], x.get("number"), x.get("supplier"), x.get("customer"), x.get("site"),
                x.get("wave"), x.get("digest"), x["amount"], x.get("starts_at", x.get("at")),
                x.get("expires_at", x.get("until")), x.get("status", "active"), _json(x)))
    return {"decision": "guarantee_issued", "id": gid, "payment_received": False,
            "replacement_accepted": False}


# ---------------------------------------------------------------------------
# Escrow and bank money
# ---------------------------------------------------------------------------

def _post(db, op, kind, account, obligation, amount, at):
    if not amount:
        return None
    lid = "{}|{}|{}|{}".format(op, kind, account, obligation or "")
    db.execute("INSERT OR IGNORE INTO plugin_ledger VALUES(?,?,?,?,?,?,?)",
               (lid, op, kind, account, obligation, amount, at))
    return {"id": lid, "kind": kind, "account": account, "amount": amount}


def _get_obligation(db, oid):
    return one(db, "SELECT * FROM plugin_obligations WHERE id=?", (oid,)) if oid else None


def escrow_deposit(db, x):
    eid = x.get("id")
    if not eid:
        return {"decision": "escrow_id_required"}
    old = one(db, "SELECT * FROM plugin_escrows WHERE id=?", (eid,))
    if old:
        return {"decision": "reused", "id": eid}
    oid = x.get("obligation")
    ob = _get_obligation(db, oid)
    if oid and not ob:
        return {"decision": "unknown_obligation", "obligation": oid}
    if ob:
        existing = one(db, "SELECT * FROM plugin_escrows WHERE obligation=? AND status='held'", (oid,))
        if existing:
            return {"decision": "duplicate_escrow", "escrow": existing["id"]}
    account = x.get("account")
    agreement = one(db, "SELECT * FROM plugin_agreements WHERE id=? AND kind='triparty'", (x.get("agreement"),)) \
        if x.get("agreement") else None
    if agreement and agreement["escrow_account"] and account != agreement["escrow_account"]:
        return {"decision": "wrong_escrow_account"}
    if not account:
        return {"decision": "escrow_account_required"}
    amount = int(x.get("amount", ob["amount"] if ob else 0))
    if amount <= 0:
        return {"decision": "invalid_amount"}
    at = x.get("at")
    usage_key = ob["usage_key"] if ob else x.get("usage_key")
    db.execute("INSERT INTO plugin_escrows VALUES(?,?,?,?,?,?,?,?,?,?)",
               (eid, oid, usage_key, account, amount, at, "held", None, None, None))
    _post(db, eid, "escrow_deposit", "escrow:" + account, oid, amount, at)
    _post(db, eid, "escrow_contra", "finance", oid, -amount, at)
    if ob:
        db.execute("UPDATE plugin_obligations SET status='escrowed',updated_at=? WHERE id=?", (at, oid))
    return {"decision": "escrowed", "id": eid, "amount": amount, "account": account,
            "supplier_paid": False, "bank_received": False}


def _recipient_right(db, recipient, ob, digest, at):
    """Check a direct licence or an assignment in the recipient's favour."""
    publisher = None
    if ob["plugin"]:
        plugin = one(db, "SELECT publisher FROM bootstrap_plugins WHERE id=?", (ob["plugin"],))
        publisher = plugin["publisher"] if plugin else None
    if recipient == publisher:
        for lic in many(db, "SELECT * FROM plugin_agreements WHERE kind IN ('license','contract') AND status='active'"):
            if lic["supplier"] not in (None, recipient):
                continue
            if at is not None and lic["effective_from"] is not None and lic["effective_from"] > at:
                continue
            if at is not None and lic["effective_to"] is not None and lic["effective_to"] < at:
                continue
            if _scope_values(lic["signatures"]) and \
                    scope_contains(lic["digest"], digest) and \
                    scope_contains(lic["site"], ob["site"]) and \
                    scope_contains(lic["wave"], ob["wave"]):
                return True, None
    verdict = assignment_verdict(db, digest, ob["site"], wave=ob["wave"], at=at)
    if verdict["covered"]:
        if recipient in (None, verdict["claimant"]):
            return True, None
        # A recorded assignee may direct release to its disclosed supplier.
        for match in verdict["matches"]:
            row_id = match["id"]
            row = one(db, "SELECT * FROM plugin_agreements WHERE id=?", (row_id,))
            if row and recipient in (row["supplier"], row["claimant"]):
                return True, None
    return False, "recipient_not_authorized"


def _release_allowed(db, escrow, x):
    ob = _get_obligation(db, escrow["obligation"]) if escrow["obligation"] else None
    # A refund to the customer is the one release that does not require a
    # supplier-rights proof; it still needs an explicit refund instruction.
    recipient = x.get("to") or x.get("recipient")
    if x.get("refund") and recipient == x.get("customer"):
        return True, None, None
    if not ob or not ob["wave"] or not ob["site"]:
        if not recipient:
            return False, "recipient_required", None
        return False, "obligation_scope_missing", None

    verdict = load_verdict(db, ob["wave"], ob["site"])
    if verdict["status"] in ("overlap_conflict", "missing", "signature_missing"):
        reason = "load_time_overlap" if verdict["status"] == "overlap_conflict" else verdict["status"]
        return False, reason, verdict
    if not recipient:
        return False, "recipient_required", verdict
    # Caller-supplied digest cannot override the signed evidence/ruling.
    digest = verdict["digest"] or ob["digest"]
    allowed, reason = _recipient_right(db, recipient, ob, digest, x.get("at"))
    if not allowed:
        # Preserve the more specific assignment scope reason when available.
        scope = assignment_verdict(db, digest, ob["site"], wave=ob["wave"], at=x.get("at"))
        if scope["reasons"]:
            reason = "assignment_out_of_scope"
        return False, reason, verdict
    return True, None, verdict


def release_escrow(db, x):
    rid = x.get("id")
    eid = x.get("escrow")
    escrow = one(db, "SELECT * FROM plugin_escrows WHERE id=?", (eid,)) if eid else None
    if not escrow:
        return {"decision": "unknown_escrow", "escrow": eid}
    if escrow["status"] == "released":
        return {"decision": "reused", "escrow": eid}
    allowed, reason, proof = _release_allowed(db, escrow, x)
    if not allowed and not x.get("override"):
        return {"decision": "escrow_locked", "escrow": eid, "reason": reason,
                "proof": proof, "status": "held"}
    to = x.get("to") or x.get("recipient")
    at = x.get("at")
    proof_id = x.get("proof") or (proof.get("ruling") if proof else None)
    db.execute("UPDATE plugin_escrows SET status='released',released_at=?,released_to=?,proof=? WHERE id=?",
               (at, to, proof_id, eid))
    _post(db, rid or eid + ":release", "escrow_release", "holder:" + str(to), escrow["obligation"],
          escrow["amount"], at)
    _post(db, rid or eid + ":release", "escrow_release", "escrow:" + escrow["account"],
          escrow["obligation"], -escrow["amount"], at)
    ob = _get_obligation(db, escrow["obligation"])
    if ob:
        db.execute("""UPDATE plugin_obligations SET status='settled',paid_amount=amount,
                      payee=?,updated_at=? WHERE id=?""", (to or ob["creditor"], at, ob["id"]))
    return {"decision": "escrow_released", "escrow": eid, "paid_to": to, "amount": escrow["amount"]}


def _receipt_obligation(db, receipt, explicit=None):
    if explicit:
        return explicit
    payment = one(db, "SELECT obligation FROM plugin_payments WHERE receipt_id=?", (receipt["id"],))
    if payment and payment["obligation"]:
        return payment["obligation"]
    link = one(db, "SELECT obligation FROM plugin_draft_links WHERE draft=?", (receipt["draft"],))
    if link:
        return link["obligation"]
    ob = _get_obligation(db, receipt["draft"])
    if ob:
        return ob["id"]
    return None


def clear_bank(db, receipt_id=None, at=None, obligation=None, event_id=None, **kwargs):
    payload = {"op": "clear_bank", "receipt": receipt_id, "at": at, "obligation": obligation}
    payload.update(kwargs)
    if event_id:
        payload["event_id"] = event_id
        return run(db, payload)
    ensure_schema(db)
    with _tx(db):
        return _clear_bank(db, payload)


def _clear_bank(db, x):
    rid = x.get("receipt") or x.get("receipt_id") or x.get("id")
    receipt = one(db, "SELECT * FROM plugin_bank_receipts WHERE id=?", (rid,))
    if not receipt:
        return {"decision": "unknown_bank_receipt", "receipt": rid}
    state = _norm_bank_state(receipt["bank_state"])
    if state == "returned":
        # A returned item must be explicitly represented as a new payment path.
        return {"decision": "bank_returned", "receipt": rid, "bank_state": "returned"}

    oid = _receipt_obligation(db, receipt, x.get("obligation"))
    ob = _get_obligation(db, oid)
    if x.get("obligation") and not ob:
        return {"decision": "unknown_obligation", "obligation": x["obligation"]}

    existing_payment = one(db, "SELECT * FROM plugin_payments WHERE receipt_id=?", (rid,))
    if existing_payment and existing_payment["state"] == "cleared":
        return {"decision": "reused", "payment": existing_payment["id"]}
    if existing_payment and existing_payment["state"] == "duplicate":
        return {"decision": "duplicate_payment", "payment": existing_payment["id"], "receipt": rid}

    if ob:
        already_row = one(db,
                          "SELECT COALESCE(SUM(amount),0) AS n FROM plugin_payments "
                          "WHERE obligation=? AND state='cleared' AND receipt_id!=?",
                          (ob["id"], rid))
        already = already_row["n"]
        if already >= ob["amount"]:
            # Record the duplicate attempt but never move money again.
            pid = x.get("payment_id") or "DUP|" + str(x.get("id") or rid)
            db.execute("INSERT OR IGNORE INTO plugin_payments VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                       (pid, ob["id"], ob["usage_key"], rid, x.get("payer"), receipt["payee"],
                        receipt["amount"], x.get("at"), "duplicate", "bank", pid,
                        _json({"reason": "obligation_already_paid", "already_paid": already})))
            return {"decision": "duplicate_payment", "obligation": ob["id"], "receipt": rid,
                    "already_paid": already}
        remaining = ob["amount"] - already
        if receipt["amount"] < remaining and not x.get("allow_partial"):
            # Partial payments are allowed only when explicitly acknowledged.
            return {"decision": "amount_mismatch", "expected": remaining, "received": receipt["amount"]}

    at = x.get("at")
    db.execute("UPDATE plugin_bank_receipts SET bank_state='cleared' WHERE id=?", (rid,))
    pid = x.get("payment_id") or x.get("id") or ("PAY|" + rid)
    oid_value = ob["id"] if ob else oid
    usage_key = ob["usage_key"] if ob else x.get("usage_key")
    db.execute("INSERT OR REPLACE INTO plugin_payments VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
               (pid, oid_value, usage_key, rid, x.get("payer"), receipt["payee"], receipt["amount"],
                at, "cleared", x.get("channel", "bank"), x.get("idem_key", pid), _json(x)))
    _post(db, pid, "bank_payment", "holder:" + str(receipt["payee"]), oid_value, receipt["amount"], at)
    _post(db, pid, "bank_clearing", "finance", oid_value, -receipt["amount"], at)
    if ob:
        paid = (ob["paid_amount"] or 0) + receipt["amount"]
        status = "settled" if paid >= ob["amount"] else "paid"
        db.execute("UPDATE plugin_obligations SET paid_amount=?,status=?,payee=?,discharge_payment=?,updated_at=? WHERE id=?",
                   (paid, status, receipt["payee"], pid, at, ob["id"]))
        if x.get("discharge_group") or one(db, "SELECT 1 FROM plugin_agreements WHERE kind='triparty' AND status='active'"):
            for other in many(db, "SELECT * FROM plugin_obligations WHERE usage_key=? AND id!=?",
                              (ob["usage_key"], ob["id"])):
                if other["status"] not in ("settled", "paid"):
                    db.execute("""UPDATE plugin_obligations SET status='discharged',
                                  reason=COALESCE(reason,'')||';paid_via:'||?, discharge_payment=?,updated_at=?
                                  WHERE id=?""", (pid, pid, at, other["id"]))
    return {"decision": "bank_cleared", "receipt": rid, "payment": pid, "obligation": oid_value,
            "payee": receipt["payee"], "amount": receipt["amount"], "supplier_received": True}


def return_bank(db, receipt_id=None, at=None, reason=None, event_id=None, **kwargs):
    payload = {"op": "return_bank", "receipt": receipt_id, "at": at, "reason": reason}
    payload.update(kwargs)
    if event_id:
        payload["event_id"] = event_id
        return run(db, payload)
    ensure_schema(db)
    with _tx(db):
        return _return_bank(db, payload)


def _return_bank(db, x):
    rid = x.get("receipt") or x.get("receipt_id")
    receipt = one(db, "SELECT * FROM plugin_bank_receipts WHERE id=?", (rid,))
    if not receipt:
        return {"decision": "unknown_bank_receipt", "receipt": rid}
    payment = one(db, "SELECT * FROM plugin_payments WHERE receipt_id=? AND state='cleared'", (rid,))
    db.execute("UPDATE plugin_bank_receipts SET bank_state='returned' WHERE id=?", (rid,))
    if not payment:
        return {"decision": "bank_returned", "receipt": rid, "supplier_received": False}
    at = x.get("at")
    db.execute("UPDATE plugin_payments SET state='returned',detail=? WHERE id=?",
               (_json({"reason": x.get("reason"), "returned_at": at}), payment["id"]))
    _post(db, payment["id"] + ":return", "bank_reversal", "holder:" + payment["payee"],
          payment["obligation"], -payment["amount"], at)
    _post(db, payment["id"] + ":return", "bank_reversal", "finance", payment["obligation"],
          payment["amount"], at)
    ob = _get_obligation(db, payment["obligation"])
    if ob:
        paid = max(0, (ob["paid_amount"] or 0) - payment["amount"])
        db.execute("UPDATE plugin_obligations SET paid_amount=?,status='dishonored',updated_at=? WHERE id=?",
                   (paid, at, ob["id"]))
        for other in many(db, "SELECT * FROM plugin_obligations WHERE discharge_payment=? AND status='discharged'",
                          (payment["id"],)):
            db.execute("UPDATE plugin_obligations SET status='confirmed',discharge_payment=NULL,updated_at=? WHERE id=?",
                       (at, other["id"]))
    return {"decision": "payment_dishonored", "receipt": rid, "payment": payment["id"],
            "supplier_received": False, "license_revoked": False}


def register_payment(db, x):
    """Register and clear a payment in one call; useful when no seeded receipt exists."""
    rid = x.get("receipt_id", x.get("id"))
    existing = one(db, "SELECT * FROM plugin_bank_receipts WHERE id=?", (rid,))
    if not existing:
        ensure_schema(db)
        db.execute("INSERT INTO plugin_bank_receipts VALUES(?,?,?,?,?)",
                   (rid, x.get("draft", x.get("obligation")), x.get("payee"), x.get("amount"),
                    x.get("bank_state", "cleared")))
    else:
        db.execute("UPDATE plugin_bank_receipts SET bank_state='cleared' WHERE id=?", (rid,))
    return _clear_bank(db, dict(x, receipt=rid))


def balances(db):
    ensure_schema(db)
    total = {r["account"]: r["total"] for r in many(
        db, "SELECT account,SUM(amount) AS total FROM plugin_ledger GROUP BY account")}
    by_kind = {}
    for row in many(db, "SELECT account,kind,SUM(amount) AS total FROM plugin_ledger GROUP BY account,kind"):
        by_kind.setdefault(row["account"], {})[row["kind"]] = row["total"]
    return {
        "total": total,
        "by_kind": by_kind,
        "bank_receipts": many(db, "SELECT * FROM plugin_bank_receipts ORDER BY id") if _table_exists(db, "plugin_bank_receipts") else [],
        "escrows": many(db, "SELECT * FROM plugin_escrows ORDER BY id"),
        "payments": many(db, "SELECT * FROM plugin_payments ORDER BY id"),
        "obligations": many(db, "SELECT * FROM plugin_obligations ORDER BY id"),
    }


def bank_settlement(db, at=None):
    ensure_schema(db)
    state = balances(db)
    good = [r for r in state["bank_receipts"] if _norm_bank_state(r["bank_state"]) == "cleared"]
    returned = [r for r in state["bank_receipts"] if _norm_bank_state(r["bank_state"]) == "returned"]
    pending = [r for r in state["bank_receipts"] if _norm_bank_state(r["bank_state"]) == "pending"]
    duplicate_payments = [p for p in state["payments"] if p["state"] == "duplicate"]
    duplicate_receipt_ids = {p["receipt_id"] for p in duplicate_payments}
    effective_good = [r for r in good if r["id"] not in duplicate_receipt_ids]
    gross_cleared = sum(r["amount"] for r in good)
    duplicate_amount = sum(p["amount"] for p in duplicate_payments)
    supplier_received = sum(r["amount"] for r in effective_good)
    escrow_held = sum(e["amount"] for e in state["escrows"] if e["status"] == "held")
    return {
        "decision": "bank_settlement",
        "at": at,
        "cleared_receipts": effective_good,
        "bank_cleared_receipts": good,
        "returned_receipts": returned,
        "pending_receipts": pending,
        "duplicate_payments": duplicate_payments,
        "supplier_received": supplier_received,
        "bank_cleared_total": gross_cleared,
        "duplicate_amount": duplicate_amount,
        "overpayment_refund_due": duplicate_amount,
        "escrow_held": escrow_held,
        "payments": state["payments"],
        "obligations": state["obligations"],
        "balances": state["total"],
    }


final_settlement = bank_settlement
settlement_report = bank_settlement


def statement(db, customer=None):
    """Customer-facing statement.

    Competing supplier claims for one same usage group are alternatives, not
    additive charges; ``total_claims`` exposes the gross claim sum separately.
    """
    ensure_schema(db)
    obligations = many(db, "SELECT * FROM plugin_obligations ORDER BY id")
    groups = {}
    for ob in obligations:
        key = ob["usage_key"] or ob["id"]
        group = groups.setdefault(key, {"usage_key": key, "claims": [], "amount_due": 0, "paid": 0})
        group["claims"].append(ob)
        group["amount_due"] = max(group["amount_due"], ob["amount"] or 0)
        group["paid"] += ob["paid_amount"] or 0
    licenses = many(db, "SELECT * FROM plugin_license_certs ORDER BY id")
    escrows = many(db, "SELECT * FROM plugin_escrows ORDER BY id")
    bank = bank_settlement(db)
    return {
        "customer": customer,
        "decision": "statement",
        "total_billed": sum(g["amount_due"] for g in groups.values()),
        "total_claims": sum(ob["amount"] or 0 for ob in obligations),
        "total_paid": sum(ob["paid_amount"] or 0 for ob in obligations),
        "usage_groups": list(groups.values()),
        "obligations": obligations,
        "licenses": licenses,
        "escrows": escrows,
        "bank": bank,
    }


def rights_report(db, customer=None):
    ensure_schema(db)
    return {
        "customer": customer,
        "licenses": many(db, "SELECT * FROM plugin_license_certs ORDER BY id"),
        "agreements": many(db, "SELECT * FROM plugin_agreements ORDER BY id"),
        "assignments": many(db, "SELECT * FROM plugin_assignments ORDER BY id")
        if _table_exists(db, "plugin_assignments") else [],
    }


def replacement_status(db, replacement_id=None, site=None, wave=None):
    ensure_schema(db)
    if replacement_id:
        return one(db, "SELECT * FROM plugin_replacements WHERE id=?", (replacement_id,))
    if site and wave:
        return one(db,
                   "SELECT * FROM plugin_replacements WHERE wave=? AND site=? ORDER BY rowid DESC LIMIT 1",
                   (wave, site))
    return None


def get_obligation(db, obligation_id):
    ensure_schema(db)
    return _get_obligation(db, obligation_id)


def obligation_status(db, obligation_id):
    ob = get_obligation(db, obligation_id)
    return ob["status"] if ob else None


def billable_usage(db, at=None):
    return [episode for episode in actual_usage(db, at) if episode.get("billable")]


def execution_quantity(db, at=None):
    report = usage_report(db, at)
    return {
        "activation_rows": report["activation_rows"],
        "usage_episodes": report["episode_count"],
        "published_sites": len(report["published_sites"]),
        "successful_waves": len(report["successful_waves"]),
        "loaded_waves": len(report["loaded_waves"]),
        "indeterminate_waves": len(report["indeterminate"]),
        "duplicate_activation_groups": len(report["duplicate_activation_groups"]),
        "retried_waves": len(report["retried_waves"]),
        "billable_episodes": len(billable_usage(db, at)),
    }


def settle(db, target=None, **kwargs):
    """Convenience entry point tolerant of receipt/escrow/obligation ids."""
    ensure_schema(db)
    if isinstance(target, dict):
        payload = dict(target)
    else:
        payload = dict(kwargs)
        if target is not None:
            if one(db, "SELECT 1 FROM plugin_bank_receipts WHERE id=?", (target,)):
                payload.setdefault("op", "clear_bank")
                payload["receipt"] = target
            elif one(db, "SELECT 1 FROM plugin_escrows WHERE id=?", (target,)):
                payload.setdefault("op", "release_escrow")
                payload["escrow"] = target
            elif _get_obligation(db, target):
                payload.setdefault("op", "reconcile")
                payload["obligation_id"] = target
            else:
                payload.setdefault("op", "reconcile")
    payload.setdefault("op", "bank_settlement" if target is None and not kwargs else "reconcile")
    return run(db, payload)


def record_receipt(db, payload=None, **kwargs):
    data = dict(payload) if isinstance(payload, dict) else {}
    data.update(kwargs)
    return register_payment(db, data)


def can_use(db, **kwargs):
    return has_continuing_rights(db, **kwargs)


# ---------------------------------------------------------------------------
# Replacement and post-signature correction
# ---------------------------------------------------------------------------

def plan_replacement(db, x):
    rid = x.get("id")
    if not rid:
        return {"decision": "replacement_id_required"}
    old = one(db, "SELECT * FROM plugin_replacements WHERE id=?", (rid,))
    if old:
        return {"decision": "reused", "id": rid}
    db.execute("""INSERT INTO plugin_replacements
        (id,wave,site,from_digest,to_digest,corrected_digest,vendor,window_start,window_end,
         installed_at,compatible,healthy,status,customer_signature,signed_at,note)
        VALUES(?,?,?,?,?,NULL,?,?,?,NULL,NULL,NULL,'planned',NULL,NULL,?)""",
               (rid, x.get("wave"), x.get("site"), x.get("from_digest"), x.get("to_digest"),
                x.get("vendor"), x.get("window_start", x.get("start")), x.get("window_end", x.get("end")),
                x.get("note")))
    return {"decision": "replacement_planned", "id": rid}


def install_replacement(db, x):
    rid = x.get("id")
    row = one(db, "SELECT * FROM plugin_replacements WHERE id=?", (rid,)) if rid else None
    if not row:
        return {"decision": "unknown_replacement", "id": rid}
    if row["status"] == "accepted":
        return {"decision": "already_accepted", "id": rid}
    compatible = x.get("compatible", row["compatible"])
    healthy = x.get("healthy", x.get("success"))
    at = x.get("at")
    if compatible is False or healthy is False:
        db.execute("UPDATE plugin_replacements SET installed_at=?,compatible=?,healthy=?,status='failed',note=? WHERE id=?",
                   (at, int(bool(compatible)) if compatible is not None else None,
                    int(bool(healthy)) if healthy is not None else None, x.get("note"), rid))
        return {"decision": "replacement_failed", "id": rid, "customer_signed": False}
    db.execute("UPDATE plugin_replacements SET installed_at=?,compatible=?,healthy=?,status='installed' WHERE id=?",
               (at, int(bool(compatible)) if compatible is not None else 1,
                int(bool(healthy)) if healthy is not None else 1, rid))
    return {"decision": "replacement_installed", "id": rid, "customer_signed": False}


def accept_replacement(db, x):
    rid = x.get("id")
    row = one(db, "SELECT * FROM plugin_replacements WHERE id=?", (rid,)) if rid else None
    if not row:
        return {"decision": "unknown_replacement", "id": rid}
    signature = x.get("customer_signature", x.get("signature", x.get("signed_by")))
    if row["status"] == "accepted":
        if row["customer_signature"] == signature:
            return {"decision": "reused", "id": rid}
        return {"decision": "acceptance_conflict", "id": rid}
    if not signature:
        return {"decision": "customer_signature_required"}
    compatible = x.get("compatible", row["compatible"])
    if compatible is False:
        db.execute("UPDATE plugin_replacements SET status='failed' WHERE id=?", (rid,))
        return {"decision": "replacement_failed", "reason": "incompatible"}
    if row["compatible"] == 0:
        return {"decision": "replacement_failed", "reason": "incompatible"}
    at = x.get("at")
    start = row["window_start"]
    end = row["window_end"]
    if start is not None and at is not None and at < start:
        return {"decision": "outside_signed_window"}
    if end is not None and at is not None and at > end:
        return {"decision": "outside_signed_window"}
    if row["status"] == "failed" and not x.get("retry_after_failure"):
        return {"decision": "replacement_failed", "id": rid}
    db.execute("""UPDATE plugin_replacements SET status='accepted',customer_signature=?,signed_at=?,
                  compatible=COALESCE(?,compatible),healthy=COALESCE(?,healthy) WHERE id=?""",
               (signature, at, int(bool(compatible)) if compatible is not None else None,
                int(bool(x["healthy"])) if "healthy" in x else None, rid))
    return {"decision": "replacement_accepted", "id": rid, "customer_signature": signature,
            "signed_at": at, "bank_guarantee_is_not_installation": True}


def correct(db, x):
    cid = x.get("id")
    if not cid:
        return {"decision": "correction_id_required"}
    if one(db, "SELECT 1 FROM plugin_corrections WHERE id=?", (cid,)):
        return {"decision": "reused", "id": cid}
    kind = x.get("kind", "evidence_correction")
    target_kind = x.get("target_kind", "replacement" if x.get("target") or x.get("replacement") else "obligation")
    target_id = x.get("target_id", x.get("target", x.get("replacement", x.get("obligation"))))
    at = x.get("at")
    db.execute("INSERT INTO plugin_corrections VALUES(?,?,?,?,?,?,?)",
               (cid, target_kind, target_id, kind, _json(x.get("changes", x)), at, x.get("reason")))
    result = {"decision": "corrected", "id": cid, "target": target_id}

    if target_kind == "replacement":
        row = one(db, "SELECT * FROM plugin_replacements WHERE id=?", (target_id,))
        if not row:
            return {"decision": "unknown_replacement", "target": target_id}
        new_digest = x.get("digest", x.get("to_digest", x.get("corrected_digest")))
        if new_digest:
            # Original signed to_digest/signature remain immutable; only the
            # separately versioned corrected_digest moves.
            db.execute("UPDATE plugin_replacements SET corrected_digest=? WHERE id=?", (new_digest, target_id))
            result["corrected_digest"] = new_digest
            result["original_signed_digest"] = row["to_digest"]
            result["customer_signature_preserved"] = row["customer_signature"]
    elif target_kind in ("obligation", "payment"):
        ob = _get_obligation(db, target_id)
        if ob and x.get("amount") is not None:
            new_amount = int(x["amount"])
            delta = new_amount - ob["amount"]
            db.execute("UPDATE plugin_obligations SET amount=?,reason=?,updated_at=? WHERE id=?",
                       (new_amount, x.get("reason", ob["reason"]), at, target_id))
            if delta:
                _post(db, cid, "correction", "holder:" + str(ob["creditor"]), ob["id"], delta, at)
                _post(db, cid, "correction_contra", "finance", ob["id"], -delta, at)
            result["delta"] = delta
    return result


def withdraw_invoice(db, x):
    """Record an invoice/claim withdrawal without erasing historical usage."""
    wid = x.get("id") or "WD|" + str(x.get("draft") or x.get("obligation") or x.get("invoice_id"))
    if one(db, "SELECT 1 FROM plugin_corrections WHERE id=?", (wid,)):
        return {"decision": "reused", "id": wid}
    oid = x.get("obligation")
    draft = x.get("draft", x.get("invoice_id"))
    at = x.get("at")
    db.execute("INSERT INTO plugin_corrections VALUES(?,?,?,?,?,?,?)",
               (wid, "invoice" if draft else "obligation", draft or oid,
                "invoice_withdrawn", _json(x), at, x.get("reason")))
    ob = _get_obligation(db, oid) if oid else None
    if ob and ob["status"] in ("confirmed", "blocked"):
        db.execute("UPDATE plugin_obligations SET status='withdrawn',updated_at=? WHERE id=?", (at, oid))
    return {"decision": "invoice_withdrawn", "id": wid, "draft": draft,
            "obligation": oid, "historical_usage_preserved": True}


def link_draft(db, draft, obligation=None, usage_key=None):
    ensure_schema(db)
    db.execute("INSERT OR REPLACE INTO plugin_draft_links VALUES(?,?,?)", (draft, obligation, usage_key))
    return {"decision": "draft_linked", "draft": draft, "obligation": obligation}


# ---------------------------------------------------------------------------
# Operation dispatcher
# ---------------------------------------------------------------------------

def handle(db, x):
    ensure_schema(db)
    op = x.get("op")
    if op in ("agreement", "contract", "terms", "term"):
        return _record_agreement(db, x, x.get("kind", "contract"))
    if op in ("license_agreement", "license_term"):
        return _record_agreement(db, x, "license")
    if op in ("assignment", "assignment_agreement"):
        return _record_agreement(db, x, "assignment")
    if op in ("price", "price_attachment", "pricing"):
        return _record_agreement(db, x, "price_attachment")
    if op == "triparty":
        return _record_agreement(db, x, "triparty")
    if op == "wave_result":
        wid = x.get("id") or "WR|{}|{}".format(x.get("wave"), x.get("site"))
        db.execute("INSERT OR REPLACE INTO plugin_wave_results VALUES(?,?,?,?,?,?,?,?,?,?)",
                   (wid, x.get("wave"), x.get("site"), x.get("status"), x.get("attempt"),
                    x.get("started_at"), x.get("success_at"), x.get("ended_at"), x.get("source"), _json(x)))
        return {"decision": "wave_result_recorded", "id": wid}
    if op == "ruling":
        rid = x.get("id")
        if not rid or not x.get("wave") or not x.get("site") or not x.get("digest"):
            return {"decision": "ruling_details_required"}
        db.execute("INSERT OR REPLACE INTO plugin_rulings VALUES(?,?,?,?,?,?,?,?)",
                   (rid, x["wave"], x["site"], x["digest"], _json(_as_list(x.get("evidence"))),
                    x.get("decided_by"), x.get("at"), x.get("reason")))
        return {"decision": "ruling_recorded", "id": rid, "digest": x["digest"]}
    if op in ("scan", "usage", "actual_usage"):
        return {"decision": "usage_scanned", **usage_report(db, x.get("at"))}
    if op in ("reconcile", "settle", "settlement"):
        return _reconcile(db, x)
    if op in ("license", "issue_license", "continuation_license"):
        return issue_license(db, x)
    if op in ("guarantee", "bank_guarantee"):
        return bank_guarantee(db, x)
    if op in ("escrow", "escrow_deposit", "deposit_escrow"):
        return escrow_deposit(db, x)
    if op in ("release_escrow", "escrow_release", "release"):
        return release_escrow(db, x)
    if op in ("clear_bank", "bank_cleared", "clear_receipt"):
        return _clear_bank(db, x)
    if op in ("return_bank", "bank_returned", "dishonor", "dishonour", "bounce"):
        return _return_bank(db, x)
    if op in ("pay", "payment", "register_payment"):
        return register_payment(db, x)
    if op in ("bank_settlement", "final_settlement"):
        return bank_settlement(db, x.get("at"))
    if op == "statement":
        return statement(db, x.get("customer"))
    if op in ("replacement", "plan_replacement", "replace"):
        return plan_replacement(db, x)
    if op in ("install_replacement", "install"):
        return install_replacement(db, x)
    if op in ("accept_replacement", "sign_replacement", "customer_acceptance"):
        return accept_replacement(db, x)
    if op in ("withdraw_invoice", "withdraw_claim", "credit_note"):
        return withdraw_invoice(db, x)
    if op == "correct":
        return correct(db, x)
    if op == "link_draft":
        return link_draft(db, x.get("draft"), x.get("obligation"), x.get("usage_key"))
    return {"decision": "unknown_operation", "op": op}


def run(db, x):
    ensure_schema(db)
    if isinstance(x, dict) and x.get("event_id"):
        from .store import act
        return act(db, "plugin_settlement", x, handle)
    with _tx(db):
        return handle(db, x)


# Convenience aliases for callers that prefer verb/function discovery.
scan_usage = usage_report
usage = usage_report
actual_executions = actual_usage
real_execution = actual_usage
execution_volume = execution_quantity
actual_execution_report = usage_report
billable_events = billable_usage
load_proof = load_verdict
rights = assignment_verdict
continuing_rights = has_continuing_rights
customer_continuation_rights = has_continuing_rights
issue_continuation_license = issue_license
continuation_license = issue_license
deposit_escrow = escrow_deposit
release = release_escrow
release_funds = release_escrow
dishonor_bank = return_bank
bounce_bank = return_bank
settle_bank = bank_settlement
final_bank_settlement = bank_settlement
final_royalty_settlement = bank_settlement
royalty_settlement = bank_settlement
settlement = settle
settle_royalty = settle
finalize = final_settlement
withdraw = withdraw_invoice
check_rights = has_continuing_rights
can_continue = can_use
