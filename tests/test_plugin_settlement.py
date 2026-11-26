import sqlite3
import unittest

from cloudcourt import plugin_settlement as ps
from cloudcourt.bootstrap_plugin import activate, catalog
from cloudcourt.plugin_evidence import assignment, bank, load
from cloudcourt.plugin_royalty import draft
from cloudcourt.store import connect


PRICE_O = "PRICE-O"
PRICE_A = "PRICE-A"
O_SITE1_ORIGINAL = "O|W1|site-1|published_site|original|" + PRICE_O
O_WAVE1_ASSIGNEE = "O|W1|site-1|first_loaded_wave|assignee|" + PRICE_A
O_SITE2_ORIGINAL = "O|W2|site-2|published_site|original|" + PRICE_O
O_SITE2_ASSIGNEE = "O|W2|site-2|first_loaded_wave|assignee|" + PRICE_A


def build_world(db, *, with_terms=True):
    """The two-site fact pattern from the task, with no money closed yet."""
    ps.ensure_schema(db)
    catalog(db, "P", "d1", "original", "L-original")
    activate(db, "A1", "P", "billing", "W1", "site-1")
    activate(db, "A2", "P", "billing", "W2", "site-2")

    db.execute("INSERT INTO boot_waves VALUES(?,?,?,?,?,?)",
               ("W1", "billing", 1, 100, 0, "complete"))
    db.execute("INSERT INTO boot_waves VALUES(?,?,?,?,?,?)",
               ("W2", "billing", 1, 100, 0, "failed"))
    db.execute("INSERT INTO start_attempts VALUES(?,?,?,?,?,?,?,?,?)",
               ("T1", "W1", "slot-1", 1, 1, 1, 19, 80, "complete"))
    db.execute("INSERT INTO start_attempts VALUES(?,?,?,?,?,?,?,?,?)",
               ("T2", "W2", "slot-2", 1, 1, 1, 20, 26, "timed_out"))
    db.execute("INSERT INTO readiness_receipts VALUES(?,?,?,?,?,?,?)",
               ("R1", "billing", 1, "T1", "published", 30, None))
    db.execute("INSERT INTO probe_samples VALUES(?,?,?,?)",
               ("PB1", "T2", 23, 0))

    load(db, "M0", "W1", "site-1", "d1", 20, 0, "attestor-A")
    load(db, "M1", "W2", "site-2", "d1", 20, 3, "attestor-A")
    load(db, "M2", "W2", "site-2", "d2", 22, 3, "attestor-B")
    assignment(db, "X1", "d1", "assignee", 18, "site-1-only")

    if with_terms:
        ps.run(db, {
            "op": "agreement", "kind": "license", "id": "L-ORIGINAL", "customer": "cust-1",
            "supplier": "original", "digests": ["*"], "sites": ["*"],
            "non_interruptible": True, "signed_at": 1, "effective_from": 1,
            "signatures": ["original-signature", "customer-signature"],
        })
        ps.run(db, {
            "op": "price", "id": PRICE_O, "customer": "cust-1",
            "supplier": "original", "model": "published_site", "unit_fee": 20,
            "digests": ["*"], "sites": ["*"], "signed_at": 2, "effective_from": 2,
            "signatures": ["original-signature", "customer-signature"],
        })
        ps.run(db, {
            "op": "price", "id": PRICE_A, "customer": "cust-1",
            "supplier": "assignee", "claimant": "assignee",
            "model": "first_loaded_wave", "unit_fee": 20,
            "digests": ["d1"], "sites": ["*"], "waves": ["*"],
            "signed_at": 18, "effective_from": 18,
            "signatures": ["assignee-signature", "customer-signature"],
        })
        ps.run(db, {
            "op": "triparty", "id": "TRI", "customer": "cust-1",
            "suppliers": ["original", "assignee"], "escrow_account": "escrow-1",
            "signed_at": 35, "effective_from": 35,
            "signatures": ["original-signature", "assignee-signature", "customer-signature"],
        })
    draft(db, "D-RAW40", "original", 20)
    return db


class PluginSettlementTest(unittest.TestCase):
    def setUp(self):
        self.db = connect(":memory:")

    def tearDown(self):
        self.db.close()

    def test_unstarted_duplicate_console_row_and_wave_retry_count_once(self):
        db = self.db
        ps.ensure_schema(db)
        catalog(db, "P", "d1", "original", "L1")
        catalog(db, "STANDBY", "d-standby", "original", "L2")
        for aid, wave, site in [
            ("A1", "W1", "site-1"),
            ("A1-DUP", "W1", "site-1"),
            ("A2", "W2", "site-2"),
            ("A3", "W3", "site-3"),
            ("A4", "W4", "site-4"),
        ]:
            activate(db, aid, "P", "billing", wave, site)

        db.execute("INSERT INTO boot_waves VALUES(?,?,?,?,?,?)", ("W1", "billing", 1, 90, 0, "complete"))
        db.execute("INSERT INTO boot_waves VALUES(?,?,?,?,?,?)", ("W2", "billing", 1, 90, 0, "failed"))
        db.execute("INSERT INTO boot_waves VALUES(?,?,?,?,?,?)", ("W3", "billing", 1, 90, 0, "planned"))
        db.execute("INSERT INTO boot_waves VALUES(?,?,?,?,?,?)", ("W4", "billing", 1, 90, 0, "complete"))
        db.execute("INSERT INTO start_attempts VALUES(?,?,?,?,?,?,?,?,?)",
                   ("T1", "W1", "s1", 1, 1, 1, 10, 80, "complete"))
        db.execute("INSERT INTO start_attempts VALUES(?,?,?,?,?,?,?,?,?)",
                   ("T2A", "W2", "s2", 1, 1, 1, 20, 26, "timed_out"))
        db.execute("INSERT INTO start_attempts VALUES(?,?,?,?,?,?,?,?,?)",
                   ("T4A", "W4", "s4", 1, 1, 1, 40, 49, "timed_out"))
        db.execute("INSERT INTO start_attempts VALUES(?,?,?,?,?,?,?,?,?)",
                   ("T4B", "W4", "s4", 1, 1, 2, 50, 80, "complete"))
        db.execute("INSERT INTO readiness_receipts VALUES(?,?,?,?,?,?,?)",
                   ("R1", "billing", 1, "T1", "published", 30, None))
        db.execute("INSERT INTO readiness_receipts VALUES(?,?,?,?,?,?,?)",
                   ("R4", "billing", 1, "T4B", "published", 60, None))
        load(db, "L1", "W1", "site-1", "d1", 11, 0, "attestor")
        load(db, "L2", "W2", "site-2", "d1", 21, 1, "attestor")
        load(db, "L4", "W4", "site-4", "d4", 51, 0, "attestor")

        report = ps.usage_report(db)
        self.assertEqual(5, report["activation_rows"])
        self.assertEqual(4, report["episode_count"])
        self.assertEqual(["site-1", "site-4"], report["published_sites"])
        self.assertEqual(["W1", "W4"], report["successful_waves"])
        self.assertIn("W4", report["retried_waves"])
        self.assertIn("P|billing|W1|site-1", report["duplicate_activation_groups"])

        by_site = {e["site"]: e for e in report["episodes"]}
        self.assertEqual(1, by_site["site-1"]["duplicate_activation_count"])
        self.assertEqual("successful", by_site["site-1"]["usage_class"])
        self.assertEqual(2, by_site["site-4"]["attempt_count"])
        self.assertEqual(1, by_site["site-4"]["retry_count"])
        self.assertEqual("published", by_site["site-4"]["outcome"])
        self.assertEqual("unstarted", by_site["site-3"]["usage_class"])
        self.assertFalse(by_site["site-3"]["billable"])
        self.assertEqual("attempted_loaded", by_site["site-2"]["usage_class"])
        self.assertEqual(["STANDBY"], [c["id"] for c in report["standby_components"]])

        # The old row-count draft still mechanically produces 100 for five rows.
        self.assertEqual(100, draft(db, "D-ROWS", "original", 20))

    def test_overlapping_signed_measurements_do_not_order_digests(self):
        db = sqlite3.connect(":memory:")
        ps.ensure_schema(db)
        load(db, "M1", "W2", "site-2", "d1", 20, 3, "attestor-A")
        load(db, "M2", "W2", "site-2", "d2", 22, 3, "attestor-B")
        verdict = ps.load_verdict(db, "W2", "site-2")
        self.assertEqual("overlap_conflict", verdict["status"])
        self.assertIsNone(verdict["digest"])
        self.assertTrue(verdict["overlap"])
        self.assertEqual(2, verdict["signed_count"])
        self.assertEqual(["d1", "d2"], verdict["candidate_digests"])

        assignment(db, "X1", "d1", "assignee", 18, "site-1-only")
        self.assertTrue(ps.assignment_covers(db, "d1", "site-1", at=20))
        self.assertFalse(ps.assignment_covers(db, "d1", "site-2", at=20))
        self.assertFalse(ps.assignment_covers(db, "d2", "site-1", at=20))
        self.assertFalse(ps.assignment_covers(db, "d2", "site-2", at=20))
        db.close()

    def test_escrow_and_joint_license_keep_service_alive_but_not_pay_supplier(self):
        db = build_world(self.db)
        raw_draft = ps.one(db, "SELECT count,amount FROM royalty_drafts WHERE id='D-RAW40'")
        self.assertEqual((2, 40), (raw_draft["count"], raw_draft["amount"]))
        result = ps.reconcile(db, at=40)
        ids = set(result["obligation_ids"])
        self.assertIn(O_SITE1_ORIGINAL, ids)
        self.assertIn(O_WAVE1_ASSIGNEE, ids)
        self.assertIn(O_SITE2_ASSIGNEE, ids)

        site2_a = ps.one(db, "SELECT * FROM plugin_obligations WHERE id=?", (O_SITE2_ASSIGNEE,))
        self.assertEqual("blocked", site2_a["status"])
        self.assertIn("digest_overlap_conflict", site2_a["reason"])

        r = ps.run(db, {
            "event_id": "escrow-1", "op": "escrow", "id": "E1",
            "obligation": O_SITE2_ASSIGNEE, "agreement": "TRI",
            "account": "escrow-1", "amount": 20, "at": 41,
        })
        self.assertEqual("escrowed", r["decision"])
        self.assertFalse(r["supplier_paid"])

        r = ps.run(db, {
            "event_id": "joint-lic", "op": "license", "id": "LIC-JOINT",
            "kind": "joint", "customer": "cust-1", "agreement": "TRI",
            "escrow": "E1", "sites": ["site-1", "site-2"], "at": 41, "until": 90,
            "suppliers": ["original", "assignee"],
            "signatures": ["original-signature", "assignee-signature", "customer-signature"],
        })
        self.assertEqual("license_issued", r["decision"])
        self.assertTrue(ps.has_continuing_rights(db, "cust-1", site="site-2", digest="d2", at=50))

        balances = ps.balances(db)["total"]
        self.assertEqual(20, balances["escrow:escrow-1"])
        self.assertNotIn("holder:original", balances)
        self.assertNotIn("holder:assignee", balances)

        denied = ps.run(db, {"event_id": "release-bad", "op": "release_escrow",
                             "id": "REL-BAD", "escrow": "E1", "at": 42})
        self.assertEqual("escrow_locked", denied["decision"])
        self.assertEqual("load_time_overlap", denied["reason"])
        self.assertEqual("held", ps.one(db, "SELECT status FROM plugin_escrows WHERE id='E1'")["status"])

    def test_cleared_site_one_payment_discharges_usage_and_rejects_duplicates(self):
        db = build_world(self.db)
        ps.reconcile(db, at=40)
        bank(db, "B1", O_SITE1_ORIGINAL, "original", 20, "pending")

        cleared = ps.clear_bank(db, "B1", at=41)
        self.assertEqual("bank_cleared", cleared["decision"])
        self.assertTrue(cleared["supplier_received"])
        self.assertEqual("settled", ps.one(db, "SELECT status FROM plugin_obligations WHERE id=?",
                                           (O_SITE1_ORIGINAL,))["status"])
        # The triparty arrangement directs payment to the original for site 1;
        # the competing assignee claim is discharged as against the customer.
        assignee_claim = ps.one(db, "SELECT * FROM plugin_obligations WHERE id=?", (O_WAVE1_ASSIGNEE,))
        self.assertEqual("discharged", assignee_claim["status"])
        self.assertIn("paid_via", assignee_claim["reason"] or "")

        # Same external receipt is idempotent.
        self.assertEqual("reused", ps.clear_bank(db, "B1", at=42)["decision"])

        # A second bank receipt for the same usage is a duplicate payment.
        bank(db, "B2", O_SITE1_ORIGINAL, "original", 20, "pending")
        duplicate = ps.clear_bank(db, "B2", at=43)
        self.assertEqual("duplicate_payment", duplicate["decision"])
        statement = ps.bank_settlement(db)
        self.assertEqual(20, statement["supplier_received"])
        self.assertEqual(1, len(statement["cleared_receipts"]))
        self.assertEqual("duplicate", ps.one(db, "SELECT state FROM plugin_payments WHERE receipt_id='B2'")["state"])

    def test_rereconcile_cures_status_after_proof_and_signed_scope(self):
        db = build_world(self.db)
        ps.reconcile(db, at=40)
        self.assertEqual("blocked", ps.obligation_status(db, O_SITE2_ASSIGNEE))

        ps.run(db, {"op": "ruling", "id": "RULE-CURE", "wave": "W2", "site": "site-2",
                    "digest": "d1", "decided_by": "expert", "at": 50})
        ps.run(db, {"op": "assignment", "id": "X2-SITE2", "supplier": "original",
                    "claimant": "assignee", "digests": ["d1"], "sites": ["site-2"],
                    "signed_at": 51, "effective_from": 1,
                    "signatures": ["original", "assignee", "cust"]})
        result = ps.reconcile(db, at=52)
        ob = ps.get_obligation(db, O_SITE2_ASSIGNEE)
        self.assertEqual("confirmed", ob["status"])
        self.assertEqual("d1", ob["digest"])
        self.assertEqual(20, ob["started_at"])
        self.assertIn(O_SITE2_ASSIGNEE, result["obligation_ids"])

    def test_direct_pay_and_second_already_cleared_receipt_is_duplicate(self):
        db = build_world(self.db)
        ps.reconcile(db, at=40)
        first = ps.run(db, {
            "op": "pay", "id": "PAY-1", "receipt_id": "EXT-1",
            "obligation": O_SITE1_ORIGINAL, "draft": O_SITE1_ORIGINAL,
            "payee": "original", "amount": 20, "at": 41,
        })
        self.assertEqual("bank_cleared", first["decision"])

        # The second external receipt may already have been deducted/cleared by
        # the bank; it is cash in transit but cannot settle the usage twice.
        second = ps.run(db, {
            "op": "pay", "id": "PAY-2", "receipt_id": "EXT-2",
            "obligation": O_SITE1_ORIGINAL, "draft": O_SITE1_ORIGINAL,
            "payee": "original", "amount": 20, "bank_state": "cleared", "at": 42,
        })
        self.assertEqual("duplicate_payment", second["decision"])
        statement = ps.bank_settlement(db)
        self.assertEqual(40, statement["bank_cleared_total"])
        self.assertEqual(20, statement["supplier_received"])
        self.assertEqual(20, statement["duplicate_amount"])

    def test_bank_dishonour_reopens_debt_but_not_historical_delivery_or_license(self):
        db = build_world(self.db)
        ps.reconcile(db, at=40)
        bank(db, "B9", O_SITE1_ORIGINAL, "original", 20, "pending")
        self.assertEqual("bank_cleared", ps.clear_bank(db, "B9", at=41)["decision"])
        bounced = ps.return_bank(db, "B9", at=44, reason="insufficient_funds")
        self.assertEqual("payment_dishonored", bounced["decision"])
        self.assertFalse(bounced["license_revoked"])

        ob = ps.one(db, "SELECT * FROM plugin_obligations WHERE id=?", (O_SITE1_ORIGINAL,))
        self.assertEqual("dishonored", ob["status"])
        self.assertEqual(0, ob["paid_amount"])
        statement = ps.bank_settlement(db)
        self.assertEqual(0, statement["supplier_received"])
        self.assertEqual(["B9"], [r["id"] for r in statement["returned_receipts"]])

        # Historical published wave remains published; non-interruption right remains.
        self.assertEqual("complete", ps.one(db, "SELECT status FROM boot_waves WHERE id='W1'")["status"])
        self.assertTrue(ps.has_continuing_rights(db, "cust-1", site="site-1", digest="d1", at=50))

        bank(db, "B10", O_SITE1_ORIGINAL, "original", 20, "pending")
        self.assertEqual("bank_cleared", ps.clear_bank(db, "B10", at=45)["decision"])
        bank(db, "B11", O_SITE1_ORIGINAL, "original", 20, "pending")
        self.assertEqual("duplicate_payment", ps.clear_bank(db, "B11", at=46)["decision"])
        self.assertEqual(20, ps.bank_settlement(db)["supplier_received"])

    def test_replacement_failure_guarantee_and_signed_correction_are_distinct(self):
        db = build_world(self.db)
        ps.run(db, {
            "op": "replacement", "id": "RP-BAD", "wave": "W2", "site": "site-2",
            "from_digest": "d1", "to_digest": "d3", "window_start": 40, "window_end": 60,
        })
        failed = ps.run(db, {"op": "install", "id": "RP-BAD", "at": 41,
                             "compatible": False, "healthy": False})
        self.assertEqual("replacement_failed", failed["decision"])
        self.assertFalse(failed["customer_signed"])
        cannot_accept = ps.run(db, {"op": "accept_replacement", "id": "RP-BAD",
                                    "at": 42, "customer_signature": "cust-sign"})
        self.assertEqual("replacement_failed", cannot_accept["decision"])
        self.assertIsNone(ps.one(db, "SELECT customer_signature FROM plugin_replacements WHERE id='RP-BAD'")["customer_signature"])

        g = ps.run(db, {
            "op": "guarantee", "id": "G1", "bank": "Bank", "number": "G-1",
            "supplier": "assignee", "customer": "cust-1", "site": "site-2",
            "amount": 20, "at": 40, "until": 90,
        })
        self.assertEqual("guarantee_issued", g["decision"])
        lic = ps.run(db, {
            "op": "license", "id": "LIC-G", "kind": "bank_guaranteed_alternative",
            "customer": "cust-1", "guarantee": "G1", "at": 40, "until": 90,
        })
        self.assertEqual("license_issued", lic["decision"])
        status = ps.license_status(db, "LIC-G", at=50)
        self.assertFalse(status["payment_received"])
        self.assertFalse(status["replacement_accepted"])
        self.assertEqual([], ps.many(db, "SELECT * FROM plugin_replacements WHERE status='accepted'"))

        ps.run(db, {
            "op": "replacement", "id": "RP-OK", "wave": "W2", "site": "site-2",
            "from_digest": "d1", "to_digest": "d2", "window_start": 40, "window_end": 60,
        })
        self.assertEqual("replacement_installed", ps.run(db, {
            "op": "install", "id": "RP-OK", "at": 45, "compatible": True, "healthy": True,
        })["decision"])
        accepted = ps.run(db, {
            "op": "accept_replacement", "id": "RP-OK", "at": 46,
            "customer_signature": "CUST-SIGNED-WINDOW", "compatible": True,
        })
        self.assertEqual("replacement_accepted", accepted["decision"])

        corrected = ps.run(db, {
            "event_id": "corr-1", "op": "correct", "id": "CORR-1",
            "target": "RP-OK", "digest": "d2-final", "at": 70,
            "reason": "post-signoff manifest correction",
        })
        self.assertEqual("corrected", corrected["decision"])
        row = ps.one(db, "SELECT * FROM plugin_replacements WHERE id='RP-OK'")
        self.assertEqual("d2", row["to_digest"])
        self.assertEqual("d2-final", row["corrected_digest"])
        self.assertEqual("CUST-SIGNED-WINDOW", row["customer_signature"])
        self.assertEqual(46, row["signed_at"])

    def test_final_escrow_release_needs_proof_and_current_scope(self):
        db = build_world(self.db)
        ps.reconcile(db, at=40)
        ps.run(db, {"op": "escrow", "id": "E1", "obligation": O_SITE2_ASSIGNEE,
                    "agreement": "TRI", "account": "escrow-1", "amount": 20, "at": 41})

        # Even a later ruling d1 cannot stretch the old site-1-only consent to site 2.
        ps.run(db, {"op": "ruling", "id": "RULE-D1", "wave": "W2", "site": "site-2",
                    "digest": "d1", "decided_by": "expert", "at": 50})
        denied_site = ps.run(db, {"op": "release_escrow", "id": "REL-1", "escrow": "E1",
                                  "to": "assignee", "at": 51})
        self.assertEqual("escrow_locked", denied_site["decision"])
        self.assertEqual("assignment_out_of_scope", denied_site["reason"])

        # A newer decisive proof says d2; the d1-only assignment still cannot cover it.
        ps.run(db, {"op": "ruling", "id": "RULE-D2", "wave": "W2", "site": "site-2",
                    "digest": "d2", "decided_by": "expert", "at": 60})
        denied_digest = ps.run(db, {"op": "release_escrow", "id": "REL-2", "escrow": "E1",
                                    "to": "assignee", "at": 61})
        self.assertEqual("escrow_locked", denied_digest["decision"])
        self.assertEqual("assignment_out_of_scope", denied_digest["reason"])

        # A new signed d2/site-2 right closes the scope; it does not rewrite X1.
        ps.run(db, {"op": "assignment", "id": "X2", "supplier": "d2-vendor",
                    "claimant": "assignee", "digests": ["d2"], "sites": ["site-2"],
                    "signed_at": 62, "effective_from": 62,
                    "signatures": ["d2-vendor", "assignee", "customer"]})
        released = ps.run(db, {"op": "release_escrow", "id": "REL-3", "escrow": "E1",
                               "to": "assignee", "at": 63})
        self.assertEqual("escrow_released", released["decision"])
        totals = ps.balances(db)["total"]
        self.assertEqual(20, totals["holder:assignee"])
        self.assertEqual(0, totals["escrow:escrow-1"])
        self.assertEqual("reused", ps.run(db, {"op": "release_escrow", "id": "REL-4",
                                               "escrow": "E1", "at": 64})["decision"])

    def test_replacement_leaves_old_digest_usage_settleable(self):
        db = self.db
        ps.ensure_schema(db)
        catalog(db, "P", "d1", "original", "L1")
        activate(db, "A5", "P", "billing", "W5", "site-5")
        db.execute("INSERT INTO boot_waves VALUES(?,?,?,?,?,?)",
                   ("W5", "billing", 1, 100, 0, "failed"))
        db.execute("INSERT INTO start_attempts VALUES(?,?,?,?,?,?,?,?,?)",
                   ("T5", "W5", "slot-5", 1, 1, 1, 70, 80, "timed_out"))
        load(db, "M5", "W5", "site-5", "d1", 71, 0, "attestor-A")
        ps.run(db, {"op": "agreement", "kind": "license", "id": "L5", "supplier": "original",
                    "digests": ["*"], "sites": ["*"], "non_interruptible": True,
                    "signed_at": 1, "effective_from": 1, "signatures": ["o", "c"]})
        ps.run(db, {"op": "price", "id": "PRICE-OLD", "supplier": "original",
                    "model": "first_loaded_wave", "unit_fee": 20, "digests": ["d1"],
                    "sites": ["*"], "signed_at": 1, "effective_from": 1,
                    "signatures": ["o", "c"]})
        ps.reconcile(db, at=90)
        oid = "O|W5|site-5|first_loaded_wave|original|PRICE-OLD"
        ob = ps.get_obligation(db, oid)
        self.assertEqual("confirmed", ob["status"])
        self.assertEqual(70, ob["started_at"])
        self.assertEqual(80, ob["ended_at"])

        ps.run(db, {"op": "replacement", "id": "RP5", "wave": "W5", "site": "site-5",
                    "from_digest": "d1", "to_digest": "d2",
                    "window_start": 72, "window_end": 90})
        ps.run(db, {"op": "install", "id": "RP5", "at": 73,
                    "compatible": True, "healthy": True})
        ps.run(db, {"op": "accept_replacement", "id": "RP5", "at": 74,
                    "customer_signature": "SIGNED-5"})
        bank(db, "B5", oid, "original", 20, "pending")
        self.assertEqual("bank_cleared", ps.clear_bank(db, "B5", at=75)["decision"])
        self.assertEqual("settled", ps.obligation_status(db, oid))
        replacement = ps.replacement_status(db, "RP5")
        self.assertEqual("accepted", replacement["status"])
        self.assertEqual("d2", replacement["to_digest"])
        self.assertEqual("SIGNED-5", replacement["customer_signature"])

    def test_invoice_withdrawal_does_not_erase_delivered_wave(self):
        db = build_world(self.db)
        ps.reconcile(db, at=40)
        result = ps.run(db, {"op": "withdraw_invoice", "id": "WD1",
                             "draft": "D-RAW40", "obligation": O_SITE1_ORIGINAL, "at": 41})
        self.assertEqual("invoice_withdrawn", result["decision"])
        self.assertTrue(result["historical_usage_preserved"])
        self.assertEqual("complete", ps.one(db, "SELECT status FROM boot_waves WHERE id='W1'")["status"])
        episode = next(e for e in ps.actual_usage(db) if e["site"] == "site-1")
        self.assertEqual("published", episode["outcome"])
        self.assertEqual("withdrawn", ps.obligation_status(db, O_SITE1_ORIGINAL))

    def test_event_identity_is_idempotent(self):
        db = self.db
        payload = {"event_id": "evt-agree", "op": "triparty", "id": "TRI",
                   "customer": "cust", "escrow_account": "escrow", "at": 1,
                   "signatures": ["a", "b", "c"]}
        first = ps.run(db, payload)
        second = ps.run(db, dict(payload))
        self.assertEqual(first, second)
        conflict = ps.run(db, dict(payload, escrow_account="other"))
        self.assertEqual("event_identity_conflict", conflict["decision"])


if __name__ == "__main__":
    unittest.main()
