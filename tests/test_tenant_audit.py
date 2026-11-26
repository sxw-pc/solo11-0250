"""租户离线审计证据包回归测试。

围绕题面固定的序号 10..15 验收日志，覆盖：
朴素服务名导出的缺陷、两种审计安排、包含/缺席证明、共享数据库回执缺失、
隐藏撤销、同名其他租户事件、删行重编号、可逆脱敏、离线客户、迟到事件、
已签收包后的更正、通知义务差异以及恢复旧库与事件回放接入。
"""

import json
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path

from cloudcourt import tenant_audit as ta
from cloudcourt.audit_slice import export as naive_export
from cloudcourt.audit_verify import provisional
from cloudcourt.store import connect, replay
from cloudcourt import contract, wave, ready


def events():
    return ta.acceptance_events()


def self_package(**kw):
    kw.setdefault('service', 'B')
    kw.setdefault('tenant', ta.ACCEPTANCE_TENANT)
    kw.setdefault('dependencies', ['D'])
    kw.setdefault('seq_window', (10, 15))
    kw.setdefault('package_id', 'PKG-T')
    return ta.build_package(events(), mode='self', **kw)


# ---------------------------------------------------------------------------
# 题面固定的 10..15 验收序列
# ---------------------------------------------------------------------------

class AcceptanceSequenceTest(unittest.TestCase):
    def test_naive_export_picks_exactly_10_13_15_and_calls_it_verified(self):
        db = ta.build_acceptance_db(':memory:')
        rows = naive_export(db, 'SLICE', 'billing')
        self.assertEqual([r['seq'] for r in rows], [10, 13, 15])
        self.assertEqual('verified', provisional(db, 'CHK', 'SLICE'))
        db.close()

    def test_dependency_aware_selection_restores_11_and_14_but_not_12_raw(self):
        selected = ta.select_scoped_events(
            events(), 'B', tenant=ta.ACCEPTANCE_TENANT, dependencies=['D'])
        seqs = sorted(e.seq for e in selected)
        self.assertIn(11, seqs)   # D 的就绪回执（不含 B 名字）
        self.assertIn(14, seqs)   # D 的撤销
        self.assertIn(10, seqs)
        self.assertIn(13, seqs)
        self.assertIn(15, seqs)
        self.assertNotIn(12, seqs)  # 另一租户对 D 的请求原文不进入限定序列

    def test_two_core_questions(self):
        verdict = ta.evaluate_case(
            events(), service='B', tenant=ta.ACCEPTANCE_TENANT,
            dependencies=['D'], seq_window=(10, 15))
        # B 在 13 具备有效启动条件（11 的 D 身份有效，14 的撤销发生在启动之后）。
        self.assertTrue(verdict.launch_conditions_valid)
        self.assertTrue(verdict.startup_order_valid)
        self.assertTrue(verdict.launch_admissible_at_13)
        # 14 已撤销 D，故 15 不能代表可对外服务（且封存裁决为 parent_receipt_stale）。
        self.assertFalse(verdict.serviceable_at_readiness)
        self.assertFalse(verdict.serviceable_at_15)
        self.assertFalse(verdict.ready_at_15)

    def test_exact_six_event_roles(self):
        by_seq = {e.seq: e for e in events()}
        self.assertEqual(by_seq[10].op, 'revision')
        self.assertEqual(by_seq[11].payload['service'], 'D')
        self.assertEqual(by_seq[12].payload['tenant'], ta.OTHER_TENANT)
        self.assertEqual(by_seq[13].op, 'launch')
        self.assertEqual(by_seq[14].op, 'revoke')
        self.assertEqual(by_seq[15].op, 'publish')

    def test_falsely_published_15_still_not_serviceable_after_14(self):
        contradictory = json.loads(json.dumps(events(), default=str))
        # 即使 15 自封 published，14 已撤销 D，审计结论仍必须是不可服务。
        for e in contradictory:
            if e['seq'] == 15:
                e['result'] = {'decision': 'published'}
        verdict = ta.evaluate_case(contradictory, service='B',
                                   tenant=ta.ACCEPTANCE_TENANT, dependencies=['D'])
        self.assertFalse(verdict.serviceable_at_15)
        self.assertIn('critical_dependency_current_at_readiness', verdict.hard_failures)

    def test_positive_control_without_revocation_is_serviceable(self):
        clean = json.loads(json.dumps(events(), default=str))
        for e in clean:
            if e['seq'] == 14:
                e['op'] = 'probe'
                e['command'] = 'probe'
                e['payload'] = {'event_id': 'p4', 'op': 'probe', 'attempt': 'try-b', 'at': 36}
                e['result'] = {'decision': 'sampled'}
            if e['seq'] == 15:
                e['result'] = {'decision': 'published'}
        verdict = ta.evaluate_case(clean, service='B',
                                   tenant=ta.ACCEPTANCE_TENANT, dependencies=['D'])
        self.assertTrue(verdict.launch_conditions_valid)
        self.assertTrue(verdict.serviceable_at_15)

    def test_both_modes_reach_same_conclusions_on_same_events(self):
        self_pkg = self_package()
        cust_pkg = ta.acceptance_package(mode='custodian', package_id='PKG-C')
        for pkg in (self_pkg, cust_pkg):
            report = ta.verify_package(pkg)
            self.assertTrue(report['verified'], report['errors'])
            c = pkg['conclusions']
            self.assertTrue(c['launch_conditions_valid'])
            self.assertFalse(c['serviceable_at_readiness'])
            # 两种安排都不向 B 泄漏另一租户材料。
            self.assertTrue(ta.privacy_audit(
                json.dumps(pkg, ensure_ascii=False), ta.OTHER_TENANT_SECRETS)['clean'])
        self.assertEqual(self_pkg['mode'], 'self')
        self.assertEqual(cust_pkg['mode'], 'custodian')
        # 自验安排给客户全量账本、包含/缺席证明，可独立复算序列。
        self.assertIn('ledger', self_pkg)
        self.assertTrue(self_pkg['inclusions'])
        self.assertTrue(any(a.get('seq_absence_holds') for a in self_pkg['absences']))
        self.assertIn('hash_chain_sequence',
                      self_pkg['obligations']['customer_recompute_scope'])
        # 托管安排不给全量账本，客户只复核保管人签名与限定序列/结论。
        self.assertNotIn('ledger', cust_pkg)
        self.assertIn('custodian_signature',
                      cust_pkg['obligations']['customer_recompute_scope'])
        self.assertTrue(cust_pkg['full_log_attestation']['custodian_saw_raw_other_tenant_events'])
        # 原件持有人与补证通知对象不同。
        self.assertEqual(self_pkg['obligations']['originals_holder'],
                         'platform_controlled_zone')
        self.assertEqual(cust_pkg['obligations']['originals_holder'],
                         'independent_audit_custodian')
        self.assertEqual(self_pkg['obligations']['supplement_notify'], ['tenant'])
        self.assertIn('custodian', cust_pkg['obligations']['supplement_notify'])

    def test_all_seven_artifact_kinds_can_be_bound_in_one_log(self):
        # 构造一条按因果顺序包含七类事件的合成序列，验证包能绑定其确切顺序。
        seq = [
            _ev(1, 'revision', {'id': 'B', 'generation': 1, 'tenant': 't'}),
            _ev(2, 'edge', {'service': 'B', 'dependency': 'D', 'critical': True, 'revision': 1}),
            _ev(3, 'seal', {'id': 'B', 'generation': 1}),
            _ev(4, 'token', {'id': 'tok', 'service': 'B', 'generation': 1, 'expires': 90, 'at': 1}),
            _ev(5, 'plan', {'id': 'w', 'service': 'B', 'generation': 1}),
            _ev(6, 'slot', {'id': 'sl', 'healthy': True, 'generation': 1}),
            _ev(7, 'launch', {'id': 'a', 'service': 'B', 'generation': 1,
                              'wave': 'w', 'slot': 'sl', 'token': 'tok', 'at': 10, 'expires': 80},
                result={'decision': 'launched'}),
            _ev(8, 'probe', {'id': 'p1', 'attempt': 'a', 'at': 11, 'healthy': True}),
            _ev(9, 'probe', {'id': 'p2', 'attempt': 'a', 'at': 12, 'healthy': True}),
            _ev(10, 'publish', {'id': 'r', 'attempt': 'a', 'service': 'B', 'generation': 1, 'at': 13},
                result={'decision': 'published'}),
        ]
        pkg = ta.build_package(seq, service='B', tenant='t', dependencies=['D'])
        kinds = {e.op for e in pkg['sequence']}
        for kind in ('revision', 'edge', 'seal', 'token', 'plan', 'slot', 'launch', 'probe', 'publish'):
            self.assertIn(kind, kinds)
        self.assertTrue(ta.verify_package(pkg)['verified'])


def _ev(seq, op, payload, result=None):
    payload = {'event_id': f'E{seq}', 'op': op, **payload}
    return ta.PDict({'seq': seq, 'event_id': f'E{seq}', 'subsystem': 'x',
                     'command': op, 'op': op, 'at': payload.get('at'),
                     'payload': payload,
                     'result': result or {'decision': 'ok'}})


# ---------------------------------------------------------------------------
# 包含证明与缺席证明
# ---------------------------------------------------------------------------

class ProofTest(unittest.TestCase):
    def test_merkle_inclusion_proof_detects_leaf_flip(self):
        leaves = ['a', 'b', 'c', 'd']
        pr = ta.inclusion_proof(leaves, 2)
        self.assertTrue(ta.verify_inclusion_proof(pr))
        bad = dict(pr)
        bad['leaf'] = 'tampered'
        self.assertFalse(ta.verify_inclusion_proof(bad))

    def test_sorted_absence_proof(self):
        keys = ['000000000011', '000000000014']
        proof = ta.sorted_absence_proof(keys, '000000000012', '000000000014')
        self.assertTrue(proof['absent'])  # [12,14) 内无键
        self.assertTrue(ta.verify_sorted_absence_proof(proof['root'], proof))
        present = ta.sorted_absence_proof(keys, '000000000013', '000000000015')
        self.assertFalse(present['absent'])

    def test_package_inclusion_and_absence_proofs_verify(self):
        pkg = self_package()
        report = ta.verify_package(pkg)
        self.assertTrue(report['proofs_valid'])
        # 11 与 14 都有包含证明。
        proof_seqs = {p['seq'] for p in pkg['inclusions']}
        self.assertIn(11, proof_seqs)
        self.assertIn(14, proof_seqs)
        # 13 之前（12..13）没有 D 撤销：缺席成立且证明有效。
        pre_launch = next(a for a in pkg['absences']
                          if a.kind == 'revocation' and a.subject == 'D'
                          and a.seq_range == [12, 13])
        self.assertTrue(pre_launch['absent'])
        self.assertTrue(pre_launch['verified'])
        # 10..15 全窗内确实有 14 的撤销：不得谎称缺席。
        window = next(a for a in pkg['absences']
                      if a.kind == 'revocation' and a.subject == 'D'
                      and a.seq_range == [10, 15])
        self.assertFalse(window['absent'])
        self.assertEqual([m['seq'] for m in window['matching']], [14])

    def test_hidden_revocation_before_launch_flips_verdict(self):
        hidden = events()
        for e in hidden:
            if e.seq == 12:  # 把“其他租户请求”替换成夹在 11 与 13 之间的隐藏撤销
                e.update(op='revoke', command='revoke', subsystem='ready')
                e.payload = {'event_id': 'e12-hidden-revoke', 'op': 'revoke',
                             'id': 'receipt-d-1', 'service': 'D',
                             'target_service': 'D', 'at': 34}
                e.result = {'decision': 'revoked'}
        absence = ta.absence_report(hidden, 'revocation', 'D', seq_range=(12, 13))
        self.assertFalse(absence['absent'])
        self.assertEqual([m['seq'] for m in absence['matching']], [12])
        verdict = ta.evaluate_case(hidden, service='B', tenant=ta.ACCEPTANCE_TENANT,
                                   dependencies=['D'])
        self.assertFalse(verdict.launch_conditions_valid)

    def test_missing_shared_database_receipt_makes_launch_invalid(self):
        without_11 = [e for e in events() if e.seq != 11]
        verdict = ta.evaluate_case(without_11, service='B',
                                   tenant=ta.ACCEPTANCE_TENANT, dependencies=['D'])
        self.assertFalse(verdict.launch_conditions_valid)
        self.assertIn('dependency_receipt_present:D', verdict.hard_failures)

    def test_dependency_identity_mismatch_detected(self):
        bad = events()
        for e in bad:
            if e.seq == 11:
                e.payload['generation'] = 9  # 与 D 已封存的 gen1 不符
        verdict = ta.evaluate_case(bad, service='B', tenant=ta.ACCEPTANCE_TENANT,
                                   dependencies=['D'])
        self.assertFalse(verdict.checks and all(
            c.ok for c in verdict.checks if c.name == 'dependency_identity_valid:D'))
        self.assertFalse(verdict.launch_conditions_valid)


# ---------------------------------------------------------------------------
# 隐私边界与可逆脱敏
# ---------------------------------------------------------------------------

class PrivacyTest(unittest.TestCase):
    def test_package_reveals_no_other_tenant_material(self):
        pkg = self_package()
        text = json.dumps(pkg, ensure_ascii=False)
        audit = ta.privacy_audit(text, ta.OTHER_TENANT_SECRETS)
        self.assertTrue(audit['clean'], audit['leaks'])
        # 即使按 ASCII 转义序列化也查得到泄漏；这里同样必须干净。
        ascii_text = json.dumps(pkg, ensure_ascii=True)
        self.assertTrue(ta.privacy_audit(ascii_text, ta.OTHER_TENANT_SECRETS)['clean'])

    def test_seq12_is_only_an_opaque_blinded_leaf(self):
        pkg = self_package()
        item = next(x for x in pkg['ledger'] if x['seq'] == 12)
        self.assertFalse(item['disclosed'])
        self.assertNotIn('event', item)
        self.assertEqual(set(item) & {'payload'}, set())
        self.assertEqual(len(item['leaf']), 64)
        # 不透明叶子不是原始 payload 的裸 SHA-256，无法做字典比对。
        raw_hash = ta.H('event-full/v1', ta.canonical(
            {k: v for k, v in next(e for e in events() if e.seq == 12).items()}))
        self.assertNotEqual(item['leaf'], raw_hash)

    def test_redaction_is_allowlist_only_and_irreversible(self):
        raw = next(e for e in events() if e.seq == 12).payload
        minimized = ta.redact_payload(raw, ta.DEPENDENCY_ALLOWED)
        # 必要的结构字段可保留，但所有对方业务/身份字段必须消失。
        for forbidden in ('tenant', 'requester', 'oncall', 'operator', 'contact',
                           'sql', 'ticket', 'note', 'request_id'):
            self.assertNotIn(forbidden, minimized)
        self.assertTrue(ta.privacy_audit(ta.canonical(minimized), ta.OTHER_TENANT_SECRETS)['clean'])
        # 请求事件在出包时被整行遮蔽，连结构字段也不进入 sequence。
        pkg = self_package()
        self.assertFalse(any(e.seq == 12 for e in pkg['sequence']))
        # 包内不得携带可逆映射。
        self.assertNotIn('redaction_map', json.dumps(pkg))
        self.assertTrue(pkg['privacy']['no_reversible_tokens'])

    def test_detector_flags_reversible_masking_antipatterns(self):
        secret = 'zhangwei.oncall'
        import base64, hashlib
        bad_doc = {
            'operator_hash': hashlib.sha256(secret.encode()).hexdigest(),
            'operator_b64': base64.b64encode(secret.encode()).decode(),
        }
        result = ta.detect_reversible_masking(ta.canonical(bad_doc), [secret])
        self.assertFalse(result['clean'])
        transforms = {leak['transform'] for leak in result['leaks']}
        self.assertIn('sha256', transforms)
        self.assertIn('base64', transforms)

    def test_same_name_other_tenant_events_do_not_pollute_b(self):
        collision = events() + [
            _ev(16, 'revision', {'id': 'B', 'name': 'billing', 'generation': 9,
                                  'tenant': ta.OTHER_TENANT},
                result={'decision': 'revision_recorded'}),
            _ev(17, 'launch', {'id': 'try-evil', 'service': 'B', 'name': 'billing',
                               'generation': 9, 'tenant': ta.OTHER_TENANT,
                               'wave': 'w', 'slot': 's', 'token': 't', 'at': 5},
                result={'decision': 'launched'}),
        ]
        verdict = ta.evaluate_case(collision, service='B',
                                   tenant=ta.ACCEPTANCE_TENANT, dependencies=['D'])
        self.assertEqual(verdict.launch_seq, 13)  # 没有被同名的 seq17 抢走
        self.assertTrue(verdict.launch_conditions_valid)
        earlier = ta.absence_report(collision, 'launch', 'B',
                                    tenant=ta.ACCEPTANCE_TENANT, seq_range=(1, 12))
        self.assertTrue(earlier['absent'])  # 同名 seq17 不算 B 的更早启动
        pkg = ta.build_package(collision, service='B', tenant=ta.ACCEPTANCE_TENANT,
                               dependencies=['D'])
        self.assertTrue(ta.verify_package(pkg)['verified'])
        self.assertTrue(ta.privacy_audit(
            json.dumps(pkg, ensure_ascii=False), ta.OTHER_TENANT_SECRETS)['clean'])
        # 16/17 作为无关租户行被整行遮蔽。
        for seq in (16, 17):
            self.assertFalse(next(x for x in pkg['ledger'] if x['seq'] == seq)['disclosed'])


# ---------------------------------------------------------------------------
# 删行、重编号与包篡改
# ---------------------------------------------------------------------------

class TamperAndRenumberTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'acceptance.sqlite')
        self.db = ta.build_acceptance_db(self.path)
        self.pkg = ta.build_package(
            self.db, service='B', tenant=ta.ACCEPTANCE_TENANT,
            dependencies=['D'], seq_window=(10, 15))

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def _tampered_copy(self, name, mutate):
        target = str(Path(self.tmp.name) / name)
        shutil.copy(self.path, target)
        conn = sqlite3.connect(target)
        mutate(conn)
        conn.commit()
        conn.close()
        return target

    def test_delete_revocation_row_detected_against_checkpoint(self):
        target = self._tampered_copy('del14.sqlite',
                                    lambda c: c.execute('DELETE FROM events WHERE seq=14'))
        report = ta.verify_database(target, self.pkg)
        self.assertFalse(report['verified'])
        self.assertIn(14, report['missing_seqs'])
        self.assertIn('rows_missing', report['database_errors'])

    def test_delete_and_renumber_detected(self):
        def mutate(c):
            c.execute('DELETE FROM events WHERE seq=14')
            c.execute('UPDATE events SET seq=14 WHERE seq=15')
        target = self._tampered_copy('renumber.sqlite', mutate)
        report = ta.verify_database(target, self.pkg)
        self.assertFalse(report['verified'])
        self.assertTrue(report['renumber_suspected'])
        self.assertIn(15, report['missing_seqs'])
        self.assertIn(14, report['tampered_seqs'])

    def test_package_content_tamper_breaks_signature(self):
        hacked = json.loads(json.dumps(self.pkg))
        hacked['conclusions']['launch_conditions_valid'] = False
        report = ta.verify_package(hacked)
        self.assertFalse(report['verified'])
        self.assertIn('document_hash_mismatch', report['errors'])

    def test_ledger_leaf_tamper_detected(self):
        hacked = json.loads(json.dumps(self.pkg))
        hidden_index = next(i for i, x in enumerate(hacked['ledger']) if x['seq'] == 12)
        hacked['ledger'][hidden_index]['leaf'] = 'f' * 64
        report = ta.verify_package(hacked)
        self.assertFalse(report['verified'])
        self.assertTrue(any('merkle_root_mismatch' in e or 'chain' in e for e in report['errors']))

    def test_disclosed_event_tamper_detected(self):
        hacked = json.loads(json.dumps(self.pkg))
        item = next(x for x in hacked['ledger'] if x['seq'] == 13)
        item['event']['payload']['at'] = 1
        report = ta.verify_package(hacked)
        self.assertFalse(report['verified'])
        self.assertTrue(any('disclosed_leaf_mismatch_seq_13' in e for e in report['errors']))

    def test_recomputed_digest_over_filtered_rows_is_not_enough(self):
        # 反例：攻击方只对筛出的 10/13/15 重算摘要并自封 verified，
        # 但它对全量检查点无效，也无法证明 12..15 之间没有撤销。
        filtered = [e for e in events() if e.seq in (10, 13, 15)]
        homemade = {'rows': filtered,
                    'digest': ta.H('filtered/v1', ta.canonical(filtered)),
                    'state': 'verified'}
        self.assertTrue(homemade['digest'])  # 自洽……
        # ……但用真正的全量缺席证明检验时，14 的存在使“无撤销”不成立；
        # 删掉 14 后，序号连续性/检查点又会失败。
        no14 = [e for e in events() if e.seq != 14]
        gap_pkg = ta.build_package(no14, service='B', tenant=ta.ACCEPTANCE_TENANT,
                                   dependencies=['D'])
        self.assertFalse(ta.verify_package(gap_pkg)['verified'])


# ---------------------------------------------------------------------------
# 封存、签收、迟到事件、更正与离线失效辨认
# ---------------------------------------------------------------------------

class SealCorrectionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / 'registry.sqlite')
        self.db = ta.build_acceptance_db(self.db_path)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def _clean_events(self):
        # 一个“15 仍可服务”的干净版本：14 是普通探针，15 真的 published。
        clean = ta.acceptance_events()
        for e in clean:
            if e.seq == 14:
                e.update(op='probe', command='probe', subsystem='ready')
                e.payload = {'event_id': 'e14-probe', 'op': 'probe', 'id': 'dp4',
                             'attempt': 'try-d', 'at': 36, 'healthy': True}
                e.result = {'decision': 'sampled'}
            if e.seq == 15:
                e.result = {'decision': 'published'}
        return clean

    def _late_revocation(self, source):
        return source + [ta.PDict({
            'seq': 16, 'event_id': 'e16-late', 'subsystem': 'ready',
            'command': 'revoke', 'op': 'revoke', 'at': 36,
            'payload': {'event_id': 'e16-late', 'op': 'revoke', 'id': 'receipt-d-1',
                        'service': 'D', 'target_service': 'D', 'at': 36},
            'result': {'decision': 'revoked', 'children': ['B']}})]

    def test_sealed_package_and_signoff_are_immutable(self):
        pkg = self_package(package_id='PKG-IMM')
        ta.seal_package(self.db, pkg)
        ta.sign_off(self.db, 'PKG-IMM', 'tenant-b', 'SIGN-1')
        # 同包重复封存/签收幂等。
        self.assertTrue(ta.seal_package(self.db, pkg)['already_sealed'])
        self.assertTrue(ta.sign_off(self.db, 'PKG-IMM', 'tenant-b')['already_signed'])
        changed = json.loads(json.dumps(pkg))
        changed['created_at'] = 'later'
        with self.assertRaises(ta.ImmutablePackageError):
            ta.seal_package(self.db, changed, 'PKG-IMM')
        # 数据库触发器也拒绝直接改文档。
        with self.assertRaises(sqlite3.Error):
            self.db.execute("UPDATE audit_packages SET document='{}' WHERE package_id='PKG-IMM'")

    def test_late_event_correction_after_signoff_and_offline_supersession(self):
        clean = self._clean_events()
        old_pkg = ta.build_package(clean, service='B', tenant=ta.ACCEPTANCE_TENANT,
                                   dependencies=['D'], seq_window=(10, 15),
                                   package_id='PKG-OLD')
        self.assertTrue(old_pkg['conclusions']['serviceable_at_15'])
        ta.seal_package(self.db, old_pkg)
        ta.sign_off(self.db, 'PKG-OLD', 'tenant-b', 'SIGN-9')

        corrected_source = self._late_revocation(clean)
        outcome = ta.issue_correction(
            self.db, 'PKG-OLD', new_source=corrected_source,
            reason='late backdated D revocation', package_id='PKG-NEW',
            service='B', tenant=ta.ACCEPTANCE_TENANT, dependencies=['D'],
            seq_window=(10, 15), mode='self')
        new_pkg = outcome['correction']
        self.assertFalse(new_pkg['conclusions']['serviceable_at_readiness'])
        self.assertTrue(new_pkg['conclusions']['launch_conditions_valid'])
        topics = {(c.get('topic')): c for c in outcome['changes']}
        self.assertIn('serviceable_at_readiness', topics)
        self.assertEqual(topics['serviceable_at_readiness']['old'], True)
        self.assertEqual(topics['serviceable_at_readiness']['new'], False)
        # 原包状态在登记处变为 superseded，但文档哈希不变。
        record = ta.get_package_record(self.db, 'PKG-OLD')
        self.assertEqual(record['status'], 'superseded')
        old_doc = json.loads(record['document'])
        self.assertEqual(old_doc['package_id'], 'PKG-OLD')

        # 客户完全离线：原包单独看仍加密有效；导入更正后能辨认其已失效。
        old_path = str(Path(self.tmp.name) / 'old.json')
        new_path = str(Path(self.tmp.name) / 'new.json')
        ta.export_package_file(old_pkg, old_path)
        ta.export_package_file(new_pkg, new_path)
        alone = ta.review_offline(old_path)
        self.assertEqual(alone['state'], 'verified')
        review = ta.review_offline(old_path, [new_path])
        self.assertTrue(review['verified'], review['errors'])
        self.assertEqual(review['effective_state'], 'superseded')
        self.assertEqual(review['superseded_by'], 'PKG-NEW')
        self.assertTrue(review['corrections'][0]['verified'])

    def test_late_backdated_revocation_caught_by_time_window(self):
        source = self._late_revocation(self._clean_events())
        report = ta.absence_report(source, 'revocation', 'D', time_range=(30, 40))
        self.assertFalse(report['absent'])
        self.assertEqual([m['seq'] for m in report['matching']], [16])

    def test_self_vs_custodian_notification_obligations_differ(self):
        self.assertEqual(ta.delivery_obligations('self')['supplement_notify'], ['tenant'])
        self.assertIn('custodian', ta.delivery_obligations('custodian')['supplement_notify'])
        pkg = self_package(package_id='PKG-NOTIFY')
        ta.seal_package(self.db, pkg)
        plan = ta.plan_delivery(self.db, 'PKG-NOTIFY')
        self.assertEqual(plan['mode'], 'self')
        self.assertTrue(plan['delivery_ids'])
        ta.mark_served(self.db, plan['delivery_ids'][0])
        row = self.db.execute('SELECT status FROM audit_deliveries WHERE id=?',
                               (plan['delivery_ids'][0],)).fetchone()
        self.assertEqual(row[0], 'served')

    def test_custodian_correction_requires_custodian_key_offline(self):
        custodian = ta.AuditCustodian(key=ta.generate_key('cust-secret'))
        clean = self._clean_events()
        old_pkg = custodian.issue(clean, service='B', tenant=ta.ACCEPTANCE_TENANT,
                                  dependencies=['D'], package_id='C-OLD')
        ta.seal_package(self.db, old_pkg)
        ta.sign_off(self.db, 'C-OLD', 'tenant-b')
        outcome = ta.issue_correction(
            self.db, 'C-OLD', new_source=self._late_revocation(clean),
            package_id='C-NEW', mode='custodian',
            custodian_key=custodian.key, custodian_id=custodian.custodian_id,
            service='B', tenant=ta.ACCEPTANCE_TENANT, dependencies=['D'])
        new_pkg = outcome['correction']
        self.assertTrue(custodian.verify(new_pkg)['verified'])
        review = ta.verify_package(old_pkg, custodian_key=custodian.key,
                                   corrections=[new_pkg])
        self.assertEqual(review['effective_state'], 'superseded')


# ---------------------------------------------------------------------------
# 恢复旧库、回放接入与临时导出
# ---------------------------------------------------------------------------

class RestoreReplayExportTest(unittest.TestCase):
    def test_restored_old_database_is_detected(self):
        with tempfile.TemporaryDirectory() as tmp:
            full_path = str(Path(tmp) / 'full.sqlite')
            full_db = ta.build_acceptance_db(full_path)
            pkg = ta.build_package(full_db, service='B', tenant=ta.ACCEPTANCE_TENANT,
                                   dependencies=['D'])
            full_db.close()
            old_path = str(Path(tmp) / 'old.sqlite')
            old_db = connect(old_path)
            ta.write_acceptance_rows(old_db, [e for e in events() if e.seq <= 13])
            old_db.close()
            report = ta.inspect_database(old_path, pkg)
            self.assertEqual(report['state'], 'restored_stale_database')
            self.assertTrue(report['comparison']['restored_stale_database'])
            self.assertEqual(report['last_seq'], 13)
            self.assertEqual(report['comparison']['expected_last_seq'], 15)

    def test_replay_integration_and_audit_tables_do_not_break_replay(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / 'billing.sqlite')
            db = connect(path)
            _build_handler_native_billing(db)
            db.close()
            self.assertTrue(replay(path)['verified'])
            pkg = ta.build_package(path, service='B', dependencies=['D'])
            self.assertTrue(ta.verify_package(pkg)['verified'])
            self.assertTrue(pkg['replay']['verified'])
            # 封存/登记会在业务库中建立审计表与触发器；全量回放仍须通过。
            reg = connect(path)
            ta.seal_package(reg, pkg, 'PKG-REPLAY')
            ta.sign_off(reg, 'PKG-REPLAY', 'tenant-b')
            reg.close()
            again = replay(path)
            self.assertTrue(again['verified'], again['mismatch'])

    def test_temporary_export_lifecycle(self):
        with ta.temporary_export(events(), service='B',
                                 tenant=ta.ACCEPTANCE_TENANT,
                                 dependencies=['D']) as (path, pkg):
            self.assertTrue(Path(path).exists())
            loaded = json.loads(Path(path).read_text(encoding='utf-8'))
            self.assertTrue(ta.verify_package(loaded)['verified'])
            self.assertEqual(loaded['package_id'], pkg['package_id'])
        self.assertFalse(Path(path).exists())

    def test_write_temporary_export_returns_verifiable_file(self):
        path, pkg = ta.write_temporary_export(
            events(), service='B', tenant=ta.ACCEPTANCE_TENANT, dependencies=['D'])
        try:
            self.assertTrue(Path(path).exists())
            self.assertTrue(ta.review_offline(path)['verified'])
        finally:
            Path(path).unlink(missing_ok=True)


def _build_handler_native_billing(db):
    commands = [
        (contract, {'event_id': 'rd', 'op': 'revision', 'id': 'D', 'generation': 1}),
        (contract, {'event_id': 'rb', 'op': 'revision', 'id': 'B', 'generation': 1}),
        (contract, {'event_id': 'edge', 'op': 'edge', 'service': 'B',
                    'dependency': 'D', 'critical': True, 'revision': 1}),
        (contract, {'event_id': 'sd', 'op': 'seal', 'id': 'D', 'generation': 1}),
        (contract, {'event_id': 'sb', 'op': 'seal', 'id': 'B', 'generation': 1}),
        (contract, {'event_id': 'td', 'op': 'token', 'id': 'td', 'service': 'D',
                    'generation': 1, 'expires': 200, 'at': 5}),
        (contract, {'event_id': 'tb', 'op': 'token', 'id': 'tb', 'service': 'B',
                    'generation': 1, 'expires': 200, 'at': 5}),
        (wave, {'event_id': 'slot', 'op': 'slot', 'id': 'slot', 'node': 'n',
                'capacity': 2, 'generation': 1, 'healthy': True}),
        (wave, {'event_id': 'wd', 'op': 'plan', 'id': 'wd', 'service': 'D',
                'generation': 1, 'deadline': 90}),
        (wave, {'event_id': 'ld', 'op': 'launch', 'id': 'tryd', 'wave': 'wd',
                'slot': 'slot', 'token': 'td', 'at': 20, 'expires': 60}),
        (ready, {'event_id': 'p1', 'op': 'probe', 'id': 'p1', 'attempt': 'tryd',
                 'at': 30, 'healthy': True}),
        (ready, {'event_id': 'p2', 'op': 'probe', 'id': 'p2', 'attempt': 'tryd',
                 'at': 31, 'healthy': True}),
        (ready, {'event_id': 'pubd', 'op': 'publish', 'id': 'rd1',
                 'attempt': 'tryd', 'at': 32}),
        (wave, {'event_id': 'wb', 'op': 'plan', 'id': 'wb', 'service': 'B',
                'generation': 1, 'deadline': 90}),
        (wave, {'event_id': 'lb', 'op': 'launch', 'id': 'tryb', 'wave': 'wb',
                'slot': 'slot', 'token': 'tb', 'at': 35, 'expires': 70}),
        (ready, {'event_id': 'bp1', 'op': 'probe', 'id': 'bp1', 'attempt': 'tryb',
                 'at': 40, 'healthy': True}),
        (ready, {'event_id': 'bp2', 'op': 'probe', 'id': 'bp2', 'attempt': 'tryb',
                 'at': 41, 'healthy': True}),
        (ready, {'event_id': 'pubb', 'op': 'publish', 'id': 'rb1',
                 'attempt': 'tryb', 'at': 42}),
    ]
    for module, payload in commands:
        result = module.run(db, payload)
        assert 'decision' in result
    return db


if __name__ == '__main__':
    unittest.main()
