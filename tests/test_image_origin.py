import json
import tempfile
import unittest
from pathlib import Path

import cloudcourt
from cloudcourt import artifact, contract, wave
from cloudcourt.store import connect, many, one, replay

FIXTURE = Path('fixtures') / 'artifact-compromised-builder.json'
SOURCE = 'commit-s'
LOCK_ORIGINAL = 'sha256:lock-1'
LOCK_ALTERNATIVE = 'sha256:lock-2-alt'
D1 = 'sha256:d1'
D2 = 'sha256:d2'
EVIL = 'sha256:evil'


def play(db, steps):
    results = {}
    for step in steps:
        payload = step['payload']
        results[payload['event_id']] = cloudcourt.MODULES[step['subsystem']].run(db, payload)
    return results


class ImageOrigin(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'origin.sqlite')
        self.db = connect(self.path)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def issue(self, payload):
        return artifact.run(self.db, payload)

    def bundle(self, bundle_id, digest, signer, tag=None):
        return self.issue({'event_id': f'bundle-{bundle_id}', 'op': 'bundle', 'id': bundle_id,
                           'service': 'svc-b', 'generation': 1, 'digest': digest,
                           'signer': signer, 'tag': tag})

    def seal(self, artifact_id=None):
        contract.run(self.db, {'event_id': 'revision-svc-b', 'op': 'revision',
                               'id': 'svc-b', 'generation': 1})
        payload = {'event_id': 'seal-svc-b', 'op': 'seal', 'id': 'svc-b', 'generation': 1}
        if artifact_id:
            payload['artifact'] = artifact_id
        return contract.run(self.db, payload)

    def evidence(self, evidence_id, bundle_id, digest, signer='release-key', builder='builder-9',
                 toolchain='tc-9', source=SOURCE, lockfile=LOCK_ORIGINAL, env='env-9', at=1):
        return self.issue({'event_id': f'evidence-{evidence_id}', 'op': 'evidence', 'id': evidence_id,
                           'bundle': bundle_id, 'source_commit': source, 'lockfile_digest': lockfile,
                           'build_env': env, 'toolchain': toolchain, 'signer': signer, 'builder': builder,
                           'artifact_digest': digest, 'service': 'svc-b', 'service_generation': 1,
                           'trust_boundary': f'{signer}/{builder}/{toolchain} trusted when recorded',
                           'at': at})

    def grant_license(self, license_id, bundle_id, lockfile=LOCK_ORIGINAL, verdict='approved', at=1):
        return self.issue({'event_id': f'license-{license_id}', 'op': 'license', 'id': license_id,
                           'bundle': bundle_id, 'lockfile_digest': lockfile, 'verdict': verdict, 'at': at})

    def observe(self, obs_id, bundle_id, kind, digest, node=None, at=1):
        return self.issue({'event_id': f'observe-{obs_id}', 'op': 'observe', 'id': obs_id,
                           'bundle': bundle_id, 'kind': kind, 'node': node, 'digest': digest, 'at': at})

    def reproduce(self, repro_id, bundle_id, builder, toolchain, result, differences=None,
                  source=SOURCE, lockfile=LOCK_ORIGINAL, env='env-r', at=1):
        return self.issue({'event_id': f'reproduce-{repro_id}', 'op': 'reproduce', 'id': repro_id,
                           'bundle': bundle_id, 'builder': builder, 'source_commit': source,
                           'lockfile_digest': lockfile, 'build_env': env, 'toolchain': toolchain,
                           'result_digest': result, 'differences': differences or [], 'at': at})

    def certify(self, cert_id, bundle_id, at=1):
        return self.issue({'event_id': f'certify-{cert_id}', 'op': 'certify', 'id': cert_id,
                           'bundle': bundle_id, 'at': at})

    def revoke(self, rev_id, kind, subject, disposition='compromised', at=1):
        return self.issue({'event_id': f'revoke-{rev_id}', 'op': 'revoke', 'id': rev_id,
                           'subject_kind': kind, 'subject': subject, 'disposition': disposition,
                           'scope': f'everything relying on {subject}', 'at': at})

    def mount(self, mount_id, bundle_id, digest, attempt='attempt-1', node='node-1', at=1):
        return self.issue({'event_id': f'mount-{mount_id}', 'op': 'mount', 'id': mount_id,
                           'bundle': bundle_id, 'attempt': attempt, 'node': node,
                           'digest': digest, 'at': at})

    def deliver(self, bundle_id, digest, signer='release-key', builder='builder-9', toolchain='tc-9'):
        self.evidence(f'e-{bundle_id}', bundle_id, digest, signer=signer, builder=builder,
                      toolchain=toolchain)
        self.grant_license(f'lic-{bundle_id}', bundle_id)
        self.observe(f'reg-{bundle_id}', bundle_id, 'registry', digest)
        self.observe(f'cache-{bundle_id}', bundle_id, 'cache', digest, node='node-1')
        return self.certify(f'cert-{bundle_id}', bundle_id)

    def play_fixture(self):
        return play(self.db, json.loads(FIXTURE.read_text(encoding='utf-8')))

    def test_fixture_end_to_end_and_replay(self):
        results = self.play_fixture()
        decisions = {event_id: result['decision'] for event_id, result in results.items()}
        self.assertEqual(decisions, {
            'fx-bundle-d1': 'bundle_registered',
            'fx-attest-d1': 'attestation_recorded',
            'fx-revision': 'revision_recorded',
            'fx-seal': 'sealed',
            'fx-evidence-d1': 'evidence_recorded',
            'fx-obs-reg-d1': 'observation_recorded',
            'fx-obs-cache-d1': 'observation_recorded',
            'fx-revoke-key': 'revocation_recorded',
            'fx-revoke-builder': 'revocation_recorded',
            'fx-license-d1': 'license_recorded',
            'fx-certify-denied': 'certification_denied',
            'fx-reproduce-d1': 'reproduction_recorded',
            'fx-certify-d1': 'certified',
            'fx-slot': 'slot_recorded',
            'fx-plan': 'planned',
            'fx-token': 'token_issued',
            'fx-launch': 'launched',
            'fx-mount-d1': 'mounted',
            'fx-bundle-d2': 'bundle_registered',
            'fx-evidence-d2': 'evidence_recorded',
            'fx-certify-d2-early': 'certification_denied',
            'fx-license-d2': 'license_recorded',
            'fx-feature-pause': 'feature_recorded',
            'fx-certify-d2': 'certified',
            'fx-obs-reg-d2': 'observation_recorded',
            'fx-mount-d2-early': 'cache_unverified',
            'fx-obs-cache-d2': 'observation_recorded',
            'fx-mount-d2': 'mounted',
            'fx-feature-restore': 'feature_recorded',
        })
        # 钥匙泄漏后,仅有旧签名不能独立证明来源
        self.assertEqual(results['fx-certify-denied']['reason'], 'signature_untrusted')
        # 独立复现:同摘要、同依赖的可复查结论
        self.assertEqual(results['fx-reproduce-d1']['conclusion'], 'reproducible')
        # 替代依赖须重新确定许可证
        self.assertEqual(results['fx-certify-d2-early']['reason'], 'license_unresolved')
        # 撤销范围
        self.assertEqual(results['fx-revoke-key']['affected'], ['b-d1'])
        self.assertEqual(results['fx-revoke-builder']['affected'], ['b-d1'])
        # 启动波次固定获准摘要,尝试继承同一摘要
        self.assertEqual(one(self.db, 'SELECT digest FROM boot_waves WHERE id=?', ('wave-b',))['digest'], D1)
        self.assertEqual(one(self.db, 'SELECT digest FROM start_attempts WHERE id=?', ('try-b',))['digest'], D1)
        # 构建证据逐层绑定:源码、锁文件、构建环境、工具链、签署身份、制品摘要、获准修订
        evidence = one(self.db, 'SELECT * FROM build_evidence WHERE id=?', ('e-1',))
        self.assertEqual(evidence['source_commit'], SOURCE)
        self.assertEqual(evidence['lockfile_digest'], LOCK_ORIGINAL)
        self.assertEqual(evidence['build_env'], 'env-1')
        self.assertEqual(evidence['toolchain'], 'tc-1')
        self.assertEqual(evidence['signer'], 'team-key')
        self.assertEqual(evidence['artifact_digest'], D1)
        self.assertEqual((evidence['service'], evidence['service_generation']), ('svc-b', 1))
        # 独立重建公开可比较的输入:同源同锁文件,构建环境与工具链独立
        reproduction = one(self.db, 'SELECT * FROM reproductions WHERE id=?', ('repro-1',))
        self.assertEqual(reproduction['source_commit'], evidence['source_commit'])
        self.assertEqual(reproduction['lockfile_digest'], evidence['lockfile_digest'])
        self.assertNotEqual(reproduction['build_env'], evidence['build_env'])
        self.assertNotEqual(reproduction['toolchain'], evidence['toolchain'])
        # 替代制品功能:先暂停,较晚取得完整能力
        features = many(self.db, 'SELECT state FROM feature_scopes WHERE bundle=? ORDER BY at', ('b-d2',))
        self.assertEqual([row['state'] for row in features], ['paused', 'available'])
        self.assertTrue(replay(self.path)['verified'])

    def test_stolen_key_signature_alone_cannot_certify(self):
        self.bundle('b-d1', D1, 'team-key')
        self.seal(artifact_id='b-d1')
        self.evidence('e-1', 'b-d1', D1, signer='team-key', builder='builder-1', toolchain='tc-1')
        self.grant_license('lic-1', 'b-d1')
        self.revoke('rev-key', 'signer', 'team-key', disposition='key_compromised')
        result = self.certify('cert-1', 'b-d1')
        self.assertEqual((result['decision'], result['reason']),
                         ('certification_denied', 'signature_untrusted'))
        # 被盗钥匙签出的假包:signer 字符串看起来有效,同样不得交付
        self.bundle('b-fake', EVIL, 'team-key')
        self.evidence('e-fake', 'b-fake', EVIL, signer='team-key', builder='builder-1', toolchain='tc-1')
        self.grant_license('lic-fake', 'b-fake')
        self.assertEqual(self.certify('cert-fake', 'b-fake')['reason'], 'signature_untrusted')
        # 原构建器自己不算独立复现者
        self.reproduce('repro-fake', 'b-fake', 'builder-1', 'tc-1', EVIL)
        self.assertEqual(self.certify('cert-fake-2', 'b-fake')['reason'], 'builder_not_independent')
        self.observe('reg-fake', 'b-fake', 'registry', EVIL)
        self.observe('cache-fake', 'b-fake', 'cache', EVIL, node='node-1')
        self.assertEqual(self.mount('m-fake', 'b-fake', EVIL)['decision'], 'provenance_missing')
        self.assertTrue(replay(self.path)['verified'])

    def test_tag_change_cannot_impersonate_approved_bytes(self):
        self.bundle('b-d1', D1, 'release-key', tag='svc-b:1.0')
        self.seal(artifact_id='b-d1')
        self.deliver('b-d1', D1)
        # 同一镜像标签被重新指向不同字节
        self.bundle('b-d2', D2, 'release-key', tag='svc-b:1.0')
        self.deliver('b-d2', D2)
        tagged = many(self.db, 'SELECT id, digest FROM image_bundles WHERE tag=?', ('svc-b:1.0',))
        self.assertEqual({row['digest'] for row in tagged}, {D1, D2})
        # 标签相同不意味着字节相同:摘要不符即拒绝
        self.assertEqual(self.mount('m-t1', 'b-d2', D1)['decision'], 'digest_mismatch')
        self.assertEqual(self.mount('m-t2', 'b-d1', D2)['decision'], 'digest_mismatch')
        # 证书绑定摘要而非标签,已批准字节仍可挂载
        self.assertEqual(self.mount('m-t3', 'b-d1', D1)['decision'], 'mounted')
        certs = many(self.db, 'SELECT id, digest FROM provenance_certificates')
        self.assertEqual({row['digest'] for row in certs}, {D1, D2})
        self.assertTrue(replay(self.path)['verified'])

    def test_contaminated_toolchain_rejected_despite_same_source(self):
        self.bundle('b-dx', 'sha256:dx', 'release-key')
        self.seal(artifact_id='b-dx')
        self.evidence('e-x', 'b-dx', 'sha256:dx', signer='release-key', builder='builder-5',
                      toolchain='tc-1')
        self.grant_license('lic-x', 'b-dx')
        self.revoke('rev-tc', 'toolchain', 'tc-1', disposition='toolchain_contaminated')
        # 源提交相同,但证据链上的工具链已被撤销
        self.assertEqual(self.certify('cert-x-1', 'b-dx')['reason'], 'signature_untrusted')
        # 用同一受污染工具链复现,即使摘要一致也不获批
        result = self.reproduce('repro-x1', 'b-dx', 'builder-6', 'tc-1', 'sha256:dx')
        self.assertEqual(result['conclusion'], 'reproducible')
        self.assertEqual(self.certify('cert-x-2', 'b-dx')['reason'], 'reproduction_untrusted')
        # 洁净工具链的独立复现才能重建来源
        self.reproduce('repro-x2', 'b-dx', 'builder-7', 'tc-7', 'sha256:dx', env='env-7', at=2)
        self.assertEqual(self.certify('cert-x-3', 'b-dx')['decision'], 'certified')
        self.assertTrue(replay(self.path)['verified'])

    def test_unexplained_reproduction_divergence_blocks_certificate(self):
        self.bundle('b-d1', D1, 'team-key')
        self.seal(artifact_id='b-d1')
        self.evidence('e-1', 'b-d1', D1, signer='team-key', builder='builder-1', toolchain='tc-1')
        self.grant_license('lic-1', 'b-d1')
        self.revoke('rev-key', 'signer', 'team-key', disposition='key_compromised')
        # 差异无法解释时不得签发来源证书
        result = self.reproduce('repro-1', 'b-d1', 'builder-2', 'tc-2', 'sha256:other')
        self.assertEqual(result['conclusion'], 'diverged_unexplained')
        self.assertEqual(self.certify('cert-1', 'b-d1')['reason'], 'reproduction_diverged')
        # 解释了差异但字节不同,同样不能为原摘要作证
        result = self.reproduce('repro-2', 'b-d1', 'builder-2', 'tc-2', 'sha256:other',
                                differences=['embedded build timestamp'], at=2)
        self.assertEqual(result['conclusion'], 'diverged_explained')
        self.assertEqual(self.certify('cert-2', 'b-d1')['reason'], 'reproduction_diverged')
        # 同摘要、同依赖的可复查结果才能重新确立来源
        result = self.reproduce('repro-3', 'b-d1', 'builder-2', 'tc-2', D1, at=3)
        self.assertEqual(result['conclusion'], 'reproducible')
        self.assertEqual(self.certify('cert-3', 'b-d1')['decision'], 'certified')
        # 可比较的输入、差异与可重复性结论全部公开在案
        rows = many(self.db, 'SELECT * FROM reproductions WHERE bundle=? ORDER BY at', ('b-d1',))
        self.assertEqual([row['conclusion'] for row in rows],
                         ['diverged_unexplained', 'diverged_explained', 'reproducible'])
        for row in rows:
            self.assertEqual(row['source_commit'], SOURCE)
            self.assertEqual(row['lockfile_digest'], LOCK_ORIGINAL)
        self.assertEqual(json.loads(rows[1]['differences']), ['embedded build timestamp'])
        self.assertTrue(replay(self.path)['verified'])

    def test_registry_cache_and_mount_measured_independently(self):
        self.bundle('b-d1', D1, 'release-key')
        self.seal(artifact_id='b-d1')
        self.deliver('b-d1', D1)
        self.assertEqual(self.mount('m-1', 'b-d1', D1)['decision'], 'mounted')
        # 仓库镜像被替换:最近一次仓库测量不符
        self.observe('reg-tampered', 'b-d1', 'registry', 'sha256:tampered', at=2)
        self.assertEqual(self.mount('m-2', 'b-d1', D1, at=3)['decision'], 'registry_unverified')
        self.observe('reg-ok', 'b-d1', 'registry', D1, at=4)
        # 节点缓存被污染:该节点最近一次缓存测量不符
        self.observe('cache-tampered', 'b-d1', 'cache', 'sha256:tampered', node='node-1', at=5)
        self.assertEqual(self.mount('m-3', 'b-d1', D1, at=6)['decision'], 'cache_unverified')
        self.observe('cache-ok', 'b-d1', 'cache', D1, node='node-1', at=7)
        # 实际挂载字节不符
        self.assertEqual(self.mount('m-4', 'b-d1', 'sha256:tampered', at=8)['decision'], 'digest_mismatch')
        self.assertEqual(self.mount('m-5', 'b-d1', D1, at=9)['decision'], 'mounted')
        kinds = {(row['kind'], row['node'] is not None)
                 for row in many(self.db, 'SELECT * FROM artifact_observations WHERE bundle=?', ('b-d1',))}
        self.assertIn(('registry', False), kinds)
        self.assertIn(('cache', True), kinds)
        self.assertIn(('mount', True), kinds)
        self.assertTrue(replay(self.path)['verified'])

    def test_revocation_appends_scope_and_preserves_history(self):
        self.play_fixture()
        tables = ['build_evidence', 'build_attestations', 'image_mounts', 'provenance_certificates']
        before = {t: one(self.db, f'SELECT COUNT(*) n FROM {t}')['n'] for t in tables}
        result = self.revoke('rev-builder-2', 'builder', 'builder-2',
                             disposition='builder-2 contract ended; do not accept new vouching', at=70)
        self.assertEqual(result['decision'], 'revocation_recorded')
        self.assertEqual(result['affected'], ['b-d1'])
        self.assertEqual(result['certificates'], ['cert-d1'])
        # 撤销只追加一行受影响范围与处置记录
        self.assertEqual(one(self.db, 'SELECT COUNT(*) n FROM revocations')['n'], 3)
        row = one(self.db, "SELECT * FROM revocations WHERE id='rev-builder-2'")
        scope = json.loads(row['scope'])
        self.assertEqual(scope['bundles'], ['b-d1'])
        self.assertEqual(scope['certificates'], ['cert-d1'])
        self.assertIn('contract ended', row['disposition'])
        # 历史表一行未少:原证据与当时信任边界保留
        after = {t: one(self.db, f'SELECT COUNT(*) n FROM {t}')['n'] for t in tables}
        self.assertEqual(before, after)
        evidence = one(self.db, 'SELECT * FROM build_evidence WHERE id=?', ('e-1',))
        self.assertEqual(evidence['signer'], 'team-key')
        self.assertIn('2026-Q3', evidence['trust_boundary'])
        self.assertEqual(one(self.db, "SELECT state FROM image_mounts WHERE id='m-1'")['state'], 'mounted')
        self.assertEqual(one(self.db, "SELECT status FROM provenance_certificates WHERE id='cert-d1'")['status'],
                         'issued')
        # 但撤销之后不再授权新的挂载与下发
        self.assertEqual(self.mount('m-9', 'b-d1', D1, attempt='try-b', at=71)['decision'],
                         'provenance_revoked')
        self.assertTrue(replay(self.path)['verified'])

    def test_wave_launch_requires_pinned_certified_digest(self):
        self.play_fixture()
        self.assertEqual(wave.run(self.db, {'event_id': 'plan-b2', 'op': 'plan', 'id': 'wave-b2',
                                            'service': 'svc-b', 'generation': 1, 'deadline': 100})['decision'],
                         'planned')
        # 波次在计划时固定获准摘要
        self.assertEqual(one(self.db, 'SELECT digest FROM boot_waves WHERE id=?', ('wave-b2',))['digest'], D1)
        # 其它字节不得下发
        self.assertEqual(wave.run(self.db, {'event_id': 'launch-b2-wrong', 'op': 'launch', 'id': 'try-b2',
                                            'wave': 'wave-b2', 'slot': 'slot-1', 'token': 'token-b',
                                            'at': 32, 'expires': 80, 'digest': D2})['decision'],
                         'digest_mismatch')
        self.assertEqual(wave.run(self.db, {'event_id': 'launch-b2', 'op': 'launch', 'id': 'try-b2',
                                            'wave': 'wave-b2', 'slot': 'slot-1', 'token': 'token-b',
                                            'at': 32, 'expires': 80, 'digest': D1})['decision'], 'launched')
        self.assertEqual(one(self.db, 'SELECT digest FROM start_attempts WHERE id=?', ('try-b2',))['digest'], D1)
        # 挂载不得偏离尝试固定的摘要
        self.assertEqual(self.mount('m-x', 'b-d2', D2, attempt='try-b2', at=33)['decision'],
                         'digest_mismatch')
        # 证书信任基础被撤销后,即使摘要正确也不得下发
        self.revoke('rev-builder-2', 'builder', 'builder-2', at=70)
        wave.run(self.db, {'event_id': 'plan-b3', 'op': 'plan', 'id': 'wave-b3',
                           'service': 'svc-b', 'generation': 1, 'deadline': 100})
        self.assertEqual(wave.run(self.db, {'event_id': 'launch-b3', 'op': 'launch', 'id': 'try-b3',
                                            'wave': 'wave-b3', 'slot': 'slot-1', 'token': 'token-b',
                                            'at': 71, 'expires': 80, 'digest': D1})['decision'],
                         'artifact_uncertified')
        self.assertTrue(replay(self.path)['verified'])

    def test_alternative_artifact_redefines_license_and_features(self):
        self.play_fixture()
        e1 = one(self.db, 'SELECT * FROM build_evidence WHERE id=?', ('e-1',))
        e2 = one(self.db, 'SELECT * FROM build_evidence WHERE id=?', ('e-2',))
        # 同一源码,替代依赖
        self.assertEqual(e1['source_commit'], e2['source_commit'])
        self.assertNotEqual(e1['lockfile_digest'], e2['lockfile_digest'])
        # 许可证按制品与锁文件分别确定,旧许可不自动覆盖新依赖
        self.assertEqual(one(self.db, 'SELECT verdict FROM artifact_licenses WHERE bundle=? AND lockfile_digest=?',
                             ('b-d1', LOCK_ORIGINAL))['verdict'], 'approved')
        self.assertEqual(one(self.db, 'SELECT verdict FROM artifact_licenses WHERE bundle=? AND lockfile_digest=?',
                             ('b-d2', LOCK_ALTERNATIVE))['verdict'], 'approved')
        self.assertIsNone(one(self.db, 'SELECT * FROM artifact_licenses WHERE bundle=? AND lockfile_digest=?',
                              ('b-d2', LOCK_ORIGINAL)))
        # 旧证明只覆盖旧字节:cert-d1 的摘要是 D1,b-d2 持有自己的证书
        self.assertEqual(one(self.db, 'SELECT digest FROM provenance_certificates WHERE id=?', ('cert-d1',))['digest'], D1)
        self.assertEqual(artifact.valid_certificate(self.db, 'b-d2')['id'], 'cert-d2')
        # bundle.state 从未成为证明依据:两个制品同为 registered,挂载许可却各自独立
        states = many(self.db, 'SELECT id, state FROM image_bundles')
        self.assertTrue(all(row['state'] == 'registered' for row in states))
        # 功能范围:受替代依赖影响的功能先暂停,较晚恢复
        features = many(self.db, 'SELECT feature, state, reason FROM feature_scopes WHERE bundle=? ORDER BY at',
                        ('b-d2',))
        self.assertEqual([f['state'] for f in features], ['paused', 'available'])
        self.assertIn('alternative dependency', features[0]['reason'])
        self.assertIsNone(one(self.db, 'SELECT * FROM feature_scopes WHERE bundle=?', ('b-d1',)))
        self.assertTrue(replay(self.path)['verified'])
