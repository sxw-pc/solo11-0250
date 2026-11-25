import json,tempfile,unittest
from pathlib import Path
from cloudcourt import contract,wave,ready
from cloudcourt.store import connect,replay,one

def fixture(db,name,module):
    return [module.run(db,x) for x in json.loads((Path('fixtures')/name).read_text(encoding='utf-8'))]

class Business(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=str(Path(self.tmp.name)/'db.sqlite');self.db=connect(self.path)
    def tearDown(self):self.db.close();self.tmp.cleanup()
    def base(self):fixture(self.db,'contract-normal.json',contract);fixture(self.db,'wave-normal.json',wave)
    def published_db(self):
        self.base()
        for step in json.loads((Path('fixtures/path.json')).read_text(encoding='utf-8')):
            from cloudcourt import MODULES
            MODULES[step['subsystem']].run(self.db,step['payload'])

    def test_cycle_and_sealed_contract(self):
        contract.run(self.db,{'event_id':'a','op':'revision','id':'a','generation':1})
        contract.run(self.db,{'event_id':'b','op':'revision','id':'b','generation':1})
        self.assertEqual(contract.run(self.db,{'event_id':'ab','op':'edge','service':'a','dependency':'b','critical':True,'revision':1})['decision'],'edge_added')
        self.assertEqual(contract.run(self.db,{'event_id':'ba','op':'edge','service':'b','dependency':'a','critical':True,'revision':1})['decision'],'cycle')
        contract.run(self.db,{'event_id':'seal','op':'seal','id':'a','generation':1})
        self.assertEqual(contract.run(self.db,{'event_id':'later','op':'edge','service':'a','dependency':'b','critical':False,'revision':1})['decision'],'edge_unavailable')
        self.assertTrue(replay(self.path)['verified'])

    def test_dependency_blocks_launch_until_current_receipt(self):
        self.base()
        wave.run(self.db,{'event_id':'plan-api','op':'plan','id':'api-wave','service':'api','generation':1,'deadline':90})
        self.assertEqual(one(self.db,'SELECT level FROM boot_waves WHERE id=?',('wave-db',))['level'],0)
        self.assertEqual(one(self.db,'SELECT level FROM boot_waves WHERE id=?',('api-wave',))['level'],1)
        self.assertEqual(wave.run(self.db,{'event_id':'too-early','op':'launch','id':'api-try','wave':'api-wave','slot':'slot-a','token':'token-api','at':30,'expires':70})['decision'],'slot_full')
        ready.run(self.db,{'event_id':'p1','op':'probe','id':'p1','attempt':'try-db','at':30,'healthy':True})
        ready.run(self.db,{'event_id':'p2','op':'probe','id':'p2','attempt':'try-db','at':31,'healthy':True})
        ready.run(self.db,{'event_id':'r','op':'publish','id':'r-db','attempt':'try-db','at':32})
        self.assertEqual(wave.run(self.db,{'event_id':'go','op':'launch','id':'api-try','wave':'api-wave','slot':'slot-a','token':'token-api','at':35,'expires':70})['decision'],'launched')
        self.assertTrue(replay(self.path)['verified'])

    def test_old_revision_cannot_publish_or_start_dependent(self):
        self.published_db()
        contract.run(self.db,{'event_id':'new-db','op':'revision','id':'database','generation':2})
        self.assertEqual(wave.run(self.db,{'event_id':'new-api','op':'launch','id':'new-api-try','wave':'wave-api','slot':'slot-a','token':'token-api','at':50,'expires':70})['decision'],'launch_denied')
        ready.run(self.db,{'event_id':'api-p1','op':'probe','id':'api-p1','attempt':'try-api','at':40,'healthy':True})
        ready.run(self.db,{'event_id':'api-p2','op':'probe','id':'api-p2','attempt':'try-api','at':41,'healthy':True})
        self.assertEqual(ready.run(self.db,{'event_id':'pub-api','op':'publish','id':'receipt-api','attempt':'try-api','at':42})['decision'],'parent_receipt_stale')

    def test_revocation_cascades_to_child_and_rollback(self):
        self.published_db();fixture(self.db,'ready-normal.json',ready)
        self.assertEqual(ready.run(self.db,{'event_id':'revoke','op':'revoke','id':'receipt-db'})['decision'],'revoked')
        self.assertEqual(one(self.db,'SELECT status FROM readiness_receipts WHERE id=?',('receipt-api',))['status'],'revoked')
        self.assertEqual(contract.run(self.db,{'event_id':'reassess','op':'reassess','service':'api'})['decision'],'dependency_contract_blocked')
        self.assertEqual(one(self.db,'SELECT status FROM service_revisions WHERE id=?',('api',))['status'],'blocked')
        self.assertEqual(ready.run(self.db,{'event_id':'rollback','op':'rollback','id':'revoke-api'})['decision'],'rolled_back')
        self.assertEqual(one(self.db,'SELECT status FROM boot_waves WHERE id=?',('wave-api',))['status'],'rolled_back')
        self.assertTrue(replay(self.path)['verified'])

    def test_timeout_fences_late_probe(self):
        self.base()
        self.assertEqual(wave.run(self.db,{'event_id':'timeout','op':'timeout','id':'try-db','at':61})['decision'],'timed_out')
        self.assertEqual(ready.run(self.db,{'event_id':'late','op':'probe','id':'late','attempt':'try-db','at':59,'healthy':True})['decision'],'probe_outside_attempt')
        self.assertEqual(wave.run(self.db,{'event_id':'retry','op':'retry','old':'try-db'})['decision'],'retry_opened')

    def test_time_bound_degrade_grant_changes_launch_and_publication(self):
        self.base()
        wave.run(self.db,{'event_id':'slot-b','op':'slot','id':'slot-b','node':'node-b','capacity':1,'generation':1,'healthy':True})
        wave.run(self.db,{'event_id':'plan-api','op':'plan','id':'api-wave','service':'api','generation':1,'deadline':90})
        payload={'op':'launch','id':'api-try','wave':'api-wave','slot':'slot-b','token':'token-api','at':35,'expires':70}
        self.assertEqual(wave.run(self.db,dict(payload,event_id='before'))['decision'],'dependency_not_ready')
        self.assertEqual(contract.run(self.db,{'event_id':'grant','op':'degrade','id':'degrade-a','service':'api','dependency':'database','approver':'operator-a','at':34,'until':40})['decision'],'degrade_allowed')
        self.assertEqual(wave.run(self.db,dict(payload,event_id='after'))['decision'],'launched')
        ready.run(self.db,{'event_id':'p1','op':'probe','id':'p1','attempt':'api-try','at':36,'healthy':True})
        ready.run(self.db,{'event_id':'p2','op':'probe','id':'p2','attempt':'api-try','at':37,'healthy':True})
        self.assertEqual(ready.run(self.db,{'event_id':'publish','op':'publish','id':'r-api','attempt':'api-try','at':41})['decision'],'parent_receipt_stale')
        self.assertEqual(ready.run(self.db,{'event_id':'publish-early','op':'publish','id':'r-api','attempt':'api-try','at':39})['decision'],'published')
        self.assertTrue(replay(self.path)['verified'])

    def test_same_business_wave_id_with_new_event_is_rejected(self):
        self.base()
        result=wave.run(self.db,{'event_id':'other-plan','op':'plan','id':'wave-db','service':'database','generation':1,'deadline':90})
        self.assertEqual(result['decision'],'business_identity_conflict')
        self.assertEqual(one(self.db,'SELECT deadline FROM boot_waves WHERE id=?',('wave-db',))['deadline'],80)
