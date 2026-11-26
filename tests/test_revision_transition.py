import tempfile,unittest
from pathlib import Path
from cloudcourt import contract,wave,ready,revision_transition as transition
from cloudcourt.store import connect,replay,one,many

class RevisionTransition(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=str(Path(self.tmp.name)/'db.sqlite')
        self.db=connect(self.path)
    def tearDown(self):
        self.db.close();self.tmp.cleanup()

    def boot_billing(self):
        # Database D at generation 4 lets billing API B (generation 1) serve writes externally.
        contract.run(self.db,{'event_id':'rev-d4','op':'revision','id':'database','generation':4})
        contract.run(self.db,{'event_id':'rev-b1','op':'revision','id':'billing-api','generation':1})
        contract.run(self.db,{'event_id':'edge-b-d','op':'edge','service':'billing-api','dependency':'database','critical':True,'revision':1})
        contract.run(self.db,{'event_id':'seal-d4','op':'seal','id':'database','generation':4})
        contract.run(self.db,{'event_id':'seal-b1','op':'seal','id':'billing-api','generation':1})
        contract.run(self.db,{'event_id':'token-d4','op':'token','id':'token-db','service':'database','generation':4,'expires':200,'at':10})
        contract.run(self.db,{'event_id':'token-b1','op':'token','id':'token-b','service':'billing-api','generation':1,'expires':200,'at':10})
        wave.run(self.db,{'event_id':'slot-a','op':'slot','id':'slot-a','node':'node-a','capacity':2,'generation':1,'healthy':True})
        wave.run(self.db,{'event_id':'wave-d4','op':'plan','id':'wave-db','service':'database','generation':4,'deadline':150})
        wave.run(self.db,{'event_id':'try-d4','op':'launch','id':'try-db','wave':'wave-db','slot':'slot-a','token':'token-db','at':20,'expires':120})
        ready.run(self.db,{'event_id':'db-p1','op':'probe','id':'db-p1','attempt':'try-db','at':30,'healthy':True})
        ready.run(self.db,{'event_id':'db-p2','op':'probe','id':'db-p2','attempt':'try-db','at':31,'healthy':True})
        self.assertEqual(ready.run(self.db,{'event_id':'pub-d4','op':'publish','id':'receipt-db','attempt':'try-db','at':32})['decision'],'published')
        wave.run(self.db,{'event_id':'wave-b1','op':'plan','id':'wave-b','service':'billing-api','generation':1,'deadline':150})
        wave.run(self.db,{'event_id':'try-b1','op':'launch','id':'try-b','wave':'wave-b','slot':'slot-a','token':'token-b','at':35,'expires':120})
        ready.run(self.db,{'event_id':'b-p1','op':'probe','id':'b-p1','attempt':'try-b','at':40,'healthy':True})
        ready.run(self.db,{'event_id':'b-p2','op':'probe','id':'b-p2','attempt':'try-b','at':41,'healthy':True})
        self.assertEqual(ready.run(self.db,{'event_id':'pub-b1','op':'publish','id':'receipt-b','attempt':'try-b','at':42})['decision'],'published')

    def bill(self,event_id,bill,amount,at,fmt='v4'):
        return transition.run(self.db,{'event_id':event_id,'op':'write','id':bill,'service':'billing-api','amount':amount,'key_format':fmt,'at':at})

    def confirm(self,event_id,bill,at):
        return transition.run(self.db,{'event_id':event_id,'op':'confirm','id':bill,'confirmation_prefix':'CUST-','at':at})

    def revise(self,event_id='t45',to=5,compatible=False,phase='migrating',until=90,at=50):
        return transition.run(self.db,{'event_id':event_id,'op':'transition','id':event_id,'service':'database','from_generation':to-1,'to_generation':to,'interface_compatible':compatible,'migration_phase':phase,'rollback_until':until,'edges':[{'service':'billing-api','dependency':'database'}],'at':at})

    def read(self,event_id,at):
        return transition.run(self.db,{'event_id':event_id,'op':'read','service':'billing-api','at':at})

    def rulings(self):
        return [(r['read_verdict'],r['write_verdict'],r['basis']) for r in many(self.db,'SELECT * FROM traffic_rulings WHERE service=? ORDER BY rowid',('billing-api',))]

    def republish_billing(self,prefix):
        wave.run(self.db,{'event_id':prefix+'-plan','op':'plan','id':prefix+'-wave','service':'billing-api','generation':1,'deadline':150})
        return prefix+'-wave'

    def test_read_only_permit_bounds_write_closure_and_rollback(self):
        self.boot_billing()
        # Before the revision the published billing API reads and writes; bills confirm under prefix CUST-.
        self.assertEqual(self.bill('w1','bill-1',100,45)['decision'],'write_recorded')
        self.assertEqual(self.confirm('c1','bill-1',45)['decision'],'confirmed')
        self.assertEqual(self.bill('w2','bill-2',200,46)['decision'],'write_recorded')
        self.confirm('c2','bill-2',46)
        self.assertEqual(self.bill('w3','bill-3',300,47)['decision'],'write_recorded')
        # Business owner signs a read-only permit on the billing-api -> database dependency until 80.
        self.assertEqual(transition.run(self.db,{'event_id':'permit-ro','op':'permit','id':'permit-ro','service':'billing-api','dependency':'database','operations':['read'],'approver':'owner-signature','until':80,'at':48})['decision'],'permit_issued')
        # D generation 5 changes the billing key format: incompatible, migrating, rollback window to 90.
        result=self.revise()
        self.assertEqual(result['decision'],'transition_recorded')
        self.assertEqual(result['readjudicated'],['billing-api'])
        self.assertEqual(one(self.db,'SELECT status FROM readiness_receipts WHERE id=?',('receipt-b',))['status'],'superseded')
        self.assertEqual(one(self.db,'SELECT status FROM readiness_receipts WHERE id=?',('receipt-db',))['status'],'published')
        self.assertEqual(one(self.db,'SELECT status FROM bootstrap_tokens WHERE id=?',('token-db',))['status'],'stale')
        # Confirmed bills complete; the unconfirmed write is held and owned by billing-api.
        self.assertEqual(one(self.db,'SELECT status FROM billing_ledger WHERE id=?',('bill-1',))['status'],'confirmed')
        held=one(self.db,'SELECT status,owner FROM billing_ledger WHERE id=?',('bill-3',))
        self.assertEqual((held['status'],held['owner']),('held','billing-api'))
        self.assertEqual(self.confirm('c3','bill-3',51)['decision'],'not_confirmable')
        # Read-only continuation: queries pass with an audit obligation; the write service is closed.
        self.assertEqual(self.read('r1',51)['decision'],'allowed')
        self.assertEqual(one(self.db,'SELECT basis,permit FROM traffic_audit WHERE service=?',('billing-api',)),{'basis':'permit','permit':'permit-ro'})
        self.assertEqual(self.bill('w4','bill-4',400,51)['decision'],'write_denied')
        # Late probes and the old attempt cannot re-publish old-generation readiness.
        self.assertEqual(ready.run(self.db,{'event_id':'late-p','op':'probe','id':'late-p','attempt':'try-b','at':52,'healthy':True})['decision'],'probe_outside_attempt')
        self.assertEqual(ready.run(self.db,{'event_id':'late-pub','op':'publish','id':'receipt-b-late','attempt':'try-b','at':52})['decision'],'attempt_unavailable')
        self.assertEqual(one(self.db,'SELECT status FROM readiness_receipts WHERE id=?',('receipt-b',))['status'],'superseded')
        # A bare generation decrement is not a rollback.
        self.assertEqual(contract.run(self.db,{'event_id':'bare-dec','op':'revision','id':'database','generation':4})['decision'],'stale_revision')
        # Migration fails; rollback verifies the old data format and customer confirmation prefix.
        self.assertEqual(transition.run(self.db,{'event_id':'mig-f','op':'migrate','id':'t45','phase':'failed','at':55})['decision'],'migration_updated')
        self.assertEqual(transition.run(self.db,{'event_id':'rb-bad','op':'rollback','id':'t45','data_format':'v4','confirmation_prefix':'WRONG-','at':56})['decision'],'rollback_evidence_invalid')
        self.assertEqual(one(self.db,'SELECT generation FROM service_revisions WHERE id=?',('database',))['generation'],5)
        self.assertEqual(transition.run(self.db,{'event_id':'rb-ok','op':'rollback','id':'t45','data_format':'v4','confirmation_prefix':'CUST-','at':60})['decision'],'rolled_back')
        self.assertEqual(one(self.db,'SELECT generation,status FROM service_revisions WHERE id=?',('database',)),{'generation':4,'status':'sealed'})
        # After rollback the old receipt stays superseded; the ingress closes until B re-certifies.
        self.assertEqual(self.read('r2',61)['decision'],'denied')
        wave.run(self.db,{'event_id':'wave-b2','op':'plan','id':'wave-b2','service':'billing-api','generation':1,'deadline':150})
        self.assertEqual(wave.run(self.db,{'event_id':'try-b2','op':'launch','id':'try-b2','wave':'wave-b2','slot':'slot-a','token':'token-b','at':63,'expires':120})['decision'],'launched')
        ready.run(self.db,{'event_id':'b2-p1','op':'probe','id':'b2-p1','attempt':'try-b2','at':64,'healthy':True})
        ready.run(self.db,{'event_id':'b2-p2','op':'probe','id':'b2-p2','attempt':'try-b2','at':65,'healthy':True})
        self.assertEqual(ready.run(self.db,{'event_id':'pub-b2','op':'publish','id':'receipt-b2','attempt':'try-b2','at':66})['decision'],'published')
        verdict=transition.run(self.db,{'event_id':'readj','op':'readjudicate','service':'billing-api','at':67})
        self.assertEqual((verdict['read'],verdict['write'],verdict['basis']),('allow','allow','current'))
        self.assertEqual(self.read('r3',68)['decision'],'allowed')
        self.assertEqual(self.bill('w5','bill-5',500,68)['decision'],'write_recorded')
        # Only the recorded owner answers for the held write.
        self.assertEqual(transition.run(self.db,{'event_id':'rs-bad','op':'resolve','id':'bill-3','resolution':'abort','by':'intruder','at':69})['decision'],'not_responsible')
        self.assertEqual(transition.run(self.db,{'event_id':'rs-ok','op':'resolve','id':'bill-3','resolution':'abort','by':'billing-api','at':69})['decision'],'resolved')
        self.assertEqual(one(self.db,'SELECT status FROM billing_ledger WHERE id=?',('bill-3',))['status'],'aborted')
        # Permit expiry neither revives old-generation readiness nor disturbs evidence-based recovery.
        self.assertEqual(transition.run(self.db,{'event_id':'exp','op':'expire','at':81})['permits'],['permit-ro'])
        self.assertEqual(self.read('r4',82)['decision'],'allowed')
        self.assertEqual(one(self.db,'SELECT status FROM readiness_receipts WHERE id=?',('receipt-b',))['status'],'superseded')
        self.assertEqual(one(self.db,'SELECT COUNT(*) n FROM traffic_audit')['n'],1)
        # The read/write adjudication sequence in event order.
        self.assertEqual(self.rulings(),[('allow','allow','current'),('allow','deny','permit'),('allow','deny','permit'),('deny','deny','pause'),('allow','allow','current'),('allow','allow','current')])
        self.assertTrue(replay(self.path)['verified'])

    def test_degrade_window_bounds_read_only_continuation(self):
        self.boot_billing()
        self.bill('w1','bill-1',100,45)
        self.confirm('c1','bill-1',45)
        transition.run(self.db,{'event_id':'permit-ro','op':'permit','id':'permit-ro','service':'billing-api','dependency':'database','operations':['read'],'approver':'owner-signature','until':60,'at':48})
        self.revise()
        self.assertEqual(self.read('r1',55)['decision'],'allowed')
        # Past the window the permit stops passing traffic even before the expire event arrives.
        self.assertEqual(self.read('r2',61)['decision'],'denied')
        self.assertEqual(self.bill('w2','bill-2',200,61)['decision'],'write_denied')
        self.assertEqual(transition.run(self.db,{'event_id':'exp','op':'expire','at':62})['permits'],['permit-ro'])
        self.assertEqual(one(self.db,'SELECT status FROM traffic_permits WHERE id=?',('permit-ro',))['status'],'expired')
        self.assertEqual(self.read('r3',63)['decision'],'denied')
        self.assertTrue(replay(self.path)['verified'])

    def test_full_pause_until_migration_and_reconciliation(self):
        self.boot_billing()
        self.bill('w1','bill-1',100,45)
        self.confirm('c1','bill-1',45)
        self.bill('w2','bill-2',200,46)
        self.confirm('c2','bill-2',46)
        self.bill('w3','bill-3',300,47)
        # Same revision, same traffic, no signed permit: the contract pauses every request.
        self.revise()
        self.assertEqual(self.read('r1',51)['decision'],'denied')
        self.assertEqual(self.bill('w4','bill-4',400,51)['decision'],'write_denied')
        self.assertEqual(many(self.db,'SELECT * FROM traffic_audit'),[])
        # D brings generation 5 online; B cannot launch against the old dependency evidence meanwhile.
        contract.run(self.db,{'event_id':'seal-d5','op':'seal','id':'database','generation':5})
        contract.run(self.db,{'event_id':'token-d5','op':'token','id':'token-db5','service':'database','generation':5,'expires':200,'at':60})
        wave.run(self.db,{'event_id':'wave-d5','op':'plan','id':'wave-db5','service':'database','generation':5,'deadline':150})
        wave.run(self.db,{'event_id':'try-d5','op':'launch','id':'try-db5','wave':'wave-db5','slot':'slot-a','token':'token-db5','at':63,'expires':120})
        ready.run(self.db,{'event_id':'d5-p1','op':'probe','id':'d5-p1','attempt':'try-db5','at':64,'healthy':True})
        ready.run(self.db,{'event_id':'d5-p2','op':'probe','id':'d5-p2','attempt':'try-db5','at':65,'healthy':True})
        wave.run(self.db,{'event_id':'wave-b2','op':'plan','id':'wave-b2','service':'billing-api','generation':1,'deadline':150})
        self.assertEqual(wave.run(self.db,{'event_id':'try-b2-early','op':'launch','id':'try-b2','wave':'wave-b2','slot':'slot-a','token':'token-b','at':66,'expires':120})['decision'],'dependency_not_ready')
        self.assertEqual(ready.run(self.db,{'event_id':'pub-d5','op':'publish','id':'receipt-db5','attempt':'try-db5','at':67})['decision'],'published')
        self.assertEqual(transition.run(self.db,{'event_id':'mig-c','op':'migrate','id':'t45','phase':'complete','at':68})['decision'],'migration_updated')
        self.assertEqual(wave.run(self.db,{'event_id':'try-b2','op':'launch','id':'try-b2','wave':'wave-b2','slot':'slot-a','token':'token-b','at':69,'expires':120})['decision'],'launched')
        ready.run(self.db,{'event_id':'b2-p1','op':'probe','id':'b2-p1','attempt':'try-b2','at':70,'healthy':True})
        ready.run(self.db,{'event_id':'b2-p2','op':'probe','id':'b2-p2','attempt':'try-b2','at':71,'healthy':True})
        self.assertEqual(ready.run(self.db,{'event_id':'pub-b2','op':'publish','id':'receipt-b2','attempt':'try-b2','at':72})['decision'],'published')
        # Migration complete is not enough: the pause holds until confirmed bills are reconciled.
        self.assertEqual(self.read('r2',73)['decision'],'denied')
        self.assertEqual(transition.run(self.db,{'event_id':'rec-early','op':'reconcile','id':'t45','service':'billing-api','at':73})['decision'],'unresolved_writes')
        self.assertEqual(transition.run(self.db,{'event_id':'rs','op':'resolve','id':'bill-3','resolution':'abort','by':'billing-api','at':74})['decision'],'resolved')
        result=transition.run(self.db,{'event_id':'rec','op':'reconcile','id':'t45','service':'billing-api','at':75})
        self.assertEqual(result['decision'],'reconciled')
        self.assertEqual(set(result['bills']),{'bill-1','bill-2'})
        self.assertEqual(one(self.db,'SELECT status FROM billing_ledger WHERE id=?',('bill-1',))['status'],'reconciled')
        self.assertEqual(self.read('r3',76)['decision'],'allowed')
        self.assertEqual(self.bill('w5','bill-4',400,76,fmt='v5')['decision'],'write_recorded')
        self.assertEqual(one(self.db,'SELECT dependency_generation FROM billing_ledger WHERE id=?',('bill-4',))['dependency_generation'],5)
        # A further upstream revision supersedes again; old receipts never come back.
        result=self.revise(event_id='t56',to=6,until=150,at=80)
        self.assertEqual(result['readjudicated'],['billing-api'])
        self.assertEqual(one(self.db,'SELECT status FROM readiness_receipts WHERE id=?',('receipt-b2',))['status'],'superseded')
        self.assertEqual(one(self.db,'SELECT status FROM readiness_receipts WHERE id=?',('receipt-b',))['status'],'superseded')
        self.assertEqual(self.read('r4',81)['decision'],'denied')
        self.assertTrue(replay(self.path)['verified'])

    def test_compatible_revision_is_not_a_failure(self):
        self.boot_billing()
        self.bill('w1','bill-1',100,45)
        self.confirm('c1','bill-1',45)
        result=self.revise(compatible=True,phase='none')
        self.assertEqual(result['decision'],'transition_recorded')
        self.assertEqual(result['readjudicated'],[])
        self.assertEqual(one(self.db,'SELECT status FROM readiness_receipts WHERE id=?',('receipt-b',))['status'],'published')
        self.assertEqual(self.read('r1',51)['decision'],'allowed')
        self.assertEqual(self.bill('w2','bill-2',200,51)['decision'],'write_recorded')
        self.assertEqual(many(self.db,'SELECT * FROM traffic_rulings WHERE service=?',('billing-api',)),[])
        self.assertTrue(replay(self.path)['verified'])

    def test_permit_scope_and_revocation(self):
        self.boot_billing()
        self.revise()
        self.assertEqual(self.read('r1',51)['decision'],'denied')
        # Permits cover only the declared dependency and business operations.
        self.assertEqual(transition.run(self.db,{'event_id':'p-bad','op':'permit','id':'p-bad','service':'billing-api','dependency':'database','operations':['read','execute'],'approver':'owner-signature','until':80,'at':52})['decision'],'permit_denied')
        self.assertEqual(transition.run(self.db,{'event_id':'p-mis','op':'permit','id':'p-mis','service':'billing-api','dependency':'other-service','operations':['read'],'approver':'owner-signature','until':80,'at':52})['decision'],'permit_denied')
        self.assertEqual(transition.run(self.db,{'event_id':'p-ro','op':'permit','id':'p-ro','service':'billing-api','dependency':'database','operations':['read'],'approver':'owner-signature','until':80,'at':52})['decision'],'permit_issued')
        self.assertEqual(self.read('r2',53)['decision'],'allowed')
        # A read-only permit must not pass writes.
        self.assertEqual(self.bill('w1','bill-1',100,53)['decision'],'write_denied')
        self.assertEqual(transition.run(self.db,{'event_id':'p-rev','op':'permit_revoke','id':'p-ro','at':54})['decision'],'permit_revoked')
        self.assertEqual(self.read('r3',55)['decision'],'denied')
        self.assertTrue(replay(self.path)['verified'])

    def test_rollback_window_and_missing_evidence(self):
        self.boot_billing()
        self.revise(until=60)
        self.assertEqual(transition.run(self.db,{'event_id':'rb-late','op':'rollback','id':'t45','data_format':'v4','confirmation_prefix':'CUST-','at':61})['decision'],'rollback_window_closed')
        self.assertEqual(transition.run(self.db,{'event_id':'rb-none','op':'rollback','id':'t45','data_format':'v4','confirmation_prefix':'CUST-','at':55})['decision'],'rollback_evidence_missing')
        self.assertEqual(one(self.db,'SELECT generation FROM service_revisions WHERE id=?',('database',))['generation'],5)
        self.assertTrue(replay(self.path)['verified'])

    def test_revision_event_must_declare_transition_evidence(self):
        self.boot_billing()
        result=transition.run(self.db,{'event_id':'t-bare','op':'transition','id':'t-bare','service':'database','from_generation':4,'to_generation':5,'at':50})
        self.assertEqual(result['decision'],'declaration_incomplete')
        self.assertEqual(one(self.db,'SELECT generation FROM service_revisions WHERE id=?',('database',))['generation'],4)
        self.assertTrue(replay(self.path)['verified'])
