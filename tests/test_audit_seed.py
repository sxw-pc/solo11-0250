import unittest

from cloudcourt.store import connect
from cloudcourt.contract import run
from cloudcourt.audit_slice import export
from cloudcourt.audit_verify import provisional


class AuditSeedTest(unittest.TestCase):
    def test_a_single_matching_event_is_called_verified(self):
        db = connect(':memory:')
        run(db, {'event_id':'E1','op':'revision','id':'billing','generation':1})
        records = export(db, 'S1', 'billing')
        self.assertEqual(1, len(records))
        self.assertEqual('verified', provisional(db, 'V1', 'S1'))
        db.close()


if __name__ == '__main__':
    unittest.main()
