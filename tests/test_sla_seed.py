import unittest

from cloudcourt.store import connect
from cloudcourt.customer_sla import subscribe
from cloudcourt.credit_draft import provisional


class SlaSeedTest(unittest.TestCase):
    def test_failed_wave_creates_credit_without_customer_impact(self):
        db = connect(':memory:')
        subscribe(db, 'S1', 'customer-A', 'billing', 2, 50)
        db.execute('INSERT INTO boot_waves VALUES(?,?,?,?,?,?)',
                   ('W1', 'billing', 1, 30, 0, 'failed'))
        self.assertEqual(50, provisional(db, 'D1', 'S1'))
        db.close()


if __name__ == '__main__':
    unittest.main()
