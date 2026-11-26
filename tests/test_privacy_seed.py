import sqlite3
import unittest

from cloudcourt.operator_record import record
from cloudcourt.operator_retention import hold, provisional_erasure


class PrivacySeedTest(unittest.TestCase):
    def test_hold_removes_one_person_from_erasure_list(self):
        db = sqlite3.connect(':memory:')
        record(db, 'R1', 'operator-A', 'A1', 'a@example.test')
        record(db, 'R2', 'operator-B', 'A1', 'b@example.test')
        self.assertEqual(['R1','R2'], provisional_erasure(db))
        hold(db, 'H1', 'R1', 'case-1', 'court')
        self.assertEqual(['R2'], provisional_erasure(db))
        db.close()


if __name__ == '__main__':
    unittest.main()
