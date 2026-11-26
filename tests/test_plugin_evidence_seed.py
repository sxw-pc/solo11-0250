import sqlite3
import unittest

from cloudcourt.plugin_evidence import load, assignment, bank


class PluginEvidenceSeedTest(unittest.TestCase):
    def test_overlapping_load_intervals_and_pending_bank(self):
        db = sqlite3.connect(':memory:')
        load(db, 'M1', 'W2', 'site-2', 'd1', 20, 3, 'attestor-A')
        load(db, 'M2', 'W2', 'site-2', 'd2', 22, 3, 'attestor-B')
        assignment(db, 'X1', 'd1', 'assignee', 18, 'site-1-only')
        bank(db, 'B1', 'D1', 'assignee', 20, 'pending')
        self.assertEqual(2, db.execute('SELECT COUNT(*) FROM plugin_loads').fetchone()[0])
        self.assertEqual('pending', db.execute('SELECT bank_state FROM plugin_bank_receipts').fetchone()[0])
        db.close()


if __name__ == '__main__':
    unittest.main()
