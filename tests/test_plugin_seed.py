import sqlite3
import unittest

from cloudcourt.bootstrap_plugin import catalog, activate
from cloudcourt.plugin_royalty import draft


class PluginSeedTest(unittest.TestCase):
    def test_two_sites_make_two_royalty_rows(self):
        db = sqlite3.connect(':memory:')
        catalog(db, 'P1', 'digest-1', 'vendor-A', 'L1')
        activate(db, 'A1', 'P1', 'billing', 'W1', 'site-1')
        activate(db, 'A2', 'P1', 'billing', 'W1', 'site-2')
        self.assertEqual(40, draft(db, 'D1', 'vendor-A', 20))
        db.close()


if __name__ == '__main__':
    unittest.main()
