import tempfile
import unittest
from pathlib import Path

from cloudcourt import thermal
from cloudcourt.store import connect, replay


class ThermalBaseline(unittest.TestCase):
    def test_zone_sample_and_check_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'thermal.sqlite')
            db = connect(path)
            self.assertEqual(thermal.run(db, {'event_id': 'z', 'op': 'zone', 'id': 'z',
                                              'rack': 'rack-a', 'limit_c': 40,
                                              'revision': 1})['decision'], 'zone_recorded')
            self.assertEqual(thermal.run(db, {'event_id': 's', 'op': 'sample', 'id': 's',
                                              'zone': 'z', 'sensor': 'inlet', 'at': 1,
                                              'celsius': 30})['decision'], 'temperature_recorded')
            self.assertEqual(thermal.run(db, {'event_id': 'c', 'op': 'check', 'id': 'c',
                                              'zone': 'z', 'node': 'n',
                                              'sample': 's'})['decision'], 'qualified')
            self.assertTrue(replay(path)['verified'])
            db.close()
