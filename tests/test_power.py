import tempfile
import unittest
from pathlib import Path

from cloudcourt import power
from cloudcourt.store import connect, replay


class PowerBaseline(unittest.TestCase):
    def test_feed_reservation_and_observation_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'power.sqlite')
            db = connect(path)
            self.assertEqual(power.run(db, {'event_id': 'f', 'op': 'feed', 'id': 'f',
                                            'rack': 'rack-a', 'rated_kw': 8,
                                            'revision': 1})['decision'], 'feed_recorded')
            self.assertEqual(power.run(db, {'event_id': 'r', 'op': 'reserve', 'id': 'r',
                                            'feed': 'f', 'node': 'node-a',
                                            'kw': 3})['decision'], 'power_reserved')
            self.assertEqual(power.run(db, {'event_id': 'o', 'op': 'observe', 'id': 'o',
                                            'feed': 'f', 'at': 2,
                                            'measured_kw': 4})['decision'], 'power_observed')
            self.assertTrue(replay(path)['verified'])
            db.close()
