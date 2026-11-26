import tempfile
import unittest
from pathlib import Path

from cloudcourt import loadtest
from cloudcourt.store import connect, replay


class LoadTestBaseline(unittest.TestCase):
    def test_trial_sample_and_report_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'trial.sqlite')
            db = connect(path)
            self.assertEqual(loadtest.run(db, {'event_id': 'p', 'op': 'plan', 'id': 'p',
                                               'service': 'api', 'generation': 1,
                                               'clients': 10, 'until': 20})['decision'], 'trial_planned')
            self.assertEqual(loadtest.run(db, {'event_id': 's', 'op': 'sample', 'id': 's',
                                               'plan': 'p', 'at': 2, 'latency_ms': 5,
                                               'ok': True})['decision'], 'trial_sampled')
            self.assertEqual(loadtest.run(db, {'event_id': 'r', 'op': 'report', 'id': 'r',
                                               'plan': 'p'})['measured'], 1)
            self.assertTrue(replay(path)['verified'])
            db.close()
