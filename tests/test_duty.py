import tempfile
import unittest
from pathlib import Path

from cloudcourt import duty
from cloudcourt.store import connect, replay


class DutyBaseline(unittest.TestCase):
    def test_shift_handoff_and_acknowledgment_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'duty.sqlite')
            db = connect(path)
            for event_id, actor, starts, ends in [('a', 'alice', 1, 5), ('b', 'bob', 5, 10)]:
                self.assertEqual(duty.run(db, {'event_id': event_id, 'op': 'shift', 'id': event_id,
                                                'actor': actor, 'site': 's', 'starts': starts,
                                                'ends': ends, 'qualification': 'startup'})['decision'], 'shift_recorded')
            self.assertEqual(duty.run(db, {'event_id': 'h', 'op': 'handoff', 'id': 'h',
                                            'outgoing': 'a', 'incoming': 'b', 'wave': 'w',
                                            'at': 5})['decision'], 'handoff_opened')
            self.assertEqual(duty.run(db, {'event_id': 'ack', 'op': 'ack', 'id': 'ack',
                                            'handoff': 'h', 'actor': 'bob', 'at': 5})['decision'], 'acknowledged')
            self.assertTrue(replay(path)['verified'])
            db.close()
