import tempfile
import unittest
from pathlib import Path

from cloudcourt import artifact
from cloudcourt.store import connect, replay


class ArtifactBaseline(unittest.TestCase):
    def test_bundle_attestation_and_mount_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'artifact.sqlite')
            db = connect(path)
            self.assertEqual(artifact.run(db, {'event_id': 'bundle', 'op': 'bundle', 'id': 'b',
                                               'service': 'api', 'generation': 1,
                                               'digest': 'sha256:abc', 'signer': 'team'})['decision'], 'bundle_registered')
            self.assertEqual(artifact.run(db, {'event_id': 'attest', 'op': 'attest', 'id': 'a',
                                               'bundle': 'b', 'builder': 'builder-1',
                                               'manifest_digest': 'sha256:manifest', 'at': 1})['decision'], 'attestation_recorded')
            self.assertEqual(artifact.run(db, {'event_id': 'mount', 'op': 'mount', 'id': 'm',
                                               'bundle': 'b', 'attempt': 'attempt-1',
                                               'node': 'node-1'})['decision'], 'mounted')
            self.assertTrue(replay(path)['verified'])
            db.close()
