"""Regression tests for the release process itself (not just the runtime)."""
from __future__ import annotations
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
import acceptance_gate
from release_gate import scan_distribution

class ReleaseAcceptancePolicyTests(unittest.TestCase):
    def test_issue_register_cannot_silently_omit_permissions(self):
        register=json.loads((ROOT/'quality/issue_register.json').read_text('utf-8'))
        ids={g['id'] for g in register['external_gates']}
        self.assertTrue({'X04','X06'}.issubset(ids))
        mandatory={g['id'] for g in register['external_gates'] if 'portfolio_publication' in g.get('blocking_for',[])}
        self.assertTrue({'X04','X06'}.issubset(mandatory))
        self.assertTrue(all(g['status']!='verified' or bool(g.get('evidence')) for g in register['external_gates']))

    def test_offline_success_never_authorizes_publication_on_its_own(self):
        with tempfile.TemporaryDirectory() as temp:
            archive=Path(temp)/'test.zip'
            with zipfile.ZipFile(archive,'w') as z:
                z.writestr('restaurant-wine-recommendation-reference/README.md','Safe')
            mocked_checks=([{'check':'mock offline checks','passed':True,'detail':''}],
                           {'portfolio_permission_pending':['X04','X06'],
                            'deployment_checks_pending':['X01'],
                            'post_publication_checks_pending':['X03'],
                            'external_not_verified':['X01','X03','X04','X06'],
                            'registered_findings':11,'critical_high_open':[], 'test_count':167})
            with patch.object(acceptance_gate,'scan_distribution',return_value=[]), \
                 patch.object(acceptance_gate,'_checks',return_value=mocked_checks):
                report=acceptance_gate.verify(archive)
            self.assertTrue(report['offline_gate_passed'])
            self.assertFalse(report['publication_ready'])
            self.assertFalse(report['production_deployment_ready'])
            self.assertIn('PERMISSION PENDING',report['decision'])

    def test_release_gate_rejects_secret_in_archive(self):
        with tempfile.TemporaryDirectory() as temp:
            archive=Path(temp)/'secret.zip'
            with zipfile.ZipFile(archive,'w') as z:
                z.writestr('restaurant-wine-recommendation-reference/examples/test.py',
                           "POSTMARK_SERVER_TOKEN" + "=" + repr("abcdefghijklmnopqrstuvwxyz"+"1234567890") + "\n")
            errors=scan_distribution(archive)
            self.assertTrue(any('suspicious credential' in e for e in errors),errors)

    def test_release_gate_rejects_unreviewed_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            archive=Path(temp)/'unsafe.zip'
            with zipfile.ZipFile(archive,'w') as z:
                z.writestr('restaurant-wine-recommendation-reference/.git/credentials','private')
            errors=scan_distribution(archive)
            self.assertTrue(any('Forbidden' in e for e in errors),errors)

if __name__=='__main__':unittest.main()
