"""Run discovered unittest cases and record actual successful test IDs."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import unittest

class TraceResult(unittest.TextTestResult):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.success_ids=[]
    def addSuccess(self,test):
        super().addSuccess(test)
        self.success_ids.append(test.id())

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--manifest',type=Path)
    args=p.parse_args()
    suite=unittest.defaultTestLoader.discover(start_dir='examples/tests',pattern='test_*.py')
    result=unittest.TextTestRunner(verbosity=1,resultclass=TraceResult).run(suite)
    if args.manifest:
        args.manifest.write_text(json.dumps({'tests_run':result.testsRun,
            'successful_test_ids':result.success_ids,'failures':len(result.failures),
            'errors':len(result.errors),'skipped':len(result.skipped),
            'skipped_test_ids':[test.id() for test,_reason in result.skipped]},indent=2)+'\n')
    return 0 if result.wasSuccessful() else 1

if __name__=='__main__':raise SystemExit(main())
