"""Verify the exact release archive and produce an explicit offline/release decision.

Usage: python -B scripts/acceptance_gate.py --archive /path/to/candidate.zip
No network requests, credentials, or publication side effects are performed.
Exit 0 means OFFLINE gate passed, not approved for publication.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
import zipfile

from release_gate import scan_distribution

TOP = 'restaurant-wine-recommendation-reference'
REQUIRED = (
    'README.md','LICENSE','ATTRIBUTION.md','examples/README.md',
    'examples/pipeline.py','examples/webhook.py','examples/webapp.py',
    'examples/delivery.py','examples/catalog_store.py','examples/postgres_catalog.py',
    'examples/providers.py','examples/migrations/001_portfolio_v2.sql',
    'scripts/release_gate.py','quality/issue_register.json',
    '.github/workflows/verify.yml',
)
LINK = re.compile(r'(?<!!)\[[^\]]*\]\(([^)]+)\)')
COVERAGE_FLOOR = 84
# Externally reviewed baseline IDs anchored to an independent canonical digest.
# The checked-in register may APPEND findings but may not erase known ones.
BASELINE_SHA256 = "262b6463b50c17ef579be4463d8b1e96c38f531f631adb1c6e03656f18bb258a"
REQUIRED_EXTERNAL_APPROVALS = frozenset({'X04','X06'})
EXECUTION_MAP_SHA256 = "03de40b9b6781b9aabdd0c8bde024e01838824803256235e589baca02bb47679"
EXECUTION_EXTENSION_V18_SHA256 = "a679f047878d695213d4494513043555a13073e7a2fa12641f3482e994b76e13"


def check_historical_issue_inventory(ledger:dict,baseline_bytes:bytes)->list[str]:
    """Check the reviewed history anchor and immutable identifiers.

    Adding findings is allowed; deleting/revising an independently audited
    finding, its regression pointer, or an external gate is not.
    """
    if hashlib.sha256(baseline_bytes).hexdigest()!=BASELINE_SHA256:
        return ['History anchor digest differs from the reviewed baseline']
    try:baseline=json.loads(baseline_bytes)
    except (ValueError,UnicodeError):return ['Invalid history anchor JSON']
    errors=[]
    checks=(('findings','immutable_findings',('severity','regression')),
            ('release_invariants','immutable_invariants',('name',)),
            ('external_gates','immutable_external_gates',('name',)))
    for section,anchor,fields in checks:
        original=baseline.get(anchor)
        actual=ledger.get(section)
        if not isinstance(original,dict) or not isinstance(actual,list):
            errors.append('Missing historical section '+section)
            continue
        current={row.get('id'):row for row in actual if isinstance(row,dict)}
        for ident,values in original.items():
            row=current.get(ident)
            for field in fields:
                expected=values.get(field) if isinstance(values,dict) else values
                if row is None or row.get(field)!=expected:
                    errors.append('Historical record removed or changed: '+ident+':'+field)
    return errors


def regression_evidence_errors(items:list[dict],successful_test_ids:list[str],
                               pinned_test_ids:dict[str,str]|None=None)->list[str]:
    """Enforce executed qualified unittest.TestCase IDs, not uncollected names."""
    if pinned_test_ids is None:
        executed={test.rsplit('.',1)[-1] for test in successful_test_ids if isinstance(test,str) and '.test_' in test}
        return [f'{item.get("id","?")}: regression not successfully executed: {item.get("regression")}'
                for item in items if not isinstance(item,dict) or item.get('regression') not in executed]
    executed=set(successful_test_ids)
    problems=[]
    for item in items:
        if not isinstance(item,dict):
            problems.append('Malformed issue or invariant evidence record')
            continue
        full_id=pinned_test_ids.get(item.get('id'))
        if not isinstance(full_id,str) or full_id not in executed or full_id.rsplit('.',1)[-1]!=item.get('regression'):
            problems.append(f'{item.get("id","?")}: pinned regression not successfully executed: {item.get("regression")}')
    return problems


def valid_test_manifest(manifest:dict) -> bool:
    """Account for passing, skipped, and failed unittest cases separately.

    Skipped tests may be legitimately optional on a specific runtime. They
    are never counted as successfully executed regression evidence.
    """
    count=manifest.get('tests_run')
    passed=manifest.get('successful_test_ids')
    skipped=manifest.get('skipped_test_ids')
    if (type(count) is not int or count<171 or not isinstance(passed,list)
        or not isinstance(skipped,list) or
        type(manifest.get('skipped')) is not int or manifest['skipped']!=len(skipped)
        or manifest.get('failures')!=0 or manifest.get('errors')!=0):
        return False
    all_ids=passed+skipped
    return (len(all_ids)==count and len(set(all_ids))==count
        and all(isinstance(t,str) and '.test_' in t for t in all_ids))


def _checks(root: Path) -> tuple[list[dict], dict]:
    checks: list[dict] = []
    def record(name: str, ok: bool, detail=''):
        checks.append({'check':name,'passed':bool(ok),'detail':str(detail)})

    for name in REQUIRED:
        record('required:'+name, (root/name).is_file())
    source_files = sorted(root.rglob('*.py'))
    syntax_errors=[]
    for f in source_files:
        try:ast.parse(f.read_text('utf-8'),filename=str(f.relative_to(root)))
        except (SyntaxError, UnicodeError, ValueError) as ex:
            syntax_errors.append(f'{f.relative_to(root)}: {ex}')
    record('Python AST syntax',not syntax_errors,'; '.join(syntax_errors))

    other_errors=[]
    for f in sorted(root.rglob('*.json')):
        try:json.loads(f.read_text('utf-8'))
        except (ValueError,UnicodeError) as ex:other_errors.append(f'{f.relative_to(root)}: {ex}')
    for f in sorted(root.rglob('*.svg')):
        try:ET.parse(f)
        except (ET.ParseError,OSError) as ex:other_errors.append(f'{f.relative_to(root)}: {ex}')
    record('JSON and SVG parse',not other_errors,'; '.join(other_errors))

    broken=[];links=0
    for f in sorted(root.rglob('*.md')):
        for match in LINK.finditer(f.read_text('utf-8')):
            link=match.group(1).strip().split()[0]
            if link.startswith(('http:','https:','mailto:','data:','#')):continue
            link=link.split('#')[0].split('?')[0]
            if not link:continue
            links+=1
            dest=(f.parent/link).resolve()
            if not dest.is_relative_to(root.resolve()) or not dest.exists():
                broken.append(f'{f.relative_to(root)}: {link}')
    record('Internal Markdown links',not broken,f'{links} checked; broken: {broken}')

    ledger_path=root/'quality/issue_register.json'
    ledger={}
    if ledger_path.is_file():
        try:ledger=json.loads(ledger_path.read_text('utf-8'))
        except ValueError:pass
    issues=ledger.get('findings',[])
    invariants=ledger.get('release_invariants',[])
    external=ledger.get('external_gates',[])
    structure_ok=all(isinstance(rows,list) and all(isinstance(item,dict) for item in rows)
                     for rows in (issues,invariants,external))
    record('Issue register section shapes',structure_ok)
    if not isinstance(issues,list):issues=[]
    if not isinstance(invariants,list):invariants=[]
    if not isinstance(external,list):external=[]
    baseline_path=root/'quality/issue_history_baseline.json'
    try:
        baseline_bytes=baseline_path.read_bytes()
        inventory_errors=check_historical_issue_inventory(ledger,baseline_bytes)
    except OSError as error:
        inventory_errors=[str(error)]
    record('Pinned issue-history baseline and immutable audited records',not inventory_errors,inventory_errors)
    duplicate_ids=[];ids=set()
    for issue in issues:
        ident=issue.get('id') if isinstance(issue,dict) else None
        if not isinstance(ident,str) or ident in ids:duplicate_ids.append(ident)
        ids.add(ident)
    for item in invariants:
        ident=item.get('id') if isinstance(item,dict) else None
        if not isinstance(ident,str) or ident in ids:duplicate_ids.append(ident)
        ids.add(ident)
    record('Unique issue and invariant IDs',not duplicate_ids and bool(issues) and bool(invariants),duplicate_ids)
    malformed=[]
    valid_states={'fixed_offline','open','risk_accepted','deferred'}
    for item in issues:
        if not isinstance(item,dict) or item.get('severity') not in ('critical','high','medium','low') or item.get('state') not in valid_states:
            malformed.append(str(item))
        elif item['state'] in ('risk_accepted','deferred') and not item.get('reason'):
            malformed.append(item['id']+': accepted/deferred issue lacks recorded justification')
        elif item['severity']=='critical' and item['state']!='fixed_offline':
            malformed.append(item['id']+': critical defects cannot be waived')
    record('Every issue has explicit disposition',not malformed,malformed)
    open_high=[item['id'] for item in issues if isinstance(item,dict) and item.get('severity') in ('critical','high') and item.get('state')!='fixed_offline']
    record('No known open critical/high source issue',not open_high,open_high)

    # The test runner records actual successful unittest.TestCase IDs, not AST
    # names that could sit in an uncollected module or a skipped method.
    with tempfile.TemporaryDirectory(prefix='palate-tests-') as run_dir:
        manifest_path=Path(run_dir)/'executed.json'
        run=subprocess.run([sys.executable,'-B','scripts/run_tests.py','--manifest',str(manifest_path)],
            cwd=root,env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'},
            capture_output=True,text=True,timeout=120)
        output=run.stdout+'\n'+run.stderr
        try:
            manifest=json.loads(manifest_path.read_text('utf-8'))
        except (OSError,ValueError):
            manifest={}
        executed=manifest.get('successful_test_ids',[])
        count=manifest.get('tests_run',0)
    valid_manifest=valid_test_manifest(manifest)
    record('Full offline regression suite',run.returncode==0 and valid_manifest,
           f'{count} executed tests; return code {run.returncode}; {output[-400:].strip()}')
    try:
        evidence_bytes=(root/'quality/regression_execution_map.json').read_bytes()
        if hashlib.sha256(evidence_bytes).hexdigest()!=EXECUTION_MAP_SHA256:
            raise ValueError('Pinned executed-test mapping was rewritten')
        execution_map=json.loads(evidence_bytes)['test_ids']
        if not isinstance(execution_map,dict):raise ValueError('Invalid executed-test mapping')
    except (OSError,ValueError,UnicodeError,KeyError,TypeError) as error:
        record('Pinned qualified regression evidence mapping',False,str(error))
        execution_map={}
    else:
        record('Pinned qualified regression evidence mapping',True,len(execution_map))
    # The original reviewed execution map remains byte-for-byte immutable.
    # A separately pinned extension records only newly introduced v18 tests.
    try:
        extension_bytes=(root/'quality/regression_execution_extension_v18.json').read_bytes()
        if hashlib.sha256(extension_bytes).hexdigest()!=EXECUTION_EXTENSION_V18_SHA256:
            raise ValueError('Pinned v18 test evidence extension was rewritten')
        extension=json.loads(extension_bytes)
        additions=extension['test_ids']
        if extension.get('schema_version')!=1 or not isinstance(additions,dict):
            raise ValueError('Malformed v18 test evidence extension')
        if set(additions)!=set(('A17-01','A17-02','A17-03','A17-04','A17-05','A17-06','A17-07')):
            raise ValueError('Missing or unexpected v18 regression evidence IDs')
        if any(k in execution_map for k in additions):
            raise ValueError('v18 evidence overwrites a historical test pin')
        execution_map={**execution_map,**additions}
        record('Pinned qualified v18 regression extension',True,len(additions))
    except (OSError,ValueError,KeyError,TypeError) as error:
        record('Pinned qualified v18 regression extension',False,str(error))
    bad_evidence=regression_evidence_errors([*issues,*invariants],executed,execution_map)
    record('Findings have successfully EXECUTED regression tests',not bad_evidence and valid_manifest,bad_evidence)

    # Coverage is an independent *measurement*, not a substitute for tests.
    # Requires requirements-quality.txt, so absence is stated, not hidden.
    try:
        installed=subprocess.run([sys.executable,'-m','coverage','--version'],capture_output=True,text=True,timeout=10)
    except (OSError,subprocess.TimeoutExpired):installed=None
    if installed is not None and installed.returncode==0:
        with tempfile.TemporaryDirectory(prefix='palate-coverage-') as tmp:
            env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1','COVERAGE_FILE':str(Path(tmp)/'.coverage')}
            cov=subprocess.run([sys.executable,'-m','coverage','run','--branch','--source=examples',
                    '--omit=*/tests/*','-m','unittest','discover','-s','examples/tests','-q'],
                    cwd=root,env=env,capture_output=True,text=True,timeout=120)
            rep=subprocess.run([sys.executable,'-m','coverage','report',f'--fail-under={COVERAGE_FLOOR}'],
                    cwd=root,env=env,capture_output=True,text=True,timeout=30)
            total=re.search(r'^TOTAL\s+.*?(\d+)%\s*$',rep.stdout,re.MULTILINE)
            record('Source branch coverage',cov.returncode==0 and rep.returncode==0,
                   f'{total.group(1) if total else "unknown"}% (floor {COVERAGE_FLOOR}%); '
                   f'{rep.stderr[-160:].strip()}')
            # Operational entrypoints and data-integrity code need meaningful
            # direct test execution, even when broad application coverage passes.
            essential=('maintenance.py','postgres_cli.py','worker.py','wsgi.py','postgres_catalog.py')
            per_module={}
            for line in rep.stdout.splitlines():
                found=re.match(r'^examples/(\S+\.py)\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)%',line.strip())
                if found:
                    per_module[found.group(1)]=int(found.group(2))
            poor={name:per_module.get(name,'missing') for name in essential
                  if per_module.get(name,0)<70}
            record('Risk-critical entrypoint and PostgreSQL coverage',not poor and cov.returncode==0,
                   {'minimum_pct':70,'observed':{name:per_module.get(name) for name in essential},
                    'below_minimum':poor})
    else:
        record('Source branch coverage',False,
               'Install pinned requirements-quality.txt; coverage required for offline release certification')

    evidence_missing=[]
    seen_gate_ids=set()
    for gate in external:
        if (not isinstance(gate,dict) or gate.get('status') not in ('verified','not_verified')
            or not isinstance(gate.get('blocking_for'),list)
            or not set(gate['blocking_for']).issubset({'portfolio_publication','production_deployment','post_publication_ci'})
            or not gate.get('blocking_for') or gate.get('id') in seen_gate_ids):
            evidence_missing.append(str(gate));continue
        seen_gate_ids.add(gate['id'])
        if gate['status']=='verified' and not gate.get('evidence'):
            evidence_missing.append(gate.get('id','unknown'))
    permissions={g.get('id') for g in external if isinstance(g,dict) and 'portfolio_publication' in g.get('blocking_for',[])}
    if not {'X04','X06'}.issubset(permissions):evidence_missing.append('Missing disclosure or explicit publication-approval gate')
    record('External gate evidence schema',not evidence_missing and len(external)>=6,evidence_missing)
    open_external=[g.get('id') for g in external if isinstance(g,dict) and g.get('status')!='verified']
    # Never treat in-repository JSON as a trusted authorization or consent channel.
    portfolio_pending=sorted(REQUIRED_EXTERNAL_APPROVALS)
    deployment_pending=[g.get('id') for g in external if isinstance(g,dict) and 'production_deployment' in g.get('blocking_for',[]) and g.get('status')!='verified']
    post_publish_pending=[g.get('id') for g in external if isinstance(g,dict) and 'post_publication_ci' in g.get('blocking_for',[]) and g.get('status')!='verified']
    return checks,{'registered_findings':len(issues),'critical_high_open':open_high,
                   'external_not_verified':open_external,'portfolio_permission_pending':portfolio_pending,
                   'deployment_checks_pending':deployment_pending,'post_publication_checks_pending':post_publish_pending,
                   'test_count':count}


def verify(archive:Path,*,expected_sha256:str|None=None,expected_git_commit:str|None=None)->dict:
    # Hash the exact artifact with bounded memory; reject before reading large
    # or non-regular inputs. The archive scanner enforces member size limits.
    if not archive.is_file() or archive.stat().st_size>20_000_000:
        raise ValueError('Release archive exceeds maximum compressed size')
    hasher=hashlib.sha256()
    with archive.open('rb') as source:
        for block in iter(lambda: source.read(1024*1024),b''):
            hasher.update(block)
    digest=hasher.hexdigest()
    report={'artifact':archive.name,'archive_sha256':digest,'offline_gate_passed':False,
            'release_candidate_verified':False,'artifact_identity_verified':False,
            'publication_ready':False,'portfolio_publication_ready':False,
            'production_deployment_ready':False,'checks':[]}
    # A hash generated from the archive is metadata, NOT an independent pin.
    # The release owner must supply the expected digest from outside the ZIP.
    if expected_sha256 is not None:
        pinned=(isinstance(expected_sha256,str) and
                re.fullmatch('[0-9a-f]{64}',expected_sha256) is not None and
                digest==expected_sha256)
        report['artifact_identity_verified']=bool(pinned)
        report['checks'].append({'check':'External expected SHA-256 matches exact archive',
                                'passed':bool(pinned),'detail':digest if pinned else 'Expected digest mismatch'})
        if not pinned:
            report['decision']='BLOCKED: expected archive digest mismatch'
            return report
    try: scan_errors=scan_distribution(archive,expected_git_commit=expected_git_commit)
    except (OSError,ValueError,zipfile.BadZipFile,RuntimeError) as ex:scan_errors=[str(ex)]
    report['checks'].append({'check':'Exact ZIP confidentiality and path scan','passed':not scan_errors,'detail':scan_errors})
    if scan_errors:
        report['decision']='BLOCKED: unsafe artifact'
        return report
    with tempfile.TemporaryDirectory(prefix='palate-release-gate-') as tmp:
        with zipfile.ZipFile(archive) as zf:zf.extractall(tmp)
        root=Path(tmp)/TOP
        checks,metadata=_checks(root)
    report.update(metadata)
    report['checks'].extend(checks)
    report['offline_gate_passed']=all(c['passed'] for c in report['checks'])
    # These flags MUST never be inferred from a self-reported, editable ZIP.
    # Publication requires out-of-band human authorization for an exact digest.
    report['portfolio_publication_ready']=False
    report['production_deployment_ready']=False
    report['publication_ready']=False
    report['release_candidate_verified']=report['offline_gate_passed'] and report['artifact_identity_verified']
    report['decision']=('OFFLINE VERIFIED / EXTERNALLY PINNED / PUBLICATION APPROVAL PENDING — do not publish'
                        if report['release_candidate_verified'] else
                        'OFFLINE CHECKS PASSED / ARCHIVE NOT PINNED / PUBLICATION PERMISSION PENDING — do not publish'
                        if report['offline_gate_passed'] else
                        'OFFLINE GATE FAILED — do not publish')
    return report


def main()->int:
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--archive',type=Path,required=True)
    p.add_argument('--expected-sha256',help='Trusted digest obtained independently of the candidate ZIP')
    p.add_argument('--expected-git-commit',help='Authenticated source commit for git-archive ZIP comment; requires exact match')
    p.add_argument('--output',type=Path)
    args=p.parse_args()
    try:report=verify(args.archive,expected_sha256=args.expected_sha256,expected_git_commit=args.expected_git_commit)
    except (OSError,subprocess.TimeoutExpired,ValueError,zipfile.BadZipFile) as ex:
        report={'artifact':str(args.archive),'offline_gate_passed':False,'publication_ready':False,'decision':'verification error','error':str(ex)}
    body=json.dumps(report,sort_keys=True,indent=2)+'\n'
    if args.output:args.output.write_text(body,encoding='utf-8')
    print(body)
    return 0 if report['offline_gate_passed'] else 1

if __name__=='__main__':raise SystemExit(main())
