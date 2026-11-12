"""Conservative offline publication preflight; disclosure approval still mandatory."""
from __future__ import annotations
import pathlib
import html
import re
import sys
import zipfile
import tempfile
import stat
import unicodedata
import struct
import zlib
import xml.etree.ElementTree as ET
from pathlib import PurePosixPath

ROOT=pathlib.Path(__file__).resolve().parents[1]
BAD_EXT={'.db','.sqlite','.sqlite3','.key','.pem','.pfx','.p12','.zip','.log','.bak','.dump'}
BAD_NAMES=re.compile(r'(?:^\.env(?:\.|$)|backup_wines|original_catalog|production_config|\.(?:db|sqlite3?)-(?:wal|shm|journal)$)',re.I)
ALLOW_CSV={'examples/synthetic_wines.csv'}
ALLOW_SQL={'examples/migrations/001_portfolio_v2.sql','examples/migrations/002_validate_tags.sql','examples/migrations/003_legacy_review.sql','examples/migrations/004_required_wine_fields.sql','examples/migrations/005_canonical_tags.sql','examples/migrations/006_review_archive_and_suggestions.sql','examples/migrations/007_required_text_contract.sql','examples/migrations/008_visible_display_text.sql','examples/migrations/009_canonical_postgres_arrays.sql'}
TEXT_EXT={'.py','.md','.yml','.yaml','.json','.txt','.html','.sql','.csv','.svg','.toml','.ini','.cfg','.sh','.xml'}
KEY_BLOCK=re.compile(r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----')
PROVIDER_TOKEN=re.compile(r'''(?ix)
    (?:\bsk-(?:proj-)?[a-z0-9_-]{20,}
      |\bgh[pousr]_[a-z0-9]{25,}
      |\bgithub_pat_[a-z0-9_]{20,}
      |\b(?:AKIA|ASIA)[A-Z0-9]{16}\b
      |\bsk_(?:test|live)_[A-Za-z0-9]{12,}\b
      |(?:postgres|postgresql)://[^\s:@]+:[^\s@]+@)
''')
CREDENTIAL_ASSIGNMENT=re.compile(r'''(?ix)
    \b(?:password|passwd|api[_-]?token|api[_-]?key|client[_-]?secret|
       postmark[_-]?server[_-]?token|aws[_-]?(?:secret[_-]?access[_-]?key|access[_-]?key[_-]?id)|
       secret[_-]?access[_-]?key|stripe[_-]?secret[_-]?key|github[_-]?(?:token|pat|personal[_-]?access[_-]?token))
    \s*[:=]\s*(?:['"])?([a-z0-9!@#$%^&*+_./=-]{12,})
''')
SAFE_PLACEHOLDERS=frozenset(('changeme','replace_me','your_key_here','your_token_here','placeholder','example','dummy','test','fake','fictional'))

SVG_NS = 'http://www.w3.org/2000/svg'
SVG_ELEMENTS = frozenset({'svg','title','desc','defs','g','rect','path','circle','ellipse',
                          'line','polyline','polygon','text','tspan','linearGradient','radialGradient',
                          'stop','marker'})
SVG_ATTRIBUTES = frozenset({'width','height','viewBox','role','aria-labelledby','id',
    'x','y','x1','x2','y1','y2','rx','ry','cx','cy','r','d','fill','stroke',
    'stroke-width','stroke-dasharray','font-size','font-family','font-weight',
    'stop-color','offset','markerWidth','markerHeight','refX','refY','orient',
    'marker-end','marker-start','text-anchor','opacity','transform',
    'stroke-linecap','stroke-linejoin','stroke-opacity','fill-opacity'})
# Only local fragment references are allowed for safe authored markers/gradients.
SVG_COLOR_REFERENCE = re.compile(r'url\(#[A-Za-z_][A-Za-z0-9_.:-]*\)\Z')

def _safe_svg(body: str) -> bool:
    if any(marker in body.lower() for marker in ('<!doctype','<!entity','<?','<![cdata[')):
        return False
    try:
        tree=ET.fromstring(body)
    except (ET.ParseError,ValueError,RecursionError):
        return False
    if tree.tag!='{'+SVG_NS+'}svg':
        return False
    for element in tree.iter():
        if element.tag not in {'{'+SVG_NS+'}'+name for name in SVG_ELEMENTS}:
            return False
        for key,value in element.attrib.items():
            # No xlink:href, namespaced attributes, onload, style or URLs.
            if key not in SVG_ATTRIBUTES or not isinstance(value,str):
                return False
            if len(value)>5000 or any(ord(c)<32 for c in value):
                return False
            lowered=value.lower()
            if 'url(' in lowered and not (key in ('fill','stroke','marker-end','marker-start')
                                         and SVG_COLOR_REFERENCE.fullmatch(value)):
                return False
            if any(word in lowered for word in ('data:','javascript:','https:','http:',
                                               'file:','vbscript:','@import','expression(')):
                return False
    return True

def _decoded_svg_credential(body: str) -> bool:
    """Reject credential patterns hidden by XML references or Unicode escapes.

    The XML parser resolves numeric and named entities before this check. We
    inspect parsed text, tails and attributes; a bounded second HTML-unescape
    pass catches double-encoded references in permitted metadata.
    """
    try:
        tree=ET.fromstring(body)
    except (ET.ParseError,ValueError,RecursionError):
        return True  # Unsafe, unparsable SVG must fail closed.
    # SVG tspan boundaries are NOT text boundaries: visible text can be split
    # into adjacent nodes to evade a raw-source regex. Inspect rendered text in
    # document order, plus attribute values and the whole flattened payload.
    visible=''.join(tree.itertext())
    attributes=' '.join(value for node in tree.iter() for value in node.attrib.values())
    expanded=visible+'\n'+attributes
    if len(expanded)>2_000_000:
        return True
    # Decode until a fixed point, rather than using a fixed small depth. Cap
    # expansion passes and total bytes so hostile nesting cannot consume memory.
    for _ in range(32):
        expanded=unicodedata.normalize('NFKC',expanded)
        if (KEY_BLOCK.search(expanded) or PROVIDER_TOKEN.search(expanded) or
            any(m.group(1).lower() not in SAFE_PLACEHOLDERS for m in CREDENTIAL_ASSIGNMENT.finditer(expanded))):
            return True
        decoded=html.unescape(expanded)
        if decoded==expanded:
            return False
        if len(decoded)>2_000_000:
            return True
        expanded=decoded
    # Deeply nested entity encodings are disallowed even when no credential
    # pattern is currently recognized: the scanner cannot certify their text.
    return True

def scan(root=None)->list[str]:
    base=pathlib.Path(root) if root is not None else ROOT
    problems=[]
    for p in base.rglob('*'):
        rel=p.relative_to(base).as_posix()
        if p.is_symlink():problems.append(f'{rel}: symbolic link');continue
        if any(part in {'.git','__pycache__','.pytest_cache'} for part in p.relative_to(base).parts):
            problems.append(f'{rel}: forbidden build/cache or repository metadata');continue
        if p.is_dir():continue
        if BAD_NAMES.search(p.name) or p.suffix.lower() in BAD_EXT:
            if p.name!='.env.example':problems.append(f'{rel}: forbidden runtime/private file')
        if p.suffix.lower()=='.csv' and rel not in ALLOW_CSV:problems.append(f'{rel}: unapproved CSV')
        if p.suffix.lower()=='.sql' and rel not in ALLOW_SQL:problems.append(f'{rel}: unapproved SQL')
        if p.stat().st_size>2_000_000:problems.append(f'{rel}: oversized file')
        if p.suffix.lower() in TEXT_EXT or p.name in {'LICENSE','.gitignore','.env.example'}:
            try:body=p.read_text(encoding='utf-8')
            except UnicodeDecodeError:
                problems.append(f'{rel}: undecodable text');continue
            if p.suffix.lower()=='.svg':
                if not _safe_svg(body):
                    problems.append(f'{rel}: unsafe SVG markup or embedded content')
                if _decoded_svg_credential(body):
                    problems.append(f'{rel}: decoded SVG metadata contains possible credential')
            if KEY_BLOCK.search(body) or PROVIDER_TOKEN.search(body):problems.append(f'{rel}: possible credential')
            for match in CREDENTIAL_ASSIGNMENT.finditer(body):
                value=match.group(1).lower()
                if value in SAFE_PLACEHOLDERS or value.startswith(('replace-with-','replace-through-')):continue
                if value.startswith(('os.getenv','env.get','config.','self.')):continue
                problems.append(f'{rel}: suspicious credential assignment');break
        elif p.suffix.lower() in {'.png','.jpg','.jpeg','.webp'}:
            # Binary containers may hold EXIF/XMP/text chunks/hidden payloads.
            # No raster format is safe to pass without a separately validated
            # byte-level sanitizer. SVGs remain supported and text-scanned.
            problems.append(f'{rel}: raster media not allowed in release (metadata confidentiality risk)')
        else:
            problems.append(f'{rel}: unapproved file format')
    return problems

def _validate_zip_envelope(archive: pathlib.Path, zf: zipfile.ZipFile, *, expected_git_commit: str|None=None) -> list[str]:
    """Check that every physical archive byte belongs to a canonical ZIP record.

    Python's ZipFile deliberately tolerates appended data and arbitrary comments.
    This release gate must not: those bytes would bypass file-content scanning.
    Git-archive commit comments are accepted ONLY when authenticated against
    an independently supplied commit ID. A hex-shaped comment alone is not
    provenance. The only permitted extra field is Git's numeric UT timestamp.
    """
    def allowed_extra(data: bytes) -> bool:
        return not data or (len(data)==9 and data[:5]==b'UT\x05\x00\x01')

    size=archive.stat().st_size  # caller enforces a 20 MB compressed size ceiling
    if size<22:
        return ['Invalid ZIP end-of-central-directory record']
    with archive.open('rb') as source:
        source.seek(max(0,size-(65535+22)))
        ending=source.read()
        candidate=ending.rfind(b'PK\x05\x06')
        if candidate<0 or len(ending)-candidate<22:
            return ['ZIP end-of-central-directory record missing']
        eocd=ending[candidate:candidate+22]
        (signature,disk,central_disk,entries_disk,entries_total,central_size,
         central_offset,comment_length)=struct.unpack('<4s4H2IH',eocd)
        comment=ending[candidate+22:]
        if len(comment)!=comment_length:
            return ['Unsafe ZIP comment or trailing content']
        if expected_git_commit is not None:
            if (not isinstance(expected_git_commit,str) or
                not re.fullmatch(r'[0-9a-f]{40}(?:[0-9a-f]{24})?',expected_git_commit) or
                comment!=expected_git_commit.encode('ascii')):
                return ['ZIP Git commit comment does not match externally supplied commit']
        elif comment:
            return ['Unauthenticated ZIP comment: expected Git commit ID required']
        if (disk or central_disk or entries_disk!=entries_total or
            entries_total!=len(zf.infolist()) or
            central_offset!=zf.start_dir or
            central_offset+central_size!=size-22-comment_length):
            return ['Noncanonical ZIP central directory or unaccounted bytes']
        # Parse central records, including their variable-length metadata,
        # rather than relying on their decoded ZipInfo representation alone.
        source.seek(central_offset)
        offset=central_offset
        for member in zf.infolist():
            if member.is_dir() and (member.file_size or member.compress_size or member.compress_type != zipfile.ZIP_STORED):
                return ['Nonempty or compressed ZIP directory payload']
            header=source.read(46)
            if len(header)!=46 or header[:4]!=b'PK\x01\x02':
                return ['Noncanonical ZIP central file record']
            fields=struct.unpack('<I6H3I5H2I',header)
            filename_length,extra_length,file_comment_length=fields[10:13]
            filename=source.read(filename_length)
            extra=source.read(extra_length)
            member_comment=source.read(file_comment_length)
            if (filename.decode('utf-8',errors='replace')!=member.filename or
                member_comment or not allowed_extra(extra)):
                return ['Unsafe ZIP member comment or extra metadata']
            offset+=46+filename_length+extra_length+file_comment_length
        if offset!=central_offset+central_size:
            return ['Unaccounted ZIP central-directory data']
        # Verify the local headers occupy all bytes before the central directory.
        # No opaque prefixes, interstitial payloads or data descriptors are
        # allowed to conceal material outside the extracted files.
        offset=0
        for member in sorted(zf.infolist(),key=lambda m:m.header_offset):
            if member.header_offset!=offset:
                return ['Unaccounted bytes before or between ZIP members']
            source.seek(offset)
            header=source.read(30)
            if len(header)!=30 or header[:4]!=b'PK\x03\x04':
                return ['Noncanonical ZIP local file header']
            fields=struct.unpack('<I5H3I2H',header)
            flags=fields[2]
            file_name_len,extra_len=fields[-2:]
            local_name=source.read(file_name_len)
            extra=source.read(extra_len)
            if (flags&0x08 or flags!=member.flag_bits or
                fields[3]!=member.compress_type or fields[6]!=member.CRC or
                fields[7]!=member.compress_size or fields[8]!=member.file_size or
                local_name.decode('utf-8',errors='replace')!=member.filename or
                not allowed_extra(extra)):
                return ['Unsafe ZIP local metadata or data descriptor']
            if member.compress_size>2_000_000 or member.file_size>2_000_000:
                return ['Oversized ZIP compressed member']
            if member.compress_type==zipfile.ZIP_DEFLATED:
                # A deflate stream may have trailing bytes *inside* the stated
                # compressed size. ZipFile.open() ignores them; do not.
                reader=zlib.decompressobj(-15)
                remaining=member.compress_size
                inflated=0
                try:
                    while remaining:
                        chunk=source.read(min(65536,remaining))
                        if not chunk:return ['Incomplete compressed ZIP member']
                        remaining-=len(chunk)
                        output=reader.decompress(chunk,max(1,2_000_001-inflated))
                        inflated+=len(output)
                        if inflated>2_000_000 or reader.unconsumed_tail or reader.unused_data:
                            return ['Hidden or oversized compressed ZIP member payload']
                except zlib.error:
                    return ['Invalid ZIP deflate member']
                if not reader.eof or inflated!=member.file_size:
                    return ['Incomplete ZIP deflate member']
            elif member.compress_type!=zipfile.ZIP_STORED:
                return ['Unsupported ZIP compression method']
            offset+=30+file_name_len+extra_len+member.compress_size
        if offset!=central_offset:
            return ['Unaccounted bytes between ZIP members and central directory']
    return []

def scan_distribution(archive: str|pathlib.Path, *, expected_git_commit: str|None=None)->list[str]:
    """Preflight every raw ZIP entry before extracting one-to-one safe members.

    Reject path aliases, case/Unicode-normalization collisions, hidden content,
    encrypted/unbounded members and oversized archives. Scanning a normalized
    extracted tree alone is unsafe: a ZIP may contain multiple original entries
    mapping to the same destination, including ones holding credentials.
    """
    archive=pathlib.Path(archive)
    if not archive.is_file() or archive.stat().st_size>20_000_000:
        return ['Missing or oversized compressed release archive']
    problems=[]
    with zipfile.ZipFile(archive) as zf:
        envelope_errors=_validate_zip_envelope(archive,zf,expected_git_commit=expected_git_commit)
        if envelope_errors:
            return envelope_errors
        members=zf.infolist()
        if not members or len(members)>300:
            return ['Archive too large or empty']
        seen=set()
        total=0
        # Reject every ambiguous path BEFORE extraction. No prior entry gets
        # overwritten by a later alias; the whole archive must be unambiguous.
        for item in members:
            name=item.filename
            path=PurePosixPath(name)
            if item.is_dir() and (item.file_size or item.compress_size or item.compress_type != zipfile.ZIP_STORED):
                problems.append(f'Uninspected ZIP directory payload: {name!r}')
                continue
            if item.is_dir() and name=='restaurant-wine-recommendation-reference/':
                continue
            canonical=path.as_posix()+('/' if item.is_dir() else '')
            if (not name or name!=canonical or path.is_absolute() or
                len(path.parts)<2 or path.parts[0]!='restaurant-wine-recommendation-reference' or
                '..' in path.parts or '\\' in name or ':' in name or any(ord(ch)<32 for ch in name) or
                unicodedata.normalize('NFC',name)!=name):
                problems.append(f'Unsafe or noncanonical archive path: {name!r}')
                continue
            normalized=unicodedata.normalize('NFC',canonical).casefold().rstrip('/')
            if normalized in seen:
                problems.append(f'Duplicate normalized archive destination: {name!r}')
                continue
            seen.add(normalized)
            if any(part in {'.git','__pycache__','.pytest_cache'} for part in path.parts):
                problems.append(f'Forbidden release archive member: {name!r}')
            mode=item.external_attr>>16
            if stat.S_IFMT(mode)==stat.S_IFLNK:
                problems.append(f'Archive symlink: {name!r}')
            if item.flag_bits & 0x1:
                problems.append(f'Encrypted archive member: {name!r}')
            if item.is_dir():continue
            total+=item.file_size
            if item.file_size>2_000_000 or total>10_000_000:
                problems.append(f'Archive size limit exceeded: {name!r}')
        if problems:
            return problems
        with tempfile.TemporaryDirectory(prefix='palate-release-') as tmp:
            staging=pathlib.Path(tmp)
            for item in members:
                if item.is_dir():continue
                path=PurePosixPath(item.filename)
                target=staging.joinpath(*path.parts)
                target.parent.mkdir(parents=True,exist_ok=True)
                with zf.open(item) as source,target.open('wb') as dest:
                    # Bounded streaming, not unbounded source.read().
                    remaining=item.file_size
                    while remaining:
                        chunk=source.read(min(65536,remaining))
                        if not chunk:raise ValueError('Incomplete compressed member')
                        dest.write(chunk)
                        remaining-=len(chunk)
            repository=staging/'restaurant-wine-recommendation-reference'
            if not repository.is_dir():problems.append('Repository root missing')
            else:problems.extend(scan(repository))
    return problems

if __name__=='__main__':
    issues=scan_distribution(sys.argv[2]) if len(sys.argv)==3 and sys.argv[1]=='--archive' else scan()
    for issue in issues:print('FAIL',issue)
    if issues:sys.exit(1)
    print('Preflight passed; staging verification and disclosure approval still required.')
