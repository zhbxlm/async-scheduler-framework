#!/usr/bin/env python3
"""Memory audit: find potential leak patterns."""
import re
from pathlib import Path

issues = []

for f in sorted(Path("src").rglob("*.py")):
    text = f.read_text()
    lines = text.split('\n')
    
    for i, line in enumerate(lines, 1):
        s = line.strip()
        
        if 'asyncio.create_task' in s or 'asyncio.ensure_future' in s:
            if '=' not in s.split('create_task')[0] if 'create_task' in s else '=' not in s.split('ensure_future')[0]:
                issues.append((f, i, 'UNTRACKED', 'create_task/ensure_future without variable - may leak'))
        
        if re.search(r'self\._\w*cache\w*\s*=\s*\{\}', s):
            issues.append((f, i, 'CACHE', 'In-memory dict cache - check for size limit'))
        
        if re.search(r'self\._\w*(?:queue|buffer|pending|events)\w*\s*=\s*\[\]', s):
            issues.append((f, i, 'UNBOUNDED', 'Unbounded list - may grow indefinitely'))
        
        if 'httpx.AsyncClient' in s or 'httpx.Client' in s:
            if 'limits' not in s and 'max_connections' not in s:
                issues.append((f, i, 'POOL', 'httpx client without connection limits'))

for f, ln, typ, desc in issues:
    print(f"{typ:12s} {str(f):45s}:{ln:4d}  {desc}")
print(f"\nTotal: {len(issues)} potential issues")
