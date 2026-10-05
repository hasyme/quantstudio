# -*- coding: utf-8 -*-
'''归档门6 停摆诊断记录：从 data/logs/duckdb_write_stall.jsonl 抽出 stock_daily 生产记录。

输出：docs/evidence/hang2_gate6_stall_jsonl.txt
'''
from __future__ import annotations

import json
import os
import sys

SRC = os.path.join('data', 'logs', 'duckdb_write_stall.jsonl')
OUT = os.path.join('docs', 'evidence', 'hang2_gate6_stall_jsonl.txt')


def main() -> None:
    rs = []
    if os.path.exists(SRC):
        with open(SRC, encoding='utf-8') as fh:
            for line in fh:
                if not line.strip():
                    continue
                r = json.loads(line)
                if r.get('table') == 'stock_daily' and r.get('db_path', '').endswith('quantstudio.db'):
                    rs.append(r)
    with open(OUT, 'w', encoding='utf-8') as fh:
        fh.write('\n'.join(json.dumps(r, ensure_ascii=False, indent=2) for r in rs) + '\n')
    sys.stdout.reconfigure(encoding='utf-8')
    print('records', len(rs), '->', OUT)


if __name__ == '__main__':
    main()
