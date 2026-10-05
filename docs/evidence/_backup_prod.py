# -*- coding: utf-8 -*-
import os, shutil, time, datetime
ROOT = os.path.abspath(".")
DB = os.path.join(ROOT, "data", "quantstudio.db")
ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
for ext in ("", ".wal"):
    src = DB + ext
    if not os.path.exists(src):
        print("skip(no)", src)
        continue
    dst = os.path.join(ROOT, "data", "bench", "prod_backup_%s.db%s" % (ts, ext))
    t0 = time.time()
    shutil.copyfile(src, dst)
    print("BACKUP", src, "->", dst, "%.2fGB %.1fs" % (os.path.getsize(dst) / 1e9, time.time() - t0))
print("BACKUP_TS=" + ts)
