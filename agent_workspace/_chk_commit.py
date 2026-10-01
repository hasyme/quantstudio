import sys; sys.path.insert(0,'.')
from quantstudio.pipeline.daemon import ResidentCollector
import inspect
src = inspect.getsource(ResidentCollector._run_with_source_streaming)
# 找 commit 相关
for i,l in enumerate(src.splitlines(),1):
    s=l.strip()
    if any(k in s for k in ('commit','rollback','transaction','BEGIN')):
        print('L%d: %s' % (i, s))
print('--- 是否出现 commit:', 'commit' in src)
