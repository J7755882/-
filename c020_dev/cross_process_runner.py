#!/usr/bin/env python3
"""LIPO-OS C020 DEV-only concurrent evidence runner.

Reads LIPO_C020_DEV_DATABASE_URL from environment. Never hard-code or persist the URL.
Runs only against the isolated DEV Neon branch/database designated by the operator.
Requires: pip install "psycopg[binary]>=3.2"

Purpose: close only T01/T02/T03/T11 missing simultaneous independent-client evidence.
This file does NOT authorize PROD binding or migration.
"""
import os, json, time, uuid, multiprocessing as mp
try:
    import psycopg
except ImportError:
    raise SystemExit('Install psycopg first: pip install "psycopg[binary]>=3.2"')
URL=os.environ.get('LIPO_C020_DEV_DATABASE_URL')
if not URL:
    raise SystemExit('Set LIPO_C020_DEV_DATABASE_URL in the process environment; do not save it in this file.')

def q(sql, params=()):
    with psycopg.connect(URL, autocommit=True) as c:
        with c.cursor() as cur:
            cur.execute(sql, params)
            try: return cur.fetchall()
            except psycopg.ProgrammingError: return []

def worker(barrier, fn, args, queue):
    try:
        with psycopg.connect(URL, autocommit=True) as c:
            pid=c.info.backend_pid
            barrier.wait()
            with c.cursor() as cur:
                cur.execute(fn, args)
                row=cur.fetchone()
            queue.put({'pid':pid,'ok':True,'result':row[0] if row else None,'ts':time.time()})
    except Exception as e:
        queue.put({'pid':None,'ok':False,'error':type(e).__name__+': '+str(e),'ts':time.time()})

def race(n, fn, arglists):
    ctx=mp.get_context('spawn')
    barrier=ctx.Barrier(n)
    queue=ctx.Queue()
    ps=[ctx.Process(target=worker,args=(barrier,fn,arglists[i],queue)) for i in range(n)]
    for p in ps: p.start()
    out=[queue.get(timeout=60) for _ in ps]
    for p in ps: p.join(60)
    return out

def register(op,key,h,target,approval=None):
    return q('SELECT lipo_atomic_dev.register_operation(%s,%s,%s,%s,%s)',(op,key,h,target,approval))[0][0]

def main():
    run='DEV_TEST_CONC_'+uuid.uuid4().hex[:10]
    results={}
    target=run+'_T01'
    ops=[]
    for i in range(2):
        op=f'{run}_T01_OP{i}'
        register(op,f'{run}_T01_ID{i}',('%064x'%(i+1))[-64:],target)
        ops.append(op)
    out=race(2,'SELECT lipo_atomic_dev.dev_test_try_reserve(%s,%s,60000,0,200)',[(ops[i],f'{run}_OWN{i}') for i in range(2)])
    results['T01']={'clients':out,'ok_count':sum('OK|' in str(x.get('result')) for x in out)}
    q("UPDATE lipo_atomic_dev.target_reservation SET lease_expires_at=clock_timestamp(),lease_owner=NULL WHERE target_scope=%s",(target,))

    target=run+'_T02'; ops=[]; n=12
    for i in range(n):
        op=f'{run}_T02_OP{i}'
        register(op,f'{run}_T02_ID{i}',('%064x'%(100+i))[-64:],target)
        ops.append(op)
    out=race(n,'SELECT lipo_atomic_dev.dev_test_try_reserve(%s,%s,60000,0,300)',[(ops[i],f'{run}_OWN{i}') for i in range(n)])
    results['T02']={'clients':out,'ok_count':sum('OK|' in str(x.get('result')) for x in out)}
    q("UPDATE lipo_atomic_dev.target_reservation SET lease_expires_at=clock_timestamp(),lease_owner=NULL WHERE target_scope=%s",(target,))

    n=4; ops=[]
    for i in range(n):
        target=f'{run}_T03_TARGET{i}'
        op=f'{run}_T03_OP{i}'
        register(op,f'{run}_T03_ID{i}',('%064x'%(200+i))[-64:],target)
        ops.append((op,target))
    t0=time.time()
    out=race(n,'SELECT lipo_atomic_dev.dev_test_try_reserve(%s,%s,60000,0,800)',[(ops[i][0],f'{run}_OWN{i}') for i in range(n)])
    elapsed=time.time()-t0
    results['T03']={'clients':out,'elapsed_sec':elapsed,'all_ok':all('OK|' in str(x.get('result')) for x in out)}

    scope=run+'_T11_SCOPE'; approval=run+'_T11_APPROVAL'
    q("INSERT INTO lipo_atomic_dev.approval_catalog(approval_id,approval_scope,single_use,status) VALUES(%s,%s,true,'ACTIVE')",(approval,scope))
    ops=[]
    for i in range(2):
        op=f'{run}_T11_OP{i}'
        register(op,f'{run}_T11_ID{i}',('%064x'%(300+i))[-64:],scope,approval)
        ops.append(op)
    out=race(2,'SELECT lipo_atomic_dev.dev_test_try_consume_approval(%s,200)',[(ops[i],) for i in range(2)])
    rows=q('SELECT approval_id,operation_id FROM lipo_atomic_dev.approval_consumption WHERE approval_id=%s',(approval,))
    results['T11']={'clients':out,'consumption_rows':rows,'row_count':len(rows)}

    results['evaluation']={
      'T01_PASS': results['T01']['ok_count']==1,
      'T02_PASS': results['T02']['ok_count']==1,
      'T03_PASS': results['T03']['all_ok'] and results['T03']['elapsed_sec'] < 2.5,
      'T11_PASS': results['T11']['row_count']==1,
    }
    print(json.dumps(results,ensure_ascii=False,indent=2,default=str))

if __name__=='__main__':
    main()
