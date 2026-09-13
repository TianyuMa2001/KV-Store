"""Five real JVM nodes, deterministic HTTP workload, repeated fault matrix.

Python standard library only. Default is a one-factor-at-a-time matrix;
--full runs the complete Cartesian product. No existing processes are touched.
"""
import argparse
import concurrent.futures
import csv
import hashlib
import http.client
import json
import os
import platform
import random
import socket
import statistics
import subprocess
import threading
import time
from pathlib import Path


def request(port, method, path, body=None, timeout=4):
    connection=http.client.HTTPConnection('127.0.0.1',port,timeout=timeout)
    try:
        connection.request(method,path,json.dumps(body) if body is not None else None,
                           {'Content-Type':'application/json'})
        response=connection.getresponse(); content=response.read()
        return response.status, json.loads(content) if content else {}
    except (OSError,ValueError,http.client.HTTPException):
        return 0,{}
    finally:
        connection.close()


class Cluster:
    def __init__(self,args,case,output,case_index=0):
        self.args=args;self.case=case;self.output=output;self.processes=[];self.logs=[]
        # Allocate a disjoint five-port block per case so a failed/slow JVM
        # shutdown cannot collide with the next matrix case.
        self.ports=list(range(args.port + case_index * 10,args.port + case_index * 10 + 5))
    def __enter__(self):
        # Refuse to share a port with an existing application.
        for port in self.ports:
            with socket.socket() as check:
                check.settimeout(.2)
                if check.connect_ex(('127.0.0.1',port)) == 0:
                    raise RuntimeError(f'Port {port} already has a listener')
        try:
            for i,port in enumerate(self.ports):
                log=(self.output/f'node{i+1}.log').open('w',encoding='utf-8');self.logs.append(log)
                env=os.environ.copy()
                env.update(ROLE='leader' if i==0 else 'follower',
                    FOLLOWER_URLS=','.join(f'http://127.0.0.1:{p}' for p in self.ports if p!=port),
                    WRITE_QUORUM_SIZE=str(self.case['w'] if i==0 else 1),READ_QUORUM_SIZE=str(self.case['r']),
                    QUORUM_TIMEOUT_MS=str(self.args.timeout_ms),
                    REPLICATION_DELAY_MS=str(self.args.timeout_ms*2 if self.case['fault']=='slow' and i==1 else self.case['delay']),
                    WRITE_DELAY_MS=str(self.case.get('write_delay',0)),READ_DELAY_MS=str(self.case.get('read_delay',0)))
                proc=subprocess.Popen([self.args.java,'-Xms64m','-Xmx192m','-XX:ActiveProcessorCount=2',
                    '-jar',str(self.args.jar.resolve()),f'--server.port={port}', '--server.address=127.0.0.1'],
                    stdout=log,stderr=subprocess.STDOUT,env=env,
                    creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
                self.processes.append(proc)
            deadline=time.monotonic()+90
            for port,proc in zip(self.ports,self.processes):
                while request(port,'GET','/local_read/ready',timeout=.5)[0] not in (200,404):
                    if proc.poll() is not None or time.monotonic()>deadline:
                        raise RuntimeError(f'Node startup failed; see {self.output}')
                    time.sleep(.1)
            if self.case['fault']=='unreachable':
                self.processes[1].terminate();self.processes[1].wait(timeout=10)
            return self
        except BaseException:
            self.__exit__(None,None,None);raise
    def __exit__(self,*unused):
        for proc in self.processes:
            if proc.poll() is None: proc.terminate()
        for proc in self.processes:
            try:proc.wait(timeout=10)
            except subprocess.TimeoutExpired:proc.kill();proc.wait()
        for log in self.logs:log.close()


def workload(cluster,args,case,repeat):
    lock=threading.Lock();known={};raw=[]
    rng=random.Random(args.seed+repeat)
    prefix=f'run{repeat}-'
    # Initialize all hot keys; failed quorums never enter the acknowledged map.
    leader_port=cluster.ports[0]
    for key in range(args.keys):
        name=prefix+str(key);status,body=request(leader_port,'PUT','/kv',{'key':name,'value':'initial'})
        if status==201:
            known[name]=body['version']
    def run_phase(count,phase):
        choices=[(i,rng.randrange(args.keys),rng.random()<case['write_ratio'],rng.randrange(5)) for i in range(count)]
        def operation(choice):
            i,key,write,node=choice;name=prefix+str(key)
            with lock: observed=known.get(name)
            started=time.perf_counter()
            if write:
                status,body=request(leader_port,'PUT','/kv',{'key':name,'value':f'{phase}-{i}'})
                if status==201:
                    with lock: known[name]=max(known.get(name,0),body['version'])
                success=status==201
            else:
                path='/kv/' if args.read_mode=='quorum' else '/local_read/'
                status,body=request(cluster.ports[node],'GET',path+name)
                success=status in (200,404)
            elapsed=(time.perf_counter()-started)*1000
            eligible=not write and observed is not None and success
            stale=eligible and (status==404 or body.get('version',0)<observed)
            return {'phase':phase,'request':i,'write':write,'node':0 if write else node,'key':name,
                    'status':status,'success':success,'latency_ms':elapsed,'expected_at_start':observed,
                    'version':body.get('version'),'stale_eligible':eligible,'stale':stale}
        start=time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as pool:
            records=list(pool.map(operation,choices))
        return records,time.perf_counter()-start
    warm,_=run_phase(args.warmup,'warmup')
    raw,seconds=run_phase(args.requests,'measurement')
    lat=sorted(row['latency_ms'] for row in raw)
    quantile=lambda p:lat[min(len(lat)-1,max(0,int(__import__('math').ceil(len(lat)*p))-1))]
    success=sum(row['success'] for row in raw);eligible=sum(row['stale_eligible'] for row in raw)
    summary=dict(case,repeat=repeat,requests=len(raw),seconds=seconds,tps=len(raw)/seconds,
        successful_tps=success/seconds,success_rate=success/len(raw),mean_ms=statistics.mean(lat),
        p95_ms=quantile(.95),p99_ms=quantile(.99),stale=sum(row['stale'] for row in raw),stale_eligible=eligible,
        stale_rate=sum(row['stale'] for row in raw)/eligible if eligible else None)
    return summary,warm+raw


def main():
    root=Path(__file__).resolve().parent
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--jar',type=Path,default=root/'node/target/node-0.0.1-SNAPSHOT.jar')
    parser.add_argument('--output',type=Path,default=root/'evidence/benchmark')
    parser.add_argument('--java',default='java');parser.add_argument('--port',type=int,default=18080)
    parser.add_argument('--threads',type=int,default=16);parser.add_argument('--keys',type=int,default=20)
    parser.add_argument('--requests',type=int,default=600);parser.add_argument('--warmup',type=int,default=200)
    parser.add_argument('--repeats',type=int,default=3);parser.add_argument('--seed',type=int,default=42)
    parser.add_argument('--timeout-ms',type=int,default=1000)
    parser.add_argument('--read-mode',choices=['local','quorum'],default='quorum')
    parser.add_argument('--full',action='store_true');parser.add_argument('--baseline',action='store_true')
    args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=True)
    if min(args.threads,args.keys,args.requests,args.repeats)<1: parser.error('Counts must be positive')
    standard={'w':3,'r':3,'write_ratio':.5,'delay':0,'fault':'none'}
    if args.baseline:
        cases=[dict(standard,delay=200,write_delay=200,read_delay=50)]
    elif args.full:
        cases=[dict(w=w,r=r,write_ratio=ratio,delay=delay,fault=fault)
               for w,r in [(1,1),(3,3),(5,1),(1,5)] for ratio in [.1,.5,.9]
               for delay in [0,50,200] for fault in ['none','slow','unreachable']]
    else:
        cases=[standard]+[dict(standard,w=w,r=r) for w,r in [(1,1),(5,1),(1,5)]]
        cases += [dict(standard,write_ratio=x) for x in [.1,.9]]
        cases += [dict(standard,delay=x) for x in [50,200]]
        cases += [dict(standard,fault=x) for x in ['slow','unreachable']]
        cases += [dict(standard,w=5,r=1,fault='unreachable'),
                  dict(standard,delay=200,write_delay=200,read_delay=50)]
    cases=[dict(write_delay=0,read_delay=0,**case) if 'write_delay' not in case else case for case in cases]
    summaries=[]
    for index,case in enumerate(cases):
        folder=args.output/f'case{index:02d}';folder.mkdir(exist_ok=True)
        with Cluster(args,case,folder,index) as cluster:
            for repeat in range(args.repeats):
                summary,raw=workload(cluster,args,case,repeat)
                summary['case']=index;summaries.append(summary)
                (folder/f'raw_{repeat}.json').write_text(json.dumps(raw,indent=2),encoding='utf-8')
                with (args.output/'results.csv').open('w',newline='',encoding='utf-8') as f:
                    writer=csv.DictWriter(f,fieldnames=list(summaries[0]));writer.writeheader();writer.writerows(summaries)
                print(json.dumps(summary),flush=True)
    aggregated=[]
    for index in range(len(cases)):
        subset=[s for s in summaries if s['case']==index]
        aggregated.append(dict(case=index,**cases[index],**{f'median_{key}':statistics.median(s[key] for s in subset)
                           for key in ['tps','successful_tps','success_rate','mean_ms','p95_ms','p99_ms']}))
    (args.output/'summary.json').write_text(json.dumps(aggregated,indent=2),encoding='utf-8')
    (args.output/'environment.json').write_text(json.dumps({'platform':platform.platform(),'python':platform.python_version(),
        'cpu_count':os.cpu_count(),'java':subprocess.run([args.java,'-version'],capture_output=True,text=True).stderr,
        'args':{k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
        'jar_sha256':hashlib.sha256(args.jar.read_bytes()).hexdigest(),
        'benchmark_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'baseline_commit':'06d39f91b6d69710f255adb506f1133367126f5f'},indent=2),encoding='utf-8')


if __name__=='__main__':main()
