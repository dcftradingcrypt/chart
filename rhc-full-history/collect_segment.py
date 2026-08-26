#!/usr/bin/env python3
from __future__ import annotations
import argparse,csv,json,random,threading,time,urllib.error,urllib.parse,urllib.request
from concurrent.futures import ThreadPoolExecutor,as_completed
from pathlib import Path
BASES=['https://robinhoodchain.blockscout.com/api','https://explorer.hoodmarketcap.com/api']
TARGETS={'seadrop':{'address':'0x00005ea00ac477b1030ce78506496e8c2de24bf5','topic0':'0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6'},'seaport':{'address':'0x0000000000000068f116a894984e2db1123eb395','topic0':'0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31'}}
UA='RHC-NFT-Full-History/2.0 (read-only)';L=threading.Lock();next_at=0.;calls=0;backoffs=0

def integer(v,d=None):
 if v is None:return d
 if isinstance(v,int):return v
 if isinstance(v,float):return int(v)
 if isinstance(v,str):
  try:return int(v,16) if v.startswith('0x') else int(v)
  except:return d
 return d

def pace():
 global next_at
 with L:
  now=time.monotonic();wait=max(0.,next_at-now);next_at=max(now,next_at)+.23
 if wait:time.sleep(wait)

def one(url,attempts=3):
 global calls,backoffs
 e=None
 for i in range(attempts):
  pace()
  with L:calls+=1
  try:
   q=urllib.request.Request(url,headers={'user-agent':UA,'accept':'application/json'})
   with urllib.request.urlopen(q,timeout=35) as r:raw=r.read()
   d=json.loads(raw.decode())
   if not isinstance(d,dict):raise RuntimeError('non-object response')
   return d
  except urllib.error.HTTPError as x:
   e=x
   if x.code==429:
    with L:backoffs+=1
    time.sleep(min(30,4+i*6+random.random()*2));continue
   if x.code in (408,425,500,502,503,504):time.sleep(min(10,2**i+random.random()));continue
   raise
  except Exception as x:
   e=x
   if i+1<attempts:time.sleep(min(8,2**i+random.random()))
 raise RuntimeError(repr(e))

def query(target,a,b):
 s=TARGETS[target];notes=[];p={'module':'logs','action':'getLogs','fromBlock':a,'toBlock':b,'address':s['address'],'topic0':s['topic0']}
 for base in BASES:
  url=base+'?'+urllib.parse.urlencode(p)
  try:d=one(url)
  except Exception as e:notes.append(base+':'+repr(e));continue
  result=d.get('result');msg=str(d.get('message',''))
  if isinstance(result,list):
   rows=[x for x in result if isinstance(x,dict)]
   if len(rows)>=1000:return 'split',rows,base+f':cap:{len(rows)}'
   return 'ok',rows,base+f':{msg}:{len(rows)}'
  text=(msg+' '+str(result)).lower()
  if any(x in text for x in ('no logs','no records','not found')):return 'ok',[],base+':'+text[:300]
  if any(x in text for x in ('1000','too many','timeout','range','response size')):return 'split',[],base+':'+text[:300]
  notes.append(base+':'+text[:300])
 return 'split',[], '|'.join(notes)

def norm(r,target):
 t=r.get('topics') or []
 if isinstance(t,str):
  try:t=json.loads(t)
  except:t=[t]
 t=[str(x).lower() for x in t]
 return {'target':target,'address':str(r.get('address') or TARGETS[target]['address']).lower(),'block_number':integer(r.get('blockNumber'),integer(r.get('block_number'))),'transaction_hash':str(r.get('transactionHash') or r.get('transaction_hash') or '').lower(),'transaction_index':integer(r.get('transactionIndex'),integer(r.get('transaction_index'))),'log_index':integer(r.get('logIndex'),integer(r.get('log_index'))),'data':str(r.get('data') or '0x').lower(),'topics':t,'topic0':t[0] if t else None,'block_hash':str(r.get('blockHash') or r.get('block_hash') or '').lower(),'raw':r}

def collect(target,idx,count,end,out,workers,window):
 start=(end+1)*idx//count;stop=(end+1)*(idx+1)//count-1;out.mkdir(parents=True,exist_ok=True)
 pending=[(a,min(a+window-1,stop)) for a in range(start,stop+1,window)];accepted=[];ranges=[];errors=[]
 while pending:
  batch=pending[:workers*3];pending=pending[workers*3:]
  with ThreadPoolExecutor(max_workers=workers) as pool:
   fs={pool.submit(query,target,a,b):(a,b) for a,b in batch}
   for f in as_completed(fs):
    a,b=fs[f]
    try:state,rows,note=f.result()
    except Exception as e:state,rows,note='split',[],repr(e)
    if state=='ok':accepted += [norm(x,target) for x in rows];ranges.append({'from_block':a,'to_block':b,'status':'ACCEPTED','row_count':len(rows),'note':note})
    elif a<b:
     m=(a+b)//2;pending += [(a,m),(m+1,b)];ranges.append({'from_block':a,'to_block':b,'status':'SPLIT','row_count':len(rows),'note':note})
    else:errors.append({'from_block':a,'to_block':b,'status':'UNRESOLVED_SINGLE_BLOCK','row_count':len(rows),'note':note});ranges.append(errors[-1])
  pending.sort()
  if len(ranges)%100<workers*3:print(target,idx,'ranges',len(ranges),'pending',len(pending),'rows',len(accepted),flush=True)
 d={}
 for r in accepted:d[(r['transaction_hash'],r['log_index'] if r['log_index'] is not None else -1)]=r
 rows=sorted(d.values(),key=lambda x:(x['block_number'] or -1,x['transaction_index'] or -1,x['log_index'] or -1))
 with (out/'logs.jsonl').open('w',encoding='utf-8') as f:
  for r in rows:f.write(json.dumps(r,ensure_ascii=False,sort_keys=True)+'\n')
 fields=['target','address','block_number','transaction_hash','transaction_index','log_index','data','topics','topic0','block_hash']
 with (out/'logs.csv').open('w',encoding='utf-8-sig',newline='') as f:
  w=csv.DictWriter(f,fieldnames=fields);w.writeheader()
  for r in rows:w.writerow({k:json.dumps(r[k],ensure_ascii=False) if isinstance(r[k],list) else r[k] for k in fields})
 for name,data in [('ranges.csv',ranges),('errors.csv',errors)]:
  with (out/name).open('w',encoding='utf-8-sig',newline='') as f:
   fields=['from_block','to_block','status','row_count','note'];w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(sorted(data,key=lambda x:(x['from_block'],x['to_block'])))
 wrong=sum(r['topic0']!=TARGETS[target]['topic0'] for r in rows);oor=sum(r['block_number'] is None or not start<=r['block_number']<=stop for r in rows)
 v={'status':'PASS' if not errors and not wrong and not oor else 'FAIL','target':target,'segment_index':idx,'segment_count':count,'from_block':start,'to_block':stop,'initial_window_blocks':window,'accepted_rows':len(rows),'duplicates_removed':len(accepted)-len(rows),'wrong_topic_rows':wrong,'out_of_range_rows':oor,'unresolved_single_block_ranges':len(errors),'range_requests':len(ranges),'http_calls':calls,'rate_limit_backoffs':backoffs}
 (out/'validation.json').write_text(json.dumps(v,indent=2,sort_keys=True),encoding='utf-8');print(json.dumps(v,sort_keys=True),flush=True)
 if v['status']!='PASS':raise SystemExit(2)

def main():
 p=argparse.ArgumentParser();p.add_argument('--target',choices=TARGETS,required=True);p.add_argument('--segment-index',type=int,required=True);p.add_argument('--segment-count',type=int,required=True);p.add_argument('--end-block',type=int,required=True);p.add_argument('--workers',type=int,default=3);p.add_argument('--window-blocks',type=int,default=100000);p.add_argument('--out',type=Path,required=True);a=p.parse_args();collect(a.target,a.segment_index,a.segment_count,a.end_block,a.out,max(1,a.workers),max(1,a.window_blocks))
if __name__=='__main__':main()
