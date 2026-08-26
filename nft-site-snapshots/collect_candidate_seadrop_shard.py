#!/usr/bin/env python3
from __future__ import annotations
import csv,json,os,random,time,urllib.error,urllib.parse,urllib.request
from collections import defaultdict
from datetime import datetime,timezone
from pathlib import Path

OUT=Path(os.getenv('OUT','out-candidate-shard'));OUT.mkdir(parents=True,exist_ok=True)
SHARD=int(os.getenv('SHARD','0'));SHARDS=int(os.getenv('SHARDS','8'))
BLOCKSCOUT='https://robinhoodchain.blockscout.com/api';RPC='https://rpc.mainnet.chain.robinhood.com/rpc'
SEADROP='0x00005ea00ac477b1030ce78506496e8c2de24bf5';TOPIC0='0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6'
UA='RHC-Paid-Candidate-Verification/0.1';last_request=0.0;calls=0;backoffs=0

def pace():
 global last_request
 left=1.1-(time.monotonic()-last_request)
 if left>0:time.sleep(left)
def get_json(url,attempts=16):
 global last_request,calls,backoffs
 last=None
 for i in range(attempts):
  pace();calls+=1
  try:
   req=urllib.request.Request(url,headers={'user-agent':UA,'accept':'application/json'})
   with urllib.request.urlopen(req,timeout=120) as r:raw=r.read();last_request=time.monotonic()
   return json.loads(raw.decode())
  except urllib.error.HTTPError as e:
   last=e;last_request=time.monotonic()
   if e.code==429:
    backoffs+=1;wait=min(150,35+i*10+random.random()*10);print(f'429 wait {wait:.1f}s',flush=True);time.sleep(wait);continue
   if e.code in (500,502,503,504):time.sleep(min(40,2**i+random.random()));continue
   raise
  except Exception as e:
   last=e;time.sleep(min(40,2**i+random.random()))
 raise RuntimeError(f'{url}: {last}')
def rpc(method,params):
 body=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params}).encode();req=urllib.request.Request(RPC,data=body,headers={'content-type':'application/json','user-agent':UA})
 with urllib.request.urlopen(req,timeout=90) as r:d=json.loads(r.read().decode())
 if 'error' in d:raise RuntimeError(d['error'])
 return d['result']
def topic_addr(a):return '0x'+'0'*24+a.lower()[2:]
def words(data):
 s=data[2:] if data.startswith('0x') else data
 return [int(s[i:i+64],16) for i in range(0,len(s),64) if len(s[i:i+64])==64]
def to_addr(t):return '0x'+t[-40:].lower()
def fetch_range(contract,start,end,depth=0):
 params={'module':'logs','action':'getLogs','fromBlock':start,'toBlock':end,'address':SEADROP,'topic0':TOPIC0,'topic1':topic_addr(contract),'topic0_1_opr':'and'}
 d=get_json(BLOCKSCOUT+'?'+urllib.parse.urlencode(params));res=d.get('result') if isinstance(d,dict) else None
 if isinstance(res,str):
  if d.get('status')=='0' or 'No logs found' in res:return []
  raise RuntimeError(res)
 res=res or []
 if len(res)<1000:return res
 if start>=end:raise RuntimeError(f'1000-log leaf at {start}')
 mid=(start+end)//2
 return fetch_range(contract,start,mid,depth+1)+fetch_range(contract,mid+1,end,depth+1)
def write_csv(path,rows):
 rows=list(rows);fields=sorted({k for r in rows for k in r}) if rows else ['empty']
 with Path(path).open('w',encoding='utf-8-sig',newline='') as f:
  w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader()
  for r in rows:w.writerow({k:json.dumps(v,ensure_ascii=False,sort_keys=True) if isinstance(v,(list,dict)) else v for k,v in r.items()})
def main():
 candidates=json.loads(Path('nft-site-snapshots/paid_candidates.json').read_text())
 selected=[x for i,x in enumerate(candidates) if i%SHARDS==SHARD];latest=int(rpc('eth_blockNumber',[]),16)
 raw=[];errors=[]
 for i,c in enumerate(selected,1):
  a=c['address'].lower();print(f'SHARD {SHARD} {i}/{len(selected)} {c["name"]} {a}',flush=True)
  try:logs=fetch_range(a,0,latest)
  except Exception as e:errors.append({'contract_address':a,'collection_name':c['name'],'error':str(e)});continue
  for x in logs:x['_name']=c['name']
  raw.extend(logs)
 dedup={(str(x.get('transactionHash')).lower(),str(x.get('logIndex'))):x for x in raw}
 events=[]
 for x in sorted(dedup.values(),key=lambda y:(int(str(y.get('blockNumber','0x0')),16),int(str(y.get('logIndex','0x0')),16))):
  ws=words(x.get('data','0x'));topics=x.get('topics') or []
  if len(ws)<5 or len(topics)<4:continue
  ts=x.get('timeStamp');ts=int(ts,16) if isinstance(ts,str) and ts.startswith('0x') else None
  events.append({'collection_name':x.get('_name'),'nft_contract':to_addr(topics[1]),'transaction_hash':str(x.get('transactionHash')).lower(),'log_index':int(str(x.get('logIndex','0x0')),16),'block_number':int(str(x.get('blockNumber','0x0')),16),'timestamp_utc':datetime.fromtimestamp(ts,timezone.utc).isoformat().replace('+00:00','Z') if ts else None,'minter':to_addr(topics[2]),'fee_recipient':to_addr(topics[3]),'payer':'0x'+ws[0].to_bytes(32,'big')[-20:].hex(),'quantity':ws[1],'unit_mint_price_wei':ws[2],'unit_mint_price_eth':ws[2]/1e18,'gross_mint_value_wei':ws[1]*ws[2],'gross_mint_value_eth':ws[1]*ws[2]/1e18,'fee_bps':ws[3],'drop_stage_index':ws[4],'is_free':ws[2]==0,'is_paid':ws[2]>0})
 by=defaultdict(list)
 for e in events:by[e['nft_contract']].append(e)
 summary=[]
 for c in selected:
  a=c['address'].lower();rows=sorted(by.get(a,[]),key=lambda e:(e['block_number'],e['log_index']));total=sum(e['quantity'] for e in rows);free=sum(e['quantity'] for e in rows if e['is_free']);paid=total-free;first=rows[0] if rows else {}
  model='NO_EVENT' if not rows else ('MIXED_FREE_AND_PAID' if free and paid else ('FREE_ONLY' if free else 'PAID_ONLY'))
  summary.append({'shard':SHARD,'contract_address':a,'collection_name':c['name'],'event_count':len(rows),'minted_quantity':total,'free_quantity':free,'paid_quantity':paid,'unique_minters':len({e['minter'] for e in rows}),'unique_payers':len({e['payer'] for e in rows}),'first_mint_timestamp_utc':first.get('timestamp_utc'),'first_mint_price_wei':first.get('unit_mint_price_wei'),'first_stage_index':first.get('drop_stage_index'),'observed_prices_wei':sorted({e['unit_mint_price_wei'] for e in rows}),'observed_stage_indexes':sorted({e['drop_stage_index'] for e in rows}),'onchain_model':model,'paid_from_first_observed':bool(rows and first.get('is_paid') and free==0),'production_approved':False})
 write_csv(OUT/'events.csv',events);write_csv(OUT/'summary.csv',summary);write_csv(OUT/'errors.csv',errors)
 with (OUT/'events.jsonl').open('w',encoding='utf-8') as f:
  for e in events:f.write(json.dumps(e,ensure_ascii=False,sort_keys=True)+'\n')
 validation={'status':'PASS' if not errors and len(summary)==len(selected) else 'PARTIAL','shard':SHARD,'shards':SHARDS,'candidate_rows':len(selected),'event_rows':len(events),'error_count':len(errors),'api_calls':calls,'rate_limit_backoffs':backoffs,'latest_block':latest}
 (OUT/'validation.json').write_text(json.dumps(validation,indent=2),encoding='utf-8');print(json.dumps(validation),flush=True)
 if len(errors)>2:raise SystemExit(1)
if __name__=='__main__':main()
