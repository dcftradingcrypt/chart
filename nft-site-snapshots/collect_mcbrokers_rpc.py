#!/usr/bin/env python3
from __future__ import annotations
import csv,json,os,random,time,urllib.request
from datetime import datetime,timezone
from pathlib import Path

OUT=Path(os.getenv('OUT','out-mcbrokers'));OUT.mkdir(parents=True,exist_ok=True)
RPC='https://rpc.mainnet.chain.robinhood.com/rpc'
SEADROP='0x00005ea00ac477b1030ce78506496e8c2de24bf5'
CONTRACT='0x444444447657f90a85c99c00c0780e4e1c40c897'
TOPIC0='0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6'
TOPIC1='0x'+'0'*24+CONTRACT[2:]
UA='RHC-McBrokers-RPC-Research/0.1';ident=0;calls=0;retries=0

def rpc(method,params,attempts=10):
 global ident,calls,retries
 last=None
 for i in range(attempts):
  ident+=1;calls+=1
  body=json.dumps({'jsonrpc':'2.0','id':ident,'method':method,'params':params}).encode()
  req=urllib.request.Request(RPC,data=body,headers={'content-type':'application/json','user-agent':UA})
  try:
   with urllib.request.urlopen(req,timeout=120) as r:d=json.loads(r.read().decode())
   if 'error' in d:raise RuntimeError(d['error'])
   return d.get('result')
  except Exception as e:
   last=e
   if i+1==attempts:break
   retries+=1;time.sleep(min(25,2**i+random.random()))
 raise RuntimeError(f'{method}: {last}')
def words(data):
 s=data[2:] if data.startswith('0x') else data
 return [int(s[i:i+64],16) for i in range(0,len(s),64) if len(s[i:i+64])==64]
def to_addr(t):return '0x'+t[-40:].lower()
def write_csv(path,rows):
 rows=list(rows);fields=sorted({k for r in rows for k in r}) if rows else ['empty']
 with Path(path).open('w',encoding='utf-8-sig',newline='') as f:
  w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader()
  for r in rows:w.writerow({k:json.dumps(v,ensure_ascii=False,sort_keys=True) if isinstance(v,(list,dict)) else v for k,v in r.items()})
def main():
 latest=int(rpc('eth_blockNumber',[]),16)
 # Binary search first code block to avoid querying chain history before deployment.
 lo,hi=0,latest
 while lo<hi:
  mid=(lo+hi)//2;code=rpc('eth_getCode',[CONTRACT,hex(mid)])
  if code not in (None,'0x','0x0'):hi=mid
  else:lo=mid+1
 first_code=lo
 print(f'first_code={first_code} latest={latest}',flush=True)
 raw=[];ranges=[];chunk=10000
 for start in range(first_code,latest+1,chunk):
  end=min(latest,start+chunk-1)
  filt={'fromBlock':hex(start),'toBlock':hex(end),'address':SEADROP,'topics':[TOPIC0,TOPIC1]}
  got=rpc('eth_getLogs',[filt]) or []
  raw.extend(got);ranges.append({'from_block':start,'to_block':end,'rows':len(got)})
  if len(ranges)%20==0:print(f'range {len(ranges)} end={end} rows={len(got)} total={len(raw)}',flush=True)
 dedup={(x.get('transactionHash'),x.get('logIndex')):x for x in raw}
 raw=sorted(dedup.values(),key=lambda x:(int(x['blockNumber'],16),int(x['logIndex'],16)))
 blocks=sorted({int(x['blockNumber'],16) for x in raw});timestamps={}
 for i in range(0,len(blocks),50):
  for n in blocks[i:i+50]:
   b=rpc('eth_getBlockByNumber',[hex(n),False]);timestamps[n]=int(b['timestamp'],16)
 events=[]
 for x in raw:
  ws=words(x.get('data','0x'));topics=x.get('topics') or []
  if len(ws)<5 or len(topics)<4:continue
  bn=int(x['blockNumber'],16);ts=timestamps.get(bn)
  events.append({'collection_name':'McBrokers','nft_contract':CONTRACT,'transaction_hash':x['transactionHash'].lower(),'log_index':int(x['logIndex'],16),'block_number':bn,'timestamp_utc':datetime.fromtimestamp(ts,timezone.utc).isoformat().replace('+00:00','Z') if ts else None,'minter':to_addr(topics[2]),'fee_recipient':to_addr(topics[3]),'payer':'0x'+ws[0].to_bytes(32,'big')[-20:].hex(),'quantity':ws[1],'unit_mint_price_wei':ws[2],'unit_mint_price_eth':ws[2]/1e18,'gross_mint_value_wei':ws[1]*ws[2],'gross_mint_value_eth':ws[1]*ws[2]/1e18,'fee_bps':ws[3],'drop_stage_index':ws[4],'is_free':ws[2]==0,'is_paid':ws[2]>0})
 total=sum(e['quantity'] for e in events);free=sum(e['quantity'] for e in events if e['is_free']);paid=total-free;first=events[0] if events else {}
 model='NO_EVENT' if not events else ('MIXED_FREE_AND_PAID' if free and paid else ('FREE_ONLY' if free else 'PAID_ONLY'))
 summary=[{'shard':3,'contract_address':CONTRACT,'collection_name':'McBrokers','event_count':len(events),'minted_quantity':total,'free_quantity':free,'paid_quantity':paid,'unique_minters':len({e['minter'] for e in events}),'unique_payers':len({e['payer'] for e in events}),'first_mint_timestamp_utc':first.get('timestamp_utc'),'first_mint_price_wei':first.get('unit_mint_price_wei'),'first_stage_index':first.get('drop_stage_index'),'observed_prices_wei':sorted({e['unit_mint_price_wei'] for e in events}),'observed_stage_indexes':sorted({e['drop_stage_index'] for e in events}),'onchain_model':model,'paid_from_first_observed':bool(events and first.get('is_paid') and free==0),'production_approved':False}]
 write_csv(OUT/'events.csv',events);write_csv(OUT/'summary.csv',summary);write_csv(OUT/'ranges.csv',ranges)
 with (OUT/'events.jsonl').open('w',encoding='utf-8') as f:
  for e in events:f.write(json.dumps(e,ensure_ascii=False,sort_keys=True)+'\n')
 validation={'status':'PASS' if events else 'FAIL','first_code_block':first_code,'latest_block':latest,'event_rows':len(events),'minted_quantity':total,'free_quantity':free,'paid_quantity':paid,'rpc_calls':calls,'rpc_retries':retries}
 (OUT/'validation.json').write_text(json.dumps(validation,indent=2),encoding='utf-8');print(json.dumps(validation),flush=True)
 if validation['status']!='PASS':raise SystemExit(1)
if __name__=='__main__':main()
