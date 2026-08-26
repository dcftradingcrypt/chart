#!/usr/bin/env python3
from __future__ import annotations
import csv,json,os,random,time,urllib.error,urllib.parse,urllib.request
from datetime import datetime,timezone
from pathlib import Path
from typing import Any

OUT=Path(os.getenv('OUT','out-global-shard'));OUT.mkdir(parents=True,exist_ok=True)
SHARD=int(os.getenv('SHARD','0'));SHARDS=int(os.getenv('SHARDS','16'))
FIXED_END_BLOCK=int(os.getenv('GLOBAL_END_BLOCK','46840468'))
BLOCKSCOUT='https://robinhoodchain.blockscout.com/api';RPC='https://rpc.mainnet.chain.robinhood.com/rpc'
SEADROP='0x00005ea00ac477b1030ce78506496e8c2de24bf5';TOPIC0='0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6'
UA=f'RHC-SeaDrop-Global-Shard/{SHARD}';last_request=0.0;calls=0;backoffs=0;ranges=[]

def pace():
 global last_request
 left=1.05-(time.monotonic()-last_request)
 if left>0:time.sleep(left)
def get_json(url,attempts=18):
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
    backoffs+=1;wait=min(180,35+i*12+random.random()*10);print(f'429 {wait:.1f}s',flush=True);time.sleep(wait);continue
   if e.code in (500,502,503,504):time.sleep(min(45,2**i+random.random()));continue
   raise
  except Exception as e:
   last=e;time.sleep(min(45,2**i+random.random()))
 raise RuntimeError(f'{url}: {last}')
def words(data):
 s=data[2:] if data.startswith('0x') else data
 return [int(s[i:i+64],16) for i in range(0,len(s),64) if len(s[i:i+64])==64]
def to_addr(t):return '0x'+t[-40:].lower()
def fetch(start,end,depth=0):
 params={'module':'logs','action':'getLogs','fromBlock':start,'toBlock':end,'address':SEADROP,'topic0':TOPIC0}
 d=get_json(BLOCKSCOUT+'?'+urllib.parse.urlencode(params));res=d.get('result') if isinstance(d,dict) else None
 if isinstance(res,str):
  if d.get('status')=='0' or 'No logs found' in res:ranges.append({'from_block':start,'to_block':end,'depth':depth,'rows':0,'action':'ACCEPT_EMPTY'});return []
  raise RuntimeError(res)
 res=res or []
 if len(res)<1000:ranges.append({'from_block':start,'to_block':end,'depth':depth,'rows':len(res),'action':'ACCEPT'});return res
 if start>=end:raise RuntimeError(f'1000-log leaf at block {start}')
 mid=(start+end)//2;ranges.append({'from_block':start,'to_block':end,'depth':depth,'rows':len(res),'action':'SPLIT'})
 return fetch(start,mid,depth+1)+fetch(mid+1,end,depth+1)
def write_csv(path,rows):
 rows=list(rows);fields=sorted({k for r in rows for k in r}) if rows else ['empty']
 with Path(path).open('w',encoding='utf-8-sig',newline='') as f:
  w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader()
  for r in rows:w.writerow({k:json.dumps(v,ensure_ascii=False,sort_keys=True) if isinstance(v,(list,dict)) else v for k,v in r.items()})
def main():
 latest=FIXED_END_BLOCK;span=latest+1;start=(span*SHARD)//SHARDS;end=(span*(SHARD+1))//SHARDS-1
 print(f'shard={SHARD}/{SHARDS} range={start}-{end} fixed_end={latest}',flush=True)
 raw=fetch(start,end);dedup={(str(x.get('transactionHash')).lower(),str(x.get('logIndex'))):x for x in raw}
 raw=sorted(dedup.values(),key=lambda x:(int(str(x.get('blockNumber','0x0')),16),int(str(x.get('logIndex','0x0')),16)))
 events=[]
 for x in raw:
  ws=words(x.get('data','0x'));topics=x.get('topics') or []
  if len(ws)<5 or len(topics)<4:continue
  ts=x.get('timeStamp');ts=int(ts,16) if isinstance(ts,str) and ts.startswith('0x') else None
  events.append({'shard':SHARD,'transaction_hash':str(x.get('transactionHash')).lower(),'log_index':int(str(x.get('logIndex','0x0')),16),'block_number':int(str(x.get('blockNumber','0x0')),16),'timestamp_utc':datetime.fromtimestamp(ts,timezone.utc).isoformat().replace('+00:00','Z') if ts else None,'nft_contract':to_addr(topics[1]),'minter':to_addr(topics[2]),'fee_recipient':to_addr(topics[3]),'payer':'0x'+ws[0].to_bytes(32,'big')[-20:].hex(),'quantity':ws[1],'unit_mint_price_wei':ws[2],'unit_mint_price_eth':ws[2]/1e18,'gross_mint_value_wei':ws[1]*ws[2],'gross_mint_value_eth':ws[1]*ws[2]/1e18,'fee_bps':ws[3],'drop_stage_index':ws[4],'is_free':ws[2]==0,'is_paid':ws[2]>0})
 write_csv(OUT/'events.csv',events);write_csv(OUT/'scan_ranges.csv',ranges)
 with (OUT/'events.jsonl').open('w',encoding='utf-8') as f:
  for e in events:f.write(json.dumps(e,ensure_ascii=False,sort_keys=True)+'\n')
 validation={'status':'PASS','shard':SHARD,'shards':SHARDS,'from_block':start,'to_block':end,'fixed_end_block':latest,'raw_rows':len(raw),'event_rows':len(events),'unique_contracts':len({e['nft_contract'] for e in events}),'api_calls':calls,'rate_limit_backoffs':backoffs,'range_rows':len(ranges)}
 (OUT/'validation.json').write_text(json.dumps(validation,indent=2),encoding='utf-8');print(json.dumps(validation),flush=True)
if __name__=='__main__':main()
