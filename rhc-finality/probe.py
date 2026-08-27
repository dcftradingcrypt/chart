#!/usr/bin/env python3
from __future__ import annotations
import hashlib,json,time,urllib.request
from pathlib import Path

RPCS=[
 'https://rpc.mainnet.chain.robinhood.com',
 'https://robinhood-rpc.publicnode.com',
 'https://rpc-robinhood.hoodmarket.io',
]
OUT=Path('out-finality');OUT.mkdir(exist_ok=True)
UA='DCF-RHC-Finality-Probe/1.0 (read-only)'

def call(url,method,params):
 payload=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params}).encode()
 request=urllib.request.Request(url,data=payload,method='POST',headers={'content-type':'application/json','accept':'application/json','user-agent':UA})
 with urllib.request.urlopen(request,timeout=45) as response:
  data=json.loads(response.read().decode())
 if data.get('error') is not None:raise RuntimeError(data['error'])
 return data.get('result')

def integer(value):return int(value,16) if isinstance(value,str) and value.startswith('0x') else int(value)

def main():
 rows=[]
 for rpc in RPCS:
  for tag in ('latest','safe','finalized'):
   try:
    block=call(rpc,'eth_getBlockByNumber',[tag,False])
    rows.append({'rpc':rpc,'tag':tag,'status':'PASS','number':integer(block['number']),'hash':str(block['hash']).lower(),'parent_hash':str(block['parentHash']).lower(),'timestamp_unix':integer(block['timestamp'])})
   except Exception as exc:
    rows.append({'rpc':rpc,'tag':tag,'status':'FAIL','error':repr(exc)})
   time.sleep(.5)
 by_tag={}
 for tag in ('latest','safe','finalized'):
  good=[r for r in rows if r['tag']==tag and r['status']=='PASS']
  by_tag[tag]={'responding_rpcs':len(good),'numbers':sorted({r['number'] for r in good}),'hashes':sorted({r['hash'] for r in good}),'all_hashes_agree':len({r['hash'] for r in good})==1 if good else False}
 finalized=[r for r in rows if r['tag']=='finalized' and r['status']=='PASS']
 safe=[r for r in rows if r['tag']=='safe' and r['status']=='PASS']
 chosen=None;mode=None
 if finalized:
  chosen=min(finalized,key=lambda r:r['number']);mode='MIN_RESPONDING_FINALIZED'
 elif safe:
  chosen=min(safe,key=lambda r:r['number']);mode='MIN_RESPONDING_SAFE_FALLBACK'
 report={'status':'PASS' if chosen is not None else 'FAIL','rows':rows,'tag_summary':by_tag,'dataset_cutoff_mode':mode,'dataset_cutoff_block':chosen['number'] if chosen else None,'dataset_cutoff_hash':chosen['hash'] if chosen else None,'dataset_cutoff_timestamp_unix':chosen['timestamp_unix'] if chosen else None}
 (OUT/'finality_report.json').write_text(json.dumps(report,indent=2,sort_keys=True),encoding='utf-8')
 manifest=[]
 for path in OUT.iterdir():
  if path.is_file():manifest.append({'path':path.name,'bytes':path.stat().st_size,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
 (OUT/'manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True),encoding='utf-8')
 print(json.dumps(report,sort_keys=True))
 if report['status']!='PASS':raise SystemExit(2)
if __name__=='__main__':main()
