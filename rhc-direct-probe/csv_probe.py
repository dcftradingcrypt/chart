#!/usr/bin/env python3
import csv, hashlib, json, urllib.request
from pathlib import Path
OUT=Path('out-csv');OUT.mkdir(exist_ok=True)
TARGETS={
 'seadrop':'0x00005ea00ac477b1030ce78506496e8c2de24bf5',
 'seaport':'0x0000000000000068f116a894984e2db1123eb395'}
summary=[]
for target,address in TARGETS.items():
 for base in ('https://robinhoodchain.blockscout.com/api/v2','https://explorer.hoodmarketcap.com/api/v2'):
  url=f'{base}/addresses/{address}/logs/csv'
  try:
   req=urllib.request.Request(url,headers={'accept':'text/csv,*/*','user-agent':'RHC-CSV-Probe/1.0'})
   with urllib.request.urlopen(req,timeout=240) as r:body=r.read();code=r.status;ctype=r.headers.get('content-type','')
   idx=len(summary);path=OUT/f'{target}_{idx}.csv';path.write_bytes(body)
   line_count=body.count(b'\n')
   summary.append({'target':target,'base':base,'http':code,'content_type':ctype,'bytes':len(body),'line_count':line_count,'sha256':hashlib.sha256(body).hexdigest(),'url':url})
  except Exception as e:summary.append({'target':target,'base':base,'http':0,'error':repr(e),'url':url})
  print(summary[-1],flush=True)
(OUT/'summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
