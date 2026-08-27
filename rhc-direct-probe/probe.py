#!/usr/bin/env python3
from __future__ import annotations
import json, urllib.parse, urllib.request
from pathlib import Path

OUT=Path('out-direct'); OUT.mkdir(exist_ok=True)
TARGETS={
 'seadrop':('0x00005ea00ac477b1030ce78506496e8c2de24bf5','0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6'),
 'seaport':('0x0000000000000068f116a894984e2db1123eb395','0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31')}
BASES=['https://robinhoodchain.blockscout.com/api','https://explorer.hoodmarketcap.com/api']
summary=[]
for target,(address,topic0) in TARGETS.items():
 for n,base in enumerate(BASES):
  params={'module':'logs','action':'getLogs','fromBlock':0,'toBlock':'latest','address':address,'topic0':topic0}
  url=base+'?'+urllib.parse.urlencode(params)
  try:
   req=urllib.request.Request(url,headers={'accept':'application/json','user-agent':'RHC-Direct-History-Probe/1.0'})
   with urllib.request.urlopen(req,timeout=180) as response:
    body=response.read(); code=response.status
   path=OUT/f'{target}_{n}.json'; path.write_bytes(body)
   try:
    data=json.loads(body.decode()); result=data.get('result') if isinstance(data,dict) else None
    rows=len(result) if isinstance(result,list) else None
    message=data.get('message') if isinstance(data,dict) else None
   except Exception as exc:
    rows=None; message=repr(exc)
   summary.append({'target':target,'base':base,'http':code,'bytes':len(body),'rows':rows,'message':message,'url':url})
  except Exception as exc:
   summary.append({'target':target,'base':base,'http':0,'bytes':0,'rows':None,'message':repr(exc),'url':url})
  print(summary[-1],flush=True)
(OUT/'summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
