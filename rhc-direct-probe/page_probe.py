#!/usr/bin/env python3
import json, urllib.parse, urllib.request
from pathlib import Path
OUT=Path('out-pages');OUT.mkdir(exist_ok=True)
BASE='https://robinhoodchain.blockscout.com/api'
TARGETS={
 'seadrop':('0x00005ea00ac477b1030ce78506496e8c2de24bf5','0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6'),
 'seaport':('0x0000000000000068f116a894984e2db1123eb395','0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31')}
summary=[]
for target,(address,topic0) in TARGETS.items():
 for page in (1,2,3,4,5,6):
  p={'module':'logs','action':'getLogs','fromBlock':0,'toBlock':'latest','address':address,'topic0':topic0,'page':page,'offset':1000}
  url=BASE+'?'+urllib.parse.urlencode(p)
  try:
   req=urllib.request.Request(url,headers={'accept':'application/json','user-agent':'RHC-Page-Probe/1.0'})
   with urllib.request.urlopen(req,timeout=120) as r:body=r.read();code=r.status
   data=json.loads(body.decode());result=data.get('result') if isinstance(data,dict) else None
   rows=result if isinstance(result,list) else []
   first=rows[0].get('blockNumber') if rows else None;last=rows[-1].get('blockNumber') if rows else None
   (OUT/f'{target}_{page}.json').write_bytes(body)
   summary.append({'target':target,'page':page,'http':code,'rows':len(rows),'first_block':first,'last_block':last,'message':data.get('message') if isinstance(data,dict) else None})
  except Exception as e:summary.append({'target':target,'page':page,'http':0,'rows':None,'error':repr(e)})
  print(summary[-1],flush=True)
(OUT/'summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
