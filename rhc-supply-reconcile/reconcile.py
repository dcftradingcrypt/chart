#!/usr/bin/env python3
from __future__ import annotations
import argparse,csv,json,random,time,urllib.error,urllib.parse,urllib.request,hashlib
from pathlib import Path
from typing import Any

EXPLORERS=['https://robinhoodchain.blockscout.com/api','https://explorer.hoodmarketcap.com/api']
RPCS=['https://rpc.mainnet.chain.robinhood.com','https://robinhood-rpc.publicnode.com','https://rpc-robinhood.hoodmarket.io']
TRANSFER='0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
ZERO_TOPIC='0x'+'0'*64
UA='DCF-RHC-Supply-Reconciliation/1.1 (read-only)'
last=0.0

def pace(seconds=1.2):
 global last
 w=seconds-(time.monotonic()-last)
 if w>0:time.sleep(w)

def req(url,payload=None,attempts=12):
 global last
 body=None;headers={'user-agent':UA,'accept':'application/json'}
 if payload is not None:
  body=json.dumps(payload).encode();headers['content-type']='application/json'
 err=None
 for i in range(attempts):
  pace()
  try:
   r=urllib.request.Request(url,data=body,headers=headers,method='POST' if body else 'GET')
   with urllib.request.urlopen(r,timeout=60) as resp:raw=resp.read();status=resp.status
   last=time.monotonic();return status,json.loads(raw.decode())
  except urllib.error.HTTPError as e:
   last=time.monotonic();err=e
   if e.code==429 or e.code>=500:
    time.sleep(min(90,5+5*i+random.random()*3));continue
   return e.code,{'error':e.read(1000).decode('utf-8','replace')}
  except Exception as e:
   last=time.monotonic();err=e;time.sleep(min(60,3+3*i+random.random()*2))
 raise RuntimeError(repr(err))

def rpc(method,params):
 errors=[]
 for url in RPCS:
  try:
   st,d=req(url,{'jsonrpc':'2.0','id':1,'method':method,'params':params})
   if st==200 and isinstance(d,dict) and 'result' in d:return d['result'],url
   errors.append(f'{url}:{d}')
  except Exception as e:errors.append(f'{url}:{e!r}')
 raise RuntimeError(' | '.join(errors))

def intval(v,d=None):
 try:return int(v,16) if isinstance(v,str) and v.startswith('0x') else int(v)
 except:return d

def get_head():return int(rpc('eth_blockNumber',[])[0],16)

def eth_call(to,data,block):
 try:
  result,source=rpc('eth_call',[{'to':to,'data':data},hex(block)])
  if isinstance(result,str) and result.startswith('0x') and len(result)>=66:return int(result,16),source,None
  return None,source,f'invalid_result:{result}'
 except Exception as e:return None,None,repr(e)

def log_query(address,topic_index,from_block,to_block):
 params={'module':'logs','action':'getLogs','fromBlock':from_block,'toBlock':to_block,'address':address,'topic0':TRANSFER,topic_index:ZERO_TOPIC,f'topic0_{topic_index[-1]}_opr':'and'}
 errors=[]
 for base in EXPLORERS:
  try:
   st,d=req(base+'?'+urllib.parse.urlencode(params))
   if st!=200 or not isinstance(d,dict):errors.append(f'{base}:HTTP{st}');continue
   res=d.get('result');msg=str(d.get('message') or '')
   if isinstance(res,list):return res,base,msg
   text=(msg+' '+str(res)).lower()
   if any(x in text for x in ('no records','no logs','not found')):return [],base,text
   errors.append(f'{base}:{text[:300]}')
  except Exception as e:errors.append(f'{base}:{e!r}')
 raise RuntimeError(' | '.join(errors))

def collect_logs(address,topic_index,head):
 pending=[(0,head)];rows=[];ranges=[];errors=[]
 while pending:
  a,b=pending.pop(0)
  try:r,source,msg=log_query(address,topic_index,a,b)
  except Exception as e:
   if a<b:
    m=(a+b)//2;pending[0:0]=[(a,m),(m+1,b)];ranges.append({'from_block':a,'to_block':b,'status':'SPLIT_ERROR','rows':0,'note':repr(e)});continue
   errors.append({'from_block':a,'to_block':b,'error':repr(e)});continue
  if len(r)>=1000 and a<b:
   m=(a+b)//2;pending[0:0]=[(a,m),(m+1,b)];ranges.append({'from_block':a,'to_block':b,'status':'SPLIT_CAP','rows':len(r),'note':source+':'+msg});continue
  rows.extend(r);ranges.append({'from_block':a,'to_block':b,'status':'ACCEPTED','rows':len(r),'note':source+':'+msg})
 d={}
 for x in rows:
  k=(str(x.get('transactionHash') or x.get('transaction_hash') or '').lower(),intval(x.get('logIndex') if x.get('logIndex') is not None else x.get('log_index'),-1));d[k]=x
 return list(d.values()),ranges,errors

def write_csv(path,rows):
 fields=sorted({k for r in rows for k in r}) if rows else ['empty']
 with path.open('w',encoding='utf-8-sig',newline='') as f:
  w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader()
  for r in rows:w.writerow({k:json.dumps(v,ensure_ascii=False,sort_keys=True) if isinstance(v,(dict,list,tuple)) else v for k,v in r.items()})

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
 p=argparse.ArgumentParser();p.add_argument('--contracts',type=Path,required=True);p.add_argument('--out',type=Path,required=True);p.add_argument('--head-block',type=int);a=p.parse_args();a.out.mkdir(parents=True,exist_ok=True)
 with a.contracts.open(encoding='utf-8-sig',newline='') as f:contracts=list(csv.DictReader(f))
 head=a.head_block if a.head_block is not None else get_head()
 summaries=[];all_ranges=[];all_errors=[]
 for idx,c in enumerate(contracts,1):
  address=c['contract_address'].lower();sdq=int(float(c.get('seadrop_quantity') or 0));print(idx,len(contracts),address,flush=True)
  total,ts_source,ts_error=eth_call(address,'0x18160ddd',head)
  supports721,si_source,si_error=eth_call(address,'0x01ffc9a7'+'80ac58cd'+'0'*56,head)
  mint_logs,mint_ranges,mint_errors=collect_logs(address,'topic1',head)
  burn_logs,burn_ranges,burn_errors=collect_logs(address,'topic2',head)
  mint_qty=len(mint_logs);burn_qty=len(burn_logs)
  implied_total_minted=(total+burn_qty) if total is not None else None
  extra=(implied_total_minted-sdq) if implied_total_minted is not None else None
  transfer_reconciles=(mint_qty-burn_qty==total) if total is not None else None
  status='PASS' if total is not None and not mint_errors and not burn_errors and supports721 not in (None,0) and implied_total_minted>=sdq else 'REVIEW'
  summaries.append({**c,'fixed_head_block':head,'total_supply_at_head':total,'total_supply_rpc':ts_source,'total_supply_error':ts_error,'supports_erc721':bool(supports721) if supports721 is not None else None,'supports_interface_rpc':si_source,'supports_interface_error':si_error,'standard_zero_address_mint_logs':mint_qty,'standard_burn_logs':burn_qty,'implied_total_minted':implied_total_minted,'seadrop_quantity':sdq,'non_seadrop_mint_quantity':extra,'standard_transfer_supply_reconciles':transfer_reconciles,'all_primary_mint_routes_reconciled':status=='PASS' and extra==0 and transfer_reconciles is True,'reconciliation_status':status})
  all_ranges += [{'contract_address':address,'direction':'MINT',**x} for x in mint_ranges]+[{'contract_address':address,'direction':'BURN',**x} for x in burn_ranges]
  all_errors += [{'contract_address':address,'direction':'MINT',**x} for x in mint_errors]+[{'contract_address':address,'direction':'BURN',**x} for x in burn_errors]
  write_csv(a.out/'supply_reconciliation.csv',summaries);write_csv(a.out/'ranges.csv',all_ranges);write_csv(a.out/'errors.csv',all_errors)
 validation={'status':'PASS' if not all_errors and all(x['reconciliation_status']=='PASS' for x in summaries) else 'FAIL','fixed_head_block':head,'contract_count':len(contracts),'rows':len(summaries),'errors':len(all_errors),'fully_reconciled_routes':sum(bool(x['all_primary_mint_routes_reconciled']) for x in summaries),'contracts_with_non_seadrop_mints':sum((x['non_seadrop_mint_quantity'] or 0)>0 for x in summaries if x['non_seadrop_mint_quantity'] is not None),'contracts_unresolved':sum(x['reconciliation_status']!='PASS' for x in summaries)}
 (a.out/'validation.json').write_text(json.dumps(validation,indent=2,sort_keys=True),encoding='utf-8')
 manifest=[]
 for path in sorted(a.out.iterdir()):
  if path.is_file() and path.name!='manifest.json':manifest.append({'path':path.name,'bytes':path.stat().st_size,'sha256':sha(path)})
 (a.out/'manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True),encoding='utf-8')
 print(json.dumps(validation,sort_keys=True))
 if validation['status']!='PASS':raise SystemExit(2)
if __name__=='__main__':main()
