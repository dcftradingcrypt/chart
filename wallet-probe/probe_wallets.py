#!/usr/bin/env python3
from __future__ import annotations
import csv, json, os, random, time, urllib.error, urllib.parse, urllib.request
from pathlib import Path
from typing import Any

BASE='https://robinscan.io'
ZERO='0x0000000000000000000000000000000000000000'
UA='RHC-Wallet-Provenance/1.0 (read-only)'
OUT=Path(os.environ.get('OUT','out-wallet-probe')); OUT.mkdir(parents=True,exist_ok=True)
CANDIDATES=json.loads(Path(os.environ.get('CANDIDATES','wallet-probe/candidates.json')).read_text())
last=0.0

def pace():
 global last
 wait=.22-(time.monotonic()-last)
 if wait>0: time.sleep(wait)

def get(path:str, params:dict[str,Any]|None=None, attempts:int=10):
 global last
 url=BASE+path
 if params:
  url+='?'+urllib.parse.urlencode({k:v for k,v in params.items() if v is not None})
 err=None
 for i in range(attempts):
  pace()
  try:
   req=urllib.request.Request(url,headers={'user-agent':UA,'accept':'application/json'})
   with urllib.request.urlopen(req,timeout=45) as r:
    raw=r.read(); status=r.status
   last=time.monotonic()
   return status,json.loads(raw.decode())
  except urllib.error.HTTPError as e:
   last=time.monotonic(); err=e
   body=e.read(1000).decode('utf-8','replace')
   if e.code in (401,403,404): return e.code,{'error':body}
   if e.code==429 or e.code>=500:
    time.sleep(min(60,2**i+random.random()*3)); continue
   raise
  except Exception as e:
   err=e
   if i+1<attempts: time.sleep(min(30,2**i+random.random()))
 raise RuntimeError(f'{url}: {err}')

def paginate(path:str):
 rows=[]; cursor=None; seen=set(); pages=0
 while True:
  status,d=get(path,{'cursor':cursor} if cursor else None)
  if status!=200: raise RuntimeError(f'{path}: HTTP {status}: {d}')
  items=d.get('items') or []
  if not isinstance(items,list): raise RuntimeError(f'{path}: invalid items')
  rows.extend(x for x in items if isinstance(x,dict)); pages+=1
  nxt=d.get('next') or d.get('next_cursor') or d.get('next_page_params')
  if not nxt: break
  if isinstance(nxt,dict):
   cursor=nxt.get('cursor') or json.dumps(nxt,separators=(',',':'),sort_keys=True)
  else: cursor=str(nxt)
  if cursor in seen: raise RuntimeError(f'{path}: repeated cursor')
  seen.add(cursor)
  if pages>10000: raise RuntimeError(f'{path}: page limit')
 return rows,pages

def addr(v):
 if isinstance(v,dict): v=v.get('hash') or v.get('address') or v.get('address_hash')
 return str(v).lower() if v else None

def write_csv(path, rows):
 fields=sorted({k for r in rows for k in r}) if rows else ['empty']
 with path.open('w',encoding='utf-8-sig',newline='') as f:
  w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader()
  for r in rows:w.writerow({k:json.dumps(v,ensure_ascii=False,sort_keys=True) if isinstance(v,(dict,list)) else v for k,v in r.items()})

summ=[]; all_txs=[]; all_transfers=[]; errors=[]; partner=[]
for c in CANDIDATES:
 a=c['address'].lower(); print('wallet',a,flush=True)
 try:
  s,counters=get(f'/api/address/{a}/counters')
  if s!=200: raise RuntimeError(f'counters HTTP {s}: {counters}')
  txs,tx_pages=paginate(f'/api/address/{a}/txs')
  transfers,tr_pages=paginate(f'/api/address/{a}/transfers')
  for x in txs: all_txs.append({'wallet_address':a,**x})
  for x in transfers: all_transfers.append({'wallet_address':a,**x})
  nft=[]
  for x in transfers:
   dec=x.get('decimals'); token=x.get('token') or {}; token_id=x.get('tokenId') or x.get('token_id') or x.get('id')
   symbol=str(token.get('symbol') or '').lower(); name=str(token.get('name') or '').lower()
   is_nft=(token_id is not None) or str(dec) in ('0','None','') or any(k in symbol+' '+name for k in ('nft','erc721','erc1155'))
   if is_nft: nft.append(x)
  mint_receives=[x for x in nft if addr(x.get('from'))==ZERO and addr(x.get('to'))==a]
  outgoing=[x for x in nft if addr(x.get('from'))==a and addr(x.get('to')) not in (None,ZERO)]
  nft_contracts=sorted({addr((x.get('token') or {}).get('address_hash') or (x.get('token') or {}).get('address')) for x in nft if addr((x.get('token') or {}).get('address_hash') or (x.get('token') or {}).get('address'))})
  summ.append({
   **c,'address':a,'counter_transactions':counters.get('transactions'),'counter_token_transfers':counters.get('tokenTransfers'),
   'tx_rows':len(txs),'tx_pages':tx_pages,'transfer_rows':len(transfers),'transfer_pages':tr_pages,
   'nft_like_transfer_rows':len(nft),'zero_address_nft_receives':len(mint_receives),'outgoing_nft_transfer_rows':len(outgoing),
   'unique_nft_contracts':len(nft_contracts),'nft_contracts_json':nft_contracts,'history_fetch_status':'PASS','production_approved':False,
   'decision_use':'WALLET_INVESTIGATION_QUEUE_ONLY'})
 except Exception as e:
  errors.append({'address':a,'error':repr(e)}); summ.append({**c,'address':a,'history_fetch_status':'FAIL','error':repr(e),'production_approved':False,'decision_use':'WALLET_INVESTIGATION_QUEUE_ONLY'})
 try:
  st,d=get(f'/api/v1/wallets/{a}/pnl',{'chainId':4663,'maxTransfers':2000,'maxTxs':2000},attempts=2)
  partner.append({'address':a,'http_status':st,'response_prefix':json.dumps(d,ensure_ascii=False)[:1000]})
 except Exception as e: partner.append({'address':a,'http_status':None,'response_prefix':repr(e)})

write_csv(OUT/'wallet_summary.csv',summ);write_csv(OUT/'transactions.csv',all_txs);write_csv(OUT/'transfers.csv',all_transfers);write_csv(OUT/'errors.csv',errors);write_csv(OUT/'partner_pnl_probe.csv',partner)
validation={
 'status':'PASS' if not errors else 'PARTIAL', 'candidate_count':len(CANDIDATES),'wallets_passed':sum(r.get('history_fetch_status')=='PASS' for r in summ),
 'wallets_failed':len(errors),'transaction_rows':len(all_txs),'transfer_rows':len(all_transfers),
 'all_candidates_have_explicit_source':all(bool(c.get('source') and c.get('evidence')) for c in CANDIDATES),
 'production_approved_wallets':sum(bool(r.get('production_approved')) for r in summ),
 'partner_pnl_publicly_accessible':any(r.get('http_status')==200 for r in partner)}
(OUT/'validation.json').write_text(json.dumps(validation,indent=2,sort_keys=True),encoding='utf-8')
print(json.dumps(validation,sort_keys=True))
