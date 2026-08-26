#!/usr/bin/env python3
from __future__ import annotations
import csv, hashlib, json, os, random, re, time
import urllib.error, urllib.parse, urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

OUT=Path(os.getenv('OUT','out-targeted'));OUT.mkdir(parents=True,exist_ok=True)
BLOCKSCOUT='https://robinhoodchain.blockscout.com/api'
RPC='https://rpc.mainnet.chain.robinhood.com/rpc'
SEADROP='0x00005ea00ac477b1030ce78506496e8c2de24bf5'
TOPIC0='0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6'
UA='RHC-SeaDrop-Targeted-Research/0.2'
ADDR_RE=re.compile(r'0x[a-fA-F0-9]{40}')

def now():return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00','Z')
def get_json(url,attempts=8,delay=.25):
    last=None
    for i in range(attempts):
        try:
            req=urllib.request.Request(url,headers={'user-agent':UA,'accept':'application/json,text/html,*/*'})
            with urllib.request.urlopen(req,timeout=90) as r:
                raw=r.read();time.sleep(delay);return json.loads(raw.decode())
        except Exception as e:
            last=e
            if i+1==attempts:break
            time.sleep(min(30,2**i+random.random()))
    raise RuntimeError(f'{url}: {last}')
def get_text(url,attempts=6):
    last=None
    for i in range(attempts):
        try:
            req=urllib.request.Request(url,headers={'user-agent':UA,'accept':'text/html,*/*'})
            with urllib.request.urlopen(req,timeout=90) as r:return r.read(20_000_000).decode('utf-8','replace')
        except Exception as e:
            last=e;time.sleep(min(20,2**i+random.random()))
    raise RuntimeError(f'{url}: {last}')
def rpc(method,params):
    body=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params}).encode()
    req=urllib.request.Request(RPC,data=body,headers={'content-type':'application/json','user-agent':UA})
    with urllib.request.urlopen(req,timeout=90) as r:data=json.loads(r.read().decode())
    if 'error' in data:raise RuntimeError(data['error'])
    return data['result']
def topic_addr(a):return '0x'+'0'*24+a.lower()[2:]
def words(data):
    s=data[2:] if data.startswith('0x') else data
    return [int(s[i:i+64],16) for i in range(0,len(s),64) if len(s[i:i+64])==64]
def to_addr(t):return '0x'+t[-40:].lower()
def write_csv(p,rows):
    rows=list(rows);fields=sorted({k for r in rows for k in r}) if rows else ['empty']
    with p.open('w',encoding='utf-8-sig',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader()
        for r in rows:w.writerow({k:json.dumps(v,ensure_ascii=False,sort_keys=True) if isinstance(v,(list,dict)) else v for k,v in r.items()})
def fetch_logs(contract,start,end,depth=0):
    params={'module':'logs','action':'getLogs','fromBlock':start,'toBlock':end,'address':SEADROP,'topic0':TOPIC0,'topic1':topic_addr(contract),'topic0_1_opr':'and'}
    url=BLOCKSCOUT+'?'+urllib.parse.urlencode(params)
    data=get_json(url)
    result=data.get('result') if isinstance(data,dict) else None
    if isinstance(result,str):
        if 'No logs found' in result or data.get('status')=='0':return []
        raise RuntimeError(result)
    result=result or []
    if len(result)<1000:return result
    if start>=end:raise RuntimeError(f'1000-log truncation at single block {start}')
    mid=(start+end)//2
    return fetch_logs(contract,start,mid,depth+1)+fetch_logs(contract,mid+1,end,depth+1)

def main():
    latest=int(rpc('eth_blockNumber',[]),16)
    source_rows=[];addresses={}
    # NFT Trencher / neverfuckingtrade current+upcoming public feed.
    html=get_text('https://www.neverfuckingtrade.com/')
    m=re.search(r'<section[^>]+data-chain="robinhood"[\s\S]*?</section>',html,re.I)
    section=m.group(0) if m else html
    for a in sorted(set(x.lower() for x in ADDR_RE.findall(section))):
        addresses.setdefault(a,set()).add('NFT_TRENCHER')
    source_rows.append({'source':'NFT_TRENCHER','address_count':sum('NFT_TRENCHER' in v for v in addresses.values())})
    # MintGo radar current/upcoming feed.
    radar=get_json('https://mintgo.fun/api/seadrop-radar?chain=robinhood')
    names={}
    for r in radar.get('items',[]):
        a=str(r.get('contractAddress') or '').lower()
        if re.fullmatch(r'0x[a-f0-9]{40}',a):
            addresses.setdefault(a,set()).add('MINTGO_RADAR');names[a]=r.get('displayName')
    source_rows.append({'source':'MINTGO_RADAR','address_count':sum('MINTGO_RADAR' in v for v in addresses.values())})
    target_rows=[{'contract_address':a,'name':names.get(a),'sources':sorted(s)} for a,s in sorted(addresses.items())]
    write_csv(OUT/'target_contracts.csv',target_rows);write_csv(OUT/'source_counts.csv',source_rows)

    raw=[];errors=[]
    for i,r in enumerate(target_rows,1):
        a=r['contract_address'];print(f'LOGS {i}/{len(target_rows)} {a} {r.get("name") or ""}',flush=True)
        try:logs=fetch_logs(a,0,latest)
        except Exception as e:errors.append({'contract_address':a,'error':str(e)});continue
        for x in logs:x['_target_contract']=a
        raw.extend(logs)
    # dedupe and decode
    d={}
    for x in raw:d[(str(x.get('transactionHash')).lower(),str(x.get('logIndex')))] = x
    raw=sorted(d.values(),key=lambda x:(int(str(x.get('blockNumber','0x0')),16),int(str(x.get('logIndex','0x0')),16)))
    events=[]
    for x in raw:
        ts=x.get('timeStamp');ts=int(ts,16) if isinstance(ts,str) and ts.startswith('0x') else None
        ws=words(x.get('data','0x'));topics=x.get('topics') or []
        if len(topics)<4 or len(ws)<5:continue
        events.append({'transaction_hash':str(x.get('transactionHash')).lower(),'log_index':int(str(x.get('logIndex','0x0')),16),
          'block_number':int(str(x.get('blockNumber','0x0')),16),'timestamp_utc':datetime.fromtimestamp(ts,timezone.utc).isoformat().replace('+00:00','Z') if ts else None,
          'nft_contract':to_addr(topics[1]),'minter':to_addr(topics[2]),'fee_recipient':to_addr(topics[3]),
          'payer':'0x'+ws[0].to_bytes(32,'big')[-20:].hex(),'quantity':ws[1],'unit_mint_price_wei':ws[2],
          'unit_mint_price_eth':ws[2]/1e18,'gross_mint_value_wei':ws[1]*ws[2],'gross_mint_value_eth':ws[1]*ws[2]/1e18,
          'fee_bps':ws[3],'drop_stage_index':ws[4],'is_free':ws[2]==0,'is_paid':ws[2]>0,'source':'BLOCKSCOUT_SEADROP_LOGS'})
    by=defaultdict(list)
    for e in events:by[e['nft_contract']].append(e)
    summary=[]
    for r in target_rows:
        a=r['contract_address'];rows=sorted(by.get(a,[]),key=lambda e:(e['block_number'],e['log_index']))
        qty=sum(e['quantity'] for e in rows);free=sum(e['quantity'] for e in rows if e['is_free']);paid=qty-free
        first=rows[0] if rows else {}
        if not rows:model='NO_SEADROP_MINT_EVENT_OBSERVED'
        elif free and paid:model='MIXED_FREE_AND_PAID_OBSERVED'
        elif free:model='FREE_ONLY_OBSERVED'
        elif paid:model='PAID_ONLY_OBSERVED'
        else:model='UNRESOLVED'
        summary.append({**r,'event_count':len(rows),'minted_quantity':qty,'free_quantity':free,'paid_quantity':paid,
          'unique_minters':len({e['minter'] for e in rows}),'unique_payers':len({e['payer'] for e in rows}),
          'first_mint_timestamp_utc':first.get('timestamp_utc'),'first_mint_price_wei':first.get('unit_mint_price_wei'),
          'first_mint_stage_index':first.get('drop_stage_index'),'observed_prices_wei':sorted({e['unit_mint_price_wei'] for e in rows}),
          'observed_stage_indexes':sorted({e['drop_stage_index'] for e in rows}),'observed_model':model,
          'paid_from_first_observed':bool(rows and first.get('is_paid') and free==0),'production_approved':False})
    write_csv(OUT/'seadrop_mint_events.csv',events);write_csv(OUT/'seadrop_target_summary.csv',summary);write_csv(OUT/'errors.csv',errors)
    with (OUT/'seadrop_mint_events.jsonl').open('w',encoding='utf-8') as f:
        for e in events:f.write(json.dumps(e,ensure_ascii=False,sort_keys=True)+'\n')
    validation={'status':'PASS' if not errors and len(target_rows)>100 else 'PARTIAL','generated_at_utc':now(),'latest_block':latest,
      'target_contracts':len(target_rows),'event_rows':len(events),'contracts_with_events':sum(bool(r['event_count']) for r in summary),
      'paid_only_contracts':sum(r['observed_model']=='PAID_ONLY_OBSERVED' for r in summary),'mixed_contracts':sum(r['observed_model']=='MIXED_FREE_AND_PAID_OBSERVED' for r in summary),
      'free_only_contracts':sum(r['observed_model']=='FREE_ONLY_OBSERVED' for r in summary),'no_event_contracts':sum(r['observed_model']=='NO_SEADROP_MINT_EVENT_OBSERVED' for r in summary),
      'error_count':len(errors)}
    (OUT/'validation.json').write_text(json.dumps(validation,indent=2),encoding='utf-8')
    print(json.dumps(validation),flush=True)
    if validation['status']=='PARTIAL' and len(errors)>10:raise SystemExit(1)
if __name__=='__main__':main()
