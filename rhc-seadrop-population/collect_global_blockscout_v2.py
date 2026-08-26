#!/usr/bin/env python3
from __future__ import annotations

import csv, json, os, random, time, urllib.error, urllib.parse, urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

OUT=Path(os.getenv('OUT','out-global-blockscout-v2'));OUT.mkdir(parents=True,exist_ok=True)
BLOCKSCOUT='https://robinhoodchain.blockscout.com/api'
RPC='https://rpc.mainnet.chain.robinhood.com/rpc'
SEADROP='0x00005ea00ac477b1030ce78506496e8c2de24bf5'
TOPIC0='0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6'
UA='RHC-SeaDrop-Global-Research/0.2'
last_request=0.0;api_calls=0;backoffs=0;ranges=[]

def now():return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00','Z')
def pace(seconds=1.05):
    global last_request
    left=seconds-(time.monotonic()-last_request)
    if left>0:time.sleep(left)
def get_json(url,attempts=20):
    global last_request,api_calls,backoffs
    last=None
    for i in range(attempts):
        pace();api_calls+=1
        try:
            req=urllib.request.Request(url,headers={'user-agent':UA,'accept':'application/json'})
            with urllib.request.urlopen(req,timeout=120) as r:raw=r.read();last_request=time.monotonic()
            return json.loads(raw.decode())
        except urllib.error.HTTPError as e:
            last=e;last_request=time.monotonic()
            if e.code==429:
                backoffs+=1
                retry=e.headers.get('Retry-After')
                wait=float(retry) if retry and retry.isdigit() else min(180,45+i*12+random.random()*10)
                print(f'429 backoff {wait:.1f}s for {url}',flush=True);time.sleep(wait);continue
            if e.code in (500,502,503,504):time.sleep(min(60,2**i+random.random()));continue
            raise
        except Exception as e:
            last=e;time.sleep(min(60,2**i+random.random()))
    raise RuntimeError(f'{url}: {last}')
def rpc(method,params):
    body=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params}).encode()
    req=urllib.request.Request(RPC,data=body,headers={'content-type':'application/json','user-agent':UA})
    with urllib.request.urlopen(req,timeout=90) as r:d=json.loads(r.read().decode())
    if 'error' in d:raise RuntimeError(d['error'])
    return d['result']
def words(data):
    s=data[2:] if data.startswith('0x') else data
    return [int(s[i:i+64],16) for i in range(0,len(s),64) if len(s[i:i+64])==64]
def to_addr(topic):return '0x'+topic[-40:].lower()
def write_csv(path,rows):
    rows=list(rows);fields=sorted({k for r in rows for k in r}) if rows else ['empty']
    with Path(path).open('w',encoding='utf-8-sig',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader()
        for r in rows:w.writerow({k:json.dumps(v,ensure_ascii=False,sort_keys=True) if isinstance(v,(list,dict)) else v for k,v in r.items()})
def request_range(start,end,depth=0):
    params={'module':'logs','action':'getLogs','fromBlock':start,'toBlock':end,'address':SEADROP,'topic0':TOPIC0}
    data=get_json(BLOCKSCOUT+'?'+urllib.parse.urlencode(params));result=data.get('result') if isinstance(data,dict) else None
    if isinstance(result,str):
        if data.get('status')=='0' or 'No logs found' in result:
            ranges.append({'from_block':start,'to_block':end,'depth':depth,'rows':0,'action':'ACCEPT_EMPTY'});return []
        raise RuntimeError(result)
    result=result or []
    if len(result)<1000:
        ranges.append({'from_block':start,'to_block':end,'depth':depth,'rows':len(result),'action':'ACCEPT'});return result
    if start>=end:raise RuntimeError(f'1000 log truncation at single block {start}')
    mid=(start+end)//2;ranges.append({'from_block':start,'to_block':end,'depth':depth,'rows':len(result),'action':'SPLIT'})
    return request_range(start,mid,depth+1)+request_range(mid+1,end,depth+1)
def main():
    latest=int(rpc('eth_blockNumber',[]),16);raw=[];fixed_chunk=250_000
    total_chunks=(latest//fixed_chunk)+1
    for idx,start in enumerate(range(0,latest+1,fixed_chunk),1):
        end=min(latest,start+fixed_chunk-1)
        got=request_range(start,end);raw.extend(got)
        print(f'chunk {idx}/{total_chunks} blocks {start}-{end} rows={len(got)} total={len(raw)}',flush=True)
    dedup={}
    for x in raw:dedup[(str(x.get('transactionHash')).lower(),str(x.get('logIndex')))] = x
    raw=sorted(dedup.values(),key=lambda x:(int(str(x.get('blockNumber','0x0')),16),int(str(x.get('logIndex','0x0')),16)))
    events=[]
    for x in raw:
        ws=words(x.get('data','0x'));topics=x.get('topics') or []
        if len(ws)<5 or len(topics)<4:continue
        ts=x.get('timeStamp');ts=int(ts,16) if isinstance(ts,str) and ts.startswith('0x') else None
        events.append({'transaction_hash':str(x.get('transactionHash')).lower(),'log_index':int(str(x.get('logIndex','0x0')),16),'block_number':int(str(x.get('blockNumber','0x0')),16),'timestamp_utc':datetime.fromtimestamp(ts,timezone.utc).isoformat().replace('+00:00','Z') if ts else None,'nft_contract':to_addr(topics[1]),'minter':to_addr(topics[2]),'fee_recipient':to_addr(topics[3]),'payer':'0x'+ws[0].to_bytes(32,'big')[-20:].hex(),'quantity':ws[1],'unit_mint_price_wei':ws[2],'unit_mint_price_eth':ws[2]/1e18,'gross_mint_value_wei':ws[1]*ws[2],'gross_mint_value_eth':ws[1]*ws[2]/1e18,'fee_bps':ws[3],'drop_stage_index':ws[4],'is_free':ws[2]==0,'is_paid':ws[2]>0,'source':'BLOCKSCOUT_CANONICAL_SEADROP_GLOBAL'})
    by=defaultdict(list);wp=defaultdict(list)
    for e in events:by[e['nft_contract']].append(e);wp[(e['minter'],e['nft_contract'])].append(e)
    collections=[]
    for c,rows in sorted(by.items(),key=lambda kv:min(e['block_number'] for e in kv[1])):
        rows=sorted(rows,key=lambda e:(e['block_number'],e['log_index']));total=sum(e['quantity'] for e in rows);free=sum(e['quantity'] for e in rows if e['is_free']);paid=total-free;first=rows[0]
        model='MIXED_FREE_AND_PAID_OBSERVED' if free and paid else ('FREE_ONLY_OBSERVED' if free else 'PAID_ONLY_OBSERVED')
        collections.append({'nft_contract':c,'first_mint_timestamp_utc':first['timestamp_utc'],'first_mint_block':first['block_number'],'first_mint_price_wei':first['unit_mint_price_wei'],'first_stage_index':first['drop_stage_index'],'last_mint_timestamp_utc':rows[-1]['timestamp_utc'],'last_mint_block':rows[-1]['block_number'],'event_count':len(rows),'minted_quantity':total,'free_quantity':free,'paid_quantity':paid,'unique_minters':len({e['minter'] for e in rows}),'unique_payers':len({e['payer'] for e in rows}),'observed_stage_indexes':sorted({e['drop_stage_index'] for e in rows}),'observed_prices_wei':sorted({e['unit_mint_price_wei'] for e in rows}),'observed_model':model,'paid_from_first_observed':bool(first['is_paid'] and free==0),'production_approved':False})
    wallet_rows=[]
    for (wallet,c),rows in sorted(wp.items()):
        rows=sorted(rows,key=lambda e:(e['block_number'],e['log_index']))
        wallet_rows.append({'wallet':wallet,'nft_contract':c,'first_entry_timestamp_utc':rows[0]['timestamp_utc'],'first_entry_block':rows[0]['block_number'],'first_entry_price_wei':rows[0]['unit_mint_price_wei'],'first_entry_stage_index':rows[0]['drop_stage_index'],'mint_event_count':len(rows),'minted_quantity':sum(e['quantity'] for e in rows),'free_quantity':sum(e['quantity'] for e in rows if e['is_free']),'paid_quantity':sum(e['quantity'] for e in rows if e['is_paid']),'total_primary_cost_wei':sum(e['gross_mint_value_wei'] for e in rows),'total_primary_cost_eth':sum(e['gross_mint_value_eth'] for e in rows),'production_approved':False})
    write_csv(OUT/'seadrop_global_events.csv',events);write_csv(OUT/'seadrop_global_collections.csv',collections);write_csv(OUT/'seadrop_global_wallet_project_entries.csv',wallet_rows);write_csv(OUT/'scan_ranges.csv',ranges)
    with (OUT/'seadrop_global_events.jsonl').open('w',encoding='utf-8') as f:
        for e in events:f.write(json.dumps(e,ensure_ascii=False,sort_keys=True)+'\n')
    validation={'status':'PASS' if events and collections and len(raw)==len(dedup) else 'FAIL','generated_at_utc':now(),'latest_block':latest,'fixed_chunk':fixed_chunk,'api_calls':api_calls,'rate_limit_backoffs':backoffs,'raw_rows':len(raw),'event_rows':len(events),'collection_rows':len(collections),'wallet_project_rows':len(wallet_rows),'paid_only_collections':sum(x['observed_model']=='PAID_ONLY_OBSERVED' for x in collections),'mixed_collections':sum(x['observed_model']=='MIXED_FREE_AND_PAID_OBSERVED' for x in collections),'free_only_collections':sum(x['observed_model']=='FREE_ONLY_OBSERVED' for x in collections),'paid_from_first_observed_collections':sum(bool(x['paid_from_first_observed']) for x in collections),'range_rows':len(ranges)}
    (OUT/'validation.json').write_text(json.dumps(validation,indent=2),encoding='utf-8');print(json.dumps(validation),flush=True)
    if validation['status']!='PASS':raise SystemExit(1)
if __name__=='__main__':main()
