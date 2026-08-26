#!/usr/bin/env python3
import json,urllib.request
from pathlib import Path
RPC='https://rpc.mainnet.chain.robinhood.com/rpc'
TX='0x3db5fe30892fe0ed96bb04de878a3220f84acd687ef486370d7973d0561914e2'
OUT=Path('out-sale-receipt');OUT.mkdir(exist_ok=True)
def call(method,params):
 body=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params}).encode();req=urllib.request.Request(RPC,data=body,headers={'content-type':'application/json','user-agent':'RHC-Sale-Research/0.1'})
 with urllib.request.urlopen(req,timeout=90) as r:d=json.loads(r.read().decode())
 if 'error' in d:raise RuntimeError(d['error'])
 return d['result']
tx=call('eth_getTransactionByHash',[TX]);receipt=call('eth_getTransactionReceipt',[TX]);block=call('eth_getBlockByNumber',[tx['blockNumber'],False])
summary={'tx_hash':TX,'to':tx.get('to'),'from':tx.get('from'),'value_wei':int(tx.get('value','0x0'),16),'input_selector':tx.get('input','0x')[:10],'block_number':int(tx['blockNumber'],16),'timestamp':int(block['timestamp'],16),'status':int(receipt['status'],16),'log_count':len(receipt.get('logs',[])),'log_emitters':sorted(set(x['address'].lower() for x in receipt.get('logs',[]))),'topic0_counts':{}}
for log in receipt.get('logs',[]):
 t0=(log.get('topics') or [None])[0]
 if t0:summary['topic0_counts'][t0]=summary['topic0_counts'].get(t0,0)+1
(OUT/'transaction.json').write_text(json.dumps(tx,indent=2),encoding='utf-8');(OUT/'receipt.json').write_text(json.dumps(receipt,indent=2),encoding='utf-8');(OUT/'block.json').write_text(json.dumps(block,indent=2),encoding='utf-8');(OUT/'summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8');print(json.dumps(summary))
