#!/usr/bin/env python3
from pathlib import Path

path = Path(__file__).with_name('collector.py')
s = path.read_text(encoding='utf-8')

old_loop = 'for pn,d,items in page(net,f"/api/address/{a}/transfers",{"scope":"token"},args.max_pages):'
new_loop = 'for pn,d,items in blockscout_pages(net,f"tokens/{a}/transfers",args.max_pages):'
if old_loop not in s:
    raise SystemExit('expected Robinscan transfer loop not found')
s = s.replace(old_loop, new_loop, 1)

old_hashes = 'txhashes=sorted({r["tx_hash"] for r in all_tr if r.get("tx_hash")})'
new_hashes = 'txhashes=sorted({r["tx_hash"] for r in all_tr if r.get("tx_hash") and r["event_kind"]=="MINT"})'
if old_hashes not in s:
    raise SystemExit('expected transaction hash selector not found')
s = s.replace(old_hashes, new_hashes, 1)

anchor = '\ndef load_contracts(path):\n'
insert = '''\ndef blockscout_pages(net,path,max_pages):
    params={}
    seen=set()
    for n in range(1,max_pages+1):
        d=net.get(BLOCKSCOUT,path,params)
        if not isinstance(d,dict):
            raise RuntimeError(f"bad Blockscout page {path}: {type(d)}")
        items=d.get("items") or []
        if not isinstance(items,list):
            raise RuntimeError(f"bad Blockscout items {path}: {type(items)}")
        yield n,d,items
        nxt=d.get("next_page_params")
        if not nxt:
            return
        key=json.dumps(nxt,sort_keys=True)
        if key in seen:
            raise RuntimeError(f"repeated Blockscout cursor {path}")
        seen.add(key)
        params={k:v for k,v in nxt.items() if v is not None}
    raise RuntimeError(f"max Blockscout pages hit: {path}")
'''
if anchor not in s:
    raise SystemExit('load_contracts anchor not found')
s = s.replace(anchor, insert + anchor, 1)
path.write_text(s, encoding='utf-8')
print('RUNTIME PATCH PASS')
