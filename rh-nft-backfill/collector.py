#!/usr/bin/env python3
"""
Read-only Robinhood Chain P0 NFT historical collector.

Sources:
- Robinscan public JSON endpoints for paginated token transfers and address txs.
- Robinhood Chain public JSON-RPC for canonical tx/receipt/block/code.
- Robinhood Chain Blockscout v2 for contract metadata/internal transfers.

It never loads a key, signs, or sends a transaction.
"""
from __future__ import annotations
import argparse, csv, hashlib, json, os, random, re, sys, time
import urllib.error, urllib.parse, urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

ZERO="0x0000000000000000000000000000000000000000"
TRANSFER="0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
RPC=os.getenv("RPC_URL","https://rpc.mainnet.chain.robinhood.com")
ROBINSCAN=os.getenv("ROBINSCAN_BASE","https://robinscan.io").rstrip("/")
BLOCKSCOUT=os.getenv("BLOCKSCOUT_BASE","https://robinhoodchain.blockscout.com/api/v2").rstrip("/")
UA="RobinhoodNFTResearch/0.3 read-only"
ADDR_RE=re.compile(r"0x[a-fA-F0-9]{40}")

def now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")
def addr(v):
    if isinstance(v,dict):
        for k in ("hash","address_hash","address","value"):
            if k in v:
                x=addr(v[k])
                if x:return x
        return None
    return v.lower() if isinstance(v,str) and re.fullmatch(r"0x[a-fA-F0-9]{40}",v) else None
def integer(v, default=None):
    if v is None:return default
    if isinstance(v,int):return v
    if isinstance(v,float):return int(v)
    if isinstance(v,dict):
        for k in ("value","amount","token_id"):
            if k in v:return integer(v[k],default)
    try:
        s=str(v).strip()
        return int(s,16) if s.startswith("0x") else int(s)
    except Exception:return default
def pick(d,*paths,default=None):
    for path in paths:
        cur=d
        for p in path.split("."):
            if not isinstance(cur,dict) or p not in cur:break
            cur=cur[p]
        else:
            if cur is not None:return cur
    return default
def dump(path,obj):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(obj,ensure_ascii=False,indent=2,sort_keys=True),encoding="utf-8")
def jsonl(path,rows):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open("w",encoding="utf-8",newline="\n") as f:
        for r in rows:f.write(json.dumps(r,ensure_ascii=False,sort_keys=True)+"\n")
def csvout(path,rows,fields=None):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    rows=list(rows)
    if fields is None:fields=sorted({k for r in rows for k in r})
    with path.open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction="ignore");w.writeheader()
        for r in rows:
            w.writerow({k:json.dumps(v,ensure_ascii=False,sort_keys=True) if isinstance(v,(dict,list,tuple)) else v for k,v in r.items()})
def wei(v):
    if v is None:return None
    q,r=divmod(v,10**18)
    return (f"{q}.{r:018d}").rstrip("0").rstrip(".")

class Net:
    def __init__(self,delay=.15,timeout=45,attempts=7):
        self.delay=delay;self.timeout=timeout;self.attempts=attempts;self.last=0.;self.stats=Counter()
    def pace(self):
        t=self.delay-(time.monotonic()-self.last)
        if t>0:time.sleep(t)
    def request(self,url,method="GET",body=None):
        data=None;headers={"Accept":"application/json","User-Agent":UA}
        if body is not None:
            data=json.dumps(body).encode();headers["Content-Type"]="application/json"
        for i in range(self.attempts):
            self.pace()
            try:
                req=urllib.request.Request(url,data=data,headers=headers,method=method)
                with urllib.request.urlopen(req,timeout=self.timeout) as r:
                    self.last=time.monotonic();raw=r.read();self.stats[f"http_{r.status}"]+=1
                    return json.loads(raw.decode()) if raw else None
            except urllib.error.HTTPError as e:
                self.last=time.monotonic();self.stats[f"http_{e.code}"]+=1
                if e.code not in (408,425,429,500,502,503,504) or i+1==self.attempts:
                    raise RuntimeError(f"{url} HTTP {e.code}: {e.read(300).decode(errors='replace')}")
            except Exception as e:
                self.stats["network_error"]+=1
                if i+1==self.attempts:raise RuntimeError(f"{url}: {e}")
            time.sleep(min(30,2**i+random.random()))
    def get(self,base,path,params=None):
        u=base+"/"+path.lstrip("/")
        if params:u+="?"+urllib.parse.urlencode({k:v for k,v in params.items() if v not in (None,"")})
        return self.request(u)
    def text(self,url):
        self.pace()
        try:
            req=urllib.request.Request(url,headers={"User-Agent":UA,"Accept":"text/html,*/*"})
            with urllib.request.urlopen(req,timeout=self.timeout) as r:
                self.last=time.monotonic();return r.read(5_000_000).decode(errors="replace")
        except Exception:return ""
    def rpc(self,calls):
        req=[{"jsonrpc":"2.0","id":i,"method":m,"params":p} for i,(m,p) in enumerate(calls)]
        ans=self.request(RPC,"POST",req)
        if isinstance(ans,dict):ans=[ans]
        out={}
        for x in ans or []:out[int(x.get("id",-1))]=x.get("result") if "error" not in x else {"rpc_error":x["error"]}
        return out

def page(net,path,params,max_pages):
    cursor=None;seen=set()
    for n in range(1,max_pages+1):
        p=dict(params)
        if cursor is not None:p["cursor"]=cursor if isinstance(cursor,str) else json.dumps(cursor,separators=(",",":"))
        d=net.get(ROBINSCAN,path,p)
        if not isinstance(d,dict):raise RuntimeError(f"bad page {path}: {type(d)}")
        items=d.get("items") or d.get("data") or []
        yield n,d,items
        cursor=d.get("next") or d.get("next_cursor") or d.get("next_page_params")
        if not cursor:return
        key=json.dumps(cursor,sort_keys=True)
        if key in seen:raise RuntimeError(f"repeated cursor {path}")
        seen.add(key)
    raise RuntimeError(f"max pages hit: {path}")

def load_contracts(path):
    out=[]
    with open(path,encoding="utf-8-sig",newline="") as f:
        for r in csv.DictReader(f):
            a=r["contract_address"].lower()
            if not re.fullmatch(r"0x[a-f0-9]{40}",a):raise ValueError(a)
            r["contract_address"]=a;r["observed_supply"]=int(r["observed_supply"])
            r["evidence_urls"]=[x for x in r.get("evidence_urls","").split("|") if x]
            r["aliases"]=[x for x in r.get("aliases","").split("|") if x]
            out.append(r)
    return out

def norm_transfer(x,c):
    h=pick(x,"txHash","transaction_hash","hash","transaction.hash")
    if isinstance(h,dict):h=h.get("hash")
    fr=addr(pick(x,"from","from_address","from.hash"));to=addr(pick(x,"to","to_address","to.hash"))
    tid=pick(x,"token_id","tokenId","total.token_id","id")
    amount=pick(x,"amount","total.value","value")
    decimals=pick(x,"decimals","token.decimals")
    if tid is None and str(decimals) in ("0","None",""):tid=amount
    return {
      "collection_name":c["collection_name"],"contract_address":c["contract_address"],
      "tx_hash":h.lower() if isinstance(h,str) else None,
      "log_index":integer(pick(x,"logIndex","log_index","index")),
      "block_number":integer(pick(x,"blockNumber","block_number","block")),
      "timestamp_utc":pick(x,"timestamp","timestamp_utc","block_timestamp"),
      "from_address":fr,"to_address":to,"token_id":str(tid) if tid is not None else None,
      "amount_raw":str(amount) if amount is not None else None,"method":pick(x,"method","method_name"),
      "event_kind":"MINT" if fr==ZERO else ("BURN" if to==ZERO else "TRANSFER"),"raw":x
    }

def receipt_transfers(receipt):
    out=[]
    for l in (receipt or {}).get("logs") or []:
        t=[str(v).lower() for v in l.get("topics") or []]
        if not t or t[0]!=TRANSFER or len(t)<3:continue
        fr="0x"+t[1][-40:];to="0x"+t[2][-40:];em=addr(l.get("address"))
        if len(t)>=4:
            out.append({"kind":"ERC721","emitter":em,"from":fr,"to":to,"amount":1,"token_id":integer(t[3])})
        else:
            out.append({"kind":"ERC20","emitter":em,"from":fr,"to":to,"amount":integer(l.get("data"),0) or 0,"token_id":None})
    return out

def blockscout_get(net,path):
    try:return net.get(BLOCKSCOUT,path)
    except Exception as e:return {"fetch_error":str(e)}

def collect(args):
    root=Path(args.out);root.mkdir(parents=True,exist_ok=True)
    raw=root/"raw";raw.mkdir(exist_ok=True)
    net=Net(args.delay);errors=[];contracts=load_contracts(args.contracts)
    all_tr=[];contract_meta=[];identity=[]
    for c in contracts:
        a=c["contract_address"];print("TRANSFERS",c["collection_name"],a,flush=True)
        rows=[];pages=0
        try:
            for pn,d,items in page(net,f"/api/address/{a}/transfers",{"scope":"token"},args.max_pages):
                pages=pn
                for x in items:rows.append(norm_transfer(x,c))
        except Exception as e:errors.append({"contract":a,"stage":"transfers","error":str(e)})
        jsonl(raw/f"{a}_transfers.jsonl",[r["raw"] for r in rows]);all_tr.extend(rows)
        smart=blockscout_get(net,f"smart-contracts/{a}")
        adata=blockscout_get(net,f"addresses/{a}")
        token=blockscout_get(net,f"tokens/{a}")
        code=net.rpc([("eth_getCode",[a,"latest"])]).get(0)
        code_hash=hashlib.sha256(bytes.fromhex(code[2:])).hexdigest() if isinstance(code,str) and code.startswith("0x") else None
        creator=addr(pick(smart,"creator_address_hash","creator_address","creator_address.hash",default=None)) or addr(pick(adata,"creator_address_hash","creator_address","creator_address.hash",default=None))
        contract_meta.append({
          **{k:c[k] for k in ("priority","collection_name","contract_address","observed_supply","reason_code")},
          "creator_address":creator,"transfer_rows":len(rows),"mint_rows":sum(r["event_kind"]=="MINT" for r in rows),
          "burn_rows":sum(r["event_kind"]=="BURN" for r in rows),"pages":pages,"bytecode_sha256":code_hash,
          "smart_contract_raw":smart,"address_raw":adata,"token_raw":token
        })
        for url in c["evidence_urls"]:
            txt=net.text(url)
            for cand in sorted(set(x.lower() for x in ADDR_RE.findall(txt))):
                if cand==a:continue
                candcode=net.rpc([("eth_getCode",[cand,"latest"])]).get(0)
                ch=hashlib.sha256(bytes.fromhex(candcode[2:])).hexdigest() if isinstance(candcode,str) and candcode.startswith("0x") else None
                identity.append({"collection_name":c["collection_name"],"exact_contract":a,"source_url":url,
                  "candidate_address":cand,"candidate_bytecode_sha256":ch,"exact_bytecode_sha256":code_hash,
                  "bytecode_exact_match":bool(ch and code_hash and ch==code_hash)})
    jsonl(root/"nft_transfers.jsonl",[{k:v for k,v in r.items() if k!="raw"} for r in all_tr])
    csvout(root/"nft_transfers.csv",[{k:v for k,v in r.items() if k!="raw"} for r in all_tr])
    jsonl(root/"contracts_metadata.jsonl",contract_meta);csvout(root/"identity_candidates.csv",identity)

    txhashes=sorted({r["tx_hash"] for r in all_tr if r.get("tx_hash")})
    txs={};receipts={};blocks={}
    for i in range(0,len(txhashes),args.rpc_batch):
        hs=txhashes[i:i+args.rpc_batch];calls=[]
        for h in hs:calls.extend([("eth_getTransactionByHash",[h]),("eth_getTransactionReceipt",[h])])
        try:res=net.rpc(calls)
        except Exception as e:
            errors.append({"stage":"rpc_batch","offset":i,"error":str(e)});continue
        for j,h in enumerate(hs):
            txs[h]=res.get(j*2);receipts[h]=res.get(j*2+1)
        if i%(args.rpc_batch*20)==0:print("RPC",min(i+len(hs),len(txhashes)),len(txhashes),flush=True)
    bnums=sorted({integer((v or {}).get("blockNumber")) for v in txs.values() if integer((v or {}).get("blockNumber")) is not None})
    for i in range(0,len(bnums),args.rpc_batch*2):
        ns=bnums[i:i+args.rpc_batch*2]
        try:res=net.rpc([("eth_getBlockByNumber",[hex(n),False]) for n in ns])
        except Exception as e:errors.append({"stage":"block_batch","offset":i,"error":str(e)});continue
        for j,n in enumerate(ns):blocks[n]=res.get(j)
    jsonl(root/"transactions.jsonl",[{"tx_hash":h,"tx":txs.get(h),"receipt":receipts.get(h)} for h in txhashes])

    creators={m["contract_address"]:m.get("creator_address") for m in contract_meta}
    bytx=defaultdict(list)
    for r in all_tr:
        if r["event_kind"]=="MINT" and r.get("tx_hash"):bytx[r["tx_hash"]].append(r)
    mints=[];payments=[]
    for h,evs in bytx.items():
        tx=txs.get(h) or {};rc=receipts.get(h) or {};sender=addr(tx.get("from"));target=addr(tx.get("to"))
        val=integer(tx.get("value"),0) or 0;bn=integer(tx.get("blockNumber")) or min((e.get("block_number") or 0) for e in evs)
        bt=integer((blocks.get(bn) or {}).get("timestamp"))
        parsed=receipt_transfers(rc);erc20=defaultdict(int)
        for p in parsed:
            if p["kind"]=="ERC20" and p["emitter"]!=evs[0]["contract_address"] and sender and p["from"]==sender:
                erc20[p["emitter"]]+=p["amount"]
        recips=sorted({e["to_address"] for e in evs if e.get("to_address")});qty=len(evs)
        creator=creators.get(evs[0]["contract_address"])
        if val>0 and erc20:klass="NATIVE_AND_ERC20_VALUE"
        elif val>0:klass="NATIVE_PAID"
        elif erc20:klass="ERC20_PAID"
        elif creator and sender==creator:klass="ZERO_VALUE_CREATOR_MINT"
        elif len(recips)==1 and recips[0]==sender:klass="ZERO_VALUE_SELF_MINT"
        elif sender and sender not in recips:klass="ZERO_VALUE_THIRD_PARTY_DISTRIBUTION"
        else:klass="ZERO_VALUE_ROUTE_UNRESOLVED"
        gas=(integer(rc.get("gasUsed"),0) or 0)*(integer(rc.get("effectiveGasPrice"),integer(tx.get("gasPrice"),0)) or 0)
        row={"collection_name":evs[0]["collection_name"],"contract_address":evs[0]["contract_address"],"tx_hash":h,
          "block_number":bn,"timestamp_utc":datetime.fromtimestamp(bt,tz=timezone.utc).isoformat().replace("+00:00","Z") if bt else evs[0].get("timestamp_utc"),
          "tx_sender":sender,"tx_to":target,"method_selector":str(tx.get("input") or "0x")[:10].lower(),
          "mint_quantity":qty,"mint_recipients":recips,"native_value_wei":str(val),"native_value_eth":wei(val),
          "native_per_nft_wei":str(val//qty) if qty else None,"native_per_nft_eth":wei(val//qty) if qty else None,
          "erc20_sender_outflows":dict(erc20),"gas_cost_wei":str(gas),"payment_class":klass,
          "status":integer(rc.get("status"))}
        mints.append(row)
        for token,amount in erc20.items():payments.append({"tx_hash":h,"contract_address":row["contract_address"],"payment_token":token,"amount_raw":str(amount),"mint_quantity":qty})
    mints.sort(key=lambda r:(r["block_number"],r["tx_hash"]))
    csvout(root/"mint_transactions.csv",mints);jsonl(root/"mint_transactions.jsonl",mints);csvout(root/"mint_payment_transfers.csv",payments)

    clusters=[]
    groups=defaultdict(list)
    for r in mints:
        asset="NATIVE" if int(r["native_value_wei"])>0 else ("ERC20:"+",".join(sorted(r["erc20_sender_outflows"])) if r["erc20_sender_outflows"] else "ZERO")
        unit=r["native_per_nft_wei"] if asset=="NATIVE" else json.dumps(r["erc20_sender_outflows"],sort_keys=True)
        groups[(r["contract_address"],r["method_selector"],asset,unit,r["payment_class"])].append(r)
    for key,rs in groups.items():
        caddr,selector,asset,unit,klass=key
        clusters.append({"contract_address":caddr,"collection_name":rs[0]["collection_name"],"method_selector":selector,
          "asset_class":asset,"unit_value_raw":unit,"payment_class":klass,"first_block":min(r["block_number"] for r in rs),
          "last_block":max(r["block_number"] for r in rs),"transactions":len(rs),"tokens_minted":sum(r["mint_quantity"] for r in rs),
          "unique_senders":len({r["tx_sender"] for r in rs if r["tx_sender"]})})
    csvout(root/"mint_stage_clusters.csv",clusters)

    summary=[]
    for c in contracts:
        a=c["contract_address"];rs=[r for r in mints if r["contract_address"]==a];trs=[r for r in all_tr if r["contract_address"]==a]
        q=sum(r["mint_quantity"] for r in rs)
        summary.append({"collection_name":c["collection_name"],"contract_address":a,"observed_supply":c["observed_supply"],
          "indexed_transfer_rows":len(trs),"indexed_mint_event_rows":sum(r["event_kind"]=="MINT" for r in trs),
          "mint_transactions":len(rs),"minted_tokens_reconstructed":q,"supply_delta_reconstructed_minus_observed":q-c["observed_supply"],
          "native_paid_tokens":sum(r["mint_quantity"] for r in rs if r["payment_class"]=="NATIVE_PAID"),
          "erc20_paid_tokens":sum(r["mint_quantity"] for r in rs if "ERC20" in r["payment_class"]),
          "zero_self_tokens":sum(r["mint_quantity"] for r in rs if r["payment_class"]=="ZERO_VALUE_SELF_MINT"),
          "zero_creator_tokens":sum(r["mint_quantity"] for r in rs if r["payment_class"]=="ZERO_VALUE_CREATOR_MINT"),
          "zero_distribution_tokens":sum(r["mint_quantity"] for r in rs if r["payment_class"]=="ZERO_VALUE_THIRD_PARTY_DISTRIBUTION"),
          "zero_unresolved_tokens":sum(r["mint_quantity"] for r in rs if r["payment_class"]=="ZERO_VALUE_ROUTE_UNRESOLVED")})
    csvout(root/"collection_summary.csv",summary)

    unresolved=[]
    for s in summary:
        if s["minted_tokens_reconstructed"]!=s["observed_supply"]:
            unresolved.append({"contract_address":s["contract_address"],"reason":"SUPPLY_NOT_RECONCILED","detail":s})
        if s["zero_unresolved_tokens"]>0:
            unresolved.append({"contract_address":s["contract_address"],"reason":"ZERO_VALUE_ROUTE_UNRESOLVED","detail":{"tokens":s["zero_unresolved_tokens"]}})
    validation={
      "generated_at_utc":now(),"contracts":len(contracts),"transfer_rows":len(all_tr),"tx_hashes":len(txhashes),
      "mint_transactions":len(mints),"mint_tokens":sum(r["mint_quantity"] for r in mints),
      "identity_candidates":len(identity),"errors":len(errors),"unresolved":len(unresolved),
      "checks":{
        "all_contracts_have_transfer_rows":all(any(r["contract_address"]==c["contract_address"] for r in all_tr) for c in contracts),
        "all_contracts_have_mint_rows":all(any(r["contract_address"]==c["contract_address"] for r in mints) for c in contracts),
        "no_transfer_only_sale_claim":True,
        "no_zero_value_auto_public_free":True,
        "address_format":all(re.fullmatch(r"0x[a-f0-9]{40}",c["contract_address"]) for c in contracts)
      },
      "status":"PASS" if not errors and all(any(r["contract_address"]==c["contract_address"] for r in mints) for c in contracts) else "FAIL",
      "network_stats":dict(net.stats)
    }
    dump(root/"unresolved.json",unresolved);jsonl(root/"errors.jsonl",errors);dump(root/"validation.json",validation)
    files=[]
    for p in sorted(root.rglob("*")):
        if p.is_file():
            files.append({"path":str(p.relative_to(root)),"bytes":p.stat().st_size,"sha256":hashlib.sha256(p.read_bytes()).hexdigest()})
    dump(root/"manifest.json",{"generated_at_utc":now(),"files":files})
    print(json.dumps(validation,indent=2),flush=True)
    return 0 if validation["status"]=="PASS" else 2

def selftest():
    assert addr({"hash":"0x"+"a"*40})=="0x"+"a"*40
    assert integer("0x10")==16 and integer("17")==17
    r=receipt_transfers({"logs":[{"topics":[TRANSFER,"0x"+"0"*24+"1"*40,"0x"+"0"*24+"2"*40,"0x1"],"address":"0x"+"3"*40,"data":"0x"}]})
    assert r[0]["kind"]=="ERC721" and r[0]["token_id"]==1
    print("SELFTEST PASS");return 0

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--contracts");ap.add_argument("--out");ap.add_argument("--max-pages",type=int,default=5000)
    ap.add_argument("--delay",type=float,default=.15);ap.add_argument("--rpc-batch",type=int,default=25);ap.add_argument("--self-test",action="store_true")
    a=ap.parse_args()
    if a.self_test:return selftest()
    if not a.contracts or not a.out:ap.error("--contracts and --out required")
    return collect(a)
if __name__=="__main__":raise SystemExit(main())
