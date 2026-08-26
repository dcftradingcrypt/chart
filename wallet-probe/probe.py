#!/usr/bin/env python3
import csv,json,random,re,time,urllib.error,urllib.parse,urllib.request
from collections import Counter,defaultdict
from pathlib import Path
B='https://robinscan.io'; Z='0x'+'0'*40
SD='0xe90cf9cc0a552cf52ea6ff74ece0f1c8ae8cc9ad630d3181f55ac43ca076b7d6'; OF='0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31'
O=Path('out-wallet-probe'); O.mkdir(exist_ok=True); (O/'raw').mkdir(exist_ok=True)
last=0.; calls=0

def get(path,params=None):
 global last,calls
 u=B+path+('?' + urllib.parse.urlencode(params) if params else '')
 e=None
 for i in range(10):
  w=.22-(time.monotonic()-last)
  if w>0: time.sleep(w)
  calls+=1
  try:
   q=urllib.request.Request(u,headers={'user-agent':'RHC-Wallet-Audit/1.0','accept':'application/json'})
   with urllib.request.urlopen(q,timeout=60) as r:x=r.read()
   last=time.monotonic(); return json.loads(x)
  except urllib.error.HTTPError as z:
   e=z; last=time.monotonic()
   if z.code not in (408,425,429,500,502,503,504): raise
  except Exception as z:e=z
  time.sleep(min(40,2**i+random.random()))
 raise RuntimeError(f'{u}: {e}')

def pages(path):
 a=[]; cur=None; seen=set()
 for _ in range(500):
  d=get(path,{'cursor':cur} if cur else None); x=d.get('items') if isinstance(d,dict) else None
  if not isinstance(x,list): raise RuntimeError('bad page '+path)
  a+=x; n=d.get('next') or d.get('next_cursor') or d.get('next_page_params')
  if not n:return a
  k=json.dumps(n,sort_keys=True)
  if k in seen:raise RuntimeError('cursor loop '+path)
  seen.add(k);cur=n if isinstance(n,str) else json.dumps(n,separators=(',',':'))
 raise RuntimeError('page limit '+path)

def ad(x):
 if isinstance(x,dict):
  for k in ('hash','address_hash','address'):
   if k in x:return ad(x[k])
 if isinstance(x,str) and re.fullmatch(r'0x[0-9a-fA-F]{40}',x):return x.lower()

def ts(x):
 a=[]
 for v in x.get('topics') or []:
  if isinstance(v,dict):v=v.get('value') or v.get('hash')
  if v:a.append(str(v).lower())
 return a

def words(s):
 s=s[2:] if s.startswith('0x') else s
 return [int(s[i:i+64],16) for i in range(0,len(s),64) if len(s[i:i+64])==64]

def ta(s):return '0x'+s[-40:].lower()
def iv(x):
 try:return int(x,16) if isinstance(x,str) and x.startswith('0x') else int(x or 0)
 except:return 0

def write(name,rows):
 f=sorted({k for r in rows for k in r}) if rows else ['empty']
 with (O/name).open('w',encoding='utf-8-sig',newline='') as h:
  w=csv.DictWriter(h,fieldnames=f,extrasaction='ignore');w.writeheader()
  for r in rows:w.writerow({k:json.dumps(v,ensure_ascii=False,sort_keys=True) if isinstance(v,(list,dict)) else v for k,v in r.items()})

C=list(csv.DictReader(open('wallet-probe/candidates.csv',encoding='utf-8-sig')))
S=[];M=[];V=[];E=[];cache={}
for n,c in enumerate(C,1):
 w=c['wallet_address'].lower();print(n,len(C),w,flush=True)
 try:
  cnt=get(f'/api/address/{w}/counters'); tr=pages(f'/api/address/{w}/transfers')
  (O/'raw'/f'{w}.json').write_text(json.dumps({'candidate':c,'counters':cnt,'transfers':tr},ensure_ascii=False),encoding='utf-8')
  mi=set();out=set()
  for r in tr:
   fr=ad(r.get('from'));to=ad(r.get('to'));h=str(r.get('txHash') or r.get('transaction_hash') or '').lower()
   if h and fr==Z and to==w:mi.add(h)
   if h and fr==w and to and to!=Z:out.add(h)
  for h in sorted(mi|out):
   try:d=cache.setdefault(h,get(f'/api/tx/{h}/logs')) if h not in cache else cache[h]
   except Exception as e:E.append({'wallet':w,'stage':'logs','tx_hash':h,'error':repr(e)});continue
   for l in d.get('items',[]) if isinstance(d,dict) else []:
    t=ts(l);t0=t[0] if t else ''
    if t0==SD:
     q=words(str(l.get('data') or '0x'))
     if len(t)>=4 and len(q)>=5:
      mn=ta(t[2]);py='0x'+q[0].to_bytes(32,'big')[-20:].hex()
      if w in (mn,py):M.append({'wallet':w,'transaction_hash':h,'block_number':iv(l.get('block_number')),'nft_contract':ta(t[1]),'minter':mn,'payer':py,'quantity':q[1],'unit_price_wei':q[2],'gross_cost_wei':q[1]*q[2],'stage_index':q[4],'is_free':q[2]==0,'is_paid':q[2]>0})
    elif t0==OF and h in out:V.append({'wallet':w,'transaction_hash':h,'block_number':iv(l.get('block_number')),'log_index':iv(l.get('index',l.get('log_index'))),'evidence':'OUTBOUND_NFT_TX_WITH_ORDER_FULFILLED'})
  m=[r for r in M if r['wallet']==w];v=[r for r in V if r['wallet']==w]
  pc=sorted({r['nft_contract'] for r in m if r['is_paid']}); pp=sorted({r['nft_contract'] for r in m if r['is_paid'] and r['stage_index']==0});fc=sorted({r['nft_contract'] for r in m if r['is_free']})
  cl='TARGET_A_REPEAT_PUBLIC_PAID_WITH_SALE_EVIDENCE' if len(pp)>=3 and v else 'TARGET_B_REPEAT_PUBLIC_PAID' if len(pp)>=2 else 'WATCH_REPEAT_PAID_INCLUDES_NONPUBLIC' if len(pc)>=2 else 'INSUFFICIENT_ONE_PAID_PROJECT' if len(pc)==1 else 'NO_PAID_SEADROP_EVIDENCE'
  S.append({**c,'robinscan_total_transactions':cnt.get('transactions'),'robinscan_total_token_transfers':cnt.get('tokenTransfers'),'transfers_fetched':len(tr),'mint_tx_candidates':len(mi),'outbound_tx_candidates':len(out),'seadrop_events':len(m),'paid_projects':len(pc),'paid_public_projects':len(pp),'free_projects':len(fc),'sale_tx_evidence':len({x['transaction_hash'] for x in v}),'paid_contracts':pc,'paid_public_contracts':pp,'free_contracts':fc,'classification':cl,'production_approved':False})
 except Exception as e:E.append({'wallet':w,'stage':'wallet','error':repr(e)});S.append({**c,'classification':'FETCH_FAILED','production_approved':False})
F=defaultdict(list)
for r in S:F[json.dumps({'p':r.get('paid_contracts',[]),'pp':r.get('paid_public_contracts',[]),'f':r.get('free_contracts',[])},sort_keys=True)].append(r['wallet_address'])
D=[{'cluster_id':f'EXACT_PORTFOLIO_{i:03d}','wallet_count':len(ws),'wallets':ws,'fingerprint':json.loads(fp),'interpretation':'DUPLICATE_BEHAVIOR_RISK_NOT_IDENTITY_PROOF'} for i,(fp,ws) in enumerate(sorted(F.items(),key=lambda x:-len(x[1])),1) if len(ws)>1]
write('wallet_summary.csv',S);write('wallet_mint_events.csv',M);write('wallet_sale_evidence.csv',V);write('duplicate_behavior_clusters.csv',D);write('errors.csv',E)
R={'candidate_wallets':len(C),'wallets_completed':sum(r['classification']!='FETCH_FAILED' for r in S),'wallets_failed':sum(r['classification']=='FETCH_FAILED' for r in S),'mint_events':len(M),'sale_evidence_rows':len(V),'classification_counts':dict(Counter(r['classification'] for r in S)),'duplicate_clusters':len(D),'http_calls':calls,'production_approved_wallets':0}
(O/'summary.json').write_text(json.dumps(R,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(R,ensure_ascii=False))
if R['wallets_failed']:raise SystemExit(2)
