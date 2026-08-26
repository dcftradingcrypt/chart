#!/usr/bin/env python3
from __future__ import annotations
import json, urllib.error, urllib.parse, urllib.request
from pathlib import Path

BASE='https://api.alphagate.io'
EXTENSION_ID='niokainjnclnhaflagpmeobbnklokill'
OUT=Path('out-alphagate-endpoints'); OUT.mkdir(parents=True,exist_ok=True)
CASES=[
 ('discover','/api/v1/ext/child/discover',{}),
 ('trending','/api/v1/ext/child/trending',{}),
 ('scan_profile','/api/v1/ext/scan-profile',{'username':'kingmakerxbt'}),
 ('scan_token','/api/v1/ext/scan-token',{'token':'0x3e47b18177330d031ea6fc81e4b0ca83fc5a85e3'}),
 ('notes','/api/v1/ext/notes',{}),
 ('tracker_profiles','/api/v1/ext/tracker/profiles',{}),
 ('tracker_subscriptions','/api/v1/ext/tracker/subscriptions',{}),
 ('socket_polling','/ext/socket.io/',{'EIO':'4','transport':'polling','t':'probe'}),
]
results=[]
for name,path,params in CASES:
    url=BASE+path
    if params: url+='?'+urllib.parse.urlencode(params)
    row={'name':name,'url':url,'request_headers':{'Accept':'application/json,text/plain,*/*','X-Origin-ID':EXTENSION_ID},'cookies_sent':False}
    req=urllib.request.Request(url,headers={'User-Agent':'Alphagate-ReadOnly-Validation/1.0','Accept':'application/json,text/plain,*/*','X-Origin-ID':EXTENSION_ID})
    try:
        with urllib.request.urlopen(req,timeout=40) as resp:
            body=resp.read(100000)
            row.update({'status':resp.status,'content_type':resp.headers.get('content-type'),'set_cookie_present':bool(resp.headers.get('set-cookie')),'body_prefix':body.decode('utf-8','replace')[:5000]})
    except urllib.error.HTTPError as e:
        body=e.read(100000)
        row.update({'status':e.code,'content_type':e.headers.get('content-type'),'set_cookie_present':bool(e.headers.get('set-cookie')),'body_prefix':body.decode('utf-8','replace')[:5000]})
    except Exception as e:
        row.update({'error':repr(e)})
    results.append(row)

public_200=[r['name'] for r in results if r.get('status')==200]
auth_blocked=[r['name'] for r in results if r.get('status') in (401,403)]
report={
 'base_url':BASE,
 'extension_id_header':EXTENSION_ID,
 'tested_without_login_or_cookies':True,
 'results':results,
 'public_200':public_200,
 'auth_blocked':auth_blocked,
 'conclusion': 'PUBLIC_READ_ENDPOINTS_CONFIRMED' if any(n in public_200 for n in ('discover','trending','scan_profile')) else ('AUTH_REQUIRED_FOR_PROJECT_FEEDS' if auth_blocked else 'NO_USABLE_PUBLIC_FEED_CONFIRMED'),
}
(OUT/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps({'conclusion':report['conclusion'],'public_200':public_200,'auth_blocked':auth_blocked},ensure_ascii=False))
