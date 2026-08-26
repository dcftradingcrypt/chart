import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import puppeteer from 'puppeteer-core';
const targets=[
 {key:'waypoint-mintscan',url:'https://waypoint.tools/mintscan/'},
 {key:'985-wallet',url:'https://985monitor.xyz/wallet/'},
];
function chrome(){for(const p of ['/usr/bin/google-chrome','/usr/bin/google-chrome-stable','/usr/bin/chromium'])if(fs.existsSync(p))return p;for(const n of ['google-chrome','google-chrome-stable','chromium']){try{const p=execFileSync('which',[n],{encoding:'utf8'}).trim();if(p)return p}catch{}}throw new Error('Chrome not found')}
function safe(url,i,ct){const u=new URL(url);let ext='.bin';if(ct.includes('json'))ext='.json';else if(ct.includes('javascript'))ext='.js';else if(ct.includes('html'))ext='.html';else if(ct.includes('css'))ext='.css';return `${String(i).padStart(4,'0')}_${(u.hostname+u.pathname).replace(/[^a-zA-Z0-9._-]+/g,'_').slice(0,160)}${ext}`}
const browser=await puppeteer.launch({executablePath:chrome(),headless:true,args:['--no-sandbox','--disable-dev-shm-usage','--window-size=1600,1200']});
for(const t of targets){
 const out=path.resolve('out',t.key),net=path.join(out,'network');fs.mkdirSync(net,{recursive:true});
 const page=await browser.newPage();await page.setViewport({width:1600,height:1200,deviceScaleFactor:1});await page.setUserAgent('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/151 Safari/537.36');
 const responses=[],consoleRows=[];let n=0;page.on('console',m=>consoleRows.push({type:m.type(),text:m.text()}));page.on('pageerror',e=>consoleRows.push({type:'pageerror',text:String(e)}));
 page.on('response',async r=>{const url=r.url(),ct=String(r.headers()['content-type']||'').toLowerCase(),status=r.status();const row={url,status,method:r.request().method(),resourceType:r.request().resourceType(),contentType:ct,savedFile:null,savedBytes:null,error:null};const save=status>=200&&status<400&&(ct.includes('json')||ct.includes('javascript')||ct.includes('html')||url.includes('/api/')||url.includes('graphql')||url.includes('supabase')||url.includes('firebase')||url.includes('_next'));if(save){try{const b=await r.buffer();if(b.length<=12_000_000){n++;const f=safe(url,n,ct);fs.writeFileSync(path.join(net,f),b);row.savedFile=`network/${f}`;row.savedBytes=b.length}else row.error=`too-large:${b.length}`}catch(e){row.error=String(e)}}responses.push(row)});
 try{
  await page.goto(t.url,{waitUntil:'networkidle2',timeout:150000});await new Promise(r=>setTimeout(r,12000));
  // Click obvious Robinhood/Hood chain controls when present, without connecting a wallet.
  const clicked=await page.evaluate(()=>{const rows=[];for(const el of [...document.querySelectorAll('button,[role="button"],a,select option')]){const text=(el.innerText||el.textContent||'').trim();if(/^(RH|HOOD|ROBINHOOD|ROBINHOOD CHAIN)$/i.test(text)){try{el.click();rows.push({text,tag:el.tagName})}catch{}}}return rows});
  if(clicked.length)await new Promise(r=>setTimeout(r,7000));
  await page.evaluate(async()=>{await new Promise(resolve=>{let y=0;const id=setInterval(()=>{window.scrollBy(0,800);y+=800;if(y>document.body.scrollHeight+1600){clearInterval(id);window.scrollTo(0,0);resolve()}},130)})});await new Promise(r=>setTimeout(r,5000));
  const data=await page.evaluate(()=>{const attrs=el=>Object.fromEntries([...el.attributes].map(a=>[a.name,a.value]));const visible=el=>{const s=getComputedStyle(el),r=el.getBoundingClientRect();return s.display!=='none'&&s.visibility!=='hidden'&&r.width>0&&r.height>0};return {title:document.title,url:location.href,bodyText:document.body.innerText,elements:[...document.querySelectorAll('h1,h2,h3,h4,h5,h6,article,section,button,[role="button"],a,input,select,option')].filter(visible).map((el,i)=>({i,tag:el.tagName,text:(el.innerText||el.textContent||'').trim(),href:el.href||null,type:el.type||null,placeholder:el.placeholder||null,attrs:attrs(el)})).filter(x=>x.text||x.href||x.placeholder),links:[...document.querySelectorAll('a[href]')].map(a=>({text:(a.innerText||'').trim(),href:a.href,attrs:attrs(a)})),scripts:[...document.scripts].map((s,i)=>({i,src:s.src||null,id:s.id||null,type:s.type||null,text:s.src?null:(s.textContent||'').slice(0,2000000)})),localStorage:Object.fromEntries(Object.keys(localStorage).map(k=>[k,localStorage.getItem(k)])),sessionStorage:Object.fromEntries(Object.keys(sessionStorage).map(k=>[k,sessionStorage.getItem(k)])),nextData:document.querySelector('#__NEXT_DATA__')?.textContent||null}});
  fs.writeFileSync(path.join(out,'page.html'),await page.content());fs.writeFileSync(path.join(out,'body.txt'),data.bodyText);fs.writeFileSync(path.join(out,'extracted.json'),JSON.stringify(data,null,2));fs.writeFileSync(path.join(out,'network_index.json'),JSON.stringify(responses,null,2));fs.writeFileSync(path.join(out,'console.json'),JSON.stringify(consoleRows,null,2));fs.writeFileSync(path.join(out,'clicked.json'),JSON.stringify(clicked,null,2));await page.screenshot({path:path.join(out,'page.png'),fullPage:true});fs.writeFileSync(path.join(out,'capture_status.json'),JSON.stringify({status:'PASS',capturedAt:new Date().toISOString(),bodyChars:data.bodyText.length,networkRows:responses.length,clicked},null,2));
 }catch(e){fs.writeFileSync(path.join(out,'capture_status.json'),JSON.stringify({status:'FAIL',capturedAt:new Date().toISOString(),error:String(e),networkRows:responses.length},null,2))}
 await page.close();
}
await browser.close();
