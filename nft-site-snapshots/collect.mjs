import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import puppeteer from 'puppeteer-core';

const targets = [
  { key: 'mintgo', url: 'https://mintgo.fun/' },
  { key: 'guap', url: 'https://guap.wtf/' },
];

function chromePath() {
  for (const p of ['/usr/bin/google-chrome','/usr/bin/google-chrome-stable','/usr/bin/chromium']) if (fs.existsSync(p)) return p;
  for (const n of ['google-chrome','google-chrome-stable','chromium']) {
    try { const p=execFileSync('which',[n],{encoding:'utf8'}).trim(); if(p) return p; } catch {}
  }
  throw new Error('Chrome not found');
}
function safeName(url, i, ct) {
  const u=new URL(url); let ext='.bin';
  if(ct.includes('json')) ext='.json'; else if(ct.includes('javascript')) ext='.js'; else if(ct.includes('html')) ext='.html'; else if(ct.includes('css')) ext='.css';
  const stem=(u.hostname+u.pathname).replace(/[^a-zA-Z0-9._-]+/g,'_').slice(0,160)||'response';
  return `${String(i).padStart(4,'0')}_${stem}${ext}`;
}

const browser=await puppeteer.launch({executablePath:chromePath(),headless:true,args:['--no-sandbox','--disable-dev-shm-usage','--window-size=1600,1200']});
for (const target of targets) {
  const out=path.resolve('out',target.key); const netdir=path.join(out,'network'); fs.mkdirSync(netdir,{recursive:true});
  const page=await browser.newPage(); await page.setViewport({width:1600,height:1200,deviceScaleFactor:1});
  await page.setUserAgent('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/151 Safari/537.36');
  const network=[]; const consoleRows=[]; let n=0;
  page.on('console',m=>consoleRows.push({type:m.type(),text:m.text()})); page.on('pageerror',e=>consoleRows.push({type:'pageerror',text:String(e)}));
  page.on('response',async r=>{
    const url=r.url(), ct=String(r.headers()['content-type']||'').toLowerCase(), status=r.status();
    const row={url,status,method:r.request().method(),resourceType:r.request().resourceType(),contentType:ct,savedFile:null,savedBytes:null,error:null};
    const save=status>=200&&status<400&&(ct.includes('json')||ct.includes('javascript')||ct.includes('html')||url.includes('/api/')||url.includes('graphql')||url.includes('supabase')||url.includes('firebase')||url.includes('_next'));
    if(save){try{const b=await r.buffer();if(b.length<=10_000_000){n++;const f=safeName(url,n,ct);fs.writeFileSync(path.join(netdir,f),b);row.savedFile=`network/${f}`;row.savedBytes=b.length;}else row.error=`too-large:${b.length}`;}catch(e){row.error=String(e)}}
    network.push(row);
  });
  try {
    await page.goto(target.url,{waitUntil:'networkidle2',timeout:150000});
    await new Promise(r=>setTimeout(r,12000));
    await page.evaluate(async()=>{await new Promise(resolve=>{let y=0;const t=setInterval(()=>{window.scrollBy(0,800);y+=800;if(y>document.body.scrollHeight+1600){clearInterval(t);window.scrollTo(0,0);resolve();}},150);});});
    await new Promise(r=>setTimeout(r,6000));
    const extracted=await page.evaluate(()=>{
      const at=el=>Object.fromEntries([...el.attributes].map(a=>[a.name,a.value]));
      const visible=el=>{const s=getComputedStyle(el),r=el.getBoundingClientRect();return s.display!=='none'&&s.visibility!=='hidden'&&r.width>0&&r.height>0};
      return {
        title:document.title,url:location.href,bodyText:document.body.innerText,
        elements:[...document.querySelectorAll('h1,h2,h3,h4,h5,h6,article,section,button,[role="button"],a')].filter(visible).map((el,i)=>({i,tag:el.tagName,text:(el.innerText||el.textContent||'').trim(),href:el.href||null,attrs:at(el)})).filter(x=>x.text||x.href),
        links:[...document.querySelectorAll('a[href]')].map(a=>({text:(a.innerText||'').trim(),href:a.href,attrs:at(a)})),
        scripts:[...document.scripts].map((s,i)=>({i,src:s.src||null,id:s.id||null,type:s.type||null,text:s.src?null:(s.textContent||'').slice(0,2000000)})),
        localStorage:Object.fromEntries(Object.keys(localStorage).map(k=>[k,localStorage.getItem(k)])),
        sessionStorage:Object.fromEntries(Object.keys(sessionStorage).map(k=>[k,sessionStorage.getItem(k)])),
        nextData:document.querySelector('#__NEXT_DATA__')?.textContent||null,
      };
    });
    fs.writeFileSync(path.join(out,'page.html'),await page.content()); fs.writeFileSync(path.join(out,'body.txt'),extracted.bodyText);
    fs.writeFileSync(path.join(out,'extracted.json'),JSON.stringify(extracted,null,2)); fs.writeFileSync(path.join(out,'network_index.json'),JSON.stringify(network,null,2));
    fs.writeFileSync(path.join(out,'console.json'),JSON.stringify(consoleRows,null,2));
    fs.writeFileSync(path.join(out,'resources.json'),JSON.stringify(await page.evaluate(()=>performance.getEntriesByType('resource').map(r=>({name:r.name,initiatorType:r.initiatorType,duration:r.duration,transferSize:r.transferSize}))),null,2));
    await page.screenshot({path:path.join(out,'page.png'),fullPage:true});
    fs.writeFileSync(path.join(out,'capture_status.json'),JSON.stringify({status:'PASS',captured_at:new Date().toISOString(),networkRows:network.length,bodyChars:extracted.bodyText.length},null,2));
  } catch(e) {
    fs.writeFileSync(path.join(out,'capture_status.json'),JSON.stringify({status:'FAIL',captured_at:new Date().toISOString(),error:String(e),networkRows:network.length},null,2));
  }
  await page.close();
}
await browser.close();
