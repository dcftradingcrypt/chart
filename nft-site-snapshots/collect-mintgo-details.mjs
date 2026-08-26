import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import puppeteer from 'puppeteer-core';

const candidates=JSON.parse(fs.readFileSync('nft-site-snapshots/paid_candidates.json','utf8'));
const OUT=path.resolve('out-details');fs.mkdirSync(OUT,{recursive:true});
function chrome(){for(const p of ['/usr/bin/google-chrome','/usr/bin/google-chrome-stable','/usr/bin/chromium'])if(fs.existsSync(p))return p;for(const n of ['google-chrome','google-chrome-stable','chromium']){try{const p=execFileSync('which',[n],{encoding:'utf8'}).trim();if(p)return p}catch{}}throw new Error('Chrome not found')}
const browser=await puppeteer.launch({executablePath:chrome(),headless:true,args:['--no-sandbox','--disable-dev-shm-usage']});
const page=await browser.newPage();await page.setUserAgent('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/151 Safari/537.36');
await page.goto('https://mintgo.fun/',{waitUntil:'networkidle2',timeout:150000});await new Promise(r=>setTimeout(r,4000));

async function getJson(url, timeout=35000){
  return await page.evaluate(async({url,timeout})=>{
    const ctrl=new AbortController();const timer=setTimeout(()=>ctrl.abort(),timeout);
    try{
      const res=await fetch(url,{credentials:'same-origin',cache:'no-store',signal:ctrl.signal});
      const text=await res.text();let data=null;try{data=JSON.parse(text)}catch{}
      return {ok:res.ok,status:res.status,url:res.url,data,text:data?null:text.slice(0,20000)};
    }catch(e){return {ok:false,status:0,url,error:String(e)}}finally{clearTimeout(timer)}
  },{url,timeout});
}

async function one(c){
  const a=c.address.toLowerCase();
  const detail=await getJson(`/api/collection/${a}?finance=1`,45000);
  let analysis=await getJson(`/api/mint-analysis/${a}?refresh=1`,45000);
  for(let i=0;i<3 && analysis?.data && ['idle','analyzing'].includes(String(analysis.data.status||''));i++){
    await new Promise(r=>setTimeout(r,3500));analysis=await getJson(`/api/mint-analysis/${a}`,45000);
  }
  const linked=await getJson(`/api/x-account-projects/${a}`,30000);
  const row={candidate:c,capturedAt:new Date().toISOString(),detail,analysis,linkedXProjects:linked};
  fs.writeFileSync(path.join(OUT,`${a}.json`),JSON.stringify(row,null,2));
  return {
    address:a,name:c.name,detailStatus:detail.status,analysisStatus:analysis.status,analysisState:analysis?.data?.status||null,
    analysisReady:analysis?.data?.ready??null,analysisReason:analysis?.data?.reason||null,
    detailKeys:detail?.data&&typeof detail.data==='object'?Object.keys(detail.data):[],
    analysisKeys:analysis?.data&&typeof analysis.data==='object'?Object.keys(analysis.data):[],
    linkedStatus:linked.status,linkedCount:Array.isArray(linked?.data?.projects)?linked.data.projects.length:null,
  };
}

const results=[];let next=0;
async function worker(){while(true){const i=next++;if(i>=candidates.length)return;const c=candidates[i];console.log(`DETAIL ${i+1}/${candidates.length} ${c.name} ${c.address}`,flush=true);try{results.push(await one(c))}catch(e){results.push({address:c.address,name:c.name,error:String(e)})}await new Promise(r=>setTimeout(r,350));}}
await Promise.all(Array.from({length:3},()=>worker()));
results.sort((a,b)=>String(a.address).localeCompare(String(b.address)));
fs.writeFileSync(path.join(OUT,'index.json'),JSON.stringify(results,null,2));
fs.writeFileSync(path.join(OUT,'summary.json'),JSON.stringify({capturedAt:new Date().toISOString(),candidateCount:candidates.length,resultCount:results.length,detailOk:results.filter(r=>r.detailStatus===200).length,analysisDone:results.filter(r=>r.analysisState==='done').length,analysisReady:results.filter(r=>r.analysisReady===true).length,errors:results.filter(r=>r.error||!r.detailStatus).length},null,2));
await browser.close();
