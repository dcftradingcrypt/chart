import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import puppeteer from 'puppeteer-core';

const OUT = path.resolve('out');
const NET = path.join(OUT, 'network');
fs.mkdirSync(NET, { recursive: true });

function findChrome() {
  const candidates = [
    process.env.CHROME_BIN,
    '/usr/bin/google-chrome',
    '/usr/bin/google-chrome-stable',
    '/usr/bin/chromium',
    '/usr/bin/chromium-browser',
  ].filter(Boolean);
  for (const p of candidates) if (fs.existsSync(p)) return p;
  for (const name of ['google-chrome', 'google-chrome-stable', 'chromium', 'chromium-browser']) {
    try {
      const p = execFileSync('which', [name], { encoding: 'utf8' }).trim();
      if (p) return p;
    } catch {}
  }
  throw new Error('Chrome/Chromium executable not found');
}

function safeName(url, index, contentType) {
  let ext = '.bin';
  if (contentType.includes('json')) ext = '.json';
  else if (contentType.includes('javascript')) ext = '.js';
  else if (contentType.includes('html')) ext = '.html';
  else if (contentType.includes('css')) ext = '.css';
  const u = new URL(url);
  const stem = (u.hostname + u.pathname)
    .replace(/[^a-zA-Z0-9._-]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 150) || 'response';
  return `${String(index).padStart(4, '0')}_${stem}${ext}`;
}

const chrome = findChrome();
const browser = await puppeteer.launch({
  executablePath: chrome,
  headless: true,
  args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--window-size=1600,1200'],
});

const page = await browser.newPage();
await page.setViewport({ width: 1600, height: 1200, deviceScaleFactor: 1 });
await page.setUserAgent('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/151 Safari/537.36');

const network = [];
let responseIndex = 0;
page.on('response', async (response) => {
  const url = response.url();
  const headers = response.headers();
  const contentType = String(headers['content-type'] || '').toLowerCase();
  const status = response.status();
  const request = response.request();
  const row = {
    url,
    status,
    method: request.method(),
    resourceType: request.resourceType(),
    contentType,
    fromCache: response.fromCache(),
    savedFile: null,
    savedBytes: null,
    saveError: null,
  };

  const shouldSave =
    status >= 200 && status < 400 &&
    (contentType.includes('json') || contentType.includes('javascript') || contentType.includes('html') ||
      url.includes('/api/') || url.includes('_next') || url.includes('supabase') || url.includes('firebase'));

  if (shouldSave) {
    try {
      const buf = await response.buffer();
      if (buf.length <= 8_000_000) {
        responseIndex += 1;
        const file = safeName(url, responseIndex, contentType);
        fs.writeFileSync(path.join(NET, file), buf);
        row.savedFile = `network/${file}`;
        row.savedBytes = buf.length;
      } else {
        row.saveError = `body too large: ${buf.length}`;
      }
    } catch (e) {
      row.saveError = String(e);
    }
  }
  network.push(row);
});

const consoleRows = [];
page.on('console', msg => consoleRows.push({ type: msg.type(), text: msg.text() }));
page.on('pageerror', err => consoleRows.push({ type: 'pageerror', text: String(err) }));

const target = 'https://www.neverfuckingtrade.com/';
await page.goto(target, { waitUntil: 'networkidle2', timeout: 150_000 });
await new Promise(r => setTimeout(r, 8_000));

// Scroll through the whole document so lazy-loaded cards and links are materialized.
await page.evaluate(async () => {
  await new Promise(resolve => {
    let total = 0;
    const step = 900;
    const timer = setInterval(() => {
      window.scrollBy(0, step);
      total += step;
      if (total >= document.body.scrollHeight + 2000) {
        clearInterval(timer);
        window.scrollTo(0, 0);
        resolve();
      }
    }, 120);
  });
});
await new Promise(r => setTimeout(r, 5_000));

const extracted = await page.evaluate(() => {
  const visible = (el) => {
    const s = getComputedStyle(el);
    const r = el.getBoundingClientRect();
    return s.display !== 'none' && s.visibility !== 'hidden' && r.width > 0 && r.height > 0;
  };
  const attrs = (el) => Object.fromEntries([...el.attributes].map(a => [a.name, a.value]));
  const elements = [...document.querySelectorAll('h1,h2,h3,h4,h5,h6,article,section,[role="button"],button,a')]
    .filter(visible)
    .map((el, i) => ({
      index: i,
      tag: el.tagName,
      text: (el.innerText || el.textContent || '').trim(),
      href: el.href || null,
      attrs: attrs(el),
    }))
    .filter(x => x.text || x.href);

  const dataElements = [...document.querySelectorAll('*')]
    .filter(el => [...el.attributes].some(a => a.name.startsWith('data-')))
    .map((el, i) => ({ index: i, tag: el.tagName, text: (el.innerText || '').trim().slice(0, 2000), attrs: attrs(el) }));

  const scripts = [...document.scripts].map((s, i) => ({
    index: i,
    src: s.src || null,
    type: s.type || null,
    id: s.id || null,
    text: s.src ? null : (s.textContent || '').slice(0, 2_000_000),
  }));

  return {
    title: document.title,
    url: location.href,
    bodyText: document.body.innerText,
    elements,
    dataElements,
    scripts,
    links: [...document.querySelectorAll('a[href]')].map(a => ({ text: (a.innerText || '').trim(), href: a.href, attrs: attrs(a) })),
    localStorage: Object.fromEntries(Object.keys(localStorage).map(k => [k, localStorage.getItem(k)])),
    sessionStorage: Object.fromEntries(Object.keys(sessionStorage).map(k => [k, sessionStorage.getItem(k)])),
    globals: {
      nextData: document.querySelector('#__NEXT_DATA__')?.textContent || null,
      nuxtData: document.querySelector('#__NUXT_DATA__')?.textContent || null,
    },
  };
});

fs.writeFileSync(path.join(OUT, 'page.html'), await page.content());
fs.writeFileSync(path.join(OUT, 'body.txt'), extracted.bodyText);
fs.writeFileSync(path.join(OUT, 'extracted.json'), JSON.stringify(extracted, null, 2));
fs.writeFileSync(path.join(OUT, 'network_index.json'), JSON.stringify(network, null, 2));
fs.writeFileSync(path.join(OUT, 'console.json'), JSON.stringify(consoleRows, null, 2));
fs.writeFileSync(path.join(OUT, 'resources.json'), JSON.stringify(await page.evaluate(() => performance.getEntriesByType('resource').map(r => ({ name: r.name, initiatorType: r.initiatorType, duration: r.duration, transferSize: r.transferSize }))), null, 2));
await page.screenshot({ path: path.join(OUT, 'page.png'), fullPage: true });

await browser.close();
console.log(JSON.stringify({ chrome, networkRows: network.length, bodyChars: extracted.bodyText.length, elementRows: extracted.elements.length, linkRows: extracted.links.length }));
