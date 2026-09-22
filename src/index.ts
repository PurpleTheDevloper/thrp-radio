import express from 'express'
import { createServer } from 'node:http'
import { WebSocketServer, WebSocket } from 'ws'

const app = express()
const server = createServer(app)

app.get('/healthz', (_req, res) => {
  res.status(200).json({ status: 'ok', service: 'thrp-radio-gateway', version: '19.0.0' })
})

app.get('/', (_req, res) => {
  res.type('html').send(`<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>THRP Radio Gateway</title>
<style>
body{margin:0;background:#090b10;color:#edf1f7;font-family:Inter,system-ui,Arial;min-height:100vh;display:grid;place-items:center}.card{width:min(760px,92vw);background:#11151d;border:1px solid #242b38;border-radius:18px;padding:26px;box-shadow:0 24px 80px #0008}.row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}input,button{background:#0b0f16;color:#fff;border:1px solid #303849;border-radius:10px;padding:12px 14px;font:inherit}button{cursor:pointer}button.ptt{background:#ff7a1a;border-color:#ff7a1a;font-weight:800;min-width:130px}.ok{color:#5ee08a}.bad{color:#ff6b6b}.muted{color:#8d98a8}.meter{height:7px;background:#202633;border-radius:20px;overflow:hidden;margin-top:18px}.meter>div{height:100%;width:0;background:#ff7a1a}.log{margin-top:16px;padding:12px;background:#090c12;border-radius:10px;min-height:54px;font-family:ui-monospace,monospace;font-size:13px;white-space:pre-wrap}</style>
</head>
<body><div class="card">
<h1>THRP Radio Gateway <span class="muted">v19</span></h1>
<p class="muted">Low-latency WebSocket test panel. Enter the same PMA frequency as the officer.</p>
<div class="row"><input id="channel" value="1.1" placeholder="Frequency"/><button id="join">JOIN</button><button class="ptt" id="ptt">HOLD PTT</button><span id="status" class="bad">Disconnected</span></div>
<div class="meter"><div id="meter"></div></div><div class="log" id="log">Ready.</div>
<script>
const $=id=>document.getElementById(id);let ws,channel='0',mic,recorder,ptt=false,sourceId='DISPATCH-'+Math.random().toString(36).slice(2,8);let mediaSource,audio,queue=[],sourceBuffer;
function log(x){$('log').textContent=x+'\n'+$('log').textContent.slice(0,900)}
function connect(){const proto=location.protocol==='https:'?'wss':'ws';ws=new WebSocket(proto+'://'+location.host+'/ws');ws.onopen=()=>{ $('status').textContent='Connected';$('status').className='ok';hello(); };ws.onclose=()=>{ $('status').textContent='Disconnected';$('status').className='bad';setTimeout(connect,1200)};ws.onmessage=e=>{if(typeof e.data!=='string')return;let m;try{m=JSON.parse(e.data)}catch{return}if(m.type==='audio'&&String(m.channel)===String(channel)&&m.source!==sourceId){playChunk(m)}}}
function hello(){if(ws?.readyState===1)ws.send(JSON.stringify({type:'hello',role:'dispatch',source:sourceId,channel}))}
$('join').onclick=()=>{channel=$('channel').value.trim()||'0';hello();log('Joined '+channel)};
async function ensureMic(){if(mic)return;mic=await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true,autoGainControl:true}})}
function toB64(buf){let b='',u=new Uint8Array(buf);for(let i=0;i<u.length;i+=0x8000)b+=String.fromCharCode(...u.subarray(i,i+0x8000));return btoa(b)}
async function startPTT(){if(ptt)return;ptt=true;await ensureMic();const mime=MediaRecorder.isTypeSupported('audio/webm;codecs=opus')?'audio/webm;codecs=opus':'audio/webm';recorder=new MediaRecorder(mic,{mimeType:mime,audioBitsPerSecond:24000});recorder.ondataavailable=async e=>{if(!e.data.size||ws?.readyState!==1)return;const ab=await e.data.arrayBuffer();ws.send(JSON.stringify({type:'audio',channel,source:sourceId,mime,data:toB64(ab)}))};recorder.start(80);ws?.send(JSON.stringify({type:'ptt',active:true,channel,source:sourceId}));$('ptt').textContent='TRANSMITTING';}
function stopPTT(){if(!ptt)return;ptt=false;try{recorder?.stop()}catch{}ws?.send(JSON.stringify({type:'ptt',active:false,channel,source:sourceId}));$('ptt').textContent='HOLD PTT'}
$('ptt').onpointerdown=e=>{e.preventDefault();startPTT().catch(err=>log(err.message))};window.addEventListener('pointerup',stopPTT);
function b64(b){const s=atob(b),u=new Uint8Array(s.length);for(let i=0;i<s.length;i++)u[i]=s.charCodeAt(i);return u}
function setupPlayer(mime){if(mediaSource&&sourceBuffer)return;audio=new Audio();audio.autoplay=true;mediaSource=new MediaSource();audio.src=URL.createObjectURL(mediaSource);mediaSource.addEventListener('sourceopen',()=>{try{sourceBuffer=mediaSource.addSourceBuffer(mime||'audio/webm; codecs="opus"');sourceBuffer.mode='sequence';sourceBuffer.addEventListener('updateend',drain);drain()}catch(e){log('Playback: '+e.message)}})}
function drain(){if(!sourceBuffer||sourceBuffer.updating||!queue.length)return;try{sourceBuffer.appendBuffer(queue.shift())}catch(e){log('Buffer: '+e.message)}}
function playChunk(m){setupPlayer(m.mime);queue.push(b64(m.data));if(queue.length>10)queue.splice(0,queue.length-4);drain();audio?.play().catch(()=>{})}
connect();
</script>
</div></body></html>`)
})

type ClientMeta = { channel: string; role: string; source: string }
const meta = new WeakMap<WebSocket, ClientMeta>()
const sockets = new Set<WebSocket>()
const wss = new WebSocketServer({ server, path: '/ws' })

function safeChannel(v: unknown) {
  const s = String(v ?? '').trim()
  return /^\d+(?:\.\d+)?$/.test(s) ? s : '0'
}

wss.on('connection', (ws) => {
  sockets.add(ws)
  meta.set(ws, { channel: '0', role: 'unknown', source: '' })

  ws.on('message', (raw) => {
    if (typeof raw !== 'object') return
    const text = raw.toString()
    if (text.length > 180_000) return

    let msg: any
    try { msg = JSON.parse(text) } catch { return }

    const current = meta.get(ws) ?? { channel: '0', role: 'unknown', source: '' }

    if (msg.type === 'hello') {
      current.channel = safeChannel(msg.channel)
      current.role = String(msg.role || 'unknown').slice(0, 20)
      current.source = String(msg.source || '').slice(0, 80)
      meta.set(ws, current)
      ws.send(JSON.stringify({ type: 'hello_ack', channel: current.channel, version: '19.0.0' }))
      return
    }

    if (msg.type === 'join') {
      current.channel = safeChannel(msg.channel)
      meta.set(ws, current)
      return
    }

    const channel = safeChannel(msg.channel || current.channel)
    if (channel === '0') return

    if (msg.type === 'audio' || msg.type === 'ptt') {
      const outbound = JSON.stringify({
        ...msg,
        channel,
        source: String(msg.source || current.source || '').slice(0, 80),
      })

      for (const peer of sockets) {
        if (peer === ws || peer.readyState !== WebSocket.OPEN) continue
        const p = meta.get(peer)
        if (!p || p.channel !== channel) continue
        peer.send(outbound)
      }
    }
  })

  ws.on('close', () => sockets.delete(ws))
  ws.on('error', () => sockets.delete(ws))
})

export default server
