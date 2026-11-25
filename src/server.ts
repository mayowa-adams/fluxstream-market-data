import {createServer} from 'node:http';
import {createHash} from 'node:crypto';
import type {Socket} from 'node:net';
import {Engine} from './engine.ts';
import {body,send} from './http.ts';
const engine=new Engine(process.env.DB_PATH||'flux.db');
const clients=new Map<Socket,string>(); let dropped=0;
function frame(value:unknown){const b=Buffer.from(JSON.stringify(value));const h=b.length<126?Buffer.from([129,b.length]):Buffer.from([129,126,b.length>>8,b.length&255]);return Buffer.concat([h,b]);}
function publish(q:any){for(const [s,p] of clients)if(p===q.pair){if(s.writableLength>65536){dropped++;s.destroy();}else s.write(frame(q));}}
const server=createServer(async(req,res)=>{try{
 const u=new URL(req.url||'/', 'http://localhost');
 if(u.pathname==='/health')return send(res,200,{ok:true});
 if(u.pathname==='/metrics'){res.writeHead(200,{'content-type':'text/plain'});return res.end(`flux_accepted_total ${engine.accepted}\nflux_rejected_total ${engine.rejected}\nflux_clients ${clients.size}\nflux_dropped_total ${dropped}\n`);}
 if(req.method==='POST'&&u.pathname==='/quotes'){const q=engine.ingest(await body(req));if(!q)return send(res,422,{error:'invalid, stale or duplicate quote'});publish(q);return send(res,202,q);}
 if(u.pathname==='/price'){const q=engine.price(u.searchParams.get('pair')||'');return send(res,q?200:404,q||{error:'no fresh price'});}
 if(u.pathname==='/history')return send(res,200,engine.history(u.searchParams.get('pair')||'',100));
 send(res,404,{error:'not found'});
 }catch{send(res,400,{error:'invalid request'});}});
server.on('upgrade',(req,socket,head)=>{const s=socket as Socket;const u=new URL(req.url||'/', 'http://localhost');const key=req.headers['sec-websocket-key'];const pair=u.searchParams.get('pair')||'';
 if(u.pathname!=='/stream'||!key||req.headers['sec-websocket-version']!=='13'||! /^[A-Z]{3}\/[A-Z]{3}$/.test(pair)||clients.size>=1000){s.end('HTTP/1.1 400 Bad Request\r\n\r\n');return;}
 const accept=createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');s.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);clients.set(s,pair);s.on('close',()=>clients.delete(s));s.on('error',()=>s.destroy());
 // Read-only demo protocol: close on any client frame; no application messages accepted.
 s.on('data',()=>s.end(Buffer.from([0x88,0])));if(head.length)s.end(Buffer.from([0x88,0]));const q=engine.price(pair);if(q)s.write(frame(q));
});
let seq=Date.now();if(process.env.SIMULATE!=='0')setInterval(()=>{for(const [pair,base] of [['EUR/USD',1080000],['GBP/USD',1270000]] as const){for(const source of ['alpha','beta','gamma']){const bid=base+Math.floor(Math.random()*100)-50;const q=engine.ingest({source,pair,bid,ask:bid+12,seq:++seq,ts:Date.now()});if(q)publish(q);}}},250).unref();
server.listen(Number(process.env.PORT||8080),'0.0.0.0');
