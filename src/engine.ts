import { DatabaseSync } from 'node:sqlite';
export type Quote = {source:string; pair:string; bid:number; ask:number; seq:number; ts:number};
export class Engine {
  db: DatabaseSync; accepted=0; rejected=0; ttl:number;
  constructor(path=':memory:',ttl=5000) {
    this.ttl=ttl; this.db=new DatabaseSync(path);
    this.db.exec(`PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS quotes(source TEXT,pair TEXT,bid INTEGER,ask INTEGER,seq INTEGER,ts INTEGER,PRIMARY KEY(source,pair)); CREATE TABLE IF NOT EXISTS history(id INTEGER PRIMARY KEY,pair TEXT,bid INTEGER,ask INTEGER,ts INTEGER); CREATE INDEX IF NOT EXISTS history_pair_ts ON history(pair,ts);`);
  }
  ingest(q:Quote,now=Date.now()) {
    if (!q || typeof q.source!=='string'|| !/^[a-zA-Z0-9_-]{1,32}$/.test(q.source)|| typeof q.pair!=='string'|| !/^[A-Z]{3}\/[A-Z]{3}$/.test(q.pair)|| ![q.bid,q.ask,q.seq,q.ts].every(Number.isSafeInteger)||q.bid<=0||q.ask<q.bid||q.seq<0||q.ts<now-this.ttl||q.ts>now+1000) {this.rejected++;return null;}
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const old=this.db.prepare('SELECT seq,ts FROM quotes WHERE source=? AND pair=?').get(q.source,q.pair);
      if(old && (q.seq<=Number(old.seq)||q.ts<Number(old.ts))) {this.db.exec('ROLLBACK');this.rejected++;return null;}
      this.db.prepare('INSERT INTO quotes VALUES(?,?,?,?,?,?) ON CONFLICT(source,pair) DO UPDATE SET bid=excluded.bid,ask=excluded.ask,seq=excluded.seq,ts=excluded.ts').run(q.source,q.pair,q.bid,q.ask,q.seq,q.ts);
      const price=this.price(q.pair,now)!;
      this.db.prepare('INSERT INTO history(pair,bid,ask,ts) VALUES(?,?,?,?)').run(q.pair,price.bid,price.ask,now);
      this.db.exec('COMMIT');this.accepted++;return price;
    } catch(e) {this.db.exec('ROLLBACK');throw e;}
  }
  price(pair:string,now=Date.now()) {
    const rows=this.db.prepare('SELECT bid,ask FROM quotes WHERE pair=? AND ts>=? AND ts<=?').all(pair,now-this.ttl,now+1000);
    if(!rows.length)return null;
    // Median of provider midpoints; use median spread, not crossed best-of-book.
    const median=(a:number[])=>{a.sort((x,y)=>x-y);return a.length%2?a[a.length>>1]:Math.round((a[a.length/2-1]+a[a.length/2])/2);};
    const mid=median(rows.map(r=>Math.round(Number(r.bid)/2+Number(r.ask)/2)));
    const spread=median(rows.map(r=>Number(r.ask)-Number(r.bid)));
    const bid=mid-Math.floor(spread/2);return {pair,bid,ask:bid+spread,mid,sources:rows.length,ts:now,scale:1000000};
  }
  history(pair:string,limit=100){return this.db.prepare('SELECT pair,bid,ask,ts FROM history WHERE pair=? ORDER BY ts DESC,id DESC LIMIT ?').all(pair,Math.min(1000,Math.max(1,limit)));}
}
