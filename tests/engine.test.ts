import {test} from 'node:test';
import assert from 'node:assert/strict';
import {Engine} from '../src/engine.ts';
const q={source:'a',pair:'EUR/USD',bid:1000000,ask:1000010,seq:1,ts:10000};

test('rejects replay, crossed and stale quotes',()=>{const e=new Engine();assert.ok(e.ingest(q,10000));assert.equal(e.ingest(q,10000),null);assert.equal(e.ingest({...q,seq:2,ask:1},10000),null);assert.equal(e.ingest({...q,seq:3},20000),null);assert.equal(e.price(q.pair,20000),null);assert.equal(e.history(q.pair).length,1);});
test('median resists an outlier',()=>{const e=new Engine();for(const [source,bid] of [['a',1000000],['b',1000010],['c',9000000]] as const)e.ingest({...q,source,bid,ask:bid+10},10000);assert.equal(e.price(q.pair,10000)?.mid,1000015);});
