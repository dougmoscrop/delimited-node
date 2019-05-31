'use strict';

const getStream = require('get-stream');
const intoStream = require('into-stream');
const test = require('ava');

const delimited = require('../..');

test('takes chunked records', async t => {
    const stringify = delimited.stringify({ fields: ['id', 'v', 'foo']});
    const stream = intoStream.object([[{ id: 'foo', v: 1, foo: 'bar' }]]);

    const result = await getStream(stream.pipe(stringify));

    t.deepEqual(result, 'id,v,foo\nfoo,1,bar');
});

test('adds empty columns', async t => {
    const stringify = delimited.stringify({ fields: ['id', 'v', 'foo']});
    const stream = intoStream.object([[{ id: 'foo', v: 1 }]]);

    const result = await getStream(stream.pipe(stringify));

    t.deepEqual(result, 'id,v,foo\nfoo,1,');
});

test('multiple chunks', async t => {
    const stringify = delimited.stringify({ fields: ['id', 'v', 'baz']});
    const stream = intoStream.object([[{ id: 'foo', v: 1 }], [{ id: 'bar', v: 2 }]]);

    const result = await getStream(stream.pipe(stringify));

    t.deepEqual(result, 'id,v,baz\nfoo,1,\nbar,2,');
});

test('non-chunk', async t => {
    const stringify = delimited.stringify({ fields: ['id', 'v']});
    const stream = intoStream.object([{ id: 'foo', v: 1 }]);

    const result = await getStream(stream.pipe(stringify));

    t.deepEqual(result, 'id,v\nfoo,1');
});
