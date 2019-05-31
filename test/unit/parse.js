'use strict';

const stream = require('stream');

const getStream = require('get-stream');
const intoStream = require('into-stream');
const test = require('ava');

const delimited = require('../..');

test('returns all data as chunked records', async t => {
    const data = 'foo,bar,baz\n1,2,3\n11,22,33';
    const test = new stream.Readable({
        read() {
            this.push(data);
            this.push(null);
        }
    });

    const parse = delimited.parse({ transform: i => i });

    const results = await getStream.array(test.pipe(parse));

    t.deepEqual(results, [
        [{ foo: '1', bar: '2', baz: '3' }],
        [{ foo: '11', bar: '22', baz: '33' }],
    ]);
});

test('handles empty column', async t => {
    const data = 'foo,bar,baz\n1,,3\n11,22,33';
    const test = new stream.Readable({
        read() {
            this.push(data);
            this.push(null);
        }
    });

    const parse = delimited.parse({ transform: i => i });

    const results = await getStream.array(test.pipe(parse));

    t.deepEqual(results, [
        [{ foo: '1', bar: '', baz: '3' }],
        [{ foo: '11', bar: '22', baz: '33' }],
    ]);
});

test('handles trailing newline', async t => {
    const data = 'foo,bar,baz\n1,2,3\n';
    const test = new stream.Readable({
        read() {
            this.push(data);
            this.push(null);
        }
    });

    const parse = delimited.parse({ transform: i => i });

    const results = await getStream.array(test.pipe(parse));

    t.deepEqual(results, [
        [{ foo: '1', bar: '2', baz: '3' }],
    ]);
});

test('applies transform', async t => {
    const data = 'foo,bar,baz\n1,2,3\n';
    const test = new stream.Readable({
        read() {
            this.push(data);
            this.push(null);
        }
    });

    const transform = record => Object.assign(record, {
        test: true
    });

    const parse = delimited.parse({ transform });

    const results = await getStream.array(test.pipe(parse));

    t.deepEqual(results, [
        [{ foo: '1', bar: '2', baz: '3', test: true }],
    ]);
});

test('incomplete headers in a chunk', async t => {
    const test = new stream.Readable({
        read() {
            this.push('foo');
            this.push(',bar');
            this.push('\n');
            this.push('1,2');
            this.push(null);
        }
    });

    const parse = delimited.parse({ transform: i => i });

    const results = await getStream.array(test.pipe(parse));

    t.deepEqual(results, [
        [{ foo: '1', bar: '2' }],
    ]);
});

test('incomplete row in a chunk', async t => {
    const chunks = ['foo,bar\n', '1', ',2', null];
    const test = new stream.Readable({
        read() {
            if (chunks.length) {
                const chunk = chunks.shift();
                setTimeout(() => {
                    this.push(chunk);
                }, 10);
            }
        }
    });

    const parse = delimited.parse({ transform: i => i });

    const results = await getStream.array(test.pipe(parse));

    t.deepEqual(results, [
        [{ foo: '1', bar: '2' }],
    ]);
});

test('rows in chunks (with synthetic delay)', async t => {
    const chunks = ['foo,bar\n', '1,2\n', '11,22', null];
    const test = new stream.Readable({
        read() {
            if (chunks.length) {
                const chunk = chunks.shift();
                setTimeout(() => {
                    this.push(chunk);
                }, 10);
            }
        }
    });

    const parse = delimited.parse({ transform: i => i });

    const results = await getStream.array(test.pipe(parse));

    t.deepEqual(results, [
        [{ foo: '1', bar: '2' }],
        [{ foo: '11', bar: '22' }],
    ]);
});

test('chunks of larger data', async t => {
    const chunkSize = 16 * 1024;
    const data = 'h1,h2,h3,h4,h5,h6,h7\n' + Array(100000).fill().map(() => {
        return 'foo,bar,baz,qwerty,asdf,1234567890,aaaaaaaabbbbbbbbcccccccc';
    }).join('\n');

    const numChunks = Math.ceil(data.length / chunkSize)
    const chunks = new Array(numChunks)
  
    for (let i = 0, o = 0; i < numChunks; ++i, o += chunkSize) {
      chunks[i] = data.substr(o, chunkSize);
    }

    const source = intoStream(chunks);
    const parse = delimited.parse({ transform: i => i });
    const results = await getStream.array(source.pipe(parse));

    const flat = results.reduce((memo, item) => memo.concat(item), []);
    t.is(flat.length, 100000);
})