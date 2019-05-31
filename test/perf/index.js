'use strict';

const { performance } = require('perf_hooks');

const chunk = require('chunk');
const csv = require('csv');
const fastCsv = require('fast-csv');
const getStream = require('get-stream');
const intoStream = require('into-stream');
const percentile = require('percentile');
const test = require('ava');

const delimited = require('../..');

test.serial('can parse 100,000 in under 200ms', async t => {
    const data = 'h1,h2,h3,h4,h5,h6,h7\n' + Array(100000).fill().map(() => {
        return 'foo,bar,baz,qwerty,asdf,1234567890,aaaaaaaabbbbbbbbcccccccc';
    }).join('\n');

    const samples = [];

    for (let i = 0; i < 20; i += 1) {
        const source = intoStream(data);
        const parse = delimited.parse();

        const begin = performance.now();
        await getStream.array(source.pipe(parse));
        const duration = performance.now() - begin;

        samples.push(duration);
    }

    const p95 = percentile(95, samples);

    t.true(p95 < 200);
});

test.serial('can stringify 100,000 records in under 500ms', async t => {
    const records = Array(100000).fill().map(() => {
        return {
            h1: 'foo',
            h2: 'bar',
            h3: 'baz',
            h4: 'qwerty',
            h5: 'asdf',
            h6: '1234567890',
            h7: 'aaaaaaaabbbbbbbbcccccccc'
        };
    });

    const samples = [];

    for (let i = 0; i < 20; i += 1) {
        const source = intoStream.object(records);
        const stringify = delimited.stringify({ fields: ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7' ]});

        const begin = performance.now();
        await getStream.array(source.pipe(stringify));
        const duration = performance.now() - begin;

        samples.push(duration);
    }

    const p95 = percentile(95, samples);

    t.true(p95 < 500);
});

test.serial('can stringify 100,000 chunked records in under 200ms', async t => {
    const records = Array(100000).fill().map(() => {
        return {
            h1: 'foo',
            h2: 'bar',
            h3: 'baz',
            h4: 'qwerty',
            h5: 'asdf',
            h6: '1234567890',
            h7: 'aaaaaaaabbbbbbbbcccccccc'
        };
    });

    const chunks = chunk(records, 10);

    const samples = [];

    for (let i = 0; i < 20; i += 1) {
        const source = intoStream.object(chunks);
        const stringify = delimited.stringify({ fields: ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7' ]});

        const begin = performance.now();
        await getStream.array(source.pipe(stringify));
        const duration = performance.now() - begin;

        samples.push(duration);
    }

    const p95 = percentile(95, samples);

    t.true(p95 < 200);
});

test.serial('parses faster than fast-csv', async t => {
    const data = 'h1,h2,h3,h4,h5,h6,h7\n' + Array(100000).fill().map(() => {
        return 'foo,bar,baz,qwerty,asdf,1234567890,aaaaaaaabbbbbbbbcccccccc';
    }).join('\n');

    const fastCsvBegin = performance.now();
    await getStream.array(intoStream(data).pipe(fastCsv.parse()));
    const fastCsvDuration = performance.now() - fastCsvBegin;

    const delimitedBegin = performance.now();
    await getStream.array(intoStream(data).pipe(delimited.parse()));
    const delimitedDuration = performance.now() - delimitedBegin;

    t.true(delimitedDuration < fastCsvDuration);
});

test.serial('parses faster than csv', async t => {
    const data = 'h1,h2,h3,h4,h5,h6,h7\n' + Array(100000).fill().map(() => {
        return 'foo,bar,baz,qwerty,asdf,1234567890,aaaaaaaabbbbbbbbcccccccc';
    }).join('\n');

    const fastCsvBegin = performance.now();
    await getStream.array(intoStream(data).pipe(csv.parse()));
    const fastCsvDuration = performance.now() - fastCsvBegin;

    const delimitedBegin = performance.now();
    await getStream.array(intoStream(data).pipe(delimited.parse()));
    const delimitedDuration = performance.now() - delimitedBegin;

    t.true(delimitedDuration < fastCsvDuration);
});

test.serial('stringify faster than fast-csv', async t => {
    const records = Array(100000).fill().map(() => {
        return {
            h1: 'foo',
            h2: 'bar',
            h3: 'baz',
            h4: 'qwerty',
            h5: 'asdf',
            h6: '1234567890',
            h7: 'aaaaaaaabbbbbbbbcccccccc'
        };
    });

    const fastCsvBegin = performance.now();
    await getStream(intoStream.object(records).pipe(fastCsv.format({ headers: ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7' ] })));
    const fastCsvDuration = performance.now() - fastCsvBegin;

    const delimitedBegin = performance.now();
    await getStream(intoStream.object(records).pipe(delimited.stringify({ fields: ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7' ] })));
    const delimitedDuration = performance.now() - delimitedBegin;

    t.true(delimitedDuration < fastCsvDuration);
});

test.serial('stringify faster than csv', async t => {
    const records = Array(100000).fill().map(() => {
        return {
            h1: 'foo',
            h2: 'bar',
            h3: 'baz',
            h4: 'qwerty',
            h5: 'asdf',
            h6: '1234567890',
            h7: 'aaaaaaaabbbbbbbbcccccccc'
        };
    });

    const fastCsvBegin = performance.now();
    await getStream(intoStream.object(records).pipe(csv.stringify({ headers: ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7' ] })));
    const fastCsvDuration = performance.now() - fastCsvBegin;

    const delimitedBegin = performance.now();
    await getStream(intoStream.object(records).pipe(delimited.stringify({ fields: ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7' ] })));
    const delimitedDuration = performance.now() - delimitedBegin;

    t.true(delimitedDuration < fastCsvDuration);
});