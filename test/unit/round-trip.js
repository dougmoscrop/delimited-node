'use strict';

const getStream = require('get-stream');
const intoStream = require('into-stream');
const test = require('ava');

const delimited = require('../..');

test('same out as in', async t => {
    const data = 'h1,h2,h3\n' + Array(100000).fill().map(() => {
        return 'foo,bar,baz';
    }).join('\n');

    const pipeline = intoStream(data).pipe(delimited.parse()).pipe(delimited.stringify({ fields: ['h1', 'h2', 'h3'] }));
    const result = await getStream(pipeline);

    t.deepEqual(result, data);
})