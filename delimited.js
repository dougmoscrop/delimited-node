'use strict';

const { Transform } = require('stream');

const flatstr = require('flatstr');

module.exports.parse = ({ transform = i => i } = {}) => {
    let headers;
    let numHeaders;
    let remainder = '';

    function getRecord(row) {
        const obj = {};
        const fields = row.split(',');

        for (let h = 0; h < numHeaders; h += 1) {
            obj[headers[h]] = fields[h];
        }

        return transform(obj);
    }

    function parseRows(str) {
        const rows = str.split('\n');
        const tail = rows.pop();

        let numRows = rows.length;

        if (numRows > 0) {
            if (headers === undefined) {
                headers = rows.shift().split(',');
                numHeaders = headers.length;
                numRows -= 1;
            }

            if (numRows > 0) {
                for (let r = 0; r < numRows; r += 1) {
                    rows[r] = getRecord(rows[r]);
                }
                this.push(rows);
            }
        }

        return tail;
    }

    return new Transform({
        objectMode: true,
        highWaterMark: 1024 * 1024,
        transform(chunk, enc, cb) {
            const data = remainder + chunk.toString();
            remainder = parseRows.call(this, data);

            cb();
        },
        flush(cb) {
            if (remainder) {
                remainder = parseRows.call(this, remainder);

                if (remainder) {
                    const record = getRecord(remainder);
                    this.push([record]);
                }
            }

            cb();
        },
    });
};

module.exports.stringify = ({ fields }) => {
    const headersRow = `${fields.join(',')}\n`;
    const numFields = fields.length;
    const lastHeader = numFields - 1;

    let headersDone = false;
    let newlinePending = false;

    return new Transform({
        highWaterMark: 2, // TODO: effect?
        objectMode: true,
        transform(chunk, enc, cb) {
            if (headersDone === false) {
                this.push(headersRow);
                headersDone = true;
            }

            const records = Array.isArray(chunk) ? chunk: [chunk];
            const numRecords = records.length;
            const lastRow = numRecords - 1;

            if (numRecords > 0) {
                let chunk = '';

                if (newlinePending) {
                    chunk = '\n';
                    newlinePending = false;
                }

                for (let r = 0; r < numRecords; r += 1) {
                    const record = records[r];

                    for (let f = 0; f < numFields; f += 1) {
                        const field = record[fields[f]];

                        if (field !== undefined && field !== null) {
                            chunk += field.toString();
                        }

                        if (f === lastHeader) {
                            if (r === lastRow) {
                                newlinePending = true;
                            } else {
                                chunk += '\n';
                            }
                        } else {
                            chunk += ',';
                        }
                    }
                }

                this.push(flatstr(chunk));
            }

            cb();
        },
    });
};