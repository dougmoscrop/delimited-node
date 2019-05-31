# delimited

At this time this library is _deliberately non-compliant_ and minimal, for use in certain scenarios where performance is critical.

It does not handle escaping characters or anything like that, only newline-delimited rows of comma-delimited values. You should neer feed it 'user input', but only use it for things like storing a big view of data (that you control the format of) in S3.

## Updates

This library is used to implement The Lambda Architecture and as part of that, the general flow is a read-update-write. For efficiency sake, 