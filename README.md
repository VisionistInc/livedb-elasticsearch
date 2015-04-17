# livedb-elasticsearch
An [Elasticsearch](https://www.elastic.co/products/elasticsearch) adapter for [livedb](https://github.com/share/livedb).

## Installation
`npm install --save VisionistInc/livedb-elasticsearch`

## Example usage
```javascript
var LiveElasticsearch = require('livedb-elasticsearch');
var liveES = new LiveElasticsearch({ host: 'localhost:9200' });

// with liveDB
var livedb = require('livedb');
var liveClient = livedb.client(liveES);

// with sharejs
var sharejs = require('share');
var share = sharejs.server.createClient({backend: liveClient});
```

## Elasticsearch
Elasticsearch 1.4+ is required.  Documents will be stored at the following locations:

### Snapshots route
`snapshot/{cName}/{docName}`

### Operations route
`/ops-{cName}/{docName}/{version}`

## Testing
Assuming elasticsearch is running at `localhost:9200`, run `npm test`.

## Limitations
Currently, does not implement the [Query support API](https://github.com/share/livedb/blob/master/lib/memory.js#L109).
