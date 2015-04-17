var elasticsearch = require('elasticsearch');

/*
 * @param {options} available options include:
 *   - host - a domainname:port combination pointing to the elasticsearch cluster
 *   - api - which version of the elasticsearch api should be used? (default 1.5)
 *   - log - log level ('trace' is good for debugging) (omitted by default)
 * url:port combination where elasticsearch cluster can be found
 */
function LiveDbElasticsearch(options) {
  if (!options || !options.host) {
    console.error("Must provide a host to instantiate liveDB-elasticsearch!");
    return null;
  }

  if (!options.api) {
    options.api = '1.5';
  }

  //console.info("Elasticsearch liveDB will be used with these settings:", options);

  this.client = new elasticsearch.Client(options);
}

// elasticsearch connections don't need to be closed
LiveDbElasticsearch.prototype.close = function(callback) {
  callback(null);
};

/*******************
 * Snapshot API
 *******************/

/*
 * Retrieve the snapshot data stored at 'snapshot/cName/docName'.
 */
LiveDbElasticsearch.prototype.getSnapshot = function(cName, docName, callback) {
  this.client.get({
    index: 'snapshot',
    type: cName,
    id: docName
  }, function(error, response) {
    if (error) {
      if (response && response.status === 404) {
        // if missing, return null (not error)
        callback(null, null);
      } else {
        callback(error, null);
      }
    } else {
      // found, return value
      callback(null, response._source);
    }
  });
};

/*
 * Store the given snapshot data at 'snapshot/cName/docName'.
 */
LiveDbElasticsearch.prototype.writeSnapshot = function(cName, docName, data, callback) {
  this.client.index({
    index: 'snapshot',
    type: cName,
    id: docName,
    body: data,
    refresh: true
  }, function(error, response) {
    if (error) callback(error, null);
    else if (response) {
      if (response.created || response._version > 1) {
        callback(null, data);
      } else {
        console.error("writeSnapshot unable to index data", response);
        callback("writeSnapshot: error, unable to index data", null);
      }
    }
  });
};

/*
 * Retrieve the specified snapshots.
 *
 * @param {requests} should be formatted like this:
 *   { collA: ['doc1', 'doc2'], collB: ['doc3'] }
 *
 * Callback accepts parameters (error, { collA: { doc1: { snapshot }} ... })
 */
LiveDbElasticsearch.prototype.bulkGetSnapshot = function(requests, callback) {
  var docs = [],
      results = {};
  for (var type in requests) {
    if (requests.hasOwnProperty(type)) {
      results[type] = {};
      for (var idCounter = 0; idCounter < requests[type].length; idCounter++) {
        docs.push({
          _type: type,
          _id: requests[type][idCounter]
        });
      }
    }
  }

  this.client.mget({
    index: 'snapshot',
    body: {
      docs: docs
    }
  }, function(error, response) {
    if (error) callback(error, null);
    else if (response) {
      for (var i = 0; i < response.docs.length; i++) {
        var doc = response.docs[i];
        if (doc.found) {
          results[doc._type][doc._id] = doc._source;
        }
      }
      callback(null, results);
    }
  });
};

/********************
 * Operation Log API
 ********************/

/*
 * Store the given opData at 'ops-<cName>/<docName>/<opData.v>', where
 * opData.v is the version number of the document.
 */
LiveDbElasticsearch.prototype.writeOp = function(cName, docName, opData, callback) {
  var index = ('ops-' + cName).toLowerCase();

  this.client.index({
    index: index,
    type: docName,
    id: opData.v,
    body: opData,
    // document is not available for search until after refresh
    refresh: true
  }, function(error, response) {
    if (error) callback(error, null);
    else if (response) {
      if (response.created || response._version > 1) {
        callback(null, opData);
      } else {
        console.error("writeOp unable to index opData", response);
        callback("writeOp: error, unable to index data", null);
      }
    }
  });
};

/*
 * Get the *next* version number to be used for the document.
 *
 * If the document does not exist, returns 0.
 * Otherwise, returns the document count (versions are 0-indexed).
 */
LiveDbElasticsearch.prototype.getVersion = function(cName, docName, callback) {
  var index = ('ops-' + cName).toLowerCase();
  this.client.count({
    index: index,
    type: docName,
    ignore_unavailable: true
  }, function(error, response) {
    if (error) callback(error, null);
    else callback(null, response.count);
  });
};

/*
 * Get operations between [start, end) non-inclusively.  (The range should
 * contain the start index but not the end).
 *
 * If end is null, return all operations from start onwards.
 *
 * Callback parameters are (error, [list of ops])
 */
LiveDbElasticsearch.prototype.getOps = function(cName, docName, start, end, callback) {
  var index = ('ops-' + cName).toLowerCase();
  var opsFilter = { gte: start };

  if (end) {
    opsFilter.lt = end;
  }

  this.client.search({
    index: index,
    type: docName,
    body: {
      query: {
        range: {
          // the _id field is not indexed, so search on the version field
          v: opsFilter
        }
      }
    },
    ignore_unavailable: true,
    // TODO elasticsearch advocates using the 'scroll' functionality when retrieving
    // entire ranges ... for now, just limiting to 100, but might need to rewrite
    // this to accumulate batch by batch up to the required amount.
    size: 100
  }, function(error, response) {
    //console.log("getops error", error);
    //console.log("getops response", response);
    if (error) callback(error, null);
    else {
      var results = [];
      for (var i = 0; i < response.hits.total; i++) {
        results.push(response.hits.hits[i]._source);
      }
      callback(null, results);
    }
  });
};

// expose the elasticsearch liveDB implementation
module.exports = LiveDbElasticsearch;
