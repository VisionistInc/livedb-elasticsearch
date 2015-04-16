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
      callback(null, response._source._val);
    }
  });
};

LiveDbElasticsearch.prototype.writeSnapshot = function(cName, docName, data, callback) {
  this.client.index({
    index: 'snapshot',
    type: cName,
    id: docName,
    body: {
      // TODO necessary?  okay to exclude other values?
      // this is just to accommodate values that aren't already key,value pairs
      _val: data
    },
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

// TODO implement optional bulkGetSnapshot
//LiveDbElasticsearch.prototype.bulkGetSnapshot(requests, callback)

/********************
 * Operation Log API
 ********************/

// ops are stored at ops-<cname>/<docName>/<opData.v>
// opData.v is the version number
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

//TODO make sure this is not off-by-one
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

// Get operations between [start, end) noninclusively. (Ie, the range should
// contain start but not end).
//
// If end is null, this function should return all operations from start onwards.
//
// The operations that getOps returns don't need to have a version: field.
// The version will be inferred from the parameters if it is missing.
//
// Callback should be called as callback(error, [list of ops]);
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

/*******************
 * Helper functions
 *******************/

/**
 * Extract data from elasticsearch responses, handle errors, handle callbacks.
 */
function parseResponse(error, response, callback) {
  //console.log("error", error);
  //console.log("response", response);

  if(error) {
    // don't treat not found as an error (is this appropriate?)
    if (error.message === "Not Found" && !response.found) {

      callback(null, null);
      return;

    } else {
      console.error("livedb-elasticsearch", error);
    }
  }

  if(!error && response) {
    response = response._source;

    if (response && response._val) {
      response = response._val;
    }

    response = JSON.stringify(response);
  }

  callback(error, response);
}

// expose the elasticsearch liveDB implementation
module.exports = LiveDbElasticsearch;
