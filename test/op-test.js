// testing the operations api calls

// adapted from:
// https://github.com/usecanvas/livedb-postgresql/blob/master/test/op-test.coffee

var elasticsearch = require('elasticsearch'),
    async = require('async'),
    expect = require('expect.js'),
    LiveES = require('../elasticsearch.js');

var es_client = new elasticsearch.Client({
  host: 'localhost:9200'
});

var callbackHandler = function(callback) {
  return function (error, response) {
    callback(error, error, response);
  };
};

describe('livedb-elasticsearch (operations)', function() {
  var liveES;

  beforeEach(function(done) {
    liveES = new LiveES({ host: 'localhost:9200' });
    liveES.writeSnapshot('coll', 'doc', {v:1}, done);
  });

  afterEach(function(done) {
    es_client.indices.delete({index: '_all'}, function(error, response) {
      done();
    });
  });

  describe('#writeOp', function() {
    it("returns the written op", function(done) {
      liveES.writeOp('coll', 'doc', {v:1}, function(error, response) {
        if (error) throw error;
        expect(response).to.eql({v:1});
        done();
      });
    });
  });

  describe('#getVersion', function() {
    it('returns 0 if there are no ops', function(done) {
      liveES.getVersion('coll', 'doc', function(error, response) {
        if (error) throw error;
        expect(response).to.eql(0);
        done();
      });
    });

    it('returns 1 if there is one op', function(done) {
      async.waterfall([
        function(callback) {
          liveES.writeOp('coll', 'doc', {v:0}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.getVersion('coll', 'doc', callback);
        },
      ], function(error, response) {
        es_client.search({}, function(e, r) {
          if (error) throw error;
          expect(response).to.eql(1);
          done();
        });

      });
    });

    it('returns the next version of the document when there are ops', function(done) {
      async.waterfall([
        function(callback) {
          liveES.writeOp('coll', 'doc', {v:0}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.writeOp('coll', 'doc', {v:1}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.writeOp('coll', 'doc', {v:2}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.getVersion('coll', 'doc', callback);
        },
      ], function(error, response) {
        if (error) throw error;
        expect(response).to.eql(3);
        done();
      });
    });
  });

  describe('#getOps', function() {
    it('returns an empty array if there are no ops in the range', function(done) {
      liveES.getOps('coll', 'doc', 1, 2, function(error, response) {
        if (error) throw error;
        expect(response).to.eql([]);
        done();
      });
    });

    it('does not return ops from other collections or docs', function(done) {
      async.waterfall([
        function(callback) {
          liveES.writeOp('collother', 'doc', {v:1}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.writeOp('coll', 'docother', {v:1}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.writeOp('coll', 'doc', {v:1}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.writeOp('coll', 'doc', {v:2}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.getOps('coll', 'doc', 1, null, callback);
        },
      ], function(error, response) {
        if (error) throw error;
        expect(response).to.eql([{v:1}, {v:2}]);
        done();
      });
    });

    it('returns the ops, non-inclusively', function(done) {
      async.waterfall([
        function(callback) {
          liveES.writeOp('coll', 'doc', {v:1}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.writeOp('coll', 'doc', {v:2}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.writeOp('coll', 'doc', {v:3}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.getOps('coll', 'doc', 1, 3, callback);
        },
      ], function(error, response) {
        if (error) throw error;
        expect(response).to.eql([{v:1}, {v:2}]);
        done();
      });
    });

    it('returns the requested until the end, if no upper limit is provided', function(done) {
      async.waterfall([
        function(callback) {
          liveES.writeOp('coll', 'doc', {v:1}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.writeOp('coll', 'doc', {v:2}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.writeOp('coll', 'doc', {v:3}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.getOps('coll', 'doc', 2, null, callback);
        },
      ], function(error, response) {
        if (error) throw error;
        expect(response).to.eql([{v:2}, {v:3}]);
        done();
      });
    });
  });
});
