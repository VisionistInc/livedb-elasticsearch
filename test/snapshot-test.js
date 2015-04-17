// testing the snapshots api calls

// adapted from:
// https://github.com/usecanvas/livedb-postgresql/blob/master/test/snapshot-test.coffee

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

describe('livedb-elasticsearch (snapshots)', function() {
  var liveES;

  beforeEach(function(done) {
    liveES = new LiveES({ host: 'localhost:9200' });
    liveES.writeOp('coll', 'doc', {v:1}, done);
  });

  afterEach(function(done) {
    es_client.indices.delete({index: '_all'}, function(error, response) {
      done();
    });
  });

  describe('#getSnapshot', function() {
    it('returns null when the document does not exist', function(done) {
      liveES.getSnapshot('coll', 'doc', function(error, response) {
        if (error) throw error;
        expect(response).to.eql(null);
        done();
      });
    });

    it('returns a document when it exists', function(done) {
      async.waterfall([
        function(callback) {
          liveES.writeSnapshot('coll', 'doc', {v:1}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.getSnapshot('coll', 'doc', callback);
        },
      ], function(error, response) {
        if (error) throw error;
        expect(response).to.eql({v:1});
        done();
      });
    });
  });

  describe("#writeSnapshot", function() {
    it('returns the value that it wrote', function(done) {
      liveES.writeSnapshot('coll', 'doc', {v:1}, function(error, response) {
        if (error) throw error;
        expect(response).to.eql({v:1});
        done();
      });
    });

    it('inserts the document when it does not exist', function(done) {
      async.waterfall([
        function(callback) {
          liveES.writeSnapshot('coll', 'doc', {v:1}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.getSnapshot('coll', 'doc', callback);
        },
      ], function(error, response) {
        if (error) throw error;
        expect(response).to.eql({v:1});
        done();
      });
    });

    it('updates the document when it already exists', function(done) {
      async.waterfall([
        function(callback) {
          liveES.writeSnapshot('coll', 'doc', {v:1}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.writeSnapshot('coll', 'doc', {v:2}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.getSnapshot('coll', 'doc', callback);
        },
      ], function(error, response) {
        if (error) throw error;
        expect(response).to.eql({v:2});
        done();
      });
    });
  });

  describe('#bulkGetSnapshot', function() {
    it('returns all documents found', function(done) {
      async.waterfall([
        function(callback) {
          liveES.writeSnapshot('collA', 'docA', {v:1}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.writeSnapshot('collA', 'docB', {v:2}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.writeSnapshot('collB', 'docC', {v:2}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.writeSnapshot('collB', 'docD', {v:2}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.bulkGetSnapshot({ collA: ['docA'], collB: ['docC', 'docD']}, callback);
        },
      ], function(error, response) {
        if (error) throw error;
        expect(response).to.eql({
          collA: { docA: {v:1}},
          collB: { docC: {v:2}, docD: {v:2}}
        });
        done();
      });
    });

    it('returns empty objects for collections with no documents found', function(done) {
      liveES.bulkGetSnapshot({'coll': ['doc']}, function(error, response) {
        if (error) throw error;
        expect(response).to.eql({coll: {}});
        done();
      });
    });

    it('does not return nonexistent documents', function(done) {
      async.waterfall([
        function(callback) {
          liveES.writeSnapshot('collA', 'docA', {v:1}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.writeSnapshot('collA', 'docB', {v:2}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.writeSnapshot('collB', 'docC', {v:2}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.writeSnapshot('collB', 'docD', {v:2}, callbackHandler(callback));
        },
        function(error, response, callback) {
          if (error) throw error;
          liveES.bulkGetSnapshot({ collA: ['docA'], collB: ['docB', 'docC']}, callback);
        },
      ], function(error, response) {
        if (error) throw error;
        expect(response).to.eql({
          collA: { docA: {v:1}},
          collB: { docC: {v:2}}
        });
        done();
      });
    });
  });
});
