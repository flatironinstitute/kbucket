exports.KBucketHubManager=KBucketHubManager;

const async=require('async')
const fs=require('fs');

const HttpOverWebSocketClient = require(__dirname + '/httpoverwebsocket.js').HttpOverWebSocketClient;

var LIMITS={
  max_kbshare_connections:1e3,
  max_files_per_kbshare_connection:30,
  max_kbhub_connections:10
};

function KBucketHubManager(config) {
  // Encapsulates some functionality of kbucket-hub
  this.findFile = function(opts, callback) {
    findFile(opts, callback);
  };
  this.connectedShareManager = function() {
    return m_connected_share_manager;
  };

  // The share manager (see KBucketConnectedShareManager)
  var m_connected_share_manager = new KBConnectedShareManager();
  // The connected hub manager (see KBConnectedHubManager)
  //var m_connected_hub_manager=new KBConnectedHubManager();

  function findFile(opts, callback) {
    // Find a file, either on the local kbucket-hub disk, on one of the connected shares, on the parent hub, or on one of the connected (child or parent) hubs
    if (!is_valid_sha1(opts.sha1)) {
      // Not a valid sha1 hash
      callback(`Invalid sha1: ${opts.sha1}`);
      return;
    }

    // We will fill in the following data and then use it below
    var hub_err = null,
      hub_resp = null;
    var shares_err = null,
      shares_resp = null;

    async.series([
      function(cb) {
        // Check on the local kbucket-hub disk
        find_file_on_hub(opts, function(err, resp) {
          hub_err = err; // the error
          hub_resp = resp || {}; // the response
          cb();
        });
      },
      function(cb) {
        // Check on the connected shares
        find_file_on_connected_shares(opts, function(err, resp) {
          shares_err = err; // the error
          shares_resp = resp || {}; // the response
          cb();
        });
      }
    ], finalize_find_file);

    function finalize_find_file() {
      // If there was just one error, it's just a warning
      if (hub_err) {
        console.warn('Problem in find_file_on_hub: ' + hub_err);
      }
      if (shares_err) {
        console.warn('Problem in find_file_on_connected_shares: ' + hub_err);
      }

      // If there were two errors, it's an actual error in the callback.
      if ((hub_err) && (shares_err)) {
        callback(`hub error and shares error: ${hub_err}:${shares_err}`);
        return;
      }

      // This is the info we are going to return
      var resp = {
        success: true,
        found: false,
        urls: []
      };
      if ((shares_resp.found) || (hub_resp.found)) {
        // Found with at least one of the methods
        resp.found = true; // report found
        if (shares_resp.found) {
          // found on at least one of the shares
          resp.urls = shares_resp.urls; // so we have direct urls
          resp.size = shares_resp.size; // we can report the size
        } else if (hub_resp.found) {
          // found on the local kbucket-hub disk
          // TODO: check for size inconsistency and report a warning or something
          resp.size = hub_resp.size;
        }
        // We also provide a proxy url in case the direct urls are not reachable
        // for example, if the share computers are behind firewalls
        var proxy_url = `${config.listenUrl()}/proxy-download/${opts.sha1}`;
        if (opts.filename) {
          // append the filename to the url so that the downloaded file has the desired name
          proxy_url += `/${opts.filename}`;
        }
        resp.urls.push(proxy_url);

      }
      // return the results
      callback(null, resp);
    }
  }

  function find_file_on_hub(opts, callback) {
    var listen_url=config.listenUrl();
    var DATA_DIRECTORY=config.kbNodeDirectory();
    var RAW_DIRECTORY=require('path').join(DATA_DIRECTORY, 'raw');

    // try to find the file on the local kbucket-hub disk
    if (!listen_url) {
      // We don't have a url, so we can't use this method
      callback('listen_url not set.');
      return;
    }

    // path to the file, if it were to exist
    var path_on_hub = require('path').join(RAW_DIRECTORY, opts.sha1);
    if (!fs.existsSync(path_on_hub)) {
      // Not found
      callback(null, {
        found: false,
        message: `File not found on hub: ${opts.sha1}`
      });
      return;
    }
    // Get the file info
    var stat = stat_file(path_on_hub);
    if (!stat) {
      // it's a problem if we can't stat the file
      callback(`Unable to stat file: ${opts.sha1}`);
      return;
    }
    // Form the download url
    var url0 = `${listen_url}/download/${opts.sha1}`;
    if (opts.filename) {
      // append the filename if present, so that the downloaded file with be correctly named on the client computer
      url0 += '/' + opts.filename;
    }
    // Return the result
    callback(null, {
      size: stat.size, // size of the file
      url: url0, // download url formed above
      found: true // yep, we found it
    });
  }

  function find_file_on_connected_shares(opts, callback) {
    // find the file on the connected share computers
    m_connected_share_manager.findFileOnConnectedShares(opts, callback);
  }
}

function KBConnectedShareManager() {
  // Manage a collection of KBConnectedShare objects, each representing a connected share (or computer running kbucket-share)
  this.addConnectedShare = function(kbnode_id, info, on_message_handler, callback) {
    addConnectedShare(kbnode_id, info, on_message_handler, callback);
  };
  this.getConnectedShare = function(kbnode_id) {
    return m_connected_shares[kbnode_id] || null;
  };
  this.removeConnectedShare = function(kbnode_id) {
    removeConnectedShare(kbnode_id);
  };
  this.findFileOnConnectedShares = function(opts, callback) {
    findFileOnConnectedShares(opts, callback);
  };

  var m_connected_shares = {};

  function addConnectedShare(kbnode_id, info, on_message_handler, callback) {
    // Add a new connected share

    var num_connected_shares = Object.keys(m_connected_shares).length;
    if (num_connected_shares >= LIMITS.max_connected_share_connections) {
      callback('Exceeded maximum number of kbshare connections.');
      return
    }

    if (kbnode_id in m_connected_shares) {
      callback(`A share with id=${kbnode_id} already exists.`);
      return;
    }
    // create a new KBConnectedShare object, and pass in the info
    // on_message_handler is a callback function that allows the share to send websocket messages back to the share
    m_connected_shares[kbnode_id] = new KBConnectedShare(kbnode_id, info, on_message_handler);
    callback(null);
  }

  function removeConnectedShare(kbnode_id) {
    // remove the share from the manager
    if (!(kbnode_id in m_connected_shares)) {
      // we don't have it anyway
      return;
    }
    // actually remove it
    delete m_connected_shares[kbnode_id];
  }

  function findFileOnConnectedShares(opts, callback) {
    // Find a file by checking all of the connected shares
    var kbnode_ids = Object.keys(m_connected_shares); // all the share keys in this manager

    // this is the stuff we will return in the callback
    var resp = {
      found: false, // whether the file was found
      size: undefined, // size of the file if found
      urls: [], // a list of direct urls (direct to the share computers)
      internal_finds: [] // a list of objects for each find (described elsewhere)
    };

    // Loop sequentially through each share key
    // TODO: shall we allow this to be parallel / asynchronous?
    async.eachSeries(kbnode_ids, function(kbnode_id, cb) {
      var SS = m_connected_shares[kbnode_id];
      if (!SS) { //maybe it disappeared
        cb(); // go to the next one
        return;
      }
      if (resp.internal_finds.length >= 10) {
        //don't return more than 10
        cb(); // go to the next one
        return;
      }
      // Find the file on this particular share
      SS.findFile(opts, function(err0, resp0) {
        if ((!err0) && (resp0.found)) {
          // We found the file
          resp.found = true;
          // TODO: we should check for consistency with size, and do something if there is an inconsistency
          resp.size = resp0.size; // record the size
          if (resp0.url) {
            // add the direct url (direct connection to the share computer)
            resp.urls.push(resp0.url);
          }
          // keep track of the info for this find
          // used when serving the file with the kbucket-hub acting as a proxy
          resp.internal_finds.push({
            kbnode_id: kbnode_id, // the share key
            path: resp0.path // the path of the file within the share
          });
        }
        cb(); // go to the next one
      });
    }, function() {
      // we checked all the shares, now return the response.
      callback(null, resp);
    });
  }
}

function KBConnectedShare(kbnode_id, info, on_message_handler) {
  // Encapsulate a single share -- a connection to a computer running kbucket-share
  this.processMessageFromConnectedShare = function(msg, callback) {
    processMessageFromConnectedShare(msg, callback);
  };
  this.processHttpRequest = function(path, req, res) {
    processHttpRequest(path, req, res);
  };
  this.findFile = function(opts, callback) {
    findFile(opts, callback);
  };

  var m_response_handlers = {}; // handlers corresponding to requests we have sent to the share

  // TODO: the following information needs to be moved to a database (not memory)
  var m_indexed_files_by_sha1 = {}; // the files on the share indexed by sha1
  var m_indexed_files_by_path = {}; // the files on the share indexed by path

  var m_http_over_websocket_client=new HttpOverWebSocketClient();

  function processMessageFromConnectedShare(msg, callback) {
    // We got a message msg from the share computer

    if (msg.message_type=='http') {
      m_http_over_websocket_client.processMessageFromServer(msg,function(err) {
        if (err) {
          callback('Error in http over websocket: '+err);
          return;
        }
        callback(null);
      });
      return;
    }

    if (msg.command == 'set_file_info') {
      // The share is sending the information for a particular file in the share

      //first remove the old record from our index, if it exists
      if (msg.path in m_indexed_files_by_path) {
        var FF = m_indexed_files_by_path[msg.path];
        delete m_indexed_files_by_sha1[FF.prv.original_checksum];
        delete m_indexed_files_by_path[msg.path];
      }

      // now add the new one to our index if the prv is specified
      // (if the prv is not defined, then we are effectively removing this record)
      if (msg.prv) {
        var FF = {
          path: msg.path, // the path of the file within the share
          prv: msg.prv, // the prv object of the file
          size: msg.prv.original_size // the size (for convenience)
        };

        // add the file to our index
        m_indexed_files_by_path[msg.path] = FF;
        m_indexed_files_by_sha1[FF.prv.original_checksum] = FF;

        var num_files = Object.keys(m_indexed_files_by_sha1).length;
        if (num_files > LIMITS.max_files_per_kbshare_connection) {
          callback(`Exceeded maximum number of files allowed (${num_files}>${LIMITS.max_files_per_kbshare_connection})`);
        }
      }
      callback(null);
    } else {
      // Unrecognized command
      callback(`Unrecognized command: ${msg.command}`);
    }
  }

  function processHttpRequest(path, req, res) {
    // Forward a http request through the websocket to the share computer (computer running kbucket-share)

    m_http_over_websocket_client.handleRequest(path, req,res,message_sender);

    function message_sender(msg) {
      send_message_to_connected_share(msg);
    }
  }

  function findFile(opts, callback) {
    // Find a file on the share by looking into the index
    if (!(opts.sha1 in m_indexed_files_by_sha1)) {
      // Nope we don't have a file with this sha1
      callback(null, {
        found: false
      });
      return;
    }
    var FF = m_indexed_files_by_sha1[opts.sha1];
    if (!FF) {
      // Not sure why this would happen
      callback(null, {
        found: false
      });
      return;
    }
    // We found the file
    var ret = {
      found: true,
      size: FF.size, // file size
      path: FF.path // file path on the share
    };
    if (info.listen_url) {
      // The share computer has reported it's ip address, etc. So we'll use that as the direct url
      ret.url = `${info.listen_url}/${kbnode_id}/download/${FF.path}`;
    }
    // return the results
    callback(null, ret);
  }

  function send_message_to_connected_share(msg) {
    // The on_message_handler callback allows us to send messages to the share
    on_message_handler(msg);
  }
}

function is_valid_sha1(sha1) {
  // check if this is a valid SHA-1 hash
    if (sha1.match(/\b([a-f0-9]{40})\b/))
        return true;
    return false;
}

function make_random_id(len) {
  // return a random string of characters
    var text = "";
    var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    for( var i=0; i < len; i++ )
        text += possible.charAt(Math.floor(Math.random() * possible.length));

    return text;
}
