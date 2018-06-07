exports.KBucketHubManager = KBucketHubManager;

const async = require('async')
const fs = require('fs');
const request = require('request');

const HttpOverWebSocketClient = require(__dirname + '/httpoverwebsocket.js').HttpOverWebSocketClient;

var LIMITS = {
  max_connected_shares: 1e3,
  max_files_per_connected_share: 30,
  max_connected_child_hubs: 10
};

function KBucketHubManager(config) {
  // Encapsulates some functionality of kbucket-hub
  this.findFile = function(opts, callback) {
    findFile(opts, callback);
  };
  this.connectedShareManager = function() {
    return m_connected_share_manager;
  };
  this.connectedChildHubManager = function() {
    return m_connected_child_hub_manager;
  };
  this.setTopHubUrl = function(url) {
    m_connected_child_hub_manager.setTopHubUrl(url);
  }

  // The share manager (see KBucketConnectedShareManager)
  var m_connected_share_manager = new KBConnectedShareManager();
  // The connected child hub manager (see KBConnectedChildHubManager)
  var m_connected_child_hub_manager = new KBConnectedChildHubManager(config);

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
        find_file_on_this_hub(opts, function(err, resp) {
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
      },
      function(cb) {
        find_file_on_connected_child_hubs(opts, function(err, resp) {
          child_hubs_err = err;
          child_hubs_resp = resp || {};
          cb();
        });
      }
    ], finalize_find_file);

    function finalize_find_file() {
      // If there was just one error, it's just a warning
      if (hub_err) {
        console.warn('Problem in find_file_on_this_hub: ' + hub_err);
      }
      if (shares_err) {
        console.warn('Problem in find_file_on_connected_shares: ' + hub_err);
      }
      if (child_hubs_err) {
        console.warn('Problem in find_file_on_connected_child_hubs: ' + child_hubs_err); 
      }

      // This is the info we are going to return
      var resp = {
        success: true,
        found: false,
        urls: [],
        internal_finds:[]
      };

      var provide_proxy_url=false;
      if ((!shares_err)&&(shares_resp.found)) {
        provide_proxy_url=true;
        resp.found=true;
        
        // TODO: check for size inconsistency and report a warning or something
        resp.size=shares_resp.size;

        for (var i in shares_resp.urls) {
          resp.urls.push(shares_resp.urls[i]);
        }

        for (var i in shares_resp.internal_finds) {
          resp.internal_finds.push(shares_resp.internal_finds[i]);
        }
      }

      if ((!child_hubs_err)&&(child_hubs_resp.found)) {
        provide_proxy_url=true;
        resp.found=true;
        
        // TODO: check for size inconsistency and report a warning or something
        resp.size=child_hubs_resp.size;

        for (var i in child_hubs_resp.urls) {
          resp.urls.push(child_hubs_resp.urls[i]);
        }

        for (var i in child_hubs_resp.internal_finds) {
          resp.internal_finds.push(child_hubs_resp.internal_finds[i]);
        }
      }

      if ((!hub_err)&&(hub_resp.found)) {
        resp.found=true;

        // TODO: check for size inconsistency and report a warning or something
        resp.size=hub_resp.size;

        var url0=`${config.listenUrl()}/${config.kbNodeId()}/download/${opts.sha1}`;
        if (opts.filename) {
          // append the filename to the url so that the downloaded file has the desired name
          url0 += `/${opts.filename}`;
        }
        resp.urls.push(url0);
      }

      if (provide_proxy_url) {
        // We also provide a proxy url in case the direct urls are not reachable
        // for example, if the share computers are behind firewalls
        var proxy_url = `${config.listenUrl()}/${config.kbNodeId()}/proxy-download/${opts.sha1}`;
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

  function find_file_on_this_hub(opts, callback) {
    var listen_url = config.listenUrl();
    var DATA_DIRECTORY = config.kbNodeDirectory();
    var RAW_DIRECTORY = require('path').join(DATA_DIRECTORY, 'raw');

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
    var url0 = `${listen_url}/${config.kbNodeId()}/download/${opts.sha1}`;
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

  function find_file_on_connected_child_hubs(opts, callback) {
    // find the file on the connected child hubs
    m_connected_child_hub_manager.findFileOnConnectedChildHubs(opts, callback);
  }
}

function KBConnectedShareManager() {
  // Manage a collection of KBConnectedShare objects, each representing a connected share (or computer running kbucket-share)
  this.addConnectedShare = function(connection_to_child_node, callback) {
    addConnectedShare(connection_to_child_node, callback);
  };
  this.getConnectedShare = function(kbnode_id) {
    return m_connected_shares[kbnode_id] || null;
  };
  this.findFileOnConnectedShares = function(opts, callback) {
    findFileOnConnectedShares(opts, callback);
  };

  var m_connected_shares = {};

  function addConnectedShare(connection_to_child_node, callback) {
    // Add a new connected share

    var num_connected_shares = Object.keys(m_connected_shares).length;
    if (num_connected_shares >= LIMITS.max_connected_shares) {
      callback('Exceeded maximum number of kbshare connections.');
      return
    }

    var kbnode_id = connection_to_child_node.childNodeId();

    if (kbnode_id in m_connected_shares) {
      callback(`A share with id=${kbnode_id} already exists.`);
      return;
    }

    connection_to_child_node.onClose(function() {
      remove_connected_share(kbnode_id);
    });

    // create a new KBConnectedShare object, and pass in the connection object
    m_connected_shares[kbnode_id] = new KBConnectedShare(connection_to_child_node);
    callback(null);
  }

  function remove_connected_share(kbnode_id) {
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
            share_id: kbnode_id, // the share key
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

function KBConnectedShare(connection_to_child_node) {
  // Encapsulate a single share -- a connection to a computer running kbucket-share
  this.processHttpRequest = function(path, req, res) {
    processHttpRequest(path, req, res);
  };
  this.findFile = function(opts, callback) {
    findFile(opts, callback);
  };

  connection_to_child_node.onMessage(function(msg) {
    process_message_from_connected_share(msg, function(err, response) {
      if (err) {
        connection_to_child_node.reportErrorAndCloseSocket(err);
        return;
      }
      if (!response) {
        response = {
          message: 'ok'
        };
      }
      connection_to_child_node.sendMessage(response);
    })
  });

  var m_response_handlers = {}; // handlers corresponding to requests we have sent to the share

  // TODO: the following information needs to be moved to a database (not memory)
  var m_indexed_files_by_sha1 = {}; // the files on the share indexed by sha1
  var m_indexed_files_by_path = {}; // the files on the share indexed by path

  // todo: move this http client to the connection_to_child_node and handle all http stuff there
  var m_http_over_websocket_client = new HttpOverWebSocketClient();

  ////////////////////////////////////////////////////////////////////////

  function process_message_from_connected_share(msg, callback) {
    // We got a message msg from the share computer

    // todo: move this http client to the connection_to_child_node and handle all http stuff there
    if (msg.message_type == 'http') {
      m_http_over_websocket_client.processMessageFromServer(msg, function(err) {
        if (err) {
          callback('Error in http over websocket: ' + err);
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
        if (num_files > LIMITS.max_files_per_connected_share) {
          callback(`Exceeded maximum number of files allowed (${num_files}>${LIMITS.max_files_per_connected_share})`);
          return;
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

    m_http_over_websocket_client.handleRequest(path, req, res, message_sender);

    function message_sender(msg) {
      connection_to_child_node.sendMessage(msg);
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
    var info = connection_to_child_node.childNodeInfo();
    var kbnode_id = connection_to_child_node.childNodeId();
    if (info.listen_url) {
      // The share computer has reported it's ip address, etc. So we'll use that as the direct url
      ret.url = `${info.listen_url}/${kbnode_id}/download/${FF.path}`;
    }
    // return the results
    callback(null, ret);
  }
}

function KBConnectedChildHubManager(config) {
  // Manage a collection of KBConnectedChildHub objects, each representing a connected child hub
  this.addConnectedChildHub = function(connection_to_child_node, callback) {
    addConnectedChildHub(connection_to_child_node, callback);
  };
  this.getConnectedChildHub = function(kbnode_id) {
    return m_connected_child_hubs[kbnode_id] || null;
  };
  this.findFileOnConnectedChildHubs = function(opts, callback) {
    findFileOnConnectedChildHubs(opts, callback);
  };

  var m_connected_child_hubs = {};

  config.onTopHubUrlChanged(function() {
    send_message_to_all_child_hubs({
      command:'set_top_hub_url',
      top_hub_url:config.topHubUrl()
    });
  });

  function send_message_to_all_child_hubs(msg) {
    for (var id in m_connected_child_hubs) {
      m_connected_child_hubs[id].sendMessage(msg);
    }
  }

  function addConnectedChildHub(connection_to_child_node, callback) {
    // Add a new connected share

    var num_connected_child_hubs = Object.keys(m_connected_child_hubs).length;
    if (num_connected_child_hubs >= LIMITS.max_connected_child_hubs) {
      callback('Exceeded maximum number of child hub connections.');
      return
    }

    var kbnode_id = connection_to_child_node.childNodeId();

    if (kbnode_id in m_connected_child_hubs) {
      callback(`A child hub with id=${kbnode_id} already exists.`);
      return;
    }

    connection_to_child_node.onClose(function() {
      remove_connected_child_hub(kbnode_id);
    });

    // create a new KBConnectedChildHub object, and pass in the connection object
    m_connected_child_hubs[kbnode_id] = new KBConnectedChildHub(connection_to_child_node,config);

    m_connected_child_hubs[kbnode_id].sendMessage({
      command:'set_top_hub_url',
      top_hub_url:config.topHubUrl()
    });

    callback(null);
  }

  function remove_connected_child_hub(kbnode_id) {
    // remove the share from the manager
    if (!(kbnode_id in m_connected_child_hubs)) {
      // we don't have it anyway
      return;
    }
    // actually remove it
    delete m_connected_child_hubs[kbnode_id];
  }

  function findFileOnConnectedChildHubs(opts, callback) {
    // Find a file by checking all of the connected shares
    var kbnode_ids = Object.keys(m_connected_child_hubs); // all the share keys in this manager

    // this is the stuff we will return in the callback
    var resp = {
      found: false, // whether the file was found
      size: undefined, // size of the file if found
      urls: [], // a list of urls
      internal_finds: [] // a list of objects for each find (described elsewhere)
    };

    // Loop sequentially through each child hub id
    // TODO: shall we allow this to be parallel / asynchronous?
    async.eachSeries(kbnode_ids, function(kbnode_id, cb) {
      var SS = m_connected_child_hubs[kbnode_id];
      if (!SS) { //maybe it disappeared
        cb(); // go to the next one
        return;
      }
      if (resp.internal_finds.length >= 10) {
        //don't return more than 10
        cb(); // go to the next one
        return;
      }
      // Find the file on this particular child hub
      SS.findFile(opts, function(err0, resp0) {
        if ((!err0) && (resp0.found)) {
          // We found the file
          resp.found = true;
          // TODO: we should check for consistency with size, and do something if there is an inconsistency
          resp.size = resp0.size; // record the size
          for (var i in resp0.urls) {
            resp.urls.push(resp0.urls[i]);
          }
          resp.internal_finds.push({
              child_hub_id: kbnode_id
          });
        }
        cb(); // go to the next one
      });
    }, function() {
      // we checked all the child hubs, now return the response.
      callback(null, resp);
    });
  }
}

function KBConnectedChildHub(connection_to_child_node,config) {
  // Encapsulate a single child hub
  this.processHttpRequest = function(path, req, res) {
    processHttpRequest(path, req, res);
  };
  this.findFile = function(opts, callback) {
    findFile(opts, callback);
  };
  this.sendMessage=function(msg) {
    connection_to_child_node.sendMessage(msg);
  }

  connection_to_child_node.onMessage(function(msg) {
    process_message_from_connected_child_hub(msg, function(err, response) {
      if (err) {
        connection_to_child_node.reportErrorAndCloseSocket(err);
        return;
      }
      if (!response) {
        response = {
          message: 'ok'
        };
      }
      connection_to_child_node.sendMessage(response);
    })
  });

  var m_response_handlers = {}; // handlers corresponding to requests we have sent to the child hub

  // todo: move this http client to the connection_to_child_node and handle all http stuff there
  var m_http_over_websocket_client = new HttpOverWebSocketClient();

  ////////////////////////////////////////////////////////////////////////

  function process_message_from_connected_child_hub(msg, callback) {
    // We got a message msg from the share computer

    // todo: move this http client to the connection_to_child_node and handle all http stuff there
    if (msg.message_type == 'http') {
      m_http_over_websocket_client.processMessageFromServer(msg, function(err) {
        if (err) {
          callback('Error in http over websocket: ' + err);
          return;
        }
        callback(null);
      });
      return;
    }
  }

  function processHttpRequest(path, req, res) {
    // Forward a http request through the websocket to the share computer (computer running kbucket-share)

    m_http_over_websocket_client.handleRequest(path, req, res, message_sender);

    function message_sender(msg) {
      connection_to_child_node.sendMessage(msg);
    }
  }

  function findFile(opts, callback) {
    var child_node_id=connection_to_child_node.childNodeId();
    var url = `${config.listenUrl()}/${config.kbNodeId()}/hub/${child_node_id}/find/${opts.sha1}`;
    get_json(url, function(err, resp) {
      if (err) {
        callback(err);
        return;
      }
      if ((resp.found)) {
        callback(null,{
          found: true,
          urls: resp.urls
        });
      } else {
        callback(null,{
          found: false
        });
      }
    });
  }

  function get_json(url, callback) {
    request(url, function(err, res, body) {
      if (err) {
        callback(err.message);
        return;
      }
      try {
        var obj = JSON.parse(body || '');
      } catch (err) {
        callback('Error parsing json response.');
        return;
      }
      callback(null, obj);
    });
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

  for (var i = 0; i < len; i++)
    text += possible.charAt(Math.floor(Math.random() * possible.length));

  return text;
}