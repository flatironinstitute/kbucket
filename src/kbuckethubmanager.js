exports.KBucketHubManager = KBucketHubManager;

const async = require('async')
const fs = require('fs');
const request = require('request');
const loki = require('lokijs');
const logger = require(__dirname + '/logger.js').logger();

const HttpOverWebSocketClient = require(__dirname + '/httpoverwebsocket.js').HttpOverWebSocketClient;

var LIMITS = {
  max_connected_shares: 1e3,
  max_files_per_connected_share: 300,
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
  this.nodeData = function() {
    return nodeData();
  }
  this.routeHttpRequestToNode=function(kbnode_id,path,req,res) {
    routeHttpRequestToNode(kbnode_id,path,req,res);
  }

  // The share manager (see KBucketConnectedShareManager)
  var m_connected_share_manager = new KBConnectedShareManager();
  // The connected child hub manager (see KBConnectedChildHubManager)
  var m_connected_child_hub_manager = new KBConnectedChildHubManager(config);

  function findFile(opts,callback) {
    find_shares_with_file(opts,function(err,results) {
      if (err) {
        callback(err);
        return;
      }
      if (results.length==0) {
        callback(null,{
          success:true,
          found:false,
          urls:[]
        });
        return;
      }
      var node_data0=nodeData();
      var urls=[];
      async.eachSeries(results,function(result,cb) {
        var kbshare_id=result.kbshare_id;
        var share0=node_data0.descendant_nodes[kbshare_id];
        if (share0.listen_url) {
          var url0=share0.listen_url+'/'+kbshare_id+'/download/'+result.path;
          urls.push(url0);
        }
        var visited={}; //prevent infinite loop
        var hub_id=share0.parent_kbnode_id;
        while (hub_id) {
          if (visited[hub_id])
            break;
          visited[hub_id]=true;
          if (hub_id in node_data0.descendant_nodes) {
            var hub0=node_data0.descendant_nodes[hub_id];
            if (hub0.listen_url) {
              var url0=hub0.listen_url+'/'+kbshare_id+'/download/'+result.path;
              urls.push(url0);
            }
            hub_id=hub0.parent_kbnode_id;
          }
          else {
            hub_id=null;
          }
        }
        {
          if (config.listenUrl()) {
            var url0=config.listenUrl()+'/'+kbshare_id+'/download/'+result.path;
            urls.push(url0);
          }
        }
        cb();
      },function() {
        callback(null,{
          success:true,
          found:true,
          urls:urls,
          results:results
        });
      });
    });
  }

  function find_shares_with_file(opts,callback) {
    if (!is_valid_sha1(opts.sha1)) {
      // Not a valid sha1 hash
      callback(`Invalid sha1: ${opts.sha1}`);
      return;
    }
    m_connected_share_manager.findSharesWithFile(opts, function(err,results1) {
      if (err) {
        callback(err);
        return;
      }
      m_connected_child_hub_manager.findSharesWithFile(opts,function(err,results2) {
        if (err) {
          callback(err);
          return;
        }
        var results=[];
        for (var i in results1) {
          results.push(results1[i]);
        }
        for (var i in results2) {
          results.push(results2[i]);
        }
        callback(null,results);
      });
    });
  }

  function nodeData() {
    var data={
      kbnode_id:config.kbNodeId(),
      descendant_nodes:{}
    }
    var kbshare_ids=m_connected_share_manager.connectedShareIds();
    for (var ii in kbshare_ids) {
      var kbshare_id=kbshare_ids[ii];
      var SS=m_connected_share_manager.getConnectedShare(kbshare_id);
      data.descendant_nodes[kbshare_id]={
        kbnode_id:kbshare_id,
        parent_kbnode_id:config.kbNodeId(),
        listen_url:SS.listenUrl(),
        kbnode_type:'share'
      };
    }
    var kbhub_ids=m_connected_child_hub_manager.connectedChildHubIds();
    for (var ii in kbhub_ids) {
      var kbhub_id=kbhub_ids[ii];
      var HH=m_connected_child_hub_manager.getConnectedChildHub(kbhub_id);
      data.descendant_nodes[kbhub_id]={
        kbnode_id:kbhub_id,
        parent_kbnode_id:config.kbNodeId(),
        listen_url:HH.listenUrl(),
        kbnode_type:'hub'
      };
      var data0=HH.childNodeData();
      data0.descendant_nodes=data0.descendant_nodes||{};
      for (var id in data0.descendant_nodes) {
        data.descendant_nodes[id]=data0.descendant_nodes[id];
      }
    }
    return data;
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

  function routeHttpRequestToNode(kbnode_id,path,req,res) {
    var SS=m_connected_share_manager.getConnectedShare(kbnode_id);
    if (SS) {
      SS.processHttpRequest(path,req,res);
      return;
    }
    var HH=m_connected_child_hub_manager.getConnectedChildHub(kbnode_id);
    if (HH) {
      HH.processHttpRequest(path,req,res);
      return;
    }
    var ids=m_connected_child_hub_manager.connectedChildHubIds();
    for (var ii in ids) {
      var id=ids[ii];
      var HH=m_connected_child_hub_manager.getConnectedChildHub(id);
      var data0=HH.childNodeData();
      var dn0=data0.descendant_nodes||{};
      if (kbnode_id in dn0) {
        HH.processHttpRequest(path,req,res);
        return;
      }
    }
    res.status(500).send({
      error: 'Unable to locate node with id: '+kbnode_id
    });
  }
}

function KBConnectedShareManager() {
  // Manage a collection of KBConnectedShare objects, each representing a connected share (or computer running kbucket-share)
  this.addConnectedShare = function(connection_to_child_node, callback) {
    addConnectedShare(connection_to_child_node, callback);
  };
  this.connectedShareIds = function() {
    return Object.keys(m_connected_shares);
  }
  this.getConnectedShare = function(kbnode_id) {
    return m_connected_shares[kbnode_id] || null;
  };
  this.findSharesWithFile=function(opts,callback) {
    findSharesWithFile(opts,callback);
  }

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
    var logmsg=`Removing child share: ${kbnode_id}`;
    logger.info(logmsg);
    console.info(logmsg);
    delete m_connected_shares[kbnode_id];
  }

  function findSharesWithFile(opts, callback) {
    // Find a file by checking all of the connected shares
    var kbnode_ids = Object.keys(m_connected_shares); // all the share keys in this manager

    var results=[];

    // Loop sequentially through each share key
    // TODO: shall we allow this to be parallel / asynchronous?
    async.eachSeries(kbnode_ids, function(kbnode_id, cb) {
      var SS = m_connected_shares[kbnode_id];
      if (!SS) { //maybe it disappeared
        cb(); // go to the next one
        return;
      }
      // Find the file on this particular share
      SS.findFile(opts, function(err0, resp0) {
        if ((!err0) && (resp0.found)) {
          results.push({
            kbshare_id:kbnode_id,
            size:resp0.size,
            path:resp0.path
          });
        }
        cb(); // go to the next one
      });
    }, function() {
      // we checked all the shares, now return the response.
      callback(null, results);
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
  this.listenUrl=function() {
    var data=connection_to_child_node.childNodeRegistrationInfo();
    return data.listen_url;
  }

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

  //var m_response_handlers = {}; // handlers corresponding to requests we have sent to the share

  // TODO: the following information needs to be moved to a non-in-memory database

  var m_db = new loki('');
  // what do indices mean?
  var m_files_collection = m_db.addCollection('files', {
    indices: ['sha1', 'path']
  });

  // todo: move this http client to the connection_to_child_node and handle all http stuff there
  var m_http_over_websocket_client = new HttpOverWebSocketClient(send_message_to_share);

  function send_message_to_share(msg) {
    connection_to_child_node.sendMessage(msg);
  }

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
      var files0 = m_files_collection.find({
        path: msg.path
      });
      m_files_collection.remove(files0);

      // now add the new one to our index if the prv is specified
      // (if the prv is not defined, then we are effectively removing this record)
      if (msg.prv) {
        var FF = {
          path: msg.path, // the path of the file within the share
          prv: msg.prv, // the prv object of the file
          size: msg.prv.original_size // the size (for convenience)
        };

        // add the file to our index
        m_files_collection.insert({
          path: msg.path,
          sha1: FF.prv.original_checksum,
          file: FF
        })


        var num_files = m_files_collection.count();
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

    m_http_over_websocket_client.handleRequest(path, req, res);
  }

  function findFile(opts, callback) {
    // Find a file on the share by looking into the index
    var file0 = m_files_collection.findOne({
      sha1: opts.sha1
    });
    if (!file0) {
      // Nope we don't have a file with this sha1
      callback(null, {
        found: false
      });
      return;
    }
    var FF = file0.file;
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
    var info = connection_to_child_node.childNodeRegistrationInfo();
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
  this.connectedChildHubIds = function() {
    return Object.keys(m_connected_child_hubs);
  }
  this.getConnectedChildHub = function(kbnode_id) {
    return m_connected_child_hubs[kbnode_id] || null;
  };
  this.findSharesWithFile = function(opts, callback) {
    findSharesWithFile(opts, callback);
  };

  var m_connected_child_hubs = {};

  config.onTopHubUrlChanged(function() {
    send_message_to_all_child_hubs({
      command: 'set_top_hub_url',
      top_hub_url: config.topHubUrl()
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
    m_connected_child_hubs[kbnode_id] = new KBConnectedChildHub(connection_to_child_node, config);

    m_connected_child_hubs[kbnode_id].sendMessage({
      command: 'set_top_hub_url',
      top_hub_url: config.topHubUrl()
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
    var logmsg=`Removing child hub: ${kbnode_id}`;
    logger.info(logmsg);
    console.info(logmsg);
    delete m_connected_child_hubs[kbnode_id];
  }

  function findSharesWithFile(opts, callback) {
    // Find a file by checking all of the connected shares
    var kbnode_ids = Object.keys(m_connected_child_hubs); // all the share keys in this manager

    var results=[];

    // Loop sequentially through each child hub id
    // TODO: shall we allow this to be parallel / asynchronous?
    async.eachSeries(kbnode_ids, function(kbnode_id, cb) {
      var SS = m_connected_child_hubs[kbnode_id];
      if (!SS) { //maybe it disappeared
        cb(); // go to the next one
        return;
      }
      SS.findSharesWithFile(opts,function(err,results0) {
        if (!err) {
          for (var i in results0) {
            results.push(results0[i]);
          }
        }
        cb();
      });
    }, function() {
      // we checked all the child hubs, now return the response.
      callback(null, results);
    });
  }
}

function KBConnectedChildHub(connection_to_child_node, config) {
  // Encapsulate a single child hub
  this.processHttpRequest = function(path, req, res) {
    processHttpRequest(path, req, res);
  };
  this.findSharesWithFile =function(opts,callback) {
    findSharesWithFile(opts,callback);
  }
  this.sendMessage = function(msg) {
    connection_to_child_node.sendMessage(msg);
  }
  this.childNodeData = function() {
    return connection_to_child_node.childNodeData();
  }
  this.listenUrl=function() {
    var data=connection_to_child_node.childNodeRegistrationInfo();
    return data.listen_url;
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

  //var m_response_handlers = {}; // handlers corresponding to requests we have sent to the child hub

  // todo: move this http client to the connection_to_child_node and handle all http stuff there
  var m_http_over_websocket_client = new HttpOverWebSocketClient(send_message_to_child_hub);

  function send_message_to_child_hub(msg) {
    connection_to_child_node.sendMessage(msg);
  }

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

    m_http_over_websocket_client.handleRequest(path, req, res);
  }

  function findSharesWithFile(opts, callback) {
    var urlpath0=`find/${opts.sha1}/${opts.filename}`;
    m_http_over_websocket_client.httpRequestJson(urlpath0,function(err,resp) {
      if (err) {
        callback(err);
        return;
      }
      callback(null,resp.results||[]);
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