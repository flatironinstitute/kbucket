exports.KBNodeHub = KBNodeHub;

const async = require('async');
const express = require('express');
const WebSocket = require('ws');
const crypto = require('crypto');
const fs = require('fs');

const KBNodeConfig = require(__dirname + '/kbnodeconfig.js').KBNodeConfig;
const KBNodeHubApi = require(__dirname + '/kbnodehubapi.js').KBNodeHubApi;
const KBucketHubManager = require(__dirname + '/kbuckethubmanager.js').KBucketHubManager;
const HttpOverWebSocketServer = require(__dirname + '/httpoverwebsocket.js').HttpOverWebSocketServer;
const PoliteWebSocket = require(__dirname + '/politewebsocket.js').PoliteWebSocket;
const KBConnectionToParentHub = require(__dirname + '/kbconnectiontoparenthub.js').KBConnectionToParentHub;
const KBConnectionToChildNode = require(__dirname + '/kbconnectiontochildnode.js').KBConnectionToChildNode;


var debugging = true;

function KBNodeHub(kbnode_directory) {
  this.initialize = function(opts, callback) {
    initialize(opts, callback);
  };

  var m_config = new KBNodeConfig(kbnode_directory);
  var m_app = null;
  var m_parent_hub_socket = null;
  // Encapsulate some functionality in a manager
  var m_manager = new KBucketHubManager(m_config);

  var m_http_over_websocket_server = new HttpOverWebSocketServer();
  var m_connection_to_parent_hub = null;

  var API = new KBNodeHubApi(m_config, m_manager);

  function initialize(opts, callback) {
    var steps = [
      create_config_if_needed,
      initialize_config,
      run_interactive_config,
      start_http_server,
      start_websocket_server,
      connect_to_parent_hub
    ];

    async.series(steps, function() {
      callback(null);
    }, function(err) {
      callback(err);
    });

    function run_interactive_config(callback) {
      m_config.runInteractiveConfiguration(opts, callback);
    }
  }

  function create_config_if_needed(callback) {
    if (!m_config.configDirExists()) {
      m_config.createNew('hub', function(err) {
        if (err) {
          callback(err);
          return;
        }
        callback(null);
      });
    } else {
      callback(null);
    }
  }

  function initialize_config(callback) {
    m_config.initialize(function(err) {
      if (err) {
        callback(err);
        return;
      }
      if (m_config.kbNodeType() != 'hub') {
        callback('Incorrect type for kbnode: ' + m_config.kbNodeType());
        return;
      }
      callback(null);
    });
  }

  function start_http_server(callback) {
    m_app = express();

    var app = m_app;
    app.set('json spaces', 4); // when we respond with json, this is how it will be formatted

    KBNodeHubApi.config = m_config;

    // API find -- find a file on the kbucket network (or on the local kbucket-hub disk)
    app.use('/find/:sha1/:filename(*)', function(req, res) {
      // Note: filename is just for convenience, only used in forming download urls
      var params = req.params;
      API.handleFind(params.sha1, params.filename, req, res);
    });
    app.use('/find/:sha1', function(req, res) {
      var params = req.params;
      API.handleFind(params.sha1, '', req, res);
    });
    // Provide "stat" synonym to "find" for backward-compatibility
    app.use('/stat/:sha1', function(req, res) {
      var params = req.params;
      API.handleFind(params.sha1, '', req, res);
    });

    // API download (direct from kbucket hub)
    // Used internally -- will be obfuscated in the future -- do not use directly
    app.use('/:code/download/:sha1/:filename(*)', function(req, res) {
      if (!check_code(req, res)) return;
      // Note: filename is just for convenience, not actually used
      var params = req.params;
      API.handleDownload(params.sha1, params.filename, req, res);
    });
    app.use('/:code/download/:sha1', function(req, res) {
      if (!check_code(req, res)) return;
      var params = req.params;
      API.handleDownload(params.sha1, params.sha1, req, res);
    });

    // API Forward http request to a kbucket share
    // Used internally -- will be obfuscated in the future -- do not use directly
    app.use('/:code/share/:kbnode_id/:path(*)', function(req, res) {
      if (!check_code(req, res)) return;
      var params = req.params;
      API.handleForwardToConnectedShare(params.kbnode_id, params.kbnode_id + '/' + params.path, req, res);
    });

    // API Forward http request to a child hub
    // Used internally -- will be obfuscated in the future -- do not use directly
    app.use('/:code/hub/:kbnode_id/:path(*)', function(req, res) {
      if (!check_code(req, res)) return;
      var params = req.params;
      API.handleForwardToConnectedChildHub(params.kbnode_id, params.path, req, res);
    });

    // API proxy download
    // Used internally -- will be obfuscated in the future -- do not use directly
    app.use('/:code/proxy-download/:sha1/:filename(*)', function(req, res) {
      if (!check_code(req, res)) return;
      // Note: filename is just for convenience, not actually used
      var params = req.params;
      API.handleProxyDownload(params.sha1, params.filename, req, res);
    });
    app.use('/:code/proxy-download/:sha1', function(req, res) {
      if (!check_code(req, res)) return;
      var params = req.params;
      API.handleProxyDownload(params.sha1, params.sha1, req, res);
    });

    // API upload -- upload a file to the kbucket-hub disk
    // TODO: think about what restrictions to place on this operation (aside from the per-upload limit)
    app.use('/upload', API.handleUpload);

    // API web
    // A web interface that will expand over time
    app.use('/web', express.static(__dirname + '/web'));

    start_http_server_2(callback);
  }

  function check_code(req, res) {
    var params = req.params;
    if (params.code != m_config.kbNodeId()) {
      var errstr = `Incorrect code: ${params.code}`;
      console.error(errstr);
      res.status(500).send({
        error: errstr
      });
      return false;
    }
    return true;
  }

  function start_http_server_2(callback) {
    var app = m_app;
    var listen_port = m_config.getConfig('listen_port');
    m_config.setListenPort(listen_port);
    m_http_over_websocket_server.setForwardUrl(`http://localhost:${listen_port}`);
    app.set('port', listen_port);
    if (process.env.SSL != null ? process.env.SSL : listen_port % 1000 == 443) {
      // The port number ends with 443, so we are using https
      app.USING_HTTPS = true;
      // Look for the credentials inside the encryption directory
      // You can generate these for free using the tools of letsencrypt.org
      const options = {
        key: fs.readFileSync(__dirname + '/encryption/privkey.pem'),
        cert: fs.readFileSync(__dirname + '/encryption/fullchain.pem'),
        ca: fs.readFileSync(__dirname + '/encryption/chain.pem')
      };

      // Create the https server and start listening
      app.server = require('https').createServer(options, app);
      app.server.listen(listen_port, function() {
        console.info('kbucket-hub is running https on port', listen_port);
        callback(null);
      });
    } else {
      // Create the http server and start listening
      app.server = require('http').createServer(app);
      app.server.listen(listen_port, function() {
        console.info('kbucket-hub is running http on port', listen_port);
        callback(null);
      });
    }
  }

  function start_websocket_server(callback) {
    //initialize the WebSocket server instance
    const wss = new WebSocket.Server({
      server: m_app.server
    });

    wss.on('connection', (ws) => {
      on_new_websocket_connection(ws);
    });

    callback(null);
  }

  function on_new_websocket_connection(ws) {
    var PWS = new PoliteWebSocket({
      wait_for_response: false,
      enforce_remote_wait_for_response: true
    });
    PWS.setSocket(ws);

    var CC = new KBConnectionToChildNode();
    CC.setWebSocket(PWS);
    CC.onRegistered(function() {
      if (CC.childNodeType() == 'share') {

        // Everything looks okay, let's add this share to our share manager
        console.info(`Registering child share: ${CC.childNodeId()}`);

        m_manager.connectedShareManager().addConnectedShare(CC, function(err) {
          if (err) {
            PWS.sendErrorAndClose(`Error adding share: ${err}`);
            return;
          }
          // acknowledge receipt of the register message so that the child node can proceed
          CC.sendMessage({
            message: 'ok',
            detail: 'registered (1)',
            command:'1'
          });
        });
        //todo: how do we free up the CC object?
      }
      else if (CC.childNodeType() == 'hub') {
        // Everything looks okay, let's add this share to our share manager
        console.info(`Registering child hub: ${CC.childNodeId()}`);

        m_manager.connectedChildHubManager().addConnectedChildHub(CC, function(err) {
          if (err) {
            PWS.sendErrorAndClose(`Error adding child hub: ${err}`);
            return;
          }
          // acknowledge receipt of the register message so that the child node can proceed
          CC.sendMessage({
            message: 'ok',
            detail: 'registered (2)',
            command:'2'
          });
        });
      }
      else {
        PWS.sendErrorAndClose('Unexpected child node type: '+CC.childNodeType());
      }
    });
  }

  function connect_to_parent_hub(callback) {
    var parent_hub_url = m_config.getConfig('parent_hub_url');
    if ((!parent_hub_url) || (parent_hub_url == '.')) {
      callback(null);
      return;
    }
    m_connection_to_parent_hub = new KBConnectionToParentHub(m_config);
    m_connection_to_parent_hub.setHttpOverWebSocketServer(m_http_over_websocket_server);
    m_connection_to_parent_hub.initialize(parent_hub_url, function(err) {
      if (err) {
        callback(err);
        return;
      }
      callback(null);
    });
  }

  function verify_message(msg, hex_signature, public_key) {
    const verifier = crypto.createVerify('sha256');
    verifier.update(JSON.stringify(msg));
    verifier.end();

    var signature = Buffer.from(hex_signature, 'hex');

    const verified = verifier.verify(public_key, signature);
    return verified;
  }
}

function get_websocket_url_from_http_url(url) {
  var URL = require('url').URL;
  var url_ws = new URL(url);
  if (url_ws.protocol == 'http:')
    url_ws.protocol = 'ws';
  else
    url_ws.protocol = 'wss';
  url_ws = url_ws.toString();
  return url_ws;
}

function is_valid_kbnode_id(key) {
  // check if a kbnode_id is valid
  // TODO: add detail and use regexp
  return ((8 <= key.length) && (key.length <= 64));
}

function is_valid_sha1(sha1) {
  // check if this is a valid SHA-1 hash
  if (sha1.match(/\b([a-f0-9]{40})\b/))
    return true;
  return false;
}