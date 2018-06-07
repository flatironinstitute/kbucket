exports.KBNodeHub = KBNodeHub;

const async = require('async');
const express = require('express');
const WebSocket = require('ws');
const crypto = require('crypto');

const KBNodeConfig = require(__dirname + '/kbnodeconfig.js').KBNodeConfig;
const KBNodeHubApi = require(__dirname + '/kbnodehubapi.js').KBNodeHubApi;
const KBucketHubManager = require(__dirname + '/kbuckethubmanager.js').KBucketHubManager;
const HttpOverWebSocketServer = require(__dirname + '/httpoverwebsocket.js').HttpOverWebSocketServer;
const PoliteWebSocket = require(__dirname + '/politewebsocket.js').PoliteWebSocket;
const KBConnectionToParentHub = require(__dirname + '/kbconnectiontoparenthub.js').KBConnectionToParentHub;

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

  var m_http_over_websocket_server=new HttpOverWebSocketServer();
  var m_connection_to_parent_hub=null;

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
    app.use('/download/:sha1/:filename(*)', function(req, res) {
      // Note: filename is just for convenience, not actually used
      var params = req.params;
      API.handleDownload(params.sha1, params.filename, req, res);
    });
    app.use('/download/:sha1', function(req, res) {
      var params = req.params;
      API.handleDownload(params.sha1, params.sha1, req, res);
    });

    // API Forward http request to a kbucket share
    // Used internally -- will be obfuscated in the future -- do not use directly
    app.use('/share/:kbnode_id/:path(*)', function(req, res) {
      var params = req.params;
      API.handleForwardToConnectedShare(params.kbnode_id, params.kbnode_id + '/' + params.path, req, res);
    });

    // API proxy download
    // Used internally -- will be obfuscated in the future -- do not use directly
    app.use('/proxy-download/:sha1/:filename(*)', function(req, res) {
      // Note: filename is just for convenience, not actually used
      var params = req.params;
      API.handleProxyDownload(params.sha1, params.filename, req, res);
    });
    app.use('/proxy-download/:sha1', function(req, res) {
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

    // A new share has connected (a computer running kbucket-share)
    var kbnode_id = ''; // will be filled in below
    var kbshare_public_key = ''; // will be filled in below
    var closed_with_error = false;
    PWS.onMessage(function(err, msg) {
      // The share has sent us a message
      if (err) {
        PWS.close();
        return;
      }

      // Set the kbshare id (should be received on first message)
      if (!kbnode_id) {
        kbnode_id = msg.kbnode_id;
      }

      if ((kbnode_id) && (msg.kbnode_id != kbnode_id)) {
        // The kbnode_id was set, but this message did not match. Close the connection.
        PWS.sendErrorAndClose('Share id does not match.');
        return;
      }

      // If we are given the public key, remember it, and compare it to the kbnode_id
      if ((msg.public_key) && (!kbshare_public_key)) {
        kbshare_public_key = msg.public_key;
        var list = msg.public_key.split('\n');
        var expected_share_id = list[1].slice(0, 12);
        if (expected_share_id != kbnode_id) {
          PWS.sendErrorAndClose(`Share id does not match public key (${kbnode_id}<>${expected_share_id})`);
          return;
        }
      }

      var X = msg.message;
      if (!X) {
        // The message is invalid. Let's close the connection.
        PWS.sendErrorAndClose('Invalid message.');
        return;
      }

      if (!verify_message(X, msg.signature || '', kbshare_public_key)) {
        PWS.sendErrorAndClose('Unable to verify message using signature');
        return;
      }

      if (X.command == 'register_kbucket_share') {
        // This is the first message we should get from the share
        if (!is_valid_kbnode_id(msg.kbnode_id || '')) {
          // Not a valid share key. Close the connection.
          PWS.sendErrorAndClose('Invalid share key');
          return;
        }
        kbnode_id = msg.kbnode_id;
        // check to see if we already have a share with this key
        if (m_manager.connectedShareManager().getConnectedShare(kbnode_id)) {
          // We already have a share with this kbnode_id. Close connection.
          PWS.sendErrorAndClose('A share with this key already exists');
          return;
        }

        if (!X.info) {
          PWS.sendErrorAndClose('No info field found in message');
          return;
        }

        // Everything looks okay, let's add this share to our share manager
        console.info(`Registering share: ${kbnode_id}`);
        // X.info has information about the share
        // send_message_to_share (defined below) is a function that allows the share object to send messages back to the share

        // Check whether client (or user of client) can attest that the data is being shared for scientific research purposes
        if (X.info.scientific_research != 'yes') {
          PWS.sendErrorAndClose('KBucket should only be used to share data for scientific research purposes');
          return;
        }
        // Check whether client (or user of client) agreed to share the data
        if (X.info.confirm_share != 'yes') {
          PWS.sendErrorAndClose('Sharing of directory has not been confirmed');
          return;
        }

        m_manager.connectedShareManager().addConnectedShare(kbnode_id, X.info, send_message_to_share, function(err) {
          if (err) {
            PWS.sendErrorAndClose(`Error adding share: ${err}`);
            return;
          }
          PWS.sendMessage({
            message: 'ok'
          });
        });
      } else {
        // Handle all other messages
        var SS = m_manager.connectedShareManager().getConnectedShare(kbnode_id);
        if (!SS) {
          // Somehow we can't find the share anymore. Close connection.
          PWS.sendErrorAndClose(`Unable to find share with key=${kbnode_id}`);
          return;
        }
        // Forward the message on to the share object
        SS.processMessageFromConnectedShare(X, function(err, response) {
          if (err) {
            // We got some kind of error from the share object. So we'll close the connection.
            PWS.sendErrorAndClose(`${err}`);
            return;
          }
          if (response) {
            // If we got a response, let's send it through the websocket back to the share.
            PWS.sendMessage(response);
          } else {
            PWS.sendMessage({
              message: 'ok'
            });
          }
        });
      }
    });

    function send_message_to_share(msg) {
      PWS.sendMessage(msg);
    }

    PWS.onClose(function() {
      // The web socket has closed
      console.info(`Websocket closed: kbnode_id=${kbnode_id}`);

      // So we should remove the share from the manager
      m_manager.connectedShareManager().removeConnectedShare(kbnode_id);
    });
  }

  function connect_to_parent_hub(callback) {
    var parent_hub_url = m_config.getConfig('parent_hub_url');
    if ((!parent_hub_url)||(parent_hub_url=='.')) {
      callback(null);
      return;
    }
    m_connection_to_parent_hub=new KBConnectionToParentHub(m_config);
    m_connection_to_parent_hub.setHttpOverWebSocketServer(m_http_over_websocket_server);
    m_connection_to_parent_hub.initialize(parent_hub_url,function(err) {
      if (err) {
        callback(err);
        return;
      }
      callback(null);
    });
  }

  function register_with_parent_hub(callback) {
    send_message_to_parent_hub({
      command: 'register_kbucket_hub',
      info: {
        hub_url: `${m_config.listenUrl()}`,
        name: m_config.getConfig('name'),
        scientific_research: m_config.getConfig('scientific_research'),
        description: m_config.getConfig('description'),
        owner: m_config.getConfig('owner'),
        owner_email: m_config.getConfig('owner_email')
      }
    });
    callback();
  }

  function sign_message(msg, private_key) {
    const signer = crypto.createSign('sha256');
    signer.update(JSON.stringify(msg));
    signer.end();

    const signature = signer.sign(private_key);
    const signature_hex = signature.toString('hex');

    return signature_hex;
  }

  function send_message_to_parent_hub(msg) {
    msg.timestamp = (new Date()) - 0;
    msg.kbnode_id = m_config.kbNodeId();
    var signature = sign_message(msg, m_config.privateKey());
    var X = {
      message: msg,
      kbnode_id: m_config.kbNodeId(),
      signature: signature
    }
    if (msg.command == 'register_kbucket_hub') {
      // send the public key on the first message
      X.public_key = m_config.publicKey();
    }

    m_parent_hub_socket.sendMessage(X);
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