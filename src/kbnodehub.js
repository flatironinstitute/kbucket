exports.KBNodeHub = KBNodeHub;

const async = require('async');
const express = require('express');
const WebSocket = require('ws');
const crypto = require('crypto');

const KBNodeConfig = require(__dirname + '/kbnodeconfig.js').KBNodeConfig;
const KBNodeHubApi = require(__dirname + '/kbnodehubapi.js').KBNodeHubApi;
const KBucketHubManager = require(__dirname + '/kbuckethubmanager.js').KBucketHubManager;
const PoliteWebSocket = require(__dirname + '/politewebsocket.js').PoliteWebSocket;

var debugging = true;

function KBNodeHub(kbnode_directory) {
  this.initialize = function(opts,callback) {
    initialize(opts,callback);
  };

  var m_config = new KBNodeConfig(kbnode_directory);
  var m_app = null;
  var m_parent_hub_socket = null;
  // Encapsulate some functionality in a manager
  var m_manager = new KBucketHubManager(m_config);

  var API = new KBNodeHubApi(m_config, m_manager);

  function initialize(opts,callback) {
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
	    m_config.runInteractiveConfiguration(opts,callback);
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

    console.log('aaaaaaa');

    // API find -- find a file on the kbucket network (or on the local kbucket-hub disk)
    app.use('/find/:sha1/:filename(*)', function(req, res) {
      // Note: filename is just for convenience, only used in forming download urls
      var params = req.params;
      API.handleFind(params.sha1, params.filename, req, res);
    });
    console.log('aaaaaaa b');
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
    app.use('/share/:kbshare_id/:path(*)', function(req, res) {
      var params = req.params;
      API.handleForwardToConnectedShare(params.kbshare_id, req.method, params.path, req, res);
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

    console.log('aaaaaaa 132');

    // API upload -- upload a file to the kbucket-hub disk
    // TODO: think about what restrictions to place on this operation (aside from the per-upload limit)
    app.use('/upload', API.handleUpload);

    console.log('aaaaaaa 133');

    // API web
    // A web interface that will expand over time
    app.use('/web', express.static(__dirname + '/web'));

    start_http_server_2(callback);
  }

  function start_http_server_2(callback) {
    var app = m_app;
    var listen_port = m_config.getConfig('listen_port');
    m_config.setListenPort(listen_port);
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
    var kbshare_id = ''; // will be filled in below
    var kbshare_public_key = ''; // will be filled in below
    var closed_with_error = false;
    PWS.onMessage(function(err, msg) {
      // The share has sent us a message
      if (err) {
        console.log(`${err}. Closing websocket.`);
        PWS.close();
        return;
      }

      // Set the kbshare id (should be received on first message)
      if (!kbshare_id) {
        kbshare_id = msg.kbshare_id;
      }

      if ((kbshare_id) && (msg.kbshare_id != kbshare_id)) {
        // The kbshare_id was set, but this message did not match. Close the connection.
        PWS.sendErrorAndClose('Share id does not match.');
        return;
      }

      // If we are given the public key, remember it, and compare it to the kbshare_id
      if ((msg.public_key) && (!kbshare_public_key)) {
        kbshare_public_key = msg.public_key;
        var list = msg.public_key.split('\n');
        var expected_share_id = list[1].slice(0, 12);
        if (expected_share_id != kbshare_id) {
          PWS.sendErrorAndClose(`Share id does not match public key (${kbshare_id}<>${expected_share_id})`);
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

      if (debugging) {
        // If DEBUG=true, let's print the message
        if (X.command != 'set_file_info') {
          console.log('====================================== received message');
          console.log(JSON.stringify(msg, null, 4).slice(0, 500));
        }
      }

      if (X.command == 'register_kbucket_share') {
        // This is the first message we should get from the share
        if (!is_valid_kbshare_id(msg.kbshare_id || '')) {
          // Not a valid share key. Close the connection.
          PWS.sendErrorAndClose('Invalid share key');
          return;
        }
        kbshare_id = msg.kbshare_id;
        // check to see if we already have a share with this key
        if (m_manager.connectedShareManager().getConnectedShare(kbshare_id)) {
          // We already have a share with this kbshare_id. Close connection.
          PWS.sendErrorAndClose('A share with this key already exists');
          return;
        }

        if (!X.info) {
          PWS.sendErrorAndClose('No info field found in message');
          return;
        }

        // Everything looks okay, let's add this share to our share manager
        console.log(`Registering share: ${kbshare_id}`);
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

        m_manager.connectedShareManager().addConnectedShare(kbshare_id, X.info, send_message_to_share, function(err) {
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
        var SS = m_manager.connectedShareManager().getConnectedShare(kbshare_id);
        if (!SS) {
          // Somehow we can't find the share anymore. Close connection.
          PWS.sendErrorAndClose(`Unable to find share with key=${kbshare_id}`);
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
      if (debugging) {
        console.log(`Websocket closed: kbshare_id=${kbshare_id}`);
      }
      // So we should remove the share from the manager
      m_manager.connectedShareManager().removeConnectedShare(kbshare_id);
    });
  }

  function connect_to_parent_hub(callback) {
    var parent_hub_url = m_config.getConfig('parent_hub_url');
    if ((!parent_hub_url)||(parent_hub_url=='.')) {
      callback(null);
      return;
    }
    var parent_hub_ws_url = get_websocket_url_from_http_url(parent_hub_url);
    m_parent_hub_socket = new PoliteWebSocket({
      wait_for_response: true,
      enforce_remote_wait_for_response: false
    });
    m_parent_hub_socket.connectToRemote(parent_hub_ws_url, function(err) {
      if (err) {
        callback(err);
        return;
      }
      register_with_parent_hub(function(err) {
        if (err) {
          callback(err);
          return;
        }
        callback(null);
      });
    });
    m_parent_hub_socket.onClose(function() {
      console.log(`Websocket closed. Aborting.`);
      process.exit(-1);
    });
    m_parent_hub_socket.onMessage(function(err, msg) {
      if (err) {
        console.log(`Error from parent hub: ${err}. Aborting.`);
        process.exit(-1);
        return;
      }
      process_message_from_parent_hub(msg);
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

  function sign_message(msg,private_key) {
    const signer = crypto.createSign('sha256');
    signer.update(JSON.stringify(msg));
    signer.end();

    const signature = signer.sign(private_key);
    const signature_hex = signature.toString('hex');

    return signature_hex;
  }

  function send_message_to_parent_hub(msg) {
    msg.timestamp = (new Date()) - 0;
    msg.kbshare_id = m_config.kbNodeId();
    var signature = sign_message(msg, m_config.privateKey());
    var X = {
      message: msg,
      kbshare_id: m_config.kbNodeId(),
      signature: signature
    }
    if (msg.command == 'register_kbucket_hub') {
      // send the public key on the first message
      X.public_key = m_config.publicKey();
    }

    if (debugging) {
    	console.log('------------------------------- sending message');
      console.log(JSON.stringify(X, null, 4).slice(0, 400));
    }

    m_parent_hub_socket.sendMessage(X);
  }

  function verify_message(msg,hex_signature,public_key) {
		const verifier = crypto.createVerify('sha256');
		verifier.update(JSON.stringify(msg));
		verifier.end();

		var signature=Buffer.from(hex_signature,'hex');

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

function is_valid_kbshare_id(key) {
	// check if a kbshare_id is valid
	// TODO: add detail and use regexp
	return ((8<=key.length)&&(key.length<=64));
}

function is_valid_sha1(sha1) {
	// check if this is a valid SHA-1 hash
    if (sha1.match(/\b([a-f0-9]{40})\b/))
        return true;
    return false;
}

/*

function connect_to_parent_hub(callback) {
      var parent_hub_url = m_config.getConfig('parent_hub_url');
      if (!parent_hub_url) {
        callback('No parent hub url specified.');
        return;
      }
      var parent_hub_ws_url = get_websocket_url_from_http_url(parent_hub_url);
      m_parent_hub_socket = new PoliteWebSocket({
        wait_for_response: true,
        enforce_remote_wait_for_response: false
      });
      m_parent_hub_socket.connectToRemote(url_ws, function(err) {
        if (err) {
          callback(err);
          return;
        }
        register_with_parent_hub(function(err) {
          if (err) {
            callback(err);
            return;
          }
          callback(null);
        });
      });
      m_parent_hub_socket.onClose(function() {
        console.log(`Websocket closed. Aborting.`);
        process.exit(-1);
      });
      m_parent_hub_socket.onMessage(function(err, msg) {
        if (err) {
          console.log(`Error from parent hub: ${err}. Aborting.`);
          process.exit(-1);
          return;
        }
        process_message_from_parent_hub(msg);
      });
    }

    function process_message_from_parent_hub(msg) {
      //Finish
    }

    function register_with_parent_hub(callback) {
      send_message_to_hub({
        command: 'register_kbucket_hub',
        info: {
          hub_url: m_config.getConfig('hub_url'),
          name: m_config.getConfig('name'),
          scientific_research: m_config.getConfig('scientific_research'),
          description: m_config.getConfig('description'),
          owner: m_config.getConfig('owner'),
          owner_email: m_config.getConfig('owner_email')
        }
      });
      callback();
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

    function check_kbnode_id(req, res) {
      var params = req.params;
      if (params.kbnode_id != m_config.kbNodeId()) {
        var errstr = `Incorrect kbucket share key: ${params.kbnode_id}`;
        console.error(errstr);
        res.status(500).send({
          error: errstr
        });
        return false;
      }
      return true;
    }

    function handle_readdir(subdirectory, req, res) {
      allow_cross_domain_requests(req, res);
      if (!is_safe_path(subdirectory)) {
        res.status(500).send({
          error: 'Unsafe path: ' + subdirectory
        });
        return;
      }
      var path0 = require('path').join(share_directory, subdirectory);
      fs.readdir(path0, function(err, list) {
        if (err) {
          res.status(500).send({
            error: err.message
          });
          return;
        }
        var files = [],
          dirs = [];
        async.eachSeries(list, function(item, cb) {
          if ((item == '.') || (item == '..') || (item == '.kbucket')) {
            cb();
            return;
          }
          fs.stat(require('path').join(path0, item), function(err0, stat0) {
            if (err0) {
              res.status(500).send({
                error: `Error in stat of file ${item}: ${err0.message}`
              });
              return;
            }
            if (stat0.isFile()) {
              files.push({
                name: item,
                size: stat0.size
              });
            } else if (stat0.isDirectory()) {
              if (!is_excluded_directory_name(item)) {
                dirs.push({
                  name: item
                });
              }
            }
            cb();
          });
        }, function() {
          res.json({
            success: true,
            files: files,
            dirs: dirs
          });
        });
      });
    }

    function handle_download(filename, req, res) {
      allow_cross_domain_requests(req, res);

      // don't worry too much because express takes care of this below (b/c we specify a root directory)
      if (!is_safe_path(filename)) {
        res.status(500).send({
          error: 'Unsafe path: ' + filename
        });
        return;
      }
      var path0 = require('path').join(share_directory, filename);
      if (!fs.existsSync(path0)) {
        res.status(404).send('404: File Not Found');
        return;
      }
      if (!fs.statSync(path0).isFile()) {
        res.status(500).send({
          error: 'Not a file: ' + filename
        });
        return;
      }
      res.sendFile(filename, {
        dotfiles: 'allow',
        root: share_directory
      });
    }

    function is_safe_path(path) {
      var list = path.split('/');
      for (var i in list) {
        var str = list[i];
        if ((str == '~') || (str == '.') || (str == '..')) return false;
      }
      return true;
    }

    function get_free_port_in_range(range, callback) {
      var findPort = require('find-port');
      if (range.length > 2) {
        callback('Invalid port range.');
        return;
      }
      if (range.length < 1) {
        callback('Invalid port range (*).');
        return;
      }
      if (range.length == 1) {
        range.push(range[0]);
      }
      range[0] = Number(range[0]);
      range[1] = Number(range[1]);
      findPort('127.0.0.1', range[0], range[1], function(ports) {
        if (ports.length == 0) {
          callback(`No free ports found in range ${range[0]}-${range[1]}`);
          return;
        }
        callback(null, ports[0]);
      });
    }

    function allow_cross_domain_requests(req, res) {
      if (req.method == 'OPTIONS') {
        res.set('Access-Control-Allow-Origin', '*');
        res.set("Access-Control-Allow-Methods", "POST, GET, HEAD, OPTIONS");
        res.set("Access-Control-Allow-Credentials", true);
        res.set("Access-Control-Max-Age", '86400'); // 24 hours
        res.set("Access-Control-Allow-Headers", "X-Requested-With, X-HTTP-Method-Override, Content-Type, Accept, Authorization, Range");
        res.status(200).send();
        return;
      } else {
        res.header("Access-Control-Allow-Origin", "*");
        res.set("Access-Control-Allow-Headers", "X-Requested-With, X-HTTP-Method-Override, Content-Type, Accept, Authorization, Range");
      }
    }

*/