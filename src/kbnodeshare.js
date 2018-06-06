exports.KBNodeShare = KBNodeShare;

const async = require('async');
const express = require('express');
const findPort = require('find-port');
const crypto = require('crypto');
const fs = require('fs');
const request = require('request');

const KBNodeConfig = require(__dirname + '/kbnodeconfig.js').KBNodeConfig;
const PoliteWebSocket = require(__dirname + '/politewebsocket.js').PoliteWebSocket;
const KBNodeShareIndexer = require(__dirname + '/kbnodeshareindexer.js').KBNodeShareIndexer;

// TODO: think of a better default range
const KBUCKET_SHARE_PORT_RANGE = process.env.KBUCKET_SHARE_PORT_RANGE || '2000-3000';
const KBUCKET_SHARE_PROTOCOL = process.env.KBUCKET_SHARE_PROTOCOL = 'http';
const KBUCKET_SHARE_HOST = process.env.KBUCKET_SHARE_HOST = 'localhost';

var debugging = true;

function KBNodeShare(kbnode_directory) {
  this.initialize = function(opts, callback) {
    initialize(opts, callback);
  };

  var m_config = new KBNodeConfig(kbnode_directory);
  var m_app = null;
  var m_parent_hub_socket = null;
  var HTTP_REQUESTS={};

  function initialize(opts, callback) {
    var steps = [
      create_config_if_needed,
      initialize_config,
      run_interactive_config,
      start_sharing,
      connect_to_parent_hub,
      start_indexing
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
      console.log(`Creating new kbucket share configuration in ${m_config.kbNodeDirectory}/.kbucket ...`);
      m_config.createNew('share', function(err) {
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
    console.log(`Initializing configuration...`);
    m_config.initialize(function(err) {
      if (err) {
        callback(err);
        return;
      }
      if (m_config.kbNodeType() != 'share') {
        callback('Incorrect type for kbnode: ' + m_config.kbNodeType());
        return;
      }
      callback(null);
    });
  }

  function start_sharing(callback) {
    m_app = express();
    var app = m_app;
    app.set('json spaces', 4); // when we respond with json, this is how it will be formatted

    // API readdir
    app.get('/:kbnode_id/api/readdir/:subdirectory(*)', function(req, res) {
      if (!check_kbnode_id(req, res)) return;
      var params = req.params;
      handle_readdir(params.subdirectory, req, res);
    });
    app.get('/:kbnode_id/api/readdir/', function(req, res) {
      if (!check_kbnode_id(req, res)) return;
      var params = req.params;
      handle_readdir('', req, res);
    });

    // API download
    app.get('/:kbnode_id/download/:filename(*)', function(req, res) {
      if (!check_kbnode_id(req, res)) return;
      var params = req.params;
      handle_download(params.filename, req, res);
    });

    // API web
    // don't really need to check the share key here because we won't be able to get anything except in the web/ directory
    app.use('/:kbnode_id/web', express.static(__dirname + '/share_web'));

    start_server(callback);
  }

  function start_server(callback) {
    get_free_port_in_range(KBUCKET_SHARE_PORT_RANGE.split('-'), function(err, listen_port) {
      if (err) {
        callback(err);
        return;
      }
      m_app.port = listen_port;
      m_config.setListenPort(listen_port);
      m_app.listen(listen_port, function() {
        console.log(`Listening on port ${listen_port}`);
        console.log(`Web interface: ${KBUCKET_SHARE_PROTOCOL}://${KBUCKET_SHARE_HOST}:${listen_port}/${m_config.kbNodeId()}/web`)
        callback(null);
      });
    });
  }

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

  function process_message_from_parent_hub(msg) {
    if (debugging) {
    	if (msg.message!='ok') {
	      console.log('====================================== received message');
	      console.log(JSON.stringify(msg, null, 4).slice(0, 400));
	    }
    }

    if (msg.error) {
      console.error(`Error from hub: ${msg.error}`);
      return;
    }

    if (msg.command == 'http_initiate_request') {
      if (msg.request_id in HTTP_REQUESTS) {
        console.log(`Request with id=${msg.request_id} already exists (in http_initiate_request). Closing websocket.`);
        PWS.close();
        return;
      }
      HTTP_REQUESTS[msg.request_id] = new HttpRequest(m_app.port,function(msg_to_hub) {
        msg_to_hub.request_id = msg.request_id;
        send_message_to_parent_hub(msg_to_hub);
      });
      HTTP_REQUESTS[msg.request_id].initiateRequest(msg);
    } else if (msg.command == 'http_write_request_data') {
      if (!(msg.request_id in HTTP_REQUESTS)) {
        console.log(`No request found with id=${msg.request_id} (in http_write_request_data). Closing websocket.`);
        PWS.close();
        return;
      }
      var REQ = HTTP_REQUESTS[msg.request_id];
      var data = Buffer.from(msg.data_base64, 'base64');
      REQ.writeRequestData(data);
    } else if (msg.command == 'http_end_request') {
      if (!(msg.request_id in HTTP_REQUESTS)) {
        console.log(`No request found with id=${msg.request_id} (in http_end_request). Closing websocket.`);
        PWS.close();
        return;
      }
      var REQ = HTTP_REQUESTS[msg.request_id];
      REQ.endRequest();
    } else if (msg.message == 'ok') {
      // just ok.
    } else {
      console.log(`Unexpected command: ${msg.command}. Closing websocket.`);
      PWS.close();
      return;
    }
  }

  function register_with_parent_hub(callback) {
  	var listen_url=m_config.listenUrl();
    send_message_to_parent_hub({
      command: 'register_kbucket_share',
      info: {
        share_url: `${listen_url}`,
        name: m_config.getConfig('name'),
        scientific_research: m_config.getConfig('scientific_research'),
        description: m_config.getConfig('description'),
        owner: m_config.getConfig('owner'),
        owner_email: m_config.getConfig('owner_email'),
        confirm_share: m_config.getConfig('confirm_share')
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
    msg.kbshare_id = m_config.kbNodeId();
    var signature = sign_message(msg, m_config.privateKey());
    var X = {
      message: msg,
      kbshare_id: m_config.kbNodeId(),
      signature: signature
    }
    if (msg.command == 'register_kbucket_share') {
      // send the public key on the first message
      X.public_key = m_config.publicKey();
    }

    if (debugging) {
      if ((!X.message) || (X.message.command != 'set_file_info')) {
        console.log('------------------------------- sending message');
        console.log(JSON.stringify(X, null, 4).slice(0, 400));
      }
    }

    m_parent_hub_socket.sendMessage(X);
  }

  function start_indexing(callback) {
    var share_indexer = new KBNodeShareIndexer(send_message_to_parent_hub, m_config);
    share_indexer.startIndexing(callback);
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
    var path0 = require('path').join(kbnode_directory, subdirectory);
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
    var path0 = require('path').join(kbnode_directory, filename);
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
      root: kbnode_directory
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

}

function is_excluded_directory_name(name) {
  var to_exclude = ['node_modules', '.git', '.kbucket'];
  return (to_exclude.indexOf(name) >= 0);
}

function HttpRequest(self_port,on_message_handler) {
  this.initiateRequest=function(msg) {initiateRequest(msg);};
  this.writeRequestData=function(data) {writeRequestData(data);};
  this.endRequest=function() {endRequest();};

  var m_request=null;

  function initiateRequest(msg) {
    /*
    var opts={
      method:msg.method,
      hostname:'localhost',
      port:KBUCKET_SHARE_PORT,
      path:msg.path,
      headers:msg.headers
    };
    */
    var opts={
      method:msg.method,
      uri:`http://localhost:${self_port}/${msg.path}`,
      headers:msg.headers,
      followRedirect:false // important because we want the proxy server to handle it instead
    }
    m_request=request(opts);
    m_request.on('response',function(resp) {
      on_message_handler({command:'http_set_response_headers',status:resp.statusCode,status_message:resp.statusMessage,headers:resp.headers});
      resp.on('error',on_response_error);
      resp.on('data',on_response_data);
      resp.on('end',on_response_end);
    });
    m_request.on('error',function(err) {
      on_message_handler({command:'http_report_error',error:'Error in request: '+err.message});
    });
  }

  function writeRequestData(data) {
    if (!m_request) {
      console.error('Unexpected: m_request is null in writeRequestData.');
      return;
    }
    m_request.write(data);
  }

  function endRequest() {
    if (!m_request) {
      console.error('Unexpected: m_request is null in endRequest.');
      return;
    }
    m_request.end();
  }

  function on_response_data(data) {
    on_message_handler({
      command:'http_write_response_data',
      data_base64:data.toString('base64')
    });
  }

  function on_response_end() {
    on_message_handler({
      command:'http_end_response'
    });
  }

  function on_response_error(err) {
    on_message_handler({
      command:'http_report_error',
      error:'Error in response: '+err.message
    });
  }
}

