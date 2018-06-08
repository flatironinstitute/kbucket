exports.KBNodeShare = KBNodeShare;

const async = require('async');
const express = require('express');
const findPort = require('find-port');
const fs = require('fs');

const KBNodeConfig = require(__dirname + '/kbnodeconfig.js').KBNodeConfig;
const KBNodeShareIndexer = require(__dirname + '/kbnodeshareindexer.js').KBNodeShareIndexer;
const HttpOverWebSocketServer = require(__dirname + '/httpoverwebsocket.js').HttpOverWebSocketServer;
const KBConnectionToParentHub = require(__dirname + '/kbconnectiontoparenthub.js').KBConnectionToParentHub;

// TODO: think of a better default range
const KBUCKET_SHARE_PORT_RANGE = process.env.KBUCKET_SHARE_PORT_RANGE || '2000-3000';
const KBUCKET_SHARE_PROTOCOL = process.env.KBUCKET_SHARE_PROTOCOL = 'http';
const KBUCKET_SHARE_HOST = process.env.KBUCKET_SHARE_HOST = 'localhost';

function KBNodeShare(kbnode_directory) {
  this.initialize = function(opts, callback) {
    initialize(opts, callback);
  };

  var m_config = new KBNodeConfig(kbnode_directory);
  var m_app = null;
  //var m_parent_hub_socket = null;
  var m_http_over_websocket_server=new HttpOverWebSocketServer();
  var m_connection_to_parent_hub=null;
  var m_share_indexer = null;

  function initialize(opts, callback) {
    var steps = [
      create_config_if_needed,
      initialize_config,
      run_interactive_config,
      start_sharing,
      connect_to_parent_hub,
      start_indexing
    ];

    async.series(steps, function(err) {
      callback(err);
    });

    function run_interactive_config(callback) {
      m_config.runInteractiveConfiguration(opts, callback);
    }
  }

  function create_config_if_needed(callback) {
    if (!m_config.configDirExists()) {
      console.info(`Creating new kbucket share configuration in ${m_config.kbNodeDirectory()}/.kbucket ...`);
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
    console.info(`Initializing configuration...`);
    m_config.initialize(function(err) {
      if (err) {
        callback(err);
        return;
      }
      if (m_config.kbNodeType() != 'share') {
        callback ('Incorrect type for kbnode: ' + m_config.kbNodeType());
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
      //var params = req.params;
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
      m_http_over_websocket_server.setForwardUrl(m_config.listenUrl());
      m_app.listen(listen_port, function() {
        console.info(`Listening on port ${listen_port}`);
        console.info(`Web interface: ${KBUCKET_SHARE_PROTOCOL}://${KBUCKET_SHARE_HOST}:${listen_port}/${m_config.kbNodeId()}/web`);
        callback(null);
      });
    });
  }

  function connect_to_parent_hub(callback) {
    var opts={
      retry_timeout_sec:4,
      retry2_timeout_sec:10
    };
    do_connect_to_parent_hub(opts,callback);
  }

  function do_connect_to_parent_hub(opts,callback) {
    var parent_hub_url = m_config.getConfig('parent_hub_url');
    if (!parent_hub_url) {
      callback('No parent hub url specified.');
      return;
    }
    console.info('Connecting to parent hub: '+parent_hub_url);
    m_connection_to_parent_hub=new KBConnectionToParentHub(m_config);
    m_connection_to_parent_hub.setHttpOverWebSocketServer(m_http_over_websocket_server);
    m_connection_to_parent_hub.onClose(function() {
      m_connection_to_parent_hub=null;
      if (opts.retry_timeout_sec) {
        console.info(`Connection to parent hub closed. Will retry in ${opts.retry_timeout_sec} seconds...`);
        setTimeout(function() {
          retry_connect_to_parent_hub(opts);
        },opts.retry_timeout_sec*1000);
      }
    });
    m_connection_to_parent_hub.initialize(parent_hub_url,function(err) {
      if (err) {
        callback(err);
        return;
      }
      console.info('Connected to parent hub: '+parent_hub_url);
      if (m_share_indexer) {
        m_share_indexer.restartIndexing(); //need to think about this...
      }
      callback(null);
    });
  }

  function retry_connect_to_parent_hub(opts) {
    do_connect_to_parent_hub(opts,function(err) {
      if (err) {
        console.error(err);
        if (opts.retry2_timeout_sec) {
          console.info(`Failed to reconnect to parent hub. Will retry in ${opts.retry2_timeout_sec} seconds...`);
          setTimeout(function() {
            retry_connect_to_parent_hub(opts);
          },opts.retry2_timeout_sec*1000);
        }
      }
    });
  }

  function send_message_to_parent_hub(msg) {
    if (!m_connection_to_parent_hub) {
      console.error('Cannot send message: m_connection_to_parent_hub is null.');
      return false;
    }
    m_connection_to_parent_hub.sendMessage(msg);
    return true;
  }

  function start_indexing(callback) {
    m_share_indexer = new KBNodeShareIndexer(send_message_to_parent_hub, m_config);
    m_share_indexer.startIndexing(callback);
  }

  function check_kbnode_id(req, res) {
    var params = req.params;
    if (params.kbnode_id != m_config.kbNodeId()) {
      var errstr = `Incorrect kbucket kbnode_id: ${params.kbnode_id}`;
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

