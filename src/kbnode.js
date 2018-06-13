exports.KBNode = KBNode;

const async = require('async');
const express = require('express');
const WebSocket = require('ws');
const findPort = require('find-port');
const fs = require('fs');
const axios = require('axios');

const KBNodeConfig = require(__dirname + '/kbnodeconfig.js').KBNodeConfig;
const KBNodeShareIndexer = require(__dirname + '/kbnodeshareindexer.js').KBNodeShareIndexer;
const HttpOverWebSocketServer = require(__dirname + '/httpoverwebsocket.js').HttpOverWebSocketServer;
const KBConnectionToChildNode = require(__dirname + '/kbconnectiontochildnode.js').KBConnectionToChildNode;
const KBConnectionToParentHub = require(__dirname + '/kbconnectiontoparenthub.js').KBConnectionToParentHub;
const KBucketHubManager = require(__dirname + '/kbuckethubmanager.js').KBucketHubManager;
const PoliteWebSocket = require(__dirname + '/politewebsocket.js').PoliteWebSocket;
const KBucketClient = require(__dirname + '/kbucketclient.js').KBucketClient;

// TODO: think of a better default range
const KBUCKET_SHARE_PORT_RANGE = process.env.KBUCKET_SHARE_PORT_RANGE || '2000-3000';
const KBUCKET_SHARE_HOST = process.env.KBUCKET_SHARE_HOST = 'localhost';

function KBNode(kbnode_directory, kbnode_type) {
  this.initialize = function(opts, callback) {
    initialize(opts, callback);
  };
  this.setKBucketUrl = function(url) {
    m_kbucket_url = url;
  }

  var m_config = new KBNodeConfig(kbnode_directory);
  var m_app = null;
  var m_connection_to_parent_hub = null;
  var m_kbucket_url = 'https://kbucket.flatironinstitute.org';

  // only used for kbnode_type='share'
  var m_share_indexer = null;

  // only used for kbnode_type='hub'
  var m_hub_manager = null;
  if (kbnode_type == 'hub')
    m_hub_manager = new KBucketHubManager(m_config);

  function initialize(opts, callback) {
    var steps = [];

    // for both types
    steps.push(create_config_if_needed);
    steps.push(initialize_config);
    steps.push(run_interactive_config);
    if (!opts.clone_only) {
      steps.push(start_http_server);
    }

    // for kbnode_type='hub'
    if (kbnode_type == 'hub') {
      steps.push(start_websocket_server);
    }

    // for both types
    if (!opts.clone_only) {
      steps.push(connect_to_parent_hub);
      steps.push(start_sending_node_data_to_parent);
    }

    // for kbnode_type='share'
    if (kbnode_type == 'share') {
      if (!opts.clone_only) {
        steps.push(start_indexing);
      }
      if (opts.clone_only) {
        steps.push(download_for_clone);
      }
    }

    async.series(steps, function(err) {
      callback(err);
    });

    function run_interactive_config(callback) {
      m_config.runInteractiveConfiguration(opts, callback);
    }

    function create_config_if_needed(callback) {
      if (!m_config.configDirExists()) {
        console.info(`Creating kbucket ${kbnode_type} configuration in ${m_config.kbNodeDirectory()}/.kbucket ...`);
        m_config.createNew(kbnode_type, opts, function(err) {
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

    function download_for_clone(callback) {
      do_download_for_clone(opts, opts.kbshare_subdirectory||'', '', callback);
    }
  }

  function initialize_config(callback) {
    console.info(`Initializing configuration...`);
    m_config.initialize(function(err) {
      if (err) {
        callback(err);
        return;
      }
      if (m_config.kbNodeType() != kbnode_type) {
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

    // API readdir
    app.get('/:kbshare_id/api/readdir/:subdirectory(*)', function(req, res) {
      var params = req.params;
      handle_readdir(params.kbshare_id, params.subdirectory, req, res);
    });
    app.get('/:kbshare_id/api/readdir/', function(req, res) {
      var params = req.params;
      handle_readdir(params.kbshare_id, '', req, res);
    });

    // API nodeinfo
    app.get('/:kbnode_id/api/nodeinfo', function(req, res) {
      var params = req.params;
      handle_nodeinfo(params.kbnode_id, req, res);
    });

    // API download
    app.get('/:kbshare_id/download/:filename(*)', function(req, res) {
      var params = req.params;
      handle_download(params.kbshare_id, params.filename, req, res);
    });

    // API find (only for kbnode_type='hub')
    app.get('/find/:sha1/:filename(*)', function(req, res) {
      var params = req.params;
      handle_find(params.sha1, params.filename, req, res);
    });
    app.get('/find/:sha1/', function(req, res) {
      var params = req.params;
      handle_find(params.sha1, '', req, res);
    });

    get_listen_port(function(err, listen_port) {
      if (err) {
        callback(err);
        return;
      }
      app.port = listen_port;
      m_config.setListenPort(listen_port);

      if (process.env.SSL != null ? process.env.SSL : listen_port % 1000 == 443) {
        // The port number ends with 443, so we are using https
        app.USING_HTTPS = true;
        app.protocol = 'https';
        // Look for the credentials inside the encryption directory
        // You can generate these for free using the tools of letsencrypt.org
        const options = {
          key: fs.readFileSync(__dirname + '/encryption/privkey.pem'),
          cert: fs.readFileSync(__dirname + '/encryption/fullchain.pem'),
          ca: fs.readFileSync(__dirname + '/encryption/chain.pem')
        };

        // Create the https server
        app.server = require('https').createServer(options, app);
      } else {
        app.protocol = 'http';
        // Create the http server and start listening
        app.server = require('http').createServer(app);
      }
      // start listening
      app.server.listen(listen_port, function() {
        console.info(`kbucket-${kbnode_type} is running ${app.protocol} on port ${app.port}`);
        callback(null);
      });
    });
  }

  function connect_to_parent_hub(callback) {
    var opts = {
      retry_timeout_sec: 4,
      retry2_timeout_sec: 10
    };
    do_connect_to_parent_hub(opts, callback);
  }

  function do_connect_to_parent_hub(opts, callback) {
    var parent_hub_url = m_config.getConfig('parent_hub_url');
    if ((!parent_hub_url) || (parent_hub_url == '.')) {
      if (kbnode_type == 'share') {
        callback('No parent hub url specified.');
      } else {
        callback(null);
      }
      return;
    }
    console.info('Connecting to parent hub: ' + parent_hub_url);
    m_connection_to_parent_hub = new KBConnectionToParentHub(m_config);
    m_connection_to_parent_hub.onClose(function() {
      m_connection_to_parent_hub = null;
      if (opts.retry_timeout_sec) {
        console.info(`Connection to parent hub closed. Will retry in ${opts.retry_timeout_sec} seconds...`);
        setTimeout(function() {
          retry_connect_to_parent_hub(opts);
        }, opts.retry_timeout_sec * 1000);
      }
    });
    m_connection_to_parent_hub.initialize(parent_hub_url, function(err) {
      if (err) {
        callback(err);
        return;
      }
      if (m_share_indexer) {
        m_share_indexer.restartIndexing(); //need to think about this...
      }
      callback(null);
    });
  }

  function retry_connect_to_parent_hub(opts) {
    do_connect_to_parent_hub(opts, function(err) {
      if (err) {
        console.error(err);
        if (opts.retry2_timeout_sec) {
          console.info(`Failed to reconnect to parent hub. Will retry in ${opts.retry2_timeout_sec} seconds...`);
          setTimeout(function() {
            retry_connect_to_parent_hub(opts);
          }, opts.retry2_timeout_sec * 1000);
        }
      }
    });
  }

  function do_download_for_clone(opts, src_subdirectory, dst_subdirectory, callback) {
    var dirpath0 = require('path').join(kbnode_directory, dst_subdirectory);
    if (dst_subdirectory != '') {
      if (fs.existsSync(dirpath0)) {
        callback('Stopping download for clone. Directory already exists: ' + dirpath0);
        return;
      }
      fs.mkdirSync(dirpath0);
    }
    readdir(src_subdirectory, function(err, files, dirs) {
      if (err) {
        callback(err);
        return;
      }
      async.series([function(cb2) {
        async.eachSeries(files, function(file0, cb) {
          if (!file0.prv) {
            callback(`Stopping download for clone. File not yet indexed: ${src_subdirectory}/${file0.name}`);
            return;
          }
          if (file0.prv.original_size <= opts.max_file_download_size_mb * (1024 * 1024)) {
            _download();
          } else {
            _write_prv();
          }

          function _download() {
            console.info(`Downloading ${dst_subdirectory}/${file0.name}`);
            download_file_from_share(`${src_subdirectory}/${file0.name}`, dirpath0 + '/' + file0.name, {
              size: file0.prv.size
            }, function(err) {
              if (err) {
                console.warn(`Problem downloading (err). Writing .prv instead.`)
                console.info('');
                _write_prv();
                return;
              }
              cb();
            });
          }

          function _write_prv() {
            console.info(`Writing ${dst_subdirectory}/${file0.name}.prv`)
            write_json_file(dirpath0 + '/' + file0.name + '.prv', file0.prv);
            cb();
          }
        }, function() {
          cb2();
        });
      }, function(cb2) {
        async.eachSeries(dirs, function(dir0, cb) {
          do_download_for_clone(opts, require('path').join(src_subdirectory, dir0.name), require('path').join(dst_subdirectory, dir0.name), function(err) {
            if (err) {
              callback(err);
              return;
            }
            cb();
          });
        }, function() {
          callback(null);
        });
      }]);
    });
  }

  function download_file_from_share(relpath_on_share, destpath, opts, callback) {
    var url = `${m_kbucket_url}/${m_config.kbNodeId()}/download/${encodeURIComponent(relpath_on_share)}`;
    download_file(url, destpath, opts, callback)
  }

  function download_file(url, dest_fname, opts, callback) {
    console.info(`Downloading [${url}] > [${dest_fname}] ...`);
    var bytes_downloaded = 0;
    var bytes_total = opts.size || null;
    var timer = new Date();
    axios.get(url, {
        responseType: 'stream'
      })
      .then(function(response) {
        response.data.on('data', function(data) {
          bytes_downloaded += data.length;
          report_progress(bytes_downloaded, bytes_total);
        });
        var write_stream = fs.createWriteStream(dest_fname+'.downloading_');
        response.data.pipe(write_stream);
        response.data.on('end', function() {
          fs.renameSync(dest_fname+'.downloading_',dest_fname);
          console.info(`Downloaded ${format_file_size(bytes_downloaded)} to ${dest_fname}.`)
          setTimeout(function() { //dont catch an error from execution of callback
            callback(null);
          }, 0);
        });
      })
      .catch(function(err) {
        callback(err.message);
      });

    function report_progress(bytes_downloaded, bytes_total) {
      var elapsed = (new Date()) - timer;
      if (elapsed < 5000) {
        return;
      }
      timer = new Date();
      if (bytes_total) {
        console.info(`Downloaded ${format_file_size(bytes_downloaded)} of ${format_file_size(bytes_total)} ...`)
      } else {
        console.info(`Downloaded ${format_file_size(bytes_downloaded)} ...`);
      }
    }

  }

  function readdir(subdirectory, callback) {
    var CC = new KBucketClient();
    CC.setKBucketUrl(m_kbucket_url);
    CC.readDir(m_config.kbNodeId(), subdirectory, function(err, files, dirs) {
      callback(err, files, dirs);
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
    console.info('Starting indexing...');
    if (kbnode_type != 'share') {
      console.error('start_indexing is only for kbnode_type=share')
      process.exit(-1);
    }
    m_share_indexer = new KBNodeShareIndexer(send_message_to_parent_hub, m_config);
    m_share_indexer.startIndexing(callback);
  }

  function start_websocket_server(callback) {
    if (kbnode_type != 'hub') {
      console.error('start_websocket_server is only for kbnode_type=hub')
      process.exit(-1);
    }
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
    if (kbnode_type != 'hub') {
      console.error('on_new_websocket_connection is only for kbnode_type=hub')
      process.exit(-1);
    }

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
        console.info(`Adding child share: ${CC.childNodeId()}`);

        m_hub_manager.connectedShareManager().addConnectedShare(CC, function(err) {
          if (err) {
            PWS.sendErrorAndClose(`Error adding share: ${err}`);
            return;
          }
          // acknowledge receipt of the register message so that the child node can proceed
          CC.sendMessage({
            command: 'confirm_registration',
            info: get_nodeinfo(false)
          });
        });
        //todo: how do we free up the CC object?
      } else if (CC.childNodeType() == 'hub') {
        // Everything looks okay, let's add this share to our share manager
        console.info(`Adding child hub: ${CC.childNodeId()}`);

        m_hub_manager.connectedChildHubManager().addConnectedChildHub(CC, function(err) {
          if (err) {
            PWS.sendErrorAndClose(`Error adding child hub: ${err}`);
            return;
          }
          // acknowledge receipt of the register message so that the child node can proceed
          CC.sendMessage({
            command: 'confirm_registration',
            info: get_nodeinfo(false)
          });
        });
      } else {
        PWS.sendErrorAndClose('Unexpected child node type: ' + CC.childNodeType());
      }
    });
  }

  function route_http_request_to_node(kbnode_id, path, req, res) {
    if (kbnode_type != 'hub') {
      res.status(500).send({
        error: 'Cannot route request from share.'
      });
      return;
    }
    m_hub_manager.routeHttpRequestToNode(kbnode_id, path, req, res);
  }

  function get_nodeinfo(include_parent_info) {
    var ret = {
      kbnode_id: m_config.kbNodeId(),
      kbnode_type: m_config.kbNodeType(),
      name: m_config.getConfig('name'),
      description: m_config.getConfig('description'),
      owner: m_config.getConfig('owner'),
      owner_email: m_config.getConfig('owner_email'),
      listen_url: m_config.listenUrl() || undefined,
      public_key: m_config.publicKey() || undefined
    };
    if ((include_parent_info) && (m_connection_to_parent_hub)) {
      ret.parent_hub_info = m_connection_to_parent_hub.parentHubInfo();
    }
    return ret;
  }

  function handle_nodeinfo(kbnode_id, req, res) {
    allow_cross_domain_requests(req, res);
    if (m_config.kbNodeId() != kbnode_id) {
      route_http_request_to_node(kbnode_id, `${kbnode_id}/api/nodeinfo`, req, res);
      return;
    }
    res.json({
      success: true,
      info: get_nodeinfo(true)
    });
  }

  function handle_readdir(kbshare_id, subdirectory, req, res) {
    allow_cross_domain_requests(req, res);
    if (!is_safe_path(subdirectory)) {
      res.status(500).send({
        error: 'Unsafe path: ' + subdirectory
      });
      return;
    }
    if (kbnode_type == 'hub') {
      var urlpath0 = `${kbshare_id}/api/readdir/${subdirectory}`;
      route_http_request_to_node(kbshare_id, urlpath0, req, res);
      return;
    }
    // so, kbnode_type = 'share'
    if (m_config.kbNodeId() != kbshare_id) {
      res.status(500).send({
        error: 'Incorrect kbshare id: ' + kbshare_id
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
        var item_path = require('path').join(path0, item);
        if (ends_with(item_path, '.prv')) {
          var item_path_1 = item_path.slice(0, item_path.length - ('.prv').length);
          if (fs.existsSync(item_path_1)) {
            //don't need to worry about it... the actual file with be sent separately
            cb();
          } else {
            var file0 = {
              name: item.slice(0, item.length - ('.prv').length),
              size: 0
            };
            var prv_obj = read_json_file(item_path);
            if (prv_obj) {
              file0.size = prv_obj.original_size;
              file0.prv = prv_obj;
            } else {
              console.warn('Unable to read file: ' + item_path);
            }
            files.push(file0);
            cb();
          }
        } else {
          fs.stat(item_path, function(err0, stat0) {
            if (err0) {
              res.status(500).send({
                error: `Error in stat of file ${item}: ${err0.message}`
              });
              return;
            }
            if (stat0.isFile()) {
              var file0 = {
                name: item,
                size: stat0.size,
              };
              var prv0 = m_share_indexer.getPrvForIndexedFile(require('path').join(subdirectory, file0.name));
              if (prv0) {
                file0.prv = prv0;
              }
              files.push(file0);
            } else if (stat0.isDirectory()) {
              if (!is_excluded_directory_name(item)) {
                dirs.push({
                  name: item
                });
              }
            }
            cb();
          });
        }
      }, function() {
        res.json({
          success: true,
          files: files,
          dirs: dirs
        });
      });
    });
  }

  function handle_download(kbshare_id, filename, req, res) {
    allow_cross_domain_requests(req, res);

    // don't worry too much because express takes care of this below (b/c we specify a root directory)
    if (!is_safe_path(filename)) {
      res.status(500).send({
        error: 'Unsafe path: ' + subdirectory
      });
      return;
    }
    if (kbnode_type == 'hub') {
      var urlpath0 = `${kbshare_id}/download/${filename}`;
      route_http_request_to_node(kbshare_id, urlpath0, req, res);
      return;
    }
    // so, kbnode_type = 'share'
    if (m_config.kbNodeId() != kbshare_id) {
      res.status(500).send({
        error: 'Incorrect kbshare id: ' + kbshare_id
      })
      return;
    }

    var path0 = require('path').join(kbnode_directory, filename);
    if ((!fs.existsSync(path0) && (fs.existsSync(path0 + '.prv')))) {
      res.status(500).send({
        error: 'File does not exist, although its .prv does exist.'
      });
      return;
    }
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

  function handle_find(sha1, filename, req, res) {
    allow_cross_domain_requests(req, res);

    if (kbnode_type != 'hub') {
      res.status(500).send({
        error: 'Cannot find. This is not a hub.'
      });
      return;
    }

    // Note: In future we should only allow method=GET
    if ((req.method == 'GET') || (req.method == 'POST')) {
      // find the file
      m_hub_manager.findFile({
        sha1: sha1,
        filename: filename //only used for convenience in appending the url, not for finding the file
      }, function(err, resp) {
        if (err) {
          // There was an error in trying to find the file
          res.status(500).send({
            error: err
          });
        } else {
          if (resp.found) {
            // The file was found!
            res.json({
              success: true,
              found: true,
              size: resp.size,
              urls: resp.urls || undefined,
              results: resp.results || undefined
            });
          } else {
            // The file was not found
            var ret = {
              success: true,
              found: false,
            };
            if (m_config.topHubUrl() != m_config.listenUrl()) {
              ret['alt_hub_url'] = m_config.topHubUrl();
            }
            res.json(ret);
          }
        }
      });
    } else {
      // Other request methods are not allowed
      res.status(405).send('Method not allowed');
    }
  }

  function is_safe_path(path) {
    var list = path.split('/');
    for (var i in list) {
      var str = list[i];
      if ((str == '~') || (str == '.') || (str == '..')) return false;
    }
    return true;
  }

  function get_listen_port(callback) {
    if (kbnode_type == 'share') {
      // TODO: figure out better method for determining port in range
      get_free_port_in_range(KBUCKET_SHARE_PORT_RANGE.split('-'), function(err, listen_port) {
        if (err) {
          callback(err);
          return;
        }
        callback(null, listen_port);
      });
    } else {
      var port = m_config.getConfig('listen_port');
      callback(null, port);
    }
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

  function start_sending_node_data_to_parent(callback) {
    do_send_node_data_to_parent();
    callback();
  }

  function do_send_node_data_to_parent() {
    if (!m_connection_to_parent_hub) {
      return;
    }
    if (kbnode_type == 'hub') {
      var node_data = m_hub_manager.nodeData();
      m_connection_to_parent_hub.sendMessage({
        command: 'report_node_data',
        data: node_data
      });
      setTimeout(function() {
        do_send_node_data_to_parent();
      }, 5000);
    }
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

function write_text_file(fname, txt) {
  try {
    require('fs').writeFileSync(fname, txt);
    return true;
  } catch (err) {
    return false;
  }
}

function write_json_file(fname, obj) {
  try {
    require('fs').writeFileSync(fname, JSON.stringify(obj, null, 4));
    return true;
  } catch (err) {
    return false;
  }
}

function parse_json(str) {
  try {
    return JSON.parse(str);
  } catch (err) {
    return null;
  }
}

function read_json_file(fname) {
  try {
    var txt = require('fs').readFileSync(fname, 'utf8')
    return parse_json(txt);
  } catch (err) {
    return null;
  }
}

function read_text_file(fname) {
  try {
    var txt = require('fs').readFileSync(fname, 'utf8')
    return txt;
  } catch (err) {
    return null;
  }
}

function format_file_size(size_bytes) {
  var a = 1024;
  var aa = a * a;
  var aaa = a * a * a;
  if (size_bytes > aaa) {
    return Math.floor(size_bytes / aaa) + ' GB';
  } else if (size_bytes > aaa) {
    return Math.floor(size_bytes / (aaa / 10)) / 10 + ' GB';
  } else if (size_bytes > aa) {
    return Math.floor(size_bytes / aa) + ' MB';
  } else if (size_bytes > aa) {
    return Math.floor(size_bytes / (aa / 10)) / 10 + ' MB';
  } else if (size_bytes > 10 * a) {
    return Math.floor(size_bytes / a) + ' KB';
  } else if (size_bytes > a) {
    return Math.floor(size_bytes / (a / 10)) / 10 + ' KB';
  } else {
    return size_bytes + ' bytes';
  }
}

function ends_with(str, str2) {
  return (str.slice(str.length - str2.length) == str2);
}