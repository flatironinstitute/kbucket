const fs = require('fs');
const async = require('async');

const logger = require(__dirname + '/hemlock/logger.js').logger();

exports.KBNodeApi = KBNodeApi;

function KBNodeApi(context) {
  this.handle_nodeinfo = handle_nodeinfo;
  this.handle_readdir = handle_readdir;
  this.handle_download = handle_download;
  this.handle_prv = handle_prv;
  this.handle_find = handle_find;

  let m_context = context;

  function handle_nodeinfo(hemlock_node_id, req, res) {
    m_context.config.incrementMetric('num_requests_nodeinfo');
    //allow_cross_domain_requests(req, res);
    if (m_context.config.hemlockNodeId() != hemlock_node_id) {
      route_http_request_to_node(hemlock_node_id, `${hemlock_node_id}/api/nodeinfo`, req, res);
      return;
    }
    let resp_msg = {
      success: true
    };
    resp_msg.info = m_context.config.getNodeInfo();
    if (m_context.config.hemlockNodeType() == 'hub') {
      resp_msg.child_hubs = {};
      let CHM = m_context.hub_manager.connectedChildHubManager();
      let child_hub_ids = CHM.connectedChildHubIds();
      for (let ii in child_hub_ids) {
        let id = child_hub_ids[ii];
        let HH = CHM.getConnectedChildHub(id);
        resp_msg.child_hubs[id] = {
          name: HH.name(),
          listen_url: HH.listenUrl()
        };
      }

      resp_msg.child_terminals = {};
      let CSM = m_context.hub_manager.connectedTerminalManager();
      let child_terminal_ids = CSM.connectedTerminalIds();
      for (let ii in child_terminal_ids) {
        let id = child_terminal_ids[ii];
        let SS = CSM.getConnectedTerminal(id);
        resp_msg.child_terminals[id] = {
          name: SS.name(),
          listen_url: SS.listenUrl()
        };
      }
    }
    if (m_context.connection_to_parent_hub) {
      resp_msg.parent_hub_info = m_context.connection_to_parent_hub.parentHubInfo();
    }
    resp_msg.metrics = m_context.config.metrics();
    res.json(resp_msg);
  }

  function fsafe_readdir(path, callback) {
    try {
      fs.readdir(path, callback);
    } catch (err) {
      callback('Error in readdir: ' + err.message);
    }
  }

  function fsafe_stat(path, callback) {
    try {
      fs.stat(path, callback);
    } catch (err) {
      callback('Error in stat: ' + err.message);
    }
  }

  function is_file(path) {
    try {
      return fs.statSync(path).isFile();
    } catch (err) {
      return false;
    }
  }

  function exists_sync(path) {
    try {
      return fs.existsSync(path);
    } catch (err) {
      return false;
    }
  }

  function handle_readdir(kbshare_id, subdirectory, req, res) {
    m_context.config.incrementMetric('num_requests_readdir');
    logger.info('handle_readdir', {
      kbshare_id: kbshare_id,
      subdirectory: subdirectory
    });
    //allow_cross_domain_requests(req, res);
    if (!is_safe_path(subdirectory)) {
      send_500(res, 'Unsafe path: ' + subdirectory);
      return;
    }
    if (m_context.config.hemlockNodeType() == 'hub') {
      var urlpath0 = `${kbshare_id}/api/readdir/${subdirectory}`;
      route_http_request_to_node(kbshare_id, urlpath0, req, res);
      return;
    }
    // so, m_context.config.hemlockNodeType() = 'share'
    if (m_context.config.hemlockNodeId() != kbshare_id) {
      send_500(res, 'Incorrect kbshare id: ' + kbshare_id);
      return;
    }
    var path0 = require('path').join(m_context.config.hemlockNodeDirectory(), subdirectory);
    fsafe_readdir(path0, function(err, list) {
      if (err) {
        send_500(res, err.message);
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
          if (exists_sync(item_path_1)) {
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
          fsafe_stat(item_path, function(err0, stat0) {
            if (err0) {
              send_500(res, `Error in stat of file ${item}: ${err0.message}`);
              return;
            }
            if (stat0.isFile()) {
              var file0 = {
                name: item,
                size: stat0.size,
              };
              var prv0 = m_context.share_indexer.getPrvForIndexedFile(require('path').join(subdirectory, file0.name));
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

  function route_http_request_to_node(hemlock_node_id, path, req, res) {
    logger.info('route_http_request_to_node', {
      node_id: hemlock_node_id,
      path: path,
      req_headers: req.headers
    });
    if (m_context.config.hemlockNodeType() != 'hub') {
      send_500(res, 'Cannot route request from non-hub.');
      return;
    }
    m_context.hub_manager.routeHttpRequestToNode(hemlock_node_id, path, req, res);
  }

  function handle_download(kbshare_id, filename, req, res) {
    m_context.config.incrementMetric('num_requests_download');
    logger.info('handle_download', {
      kbshare_id: kbshare_id,
      filename: filename
    });
    //allow_cross_domain_requests(req, res);

    // don't worry too much because express takes care of this below (b/c we specify a root directory)
    if (!is_safe_path(filename)) {
      send_500(res, 'Unsafe path: ' + subdirectory);
      return;
    }
    if (m_context.config.hemlockNodeType() == 'hub') {
      var urlpath0 = `${kbshare_id}/download/${filename}`;
      route_http_request_to_node(kbshare_id, urlpath0, req, res);
      return;
    }
    // so, m_context.config.hemlockNodeType() = 'share'
    if (m_context.config.hemlockNodeId() != kbshare_id) {
      send_500(res, 'Incorrect kbshare id: ' + kbshare_id);
      return;
    }

    var path0 = require('path').join(m_context.config.hemlockNodeDirectory(), filename);
    if ((!exists_sync(path0) && (exists_sync(path0 + '.prv')))) {
      send_500(res, 'File does not exist, although its .prv does exist.');
      return;
    }
    if (!exists_sync(path0)) {
      send_404(res);
      return;
    }
    if (!is_file(path0)) {
      send_500(res, 'Not a file: ' + filename);
      return;
    }
    try {
      res.sendFile(filename, {
        dotfiles: 'allow',
        root: m_context.config.hemlockNodeDirectory()
      });
    } catch (err) {
      logger.error('Caught exception from res.sendFile: ' + filename, {
        error: error.message
      });
    }
  }

  function handle_prv(kbshare_id, filename, req, res) {
    m_context.config.incrementMetric('num_requests_prv');
    logger.info('handle_prv', {
      kbshare_id: kbshare_id,
      filename: filename
    });
    //allow_cross_domain_requests(req, res);

    // don't worry too much because express takes care of this below (b/c we specify a root directory)
    if (!is_safe_path(filename)) {
      send_500(res, 'Unsafe path: ' + subdirectory);
      return;
    }
    if (m_context.config.hemlockNodeType() == 'hub') {
      var urlpath0 = `${kbshare_id}/prv/${filename}`;
      route_http_request_to_node(kbshare_id, urlpath0, req, res);
      return;
    }
    // so, m_context.config.kbNodeType() = 'share'
    if (m_context.config.hemlockNodeId() != kbshare_id) {
      send_500(res, 'Incorrect kbshare id: ' + kbshare_id);
      return;
    }

    let prv0 = m_context.share_indexer.getPrvForIndexedFile(filename);
    if (!prv0) {
      send_500(res, 'File is not yet indexed or does not exist.');
      return;
    }
    res.json(prv0);
  }

  function handle_find(sha1, filename, req, res) {
    m_context.config.incrementMetric('num_requests_find');
    logger.info('handle_find', {
      sha1: sha1,
      filename: filename
    });
    //allow_cross_domain_requests(req, res);

    if (m_context.config.hemlockNodeType() != 'hub') {
      send_500(res, 'Cannot find. This is not a hub.');
      return;
    }

    // Note: In future we should only allow method=GET
    if ((req.method == 'GET') || (req.method == 'POST')) {
      // find the file
      find_file(context, {
        sha1: sha1,
        filename: filename //only used for convenience in appending the url, not for finding the file
      }, function(err, resp) {
        if (err) {
          // There was an error in trying to find the file
          send_500(res, err);
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
            if (m_context.config.topHubUrl() != m_context.config.listenUrl()) {
              ret['alt_hub_url'] = m_context.config.topHubUrl();
            }
            res.json(ret);
          }
        }
      });
    } else {
      // Other request methods are not allowed
      try {
        res.status(405).send('Method not allowed');
      } catch (err) {}
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
}

function find_file(context, opts, callback) {
  find_shares_with_file(context, opts, function(err, results) {
    if (err) {
      callback(err);
      return;
    }
    var node_data0 = context.hub_manager.nodeDataForParent();
    var urls = [];
    async.eachSeries(results, function(result, cb) {
      var kbshare_id = result.kbshare_id;
      var share0 = node_data0.descendant_nodes[kbshare_id];
      if (share0.listen_url) {
        const url0 = share0.listen_url + '/' + kbshare_id + '/download/' + result.path;
        urls.push(url0);
      }
      var visited = {}; //prevent infinite loop
      var hub_id = share0.parent_hemlock_node_id;
      while (hub_id) {
        if (visited[hub_id])
          break;
        visited[hub_id] = true;
        if (hub_id in node_data0.descendant_nodes) {
          var hub0 = node_data0.descendant_nodes[hub_id];
          if (hub0.listen_url) {
            const url0 = hub0.listen_url + '/' + kbshare_id + '/download/' + result.path;
            urls.push(url0);
          }
          hub_id = hub0.parent_hemlock_node_id;
        } else {
          hub_id = null;
        }
      }
      if (context.config.listenUrl()) {
        const url0 = context.config.listenUrl() + '/' + kbshare_id + '/download/' + result.path;
        urls.push(url0);
      }
      cb();
    }, function() {
      callback(null, {
        success: true,
        found: true,
        urls: urls,
        results: results
      });
    });
  });
}

function find_shares_with_file(context, opts, callback) {
  find_child_shares_with_file(context, opts, function(err, results1) {
    if (err) {
      callback(err);
      return;
    }
    find_shares_with_file_in_child_hubs(context, opts, function(err, results2) {
      if (err) {
        callback(err);
        return;
      }
      var results = [];
      for (let i in results1) {
        results.push(results1[i]);
      }
      for (let i in results2) {
        results.push(results2[i]);
      }
      callback(null, results);
    });
  });
}

function find_shares_with_file_in_child_hubs(context, opts, callback) {
  // Find a file by checking all of the connected shares
  var kbnode_ids = context.hub_manager.connectedChildHubManager().connectedChildHubIds();

  var results = [];

  // Loop sequentially through each child hub id
  // TODO: shall we allow this to be parallel / asynchronous?
  async.eachSeries(kbnode_ids, function(kbnode_id, cb) {
    var SS = context.hub_manager.connectedChildHubManager().getConnectedChildHub(kbnode_id);
    if (!SS) { //maybe it disappeared
      cb(); // go to the next one
      return;
    }
    var urlpath0 = `find/${opts.sha1}/${opts.filename}`;
    SS.httpOverWebSocketClient().httpRequestJson(urlpath0, function(err, resp) {
      if (!err) {
        let results0 = resp.results || [];
        for (let i in results0) {
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

function find_child_shares_with_file(context, opts, callback) {
  // Find a file by checking all of the connected shares
  var kbnode_ids = context.hub_manager.connectedTerminalManager().connectedTerminalIds();

  var results = [];

  // Loop sequentially through each share key
  // TODO: shall we allow this to be parallel / asynchronous?
  async.eachSeries(kbnode_ids, function(kbnode_id, cb) {
    var SS = context.hub_manager.connectedTerminalManager().getConnectedTerminal(kbnode_id);
    if (!SS) { //maybe it disappeared
      cb(); // go to the next one
      return;
    }
    // Find the file on this particular share

    let connection_to_child_node = SS.connectionToChildNode();
    var data0 = connection_to_child_node.childNodeData();
    var files_by_sha1 = data0.files_by_sha1 || {};
    if (opts.sha1 in files_by_sha1) {
      const kbnode_id = connection_to_child_node.childNodeId();
      results.push({
        kbshare_id: kbnode_id,
        size: files_by_sha1[opts.sha1].size,
        path: files_by_sha1[opts.sha1].path
      });
      cb();
    } else {
      cb();
    }
  }, function() {
    callback(null, results);
  });
}



/*
function allow_cross_domain_requests(req, res) {
  try {
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
  } catch (err) {
    logger.error(`Caught exception in allow_cross_domain_requests (${req.method}): ${err.message}`);
  }
}
*/

function send_404(res) {
  try {
    res.status(404).send('404: File Not Found');
  } catch (err2) {
    console.error('Problem sending 404 response: ' + err2.message);
  }
}

function send_500(res, err) {
  logger.error('send_500', {
    error: err
  });
  try {
    res.status(500).send({
      error: err
    });
  } catch (err2) {
    console.error('Problem sending 500 response: ' + err + ':' + err2.message);
  }
}

function ends_with(str, str2) {
  return (str.slice(str.length - str2.length) == str2);
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

function is_excluded_directory_name(name) {
  if (name.startsWith('.')) return true;
  var to_exclude = ['node_modules', '.git', '.kbucket'];
  return (to_exclude.indexOf(name) >= 0);
}