const fs = require('fs');
const async = require('async');

const logger = require(__dirname + '/logger.js').logger();

exports.KBNodeApi = KBNodeApi;

function KBNodeApi(config, context) {
  this.handle_nodeinfo = handle_nodeinfo;
  this.handle_readdir = handle_readdir;
  this.handle_download = handle_download;
  this.handle_find = handle_find;

  var m_config = config;
  var m_context = context;

  function handle_nodeinfo(kbnode_id, req, res) {
    allow_cross_domain_requests(req, res);
    if (m_config.kbNodeId() != kbnode_id) {
      route_http_request_to_node(kbnode_id, `${kbnode_id}/api/nodeinfo`, req, res);
      return;
    }
    var info=m_config.getNodeInfo();
    if (m_context.connection_to_parent_hub) {
      info.parent_hub_info = m_context.connection_to_parent_hub.parentHubInfo();
    }
    res.json({
      success: true,
      info: info
    });
  }

  function fsafe_readdir(path,callback) {
    try {
      fs.readdir(path,callback);
    }
    catch(err) {
      callback('Error in readdir: '+err.message);
    }
  }

  function fsafe_stat(path,callback) {
    try {
      fs.stat(path,callback);
    }
    catch(err) {
      callback('Error in stat: '+err.message);
    }
  }

  function is_file(path) {
    try {
      return fs.statSync(path).isFile();
    }
    catch(err) {
      return false;
    }
  }

  function exists_sync(path) {
    try {
      return fs.existsSync(path);
    }
    catch {
      return false;
    }
  }

  function handle_readdir(kbshare_id, subdirectory, req, res) {
    logger.info('handle_readdir',{kbshare_id:kbshare_id,subdirectory:subdirectory});
    allow_cross_domain_requests(req, res);
    if (!is_safe_path(subdirectory)) {
      send_500(res, 'Unsafe path: ' + subdirectory);
      return;
    }
    if (kbnode_type == 'hub') {
      var urlpath0 = `${kbshare_id}/api/readdir/${subdirectory}`;
      route_http_request_to_node(kbshare_id, urlpath0, req, res);
      return;
    }
    // so, kbnode_type = 'share'
    if (m_config.kbNodeId() != kbshare_id) {
      send_500(res, 'Incorrect kbshare id: ' + kbshare_id);
      return;
    }
    var path0 = require('path').join(kbnode_directory, subdirectory);
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

  function route_http_request_to_node(kbnode_id, path, req, res) {
    logger.info('route_http_request_to_node', {
      kbnode_id: kbnode_id,
      path: path,
      req_headers: req.headers
    });
    if (kbnode_type != 'hub') {
      send_500(res, 'Cannot route request from share.');
      return;
    }
    m_context.hub_manager.routeHttpRequestToNode(kbnode_id, path, req, res);
  }

  function handle_download(kbshare_id, filename, req, res) {
    logger.info('handle_download',{kbshare_id:kbshare_id,filename:filename});
    allow_cross_domain_requests(req, res);

    // don't worry too much because express takes care of this below (b/c we specify a root directory)
    if (!is_safe_path(filename)) {
      send_500(res, 'Unsafe path: ' + subdirectory);
      return;
    }
    if (kbnode_type == 'hub') {
      var urlpath0 = `${kbshare_id}/download/${filename}`;
      route_http_request_to_node(kbshare_id, urlpath0, req, res);
      return;
    }
    // so, kbnode_type = 'share'
    if (m_config.kbNodeId() != kbshare_id) {
      send_500(res, 'Incorrect kbshare id: ' + kbshare_id);
      return;
    }

    var path0 = require('path').join(kbnode_directory, filename);
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
        root: kbnode_directory
      });
    } catch (err) {
      logger.error('Caught exception from res.sendFile: ' + filename, {
        error: error.message
      });
    }
  }

  function handle_find(sha1, filename, req, res) {
    logger.info('handle_find',{sha1:sha1,filename:filename});
    allow_cross_domain_requests(req, res);

    if (kbnode_type != 'hub') {
      send_500(res, 'Cannot find. This is not a hub.');
      return;
    }

    // Note: In future we should only allow method=GET
    if ((req.method == 'GET') || (req.method == 'POST')) {
      // find the file
      m_context.hub_manager.findFile({
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
            if (m_config.topHubUrl() != m_config.listenUrl()) {
              ret['alt_hub_url'] = m_config.topHubUrl();
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
  var to_exclude = ['node_modules', '.git', '.kbucket'];
  return (to_exclude.indexOf(name) >= 0);
}