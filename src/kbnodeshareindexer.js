exports.KBNodeShareIndexer = KBNodeShareIndexer;
exports.computePrvObject = computePrvObject;
exports.computePrvDirectoryObject = computePrvDirectoryObject;

const fs = require('fs');
const watcher = require('chokidar');
const async = require('async');
const sha1 = require('node-sha1');

function KBNodeShareIndexer(send_message_to_parent_hub, config) {
  this.startIndexing = function() {
    startIndexing();
  };
  this.restartIndexing = function() {
    restartIndexing();
  };
  this.getPrvForIndexedFile = function(relfilepath) {
    if (!(relfilepath in m_indexed_files)) {
      return null;
    }
    return m_indexed_files[relfilepath].prv;
  }

  var m_share_directory = config.kbNodeDirectory();
  var m_is_indexing_queued_files = false;
  var m_queued_files_for_indexing = {};
  var m_indexed_files = {};

  function startIndexing() {
    if (m_is_indexing_queued_files) return;
    start_indexing_queued_files();
  }

  function restartIndexing() {
    index_files_in_subdirectory('',function(err) {
      if (err) {
        console.error(`Error indexing files: ${err}. Aborting.`);
        process.exit(-1);
      }  
      startIndexing();
    });
  }

  

  restartIndexing();
  start_watching();

  function start_indexing_queued_files() {
    m_is_indexing_queued_files = true;
    var num_before = Object.keys(m_queued_files_for_indexing).length;
    index_queued_files(function(err) {
      if (err) {
        console.error('Error indexing queued files: ' + err);
        m_is_indexing_queued_files = false;
        return;
      }
      setTimeout(function() {
        var num_after = Object.keys(m_queued_files_for_indexing).length;
        if ((num_before > 0) && (num_after == 0)) {
          console.info(`Done indexing ${Object.keys(m_indexed_files).length} files.`);
        }
        start_indexing_queued_files();
      }, 1);
    });
  }

  function index_queued_files(callback) {
    var keys = Object.keys(m_queued_files_for_indexing);
    async.eachSeries(keys, function(key, cb) {
      index_queued_file(key, function(err) {
        if (err) {
          callback(err);
          return;
        }
        cb();
      });
    }, function() {
      callback();
    });
  }

  function index_queued_file(key, callback) {
    if (!(key in m_queued_files_for_indexing)) {
      callback();
      return;
    }
    var relfilepath = key;
    delete m_queued_files_for_indexing[key];
    if (!require('fs').existsSync(m_share_directory + '/' + relfilepath)) {
      console.info('File no longer exists: ' + relfilepath);
      if (!send_message_to_parent_hub({
          command: 'set_file_info',
          path: relfilepath,
          prv: undefined
        })) {
        callback('Unable to set file info for removed file: ' + relfilepath);
        return;
      }
      if (relfilepath in m_indexed_files)
        delete m_indexed_files[relfilepath];
      callback();
      return;
    }
    console.info(`Computing prv for: ${relfilepath}...`);
    compute_prv(relfilepath, function(err, prv) {
      if (err) {
        callback(err);
        return;
      }
      if (!send_message_to_parent_hub({
          command: 'set_file_info',
          path: relfilepath,
          prv: prv
        })) {
        callback('Unable to set file info for: ' + relfilepath);
      }
      m_indexed_files[relfilepath] = {prv:prv};
      callback();
    });
  }

  function start_watching() {
    watcher.watch(m_share_directory, {
      ignoreInitial: true
    }).on('all', function(evt, path) {
      if (!path.startsWith(m_share_directory + '/')) {
        console.warn('Watched file does not start with expected directory', path, m_share_directory);
        return;
      }
      var relpath = path.slice((m_share_directory + '/').length);
      if (relpath.startsWith('.kbucket')) {
        return;
      }
      if (is_indexable(relpath)) {
        m_queued_files_for_indexing[relpath] = true;
      }
    });
  }

  function index_files_in_subdirectory(subdirectory, callback) {
    var path0 = require('path').join(m_share_directory, subdirectory);
    if (!fs.existsSync(path0)) {
      callback('Directory does not exist: '+path0);
      return;
    }
    fs.readdir(path0, function(err, list) {
      if (err) {
        callback(err.message);
        return;
      }
      var relfilepaths = [],
        reldirpaths = [];
      async.eachSeries(list, function(item, cb) {
        if ((item == '.') || (item == '..') || (item == '.kbucket')) {
          cb();
          return;
        }
        fs.stat(require('path').join(path0, item), function(err0, stat0) {
          if (err0) {
            callback(`Error in stat of file ${item}: ${err0.message}`);
            return;
          }
          if (stat0.isFile()) {
            relfilepaths.push(require('path').join(subdirectory, item));
          } else if (stat0.isDirectory()) {
            if (!is_excluded_directory_name(item)) {
              reldirpaths.push(require('path').join(subdirectory, item));
            }
          }
          cb();
        });
      }, function() {
        for (var i in relfilepaths) {
          if (is_indexable(relfilepaths[i])) {
            m_queued_files_for_indexing[relfilepaths[i]] = true;
          }
        }
        async.eachSeries(reldirpaths, function(reldirpath, cb) {
          index_files_in_subdirectory(reldirpath, function(err) {
            if (err) {
              console.warn(err);
              cb();
              return;
            }
            cb();
          });
        }, function() {
          callback(null);
        });
      });
    });
  }

  // used previously
  /*
  function filter_file_name_for_cmd(fname) {
    fname = fname.split(' ').join('\\ ');
    fname = fname.split('$').join('\\$');
    return fname;
  }
  */

  function compute_prv(relpath, callback) {
    var prv_obj = config.getPrvFromCache(relpath);
    if (prv_obj) {
      callback(null, prv_obj);
      return;
    }
    computePrvObject(m_share_directory + '/' + relpath, function(err, obj) {
      if (err) {
        callback(err);
        return;
      }
      config.savePrvToCache(relpath, obj);
      callback(null, obj);
    });
  }

  /*
  function parse_json(str) {
    try {
      return JSON.parse(str);
    } catch (err) {
      return null;
    }
  }
  */

  /*
  function run_command_and_read_stdout(cmd, callback) {
    var P;
    try {
      P = require('child_process').spawn(cmd, {
        shell: true
      });
    } catch (err) {
      callback(`Problem launching ${cmd}: ${err.message}`);
      return;
    }
    var txt = '';
    P.stdout.on('data', function(chunk) {
      txt += chunk.toString();
    });
    P.on('close', function(code) {
      callback(null, txt);
    });
    P.on('error', function(err) {
      callback(`Problem running ${cmd}: ${err.message}`);
    })
  }
  */

}

function stat_file(fname) {
  try {
    return require('fs').statSync(fname);
  } catch (err) {
    return null;
  }
}

function computePrvDirectoryObject(path0, callback) {
  var ret = {
    files: {},
    dirs: {}
  };
  fs.readdir(path0, function(err, list) {
    if (err) {
      callback(err.message);
      return;
    }
    async.eachSeries(list, function(item, cb) {
      if ((item == '.') || (item == '..') || (item == '.kbucket')) {
        cb();
        return;
      }
      var path1 = require('path').join(path0, item);
      fs.stat(path1, function(err0, stat0) {
        if (err0) {
          callback(`Error in stat of file ${path1}: ${err0.message}`);
          return;
        }
        if (stat0.isFile()) {
          console.info(`Computing prv for ${path1} ...`);
          computePrvObject(path1, function(err1, prv_obj1) {
            if (err1) {
              callback(`Error for file ${path1}: ${err1}`);
              return;
            }
            ret.files[item] = prv_obj1;
            cb();
          })
        } else if (stat0.isDirectory()) {
          if (is_excluded_directory_name(item)) {
            cb();
            return;
          }
          computePrvDirectoryObject(path1, function(err1, prvdir_obj1) {
            if (err1) {
              callback(err1);
              return;
            }
            ret.dirs[item] = prvdir_obj1;
            cb();
          })
        } else {
          callback('Error in stat object for file: ' + path1);
          return;
        }
      });
    }, function() {
      callback(null, ret);
    });
  });
}

function computePrvObject(fname, callback) {
  var stat0 = stat_file(fname);
  if (!stat0) {
    callback('Cannot stat file: ' + fname);
    return;
  }
  compute_file_sha1(fname, {}, function(err, sha1) {
    if (err) {
      callback(err);
      return;
    }
    compute_file_sha1(fname, {
      start_byte: 0,
      end_byte: 999
    }, function(err, sha1_head) {
      if (err) {
        callback(err);
        return;
      }
      var fcs = 'head1000-' + sha1_head;
      var obj = {
        original_checksum: sha1,
        original_size: stat0.size,
        original_fcs: fcs,
        original_path: require('path').resolve(fname),
        prv_version: '0.11'
      };
      callback('', obj);
    });
  });
}

function compute_file_sha1(path, opts, callback) {
  var opts2 = {};
  if (opts.start_byte) opts2.start = opts.start_byte;
  if (opts.end_byte) opts2.end = opts.end_byte;
  var stream = require('fs').createReadStream(path, opts2);
  sha1(stream, function(err, hash) {
    if (err) {
      callback('Error: ' + err);
      return;
    }
    callback(null, hash);
  });
}

function is_indexable(relpath) {
  var list = relpath.split('/');
  for (var i = 0; i < list.length - 1; i++) {
    if (is_excluded_directory_name(list[i]))
      return false;
  }
  return true;
}

function is_excluded_directory_name(name) {
  var to_exclude = ['node_modules', '.git', '.kbucket'];
  return (to_exclude.indexOf(name) >= 0);
}