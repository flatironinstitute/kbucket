exports.KBNodeShareIndexer = KBNodeShareIndexer;

const fs = require('fs');
const watcher = require('chokidar');
const async = require('async');

function KBNodeShareIndexer(send_message_to_parent_hub, config) {
  this.startIndexing = function(callback) {
    startIndexing(callback);
  };

  var m_share_directory = config.kbNodeDirectory();

  function startIndexing(callback) {
    index_files_in_subdirectory('', function(err) {
      if (err) {
        console.error(`Error indexing files: ${err}. Aborting.`);
        process.exit(-1);
      }
    });
    start_watching();
    start_indexing_queued_files();
  }

  var queued_files_for_indexing = {};
  var indexed_files = {};

  function start_indexing_queued_files() {
    var num_before = Object.keys(queued_files_for_indexing).length;
    index_queued_files(function() {
      setTimeout(function() {
        var num_after = Object.keys(queued_files_for_indexing).length;
        if ((num_before > 0) && (num_after == 0)) {
          console.info(`Done indexing ${Object.keys(indexed_files).length} files.`);
        }
        start_indexing_queued_files();
      }, 100);
    });
  }

  function index_queued_files(callback) {
    var keys = Object.keys(queued_files_for_indexing);
    async.eachSeries(keys, function(key, cb) {
      index_queued_file(key, function() {
        cb();
      });
    }, function() {
      callback();
    });
  }

  function index_queued_file(key, callback) {
    if (!(key in queued_files_for_indexing)) {
      callback();
      return;
    }
    var relfilepath = key;
    delete queued_files_for_indexing[key];
    if (!require('fs').existsSync(m_share_directory + '/' + relfilepath)) {
      console.info('File no longer exists: ' + relfilepath);
      send_message_to_parent_hub({
        command: 'set_file_info',
        path: relfilepath,
        prv: undefined
      });
      if (relfilepath in indexed_files)
        delete indexed_files[relfilepath];
      callback();
      return;
    }
    console.info(`Computing prv for: ${relfilepath}...`);
    compute_prv(relfilepath, function(err, prv) {
      if (err) {
        callback(err);
        return;
      }
      send_message_to_parent_hub({
        command: 'set_file_info',
        path: relfilepath,
        prv: prv
      });
      indexed_files[relfilepath] = true;
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
        queued_files_for_indexing[relpath] = true;
      }
    });
  }

  function index_files_in_subdirectory(subdirectory, callback) {
    var path0 = require('path').join(m_share_directory, subdirectory);
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
            queued_files_for_indexing[relfilepaths[i]] = true;
          }
        }
        async.eachSeries(reldirpaths, function(reldirpath, cb) {
          index_files_in_subdirectory(reldirpath, function(err) {
            if (err) {
              callback(err);
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

  function filter_file_name_for_cmd(fname) {
    fname = fname.split(' ').join('\\ ');
    fname = fname.split('$').join('\\$');
    return fname;
  }

  function compute_prv(relpath, callback) {
    var prv_obj = config.getPrvFromCache(relpath);
    if (prv_obj) {
      callback(null, prv_obj);
      return;
    }
    var cmd = `ml-prv-stat ${filter_file_name_for_cmd(m_share_directory+'/'+relpath)}`;
    run_command_and_read_stdout(cmd, function(err, txt) {
      if (err) {
        callback(err);
        return;
      }
      var obj = parse_json(txt.trim());
      if (!obj) {
        callback(`Error parsing json output in compute_prv for file: ${relpath}`);
        return;
      }
      config.savePrvToCache(relpath, obj);
      callback(null, obj);
    });
  }
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

function parse_json(str) {
  try {
    return JSON.parse(str);
  } catch (err) {
    return null;
  }
}

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