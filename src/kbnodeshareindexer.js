exports.KBNodeShareIndexer = KBNodeShareIndexer;
exports.computePrvObject = computePrvObject;
exports.computePrvDirectoryObject = computePrvDirectoryObject;

const fs = require('fs');
const watcher = require('chokidar');
const async = require('async');
const sha1 = require('node-sha1');

const SteadyFileIterator = require(__dirname + '/steadyfileiterator.js').SteadyFileIterator;

function KBNodeShareIndexer(config) {
  this.startIndexing = function() {
    startIndexing();
  };
  this.restartIndexing = function() {
    m_indexed_files = {};
    m_queued_files = {};
    m_file_iterator.restart();
  };
  this.getPrvForIndexedFile = function(relpath) {
    if (!(relpath in m_indexed_files)) {
      return null;
    }
    return m_indexed_files[relpath].prv;
  };
  this.nodeDataForParent=function() {
    var files_by_sha1={};
    for (var relpath in m_indexed_files) {
      const file0=m_indexed_files[relpath];
      const sha1=file0.prv.original_checksum;
      files_by_sha1[sha1]={
        path:relpath,
        size:file0.prv.original_size
      };
    }
    return {
      files_by_sha1:files_by_sha1
    };
  };

  var m_queued_files = {};
  var m_indexed_files = {};
  let m_indexed_something = false;

  var m_file_iterator = new SteadyFileIterator(config.kbNodeDirectory());
  m_file_iterator.onUpdateFile(function(relpath, stat0) { //note: stat is not really used
    update_file(relpath, stat0);
  });
  m_file_iterator.onRemoveFile(function(relpath) {
    remove_file(relpath);
  });

  function update_file(relpath,stat0) {
    m_queued_files[relpath]=true;
  }

  function remove_file(relpath) {
    if (relpath in m_queued_files) {
      delete m_queued_files[relpath];
    }
    if (relpath in m_indexed_files) {
      delete m_indexed_files[relpath];
    }
  }

  function startIndexing() {
    m_file_iterator.start();
  }

  handle_queued_files();
  function handle_queued_files() {
    do_handle_queued_files(function(err) {
      if (err) {
        console.warn('Problem handling queued files: '+err);
      }
      setTimeout(function() {
        handle_queued_files();
        report_changes();
      }, 100);
    });
  }

  let s_last_report={};
  function report_changes() {
    var report={
      num_queued_files:Object.keys(m_queued_files).length,
      num_indexed_files:Object.keys(m_indexed_files).length
    };
    if (JSON.stringify(report)!=JSON.stringify(s_last_report)) {
      if ((report.num_queued_files===0)&&(m_indexed_something)) {
        console.info(`Indexed ${report.num_indexed_files} files.`);
      }
      s_last_report=report;
    }
  }

  function do_handle_queued_files(callback) {
    var relpaths = Object.keys(m_queued_files);
    async.eachSeries(relpaths, function(relpath, cb) {
      handle_queued_file(relpath, function(err) {
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

  function handle_queued_file(relpath, callback) {
    if (!(relpath in m_queued_files)) {
      callback();
      return;
    }

    delete m_queued_files[relpath];

    const fullpath=require('path').join(config.kbNodeDirectory(),relpath);
    if (!exists_sync(fullpath)) {
      if (relpath in m_indexed_files) {
        delete m_indexed_files[relpath];
        callback(null);
        return;
      }
    }

    compute_prv(relpath,function(err,prv) {
      if (err) {
        callback(err);
        return;
      }
      m_indexed_something=true;
      m_indexed_files[relpath]={
        prv:prv
      };
      callback();
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
    const fullpath=require('path').join(config.kbNodeDirectory(),relpath);
    console.info(`Computing prv for: ${relpath}`);
    computePrvObject(fullpath, function(err, obj) {
      if (err) {
        callback(err);
        return;
      }
      config.savePrvToCache(relpath, obj);
      callback(null, obj);
    });
  }
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
      if (!include_file_name(item)) {
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
          });
        } else if (stat0.isDirectory()) {
          computePrvDirectoryObject(path1, function(err1, prvdir_obj1) {
            if (err1) {
              callback(err1);
              return;
            }
            ret.dirs[item] = prvdir_obj1;
            cb();
          });
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

function include_file_name(name) {
  if (name.startsWith('.')) return false;
  if (name == 'node_modules') return false;
  return true;
}

function exists_sync(path) {
  try {
    return require('fs').existsSync(path);
  }
  catch(err) {
    return false;
  }
}