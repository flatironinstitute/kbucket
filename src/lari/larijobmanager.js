exports.LariJobManager = LariJobManager;
exports.LariProcessorJob = LariProcessorJob;

const async = require('async');
const sha1 = require('node-sha1');

console.info('Checking for bundled mountainlab installation.');
let node_modules_dir=find_node_modules_dir_at(`${__dirname}/../..`);
if (!node_modules_dir) {
  console.error('Unable to find node modules dir.');
  process.exit(-1);
}
const ml_command_prefix = `${node_modules_dir}/mountainlab/bin`;
if (!require('fs').existsSync(ml_command_prefix+'/ml-run-process')) {
  console.error('File does not exist: '+ml_command_prefix+'/ml-run-process');
  process.exit(-1);  
}

function LariJobManager() {
  this.addJob = function(J) {
    if (J.jobId() in m_jobs) {
      console.warn('Cannot add job. Job with id already exists: ' + J.jobId());
      return;
    }
    m_jobs[J.jobId()] = J;
  };
  this.job = function(job_id) {
    return job(job_id);
  };
  this.removeJob = function(job_id) {
    removeJob(job_id);
  };
  this.getProcessorSpec = function(processor_name, callback) {
    let exe = 'ml-spec';
    exe = `${ml_command_prefix}/${exe}`;
    let args = [processor_name];
    execute_and_read_output(exe, args, {
      on_stdout: function() {},
      on_stderr: function() {}
    }, function(err, stdout, stderr, exit_code) {
      stdout = stdout.trim();
      if (exit_code) {
        callback('Non-zero exit code: ' + exit_code);
        return;
      }
      if (!stdout) {
        console.error(stderr);
        callback('Empty output for command: ' + exe + ' ' + args.join(' '));
        return;
      }
      let spec;
      try {
        spec = JSON.parse(stdout);
      } catch (err) {
        callback('Error parsing output of ml-spec.');
        return;
      }
      callback(null, spec);
    });
  };

  let m_jobs = {};

  /*
  function housekeeping() {
    //cleanup here
    setTimeout(housekeeping, 10000);
  }
  */
  //setTimeout(housekeeping,10000);	
  function removeJob(job_id) {
    delete m_jobs[job_id];
  }

  function job(job_id) {
    if (job_id in m_jobs) {
      return m_jobs[job_id];
    } else return null;
  }
}

function LariProcessorJob() {
  let that = this;

  this.setLariDirectory = function(directory) {
    m_lari_directory = directory;
  };
  this.setShareIndexer = function(indexer) {
    m_share_indexer = indexer;
  };
  this.jobId = function() {
    return m_job_id;
  };
  this.start = function(processor_name, inputs, outputs, parameters, opts, callback) {
    start(processor_name, inputs, outputs, parameters, opts, callback);
  };
  this.keepAlive = function() {
    m_alive_timer = new Date();
  };
  this.cancel = function(callback) {
    cancel(callback);
  };
  this.isComplete = function() {
    return m_is_complete;
  };
  this.result = function() {
    return m_result;
  };
  this.elapsedSinceKeepAlive = function() {
    return (new Date()) - m_alive_timer;
  };

  //this.outputFilesStillValid=function() {return outputFilesStillValid();};
  this.takeLatestConsoleOutput = function() {
    return takeLatestConsoleOutput();
  };

  let m_result = null;
  let m_alive_timer = new Date();
  let m_is_complete = false;
  let m_process_object = null;
  //let m_output_file_stats={};
  let m_latest_console_output = '';
  let m_job_id = make_random_id(10); //internal for now (just for naming the temporary files)
  let m_lari_directory = '';
  let m_share_indexer = null;

  function start(processor_name, inputs, outputs, parameters, opts, callback) {
    if (!m_lari_directory) {
      callback('Lari directory not set.');
      return;
    }

    let job_signature = compute_job_signature(processor_name, inputs, outputs, parameters);

    let exe = 'ml-run-process';
    if (opts.mode == 'exec') exe = 'ml-exec-process';
    else if (opts.mode == 'run') exe = 'ml-run-process';
    else if (opts.mode == 'queue') exe = 'ml-queue-process';

    exe = `${ml_command_prefix}/${exe}`;

    let args = [];
    args.push(processor_name);

    // Handle inputs
    args.push('--inputs');
    for (let key in inputs) {
      let val = inputs[key];
      if (val instanceof Array) {
        for (let jj = 0; jj < val.length; jj++) {
          let val0 = val[jj];
          val0 = input_to_string(val0, key + '_' + jj, job_signature);
          if (!val0) {
            callback(`Invalid input: ${key}[${jj}$]`);
            return;
          }
          args.push(key + ':' + val0);
        }
      } else {
        let val_str = input_to_string(val, key, job_signature);
        if (!val_str) {
          console.error(`Invalid input ${key} `, val);
          callback(`Invalid input: ${key}`);
          return;
        }
        args.push(key + ':' + val_str);
      }
    }

    // Handle parameters
    args.push('--parameters');
    for (let key in parameters) {
      let val = parameters[key];
      if (typeof(val) != 'object') {
        args.push(key + ':' + val);
      } else {
        for (let ii in val) {
          args.push(key + ':' + val[ii]);
        }
      }
    }

    let rel_outputs_dir = 'outputs';
    mkdir_if_needed(m_lari_directory + '/' + rel_outputs_dir);

    // Handle outputs
    args.push('--outputs');
    let rel_local_output_files = {};
    for (let key in outputs) {
      if (outputs[key]) {
        let rel_local_fname = rel_outputs_dir + '/' + job_signature + '_' + key;
        args.push(key + ':' + m_lari_directory + '/' + rel_local_fname);
        rel_local_output_files[key] = rel_local_fname;
      }
    }

    if ('processor_command_prefix' in opts) {
      // note that the double quotes caused a weird problem. will need to address in the future when we need command prefixes with spaces
      // args.push(`--processor_command_prefix="${opts.processor_command_prefix}"`);
      args.push(`--processor_command_prefix=${opts.processor_command_prefix}`);
    }

    if (opts.force_run) {
      args.push('--force_run');
    }

    // Start housekeeping
    setTimeout(housekeeping, 1000);

    // Start process
    m_process_object = execute_and_read_output(exe, args, {
      on_stdout: on_stdout,
      on_stderr: on_stderr
    }, function(err, stdout, stderr, exit_code) {
      if (err) {
        m_result = {
          success: false,
          error: err
        };
        m_is_complete = true;
        return;
      }
      if (exit_code != 0) {
        m_result = {
          success: false,
          error: `Exit code is non-zero (${exit_code})`
        };
        m_is_complete = true;
        return;
      }
      let output_prv_objects = {};
      let rel_local_output_file_keys = Object.keys(rel_local_output_files);
      async.eachSeries(rel_local_output_file_keys, function(key, cb) {
        let rel_local_fname = rel_local_output_files[key];
        if (!require('fs').existsSync(m_lari_directory + '/' + rel_local_fname)) {
          m_result = {
            success: false,
            error: `Missing output file ${key}`
          };
          m_is_complete = true;
          return;
        }
        console_msg('Waiting for prv object for output: ' + key);
        m_share_indexer.waitForPrvForIndexedFile(rel_local_fname, function(err, prv) {
          if (err) {
            m_result = {
              success: false,
              error: `Problem waiting for prv object of output file  ${key}`
            };
            m_is_complete = true;
            return;
          }
          output_prv_objects[key] = prv;
          cb();
        });
      }, function() {
        m_result = {
          success: true,
          outputs: output_prv_objects
        };
        m_is_complete = true;
      });

    });

    function console_msg(txt) {
      m_latest_console_output += txt + '\n';
    }

    function on_stdout(txt) {
      m_latest_console_output += txt;
    }

    function on_stderr(txt) {
      m_latest_console_output += txt;
    }
    callback(null);
  }

  function compute_job_signature(processor_name, inputs, outputs, parameters) {
    let obj = {
      processor_name: processor_name,
      inputs: inputs,
      outputs: outputs,
      parameters: parameters
    };
    return sha1(JSON.stringify(obj)).slice(0, 6);
  }

  function input_to_string(X, key, job_signature) {
    if (typeof(X) == 'string') {
      if ((X.startsWith('kbucket://')) || (X.startsWith('sha1://'))) {
        return X;
      }
      return null;
    } else if (typeof(X) == 'object') {
      if (!('original_checksum' in X)) {
        return null;
      }
      let inputs_dir = m_lari_directory + '/inputs';
      mkdir_if_needed(inputs_dir);
      let local_fname = inputs_dir + '/' + job_signature + '_' + key + '.prv';
      if (!lari_write_text_file(local_fname, JSON.stringify(X, null, 4))) {
        return null;
      }
      return local_fname;
    } else {
      return null;
    }

  }

  function takeLatestConsoleOutput() {
    let ret = m_latest_console_output;
    m_latest_console_output = '';
    return ret;
  }

  function cancel(callback) {
    if (m_is_complete) {
      if (callback) callback(null); //already complete
      return;
    }
    if (m_process_object) {
      console.info('Canceling process: ' + m_process_object.pid);
      m_process_object.stdout.pause();
      m_process_object.kill('SIGTERM');
      m_is_complete = true;
      m_result = {
        success: false,
        error: 'Process canceled'
      };
      if (callback) callback(null);
    } else {
      if (callback) callback('m_process_object is null.');
    }
  }

  function housekeeping() {
    if (m_is_complete) return;
    let timeout = 60000;
    let elapsed_since_keep_alive = that.elapsedSinceKeepAlive();
    if (elapsed_since_keep_alive > timeout) {
      console.info('Canceling process due to keep-alive timeout');
      cancel();
    } else {
      setTimeout(housekeeping, 1000);
    }
  }
  /*
  function compute_output_file_stats(outputs) {
  	let stats={};
  	for (let key in outputs) {
  		stats[key]=compute_output_file_stat(outputs[key].original_path);
  	}
  	return stats;
  }
  */
  /*
  function compute_output_file_stat(path) {
  	try {
  		let ss=require('fs').statSync(path);
  		return {
  			exists:require('fs').existsSync(path),
  			size:ss.size,
  			last_modified:(ss.mtime+'') //make it a string
  		};
  	}	
  	catch(err) {
  		return {};
  	}
  }
  */
  /*
  function outputFilesStillValid() {
  	let outputs0=(m_result||{}).outputs||{};
  	let stats0=m_output_file_stats||{};
  	let stats1=compute_output_file_stats(outputs0);
  	for (let key in stats0) {
  		let stat0=stats0[key]||{};
  		let stat1=stats1[key]||{};
  		if (!stat1.exists) {
  			return false;
  		}
  		if (stat1.size!=stat0.size) {
  			return false;
  		}
  		if (stat1.last_modified!=stat0.last_modified) {
  			return false;
  		}
  	}
  	return true;
  }
  */
}

function lari_write_text_file(fname, txt) {
  try {
    require('fs').writeFileSync(fname, txt, 'utf8');
    return true;
  } catch (e) {
    console.error('Problem writing file: ' + fname);
    return false;
  }
}

function execute_and_read_output(exe, args, opts, callback) {
  console.info('RUNNING: ' + exe + ' ' + args.join(' '));
  let P;
  try {
    P = require('child_process').spawn(exe, args);
  } catch (err) {
    console.error(err);
    callback("Problem launching: " + exe + " " + args.join(" "));
    return;
  }
  let txt_stdout = '';
  let txt_stderr = '';
  let error = '';
  P.stdout.on('data', function(chunk) {
    txt_stdout += chunk;
    if (opts.on_stdout) {
      opts.on_stdout(chunk);
    }
  });
  P.stderr.on('data', function(chunk) {
    txt_stderr += chunk;
    if (opts.on_stderr) {
      opts.on_stderr(chunk);
    }
  });
  P.on('close', function(code) {
    callback(error, txt_stdout, txt_stderr, code);
  });
  P.on('error', function() {
    error = 'Error running: ' + exe + ' ' + args.join(' ');
  });
  return P;
}

function remove_file(fname) {
  try {
    require('fs').unlinkSync(fname);
    return true;
  } catch (err) {
    return false;
  }
}

function read_text_file(fname) {
  try {
    return require('fs').readFileSync(fname, 'utf8');
  } catch (err) {
    return '';
  }
}

function read_json_file(fname) {
  try {
    return JSON.parse(read_text_file(fname));
  } catch (err) {
    return '';
  }
}

function make_random_id(len) {
  let text = '';
  let possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

  for (let i = 0; i < len; i++)
    text += possible.charAt(Math.floor(Math.random() * possible.length));

  return text;
}

function mkdir_if_needed(path) {
  try {
    require('fs').mkdirSync(path);
  } catch (err) {}
}

function find_node_modules_dir_at(path) {
  if (path.length<=1) return null;
  if (require('fs').existsSync(path+'/node_modules'))
    return path+'/node_modules';
  let parent_path=require('path').dirname(path);
  return find_node_modules_dir_at(parent_path);
}