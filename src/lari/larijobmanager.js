exports.LariJobManager = LariJobManager;
exports.LariProcessorJob = LariProcessorJob;

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

  function start(processor_name, inputs, outputs, parameters, opts, callback) {
    if (!m_lari_directory) {
      callback('Lari directory not set.');
      return;
    }
    let exe = 'ml-queue-process';
    if (opts.run_mode == 'exec') exe = 'ml-exec-process';
    else if (opts.run_mode == 'run') exe = 'ml-run-process';
    else if (opts.run_mode == 'queue') exe = 'ml-queue-process';
    let args = [];
    args.push(processor_name);

    // Handle inputs
    args.push('--inputs');
    for (let key in inputs) {
      let val = inputs[key];
      if (val instanceof Array) {
        for (let jj = 0; jj < val.length; jj++) {
          let val0 = val[jj];
          val0 = input_to_string(val0, key + '_' + jj);
          if (!val0) {
            callback(`Invalid input: ${key}[${jj}$]`);
            return;
          }
          args.push(key + ':' + val0);
        }
      } else {
        val = input_to_string(val, key);
        if (!val) {
          callback(`Invalid input: ${key}`);
          return;
        }
        args.push(key + ':' + val);
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

    let tmp_dir = m_lari_directory + '/tmp';
    mkdir_if_needed(tmp_dir);

    // Handle outputs
    args.push('--outputs');
    let tmp_output_files = {};
    for (let key in outputs) {
      if (outputs[key]) {
        let tmp_fname = tmp_dir + '/lari_output_' + m_job_id + '_' + key + '.prv';
        args.push(key + ':' + tmp_fname);
        tmp_output_files[key] = tmp_fname;
      }
    }

    // Start housekeeping
    setTimeout(housekeeping, 1000);

    // Start process
    console.log('Running: ' + exe + ' ' + args.join(' '));
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
      let missing_outputs = false;
      for (let key in tmp_output_files) {
        let tmp_fname = tmp_output_files[key];
        if (require('fs').existsSync(tmp_fname)) {
          let obj = read_json_file(tmp_fname);
          output_prv_objects[key] = obj;
          remove_file(tmp_fname);
        } else {
          missing_outputs = true;
          output_prv_objects[key] = null;
        }
      }
      if (!missing_outputs) {
        m_result = {
          success: true,
          outputs: output_prv_objects
        };
        m_is_complete = true;
      } else {
        m_result = {
          success: false,
          error: 'Some outputs were missing.'
        };
        m_is_complete = true;
      }
    });

    function on_stdout(txt) {
      m_latest_console_output += txt;
    }

    function on_stderr(txt) {
      m_latest_console_output += txt;
    }
    callback(null);
  }

  function input_to_string(X, key) {
    if (typeof(X) == 'string') {
      if ((X.startsWith('kbucket://')) || (X.startsWith('sha1://'))) {
        return X;
      }
      return null;
    } else if (typeof(X) == 'object') {
      if (!('original_checksum' in X)) {
        return null;
      }
      let tmp_dir = m_lari_directory + '/tmp';
      mkdir_if_needed(tmp_dir);
      let tmp_fname = tmp_dir + '/lari_input_' + m_job_id + '_' + key + '.prv';
      if (!lari_write_text_file(tmp_fname, JSON.stringify(X, null, 4))) {
        return null;
      }
      return tmp_fname;
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
      console.log('Canceling process: ' + m_process_object.pid);
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
      console.log('Canceling process due to keep-alive timeout');
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
  console.log('RUNNING: ' + exe + ' ' + args.join(' '));
  let P;
  try {
    P = require('child_process').spawn(exe, args);
  } catch (err) {
    console.log(err);
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