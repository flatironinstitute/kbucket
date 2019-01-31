#!/usr/bin/env node

/*

casuploadserver enables http uploads of files and
stores them in a directory on the server, named by SHA-1 hash.
It is intended to be used in conjunction with kbucket, and so does
not provide any mechanism for downloading the files. The API is
minimalistic. The client needs to supply the size and SHA-1 hash
in advance, and then posts the file data. If a file with this 
hash already exists on the server (i.e., was previously uploaded),
then the upload is interrupted with a message that the file already
exists. Otherwise, the server computes the SHA-1 hash of the uploaded
file, verifies it matches the request, and returns whether the upload was successful.

Before running the server, you should set the following environment
variables:

PORT = the listen port
CAS_UPLOAD_DIR = the absolute path of the directory to store uploaded files
CAS_UPLOAD_TOKEN (see below)

The names of the uploaded files will be:
${CAS_UPLOAD_DIR}/a/bc/abcdefg...

API:
POST:/upload/[sha1]?signature=[signature]
  The raw data of the file is the body of the POST request

  where [signature] is the SHA-1 hash of the JSON string associated
  with the object:

  {
      path:[path of POST request],
      token:[token]
  }

  and [token] is the secret upload token from the environment variable mentioned above


  Returns the JSON of the following object
  {
    success:[boolean:success]
  }

*/

require('dotenv').config({ path: '.env' });

const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const fs=require('fs');

const MAX_FILE_SIZE=Number(process.env['CAS_UPLOAD_MAX_SIZE'])||(1024*1024*100);
const TEST_SIGNATURE='sigtest';

function CASUploadServer() {
  this.app = function() {
    return m_app;
  };

  if (!process.env.CAS_UPLOAD_DIR) {
    console.error('Environment variable not set: CAS_UPLOAD_DIR');
    process.exit(-1);
  }
  if (!process.env.CAS_UPLOAD_TOKEN) {
    console.error('Environment variable not set: CAS_UPLOAD_TOKEN');
    process.exit(-1);
  }

  let m_upload_dir=process.env.CAS_UPLOAD_DIR;

  let m_sha1_cache=new Sha1Cache(m_upload_dir);
  m_sha1_cache.onFileAdded(on_sha1_cache_file_added);
  let m_app = express();
  m_app.set('json spaces', 4); // when we respond with json, this is how it will be formatted
  m_app.use(cors());

  m_app.use(express.json());

  // API /upload/:sha1?signature=[signature]
  m_app.get('/probe', function(req, res) {
    res.json({success:true});
  });
  m_app.get('/check/:sha1', function(req, res) {
    let params = req.params;
    let query = req.query;
    if (!query.signature) {
      error_response(req,res,500,'Missing query parameter: signature')
      return;
    }
    if (params.sha1.length!=40) {
      error_response(req,res,500,'Invalid sha1 string.')
      return;  
    }
    let ok=verify_signature(`/check/${params.sha1}`,query.signature);
    if (!ok) {
      error_response(req,res,500,'Invalid signature')
      return;  
    }
    let file_size=Number(query.size);
    let X=new UploadHandler(m_sha1_cache);
    X.setSha1(params.sha1);
    X.setFileSize(file_size);
    let found=X.checkExists();
    let okay_to_upload=true;
    let message='';
    if (file_size>MAX_FILE_SIZE) {
      okay_to_upload=false;
      message=`File too large: ${file_size}>${MAX_FILE_SIZE}`;
    }
    res.json({success:true,found:found,okay_to_upload:okay_to_upload,message:message});
  })
  m_app.post('/upload/:sha1', function(req, res) {
    let params = req.params;
    let query = req.query;
    if (!query.signature) {
      error_response(req,res,500,'Missing query parameter: signature')
      return;
    }
    if (params.sha1.length!=40) {
      error_response(req,res,500,'Invalid sha1 string.')
      return;  
    }
    let ok=verify_signature(`/upload/${params.sha1}`,query.signature);
    if (!ok) {
      error_response(req,res,500,'Invalid signature')
      return;  
    }
    let file_size=req.headers['content-length'];
    if (file_size>MAX_FILE_SIZE) {
      error_response(req,res,500,`File too large: ${file_size}>${MAX_FILE_SIZE}`);
      return;
    }

    let X=new UploadHandler(m_sha1_cache);
    req.on('data', function(chunk) {
      // hmmm, will all chunks get processed before
      // the write stream emits the 'close' event?
      // not sure, but i hope so, and I think so.
      X.processChunk(chunk);
    });
    //res.on('end', function() {
      //not sure why this is not firing.
    //});
    req.on('close', function (err){
       X.cancel();
    });
    req.on('error', function(e) {
      X.cancel();
    });
    X.setSha1(params.sha1);
    X.setFileSize(file_size);
    if (X.checkExists()) {
      error_response(req,res,500,'File already exists on server.');
      return;
    }
    let timer=new Date();

    let sent=false;
    X.onFinished(function(err) {
      if (err) {
        if (!sent) {
          error_response(req,res,500,'Error: '+err)
        }
        sent=true;
        return;
      }
      let elapsed=((new Date())-timer)/1000;
      res.json({success:true,message:`Uploaded ${file_size} bytes in ${elapsed} seconds`});
    });
    X.initialize(function(err,write_stream) {
      if (err) {
        if (!sent) {
          error_response(req,res,500,'Error initializing upload: '+err)
        }
        sent=true;
        return;
      }
      write_stream.on('finished',function() {
        X.end();
      });
      write_stream.on('close',function() {
        X.end();
      });
      req.pipe(write_stream);
    });
  });

  function on_sha1_cache_file_added(rel_fname) {
  	let hints_dir=m_upload_dir+'/hints';
  	if (!fs.existsSync(hints_dir))
  		fs.mkdirSync(hints_dir);
  	let hint_fname=hints_dir+'/'+sha1_of_object({fname:rel_fname});
  	if (!write_json_file(hint_fname,{path:rel_fname})) {
  		console.warn('Problem writing hint json file: '+hint_fname);
  	}
  }

  function sha1_of_object(obj) {
    let shasum = crypto.createHash('sha1');
    shasum.update(JSON.stringify(obj));
    return shasum.digest('hex');
  }
  function verify_signature(path,signature) {
    if (TEST_SIGNATURE) {
      if (signature==TEST_SIGNATURE) return true;
    }
    if (!process.env['CAS_UPLOAD_TOKEN']) {
      console.error('CAS_UPLOAD_TOKEN not set.');
      return false;
    }
    let sig=sha1_of_object({path:path,token:process.env['CAS_UPLOAD_TOKEN']});
    return (signature==sig);
  }
}

function UploadHandler(sha1_cache) {
  let m_sha1=null;
  let m_file_size=null;
  let m_on_finished_handler=null;
  let m_finalized=false;
  let m_upload_id=null;
  let m_bytes_processed=0;

  this.setSha1=function(sha1) {
    m_sha1=sha1;
  }
  this.setFileSize=function(size) {
    m_file_size=size;
  }
  this.onFinished=function(handler) {
    m_on_finished_handler=handler;
  }
  this.checkExists=function() {
    return sha1_cache.checkExists(m_sha1);
  }
  this.initialize=function(callback) {
    initialize(callback);
  }
  this.cancel=function() {
    cancel();
  }
  this.processChunk=function(chunk) {
    process_chunk(chunk);
  }
  this.end=function() {
    end();
  }

  function initialize(callback) {
    console.info('Initializing upload: '+m_sha1+' '+m_file_size);
    sha1_cache.initializeUpload(m_sha1,m_file_size,function(err,upload_id,write_stream) {
      if (err) {
        callback(err);
        return;
      }
      m_upload_id=upload_id;
      callback(null,write_stream);
      return;
    });
  }
  function cancel() {
    if (m_finalized) return;
    sha1_cache.cancelUpload(m_upload_id);
    finalize('Upload canceled.');
  }
  function process_chunk(chunk) {
    if (m_finalized) return;
    sha1_cache.processChunk(m_upload_id,chunk,function() {
      m_bytes_processed+=chunk.byteLength;
      if (m_bytes_processed>m_file_size) {
        finalize('Too many bytes processed. Aborting.');
        return;
      }
    });
  }
  function end() {
    if (m_finalized) return;
    sha1_cache.finalizeUpload(m_upload_id,function(err) {
      if (err) {
        finalize('Error ending upload: '+err);
        return;
      }
      finalize(null);
    });  
  }
  function finalize(err) {
    if (m_finalized) return;
    m_finalized=true;
    if (err) {
      console.info('Error in upload: '+m_sha1+' '+m_file_size+' : '+err);
    }
    else {
      console.info('Uploaded: '+m_sha1+' '+m_file_size);  
    }
    m_on_finished_handler(err);
    sha1_cache.cancelUpload(m_upload_id);
  }
}

function Sha1Cache(directory) {
  let m_uploads={};
  let m_last_upload_id=100;
  let m_file_added_handlers=[];

  this.initializeUpload=function(sha1,file_size,callback) {
    initialize_upload(sha1,file_size,callback);
  }
  this.cancelUpload=function(upload_id) {
    cancel_upload(upload_id);
  }
  this.processChunk=function(upload_id,chunk,callback) {
    process_chunk(upload_id,chunk,callback);
  }
  this.finalizeUpload=function(upload_id,callback) {
    finalize_upload(upload_id,callback);
  }
  this.checkExists=function(sha1) {
    let path=get_upload_path(sha1);
    return fs.existsSync(path);
  }
  this.onFileAdded=function(callback) {
  	m_file_added_handlers.push(callback);
  }


  function initialize_upload(sha1,file_size,callback) {
    let path=get_upload_path(sha1);
    if (fs.existsSync(path)) {
      callback('File already exists on server.');
      return;
    }
    let tmp_fname=path+'.uploading.'+make_random_id(6);
    let write_stream=fs.createWriteStream(tmp_fname);
    let upload_id=m_last_upload_id+1;
    m_last_upload_id++;
    m_uploads[upload_id]={
      sha1:sha1,
      file_size:file_size,
      write_stream:write_stream,
      tmp_fname:tmp_fname,
      shasum:crypto.createHash('sha1')
    }
    write_stream.on('error',function(err) {
      console.error('Error writing file.');
      cancel_upload(upload_id);
    });
    callback(null,upload_id,write_stream);
  }
  function cancel_upload(upload_id) {
    if (!(upload_id in m_uploads)) {
      return;
    }
    let X=m_uploads[upload_id];
    delete m_uploads[upload_id];
    try {
      X.write_stream.close();
    }
    catch(err) {
    }
    if (fs.existsSync(X.tmp_fname))
      fs.unlinkSync(X.tmp_fname);
  }
  function process_chunk(upload_id,chunk,callback) {
    if (!(upload_id in m_uploads)) {
      callback('Unexpected: unable to find upload with id: '+upload_id);
      return;
    }
    let X=m_uploads[upload_id];
    X.shasum.update(chunk);
    callback(null);
  }
  function finalize_upload(upload_id,callback) {
    if (!(upload_id in m_uploads)) {
      callback('Unexpected: unable to find upload with id: '+upload_id);
      return;
    }
    let X=m_uploads[upload_id];
    delete m_uploads[upload_id];

    verify_and_move_temporary_file_to_final_location(X,function(err) {
      if (err) {
        callback(err);
        return;
      }
      callback(null);
    });
  }
  function close_file(file,callback) {
    file.close(function(err) {
      if (err) {
        callback(err.message);
        return;
      }
      callback(null);
    });
  }
  function verify_and_move_temporary_file_to_final_location(X,callback) {
    let stat;
    try {
      stat=fs.statSync(X.tmp_fname);
    }
    catch(err) {
      callback('Error in statSync: '+err.message);
      return;
    }
    if (stat.size!=X.file_size) {
      callback(`Incorrect file size: ${stat.size}<>${X.file_size}`);
      return;
    }
    let sha1_calc=X.shasum.digest('hex');
    if (sha1_calc!=X.sha1) {
      callback(`SHA-1 does not match: ${sha1_calc}<>${X.sha1}`);
      return;
    }
    let rel_fname=get_rel_upload_path(X.sha1);
    let fname=directory+'/'+rel_fname;
    try {
      fs.renameSync(X.tmp_fname,fname);
    }
    catch(err) {
      callback('Error renaming file: '+err.message);
      return;
    }
    for (i in m_file_added_handlers) {
    	m_file_added_handlers[i](rel_fname);
    }
    callback(null);
  }
  /*
  function write_chunk_to_file(file,chunk,callback) {
    fs.write(file,chunk,function(err) {
      if (err) {
        callback(error.message);
        return;
      }
      callback(null);
    });
  }
  */
  function get_upload_path(sha1) {
    return directory+'/'+get_rel_upload_path(sha1);
  }
  function get_rel_upload_path(sha1) {
  	if (!fs.existsSync(directory)) fs.mkdirSync(directory);
  	let relpath=`${sha1[0]}`;
    if (!fs.existsSync(directory+'/'+relpath)) fs.mkdirSync(directory+'/'+relpath);
    relpath=`${sha1[0]}/${sha1[1]}${sha1[2]}`;
    if (!fs.existsSync(directory+'/'+relpath)) fs.mkdirSync(directory+'/'+relpath);
    return relpath+'/'+sha1;
  }
}

function make_random_id(num_chars) {
    var text = "";
    var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    for( var i=0; i < num_chars; i++ )
        text += possible.charAt(Math.floor(Math.random() * possible.length));
    return text;
}

function error_response(req,res,code,err) {
  console.log(`Responding with error: ${code} ${err}`);
  res.status(code).send(err);
  setTimeout(function() {
    try {
      req.connection.destroy();
    }
    catch(err) {
    }
  },100);
}

async function main() {
  start_checking_for_kill_file();
  let SERVER=new CASUploadServer();
  try {
    await start_http_server(SERVER.app());
  }
  catch(err) {
    console.error(err);
    console.error('Error starting server: '+err.message);
    process.exit(-1);
  }
}
main();

function start_checking_for_kill_file() {
    let kill_fname=process.env.CAS_UPLOAD_DIR+'/casuploadserver.kill';
    if (fs.existsSync(kill_fname)) {
        fs.unlinkSync(kill_fname);
    }
    write_text_file(kill_fname+'.readme',`If you create a kill file (${kill_fname}), the casuploadserver will exit.`);
    do_check();

    function do_check() {
        if (fs.existsSync(kill_fname)) {
            console.info('Kill file exists. Exiting.');
            process.exit(-1);
        }
        setTimeout(function() {
            do_check();
        }, 3000);
    }
}

async function start_http_server(app) {
  let listen_port=process.env.PORT||25341;
  app.port=listen_port;
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
  await app.server.listen(listen_port);
  console.info(`Server is running ${app.protocol} on port ${app.port}`);
}

function write_text_file(fname,txt) {
	try {
		fs.writeFileSync(fname,txt);
		return true;
	}
	catch(err) {
		return false;
	}
}

function write_json_file(fname,obj) {
	try {
		require('fs').writeFileSync(fname,JSON.stringify(obj,null,4));
		return true;
	}
	catch(err) {
		return false;
	}
}