#!/usr/bin/env node

const fs=require('fs');
const KBucketClient=require(__dirname+'/kbucketclient.js').KBucketClient;

//const KBNodeShare=require(__dirname+'/kbnodeshare.js').KBNodeShare;
const KBNode=require(__dirname+'/kbnode.js').KBNode;

function print_usage() {
  console.info('Usage:');
  console.info('kbucket-clone [share id] [directory]');
}

const CLP=new CLParams(process.argv);

const kbshare_id=CLP.unnamedParameters[0];
var share_directory0=CLP.unnamedParameters[1];
if ((!kbshare_id)||(!share_directory0)) {
  print_usage();
  process.exit(-1);
}
const share_directory=require('path').resolve(share_directory0);
if (fs.existsSync(share_directory)) {
  console.error('Unable to clone kbucket share. Directory already exist: '+share_directory);
  process.exit(-1);
}

const max_download_size_mb=CLP['max_download_size_mb']||1;

get_node_info(kbshare_id,function(err,info) {
  if (err) {
    console.error(err);
    process.exit(-1);
  }
  if (info.kbnode_type!='share') {
    console.error(`Can only clone KBucket nodes of type 'share'.`);
    process.exit(-1);
  }
  var do_clone_opts={
    max_download_size_mb:max_download_size_mb
  };
  do_clone(info,do_clone_opts,share_directory);
});

function get_node_info(kbnode_id,callback) {
  var CC=new KBucketClient();
  CC.getNodeInfo(kbnode_id,function(err,info) {
    callback(err,info);
  });
}

function do_clone(info,opts,share_directory) {
  fs.mkdirSync(share_directory);
  var X=new KBNode(share_directory,'share');
  var init_opts={
    clone_only:true,
    info:info,
    max_download_size_mb:Number(max_download_size_mb)
  };
  console.info(`Using max_download_size_mb=${opts.max_download_size_mb}`);
  X.initialize(init_opts,function(err) {
    if (err) {
      console.error(err);
      process.exit(-1);
    }
  });
}

//todo: use this and feed into node
function find_lowest_accessible_hub_url(kbnode_id, callback) {
    var CC=new KBucketClient();
    CC.getNodeInfo(kbnode_id, function(err, info, accessible) {
      if (err) {
        callback(err);
        return;
      }
      if ((accessible) && (info.kbnode_type == 'hub')) {
        callback(null, info.listen_url);
        return;
      }
      if (!info.parent_hub_info) {
        callback('Unable to find accessible hub.');
        return;
      }
      find_lowest_accessible_hub_url(info.parent_hub_info.kbnode_id, callback);
    });
  }

/*
fs.mkdirSync(share_directory);

const max_download_size_mb=CLP['max_download_size_mb']||1;

console.info('Using maximum download size (MB): '+max_download_size_mb);

var X=new KBNode(share_directory,'share');
X.initialize(init_opts,function(err) {
	if (err) {
		console.error(err);
		process.exit(-1);
	}
});
*/

function CLParams(argv) {
  this.unnamedParameters=[];
  this.namedParameters={};

  var args=argv.slice(2);
  for (var i=0; i<args.length; i++) {
    var arg0=args[i];
    if (arg0.indexOf('--')===0) {
      arg0=arg0.slice(2);
      var ind=arg0.indexOf('=');
      if (ind>=0) {
        this.namedParameters[arg0.slice(0,ind)]=arg0.slice(ind+1);
      }
      else {
        this.namedParameters[arg0]='';
        if (i+1<args.length) {
          var str=args[i+1];
          if (str.indexOf('-')!=0) {
            this.namedParameters[arg0]=str;
            i++;  
          }
        }
      }
    }
    else if (arg0.indexOf('-')===0) {
      arg0=arg0.slice(1);
      this.namedParameters[arg0]='';
    }
    else {
      this.unnamedParameters.push(arg0);
    }
  }
};