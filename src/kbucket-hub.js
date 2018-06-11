#!/usr/bin/env node

const fs=require('fs');

//const KBNodeHub=require(__dirname+'/kbnodehub.js').KBNodeHub;
const KBNode=require(__dirname+'/kbnode.js').KBNode;

var CLP=new CLParams(process.argv);

var hub_directory=CLP.unnamedParameters[0]||'.';
hub_directory=require('path').resolve(hub_directory);
if (!fs.existsSync(hub_directory)) {
  console.error('Directory does not exist: '+hub_directory);
  process.exit(-1);
}
if (!fs.statSync(hub_directory).isDirectory()) {
  console.error('Not a directory: '+hub_directory);
  process.exit(-1);
}

var init_opts={};
if ('auto' in CLP.namedParameters) {
  init_opts.auto_use_defaults=true;
}

var X=new KBNode(hub_directory,'hub');
X.initialize(init_opts,function(err) {
  if (err) {
    console.error(err);
    process.exit(-1);
  }
});

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