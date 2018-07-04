#!/usr/bin/env node

const fs = require('fs');

const HemlockNode = require(__dirname + '/../hemlock/hemlocknode.js').HemlockNode;
const LariHttpServer=require(__dirname+'/larihttpserver.js').LariHttpServer;
const LariNodeApi=require(__dirname+'/larinodeapi.js').LariNodeApi;

var CLP = new CLParams(process.argv);

var node_directory = CLP.unnamedParameters[0] || '.';
node_directory = require('path').resolve(node_directory);
if (!fs.existsSync(node_directory)) {
  console.error('Directory does not exist: ' + node_directory);
  process.exit(-1);
}
if (!fs.statSync(node_directory).isDirectory()) {
  console.error('Not a directory: ' + node_directory);
  process.exit(-1);
}

var init_opts = {};
if ('auto' in CLP.namedParameters) {
  init_opts.auto_use_defaults = true;
}
init_opts.config_directory_name='.lari';
init_opts.config_file_name='larinode.json';
init_opts.node_type_label='leaf';
init_opts.network_type='lari';

var X = new HemlockNode(node_directory, 'leaf');
let context=X.context();
let API=new LariNodeApi(context);
let SS=new LariHttpServer(API);
X.setHttpServer(SS.app());
let TM=new LeafManager();
X.setLeafManager(TM);
X.initialize(init_opts, function(err) {
  if (err) {
    console.error(err);
    process.exit(-1);
  }  
});

function LeafManager() {
  this.nodeDataForParent = function() {
    return {};
  };
  this.restart=function() {
  };
}

function CLParams(argv) {
  this.unnamedParameters = [];
  this.namedParameters = {};

  var args = argv.slice(2);
  for (var i = 0; i < args.length; i++) {
    var arg0 = args[i];
    if (arg0.indexOf('--') === 0) {
      arg0 = arg0.slice(2);
      var ind = arg0.indexOf('=');
      if (ind >= 0) {
        this.namedParameters[arg0.slice(0, ind)] = arg0.slice(ind + 1);
      } else {
        this.namedParameters[arg0] = '';
        if (i + 1 < args.length) {
          var str = args[i + 1];
          if (str.indexOf('-') != 0) {
            this.namedParameters[arg0] = str;
            i++;
          }
        }
      }
    } else if (arg0.indexOf('-') === 0) {
      arg0 = arg0.slice(1);
      this.namedParameters[arg0] = '';
    } else {
      this.unnamedParameters.push(arg0);
    }
  }
}