exports.start_test_nodes = start_test_nodes;
exports.stop_test_nodes = stop_test_nodes;

var spawn = require('child_process').spawn;
var children = [];

let all_child_processes = [];
process.on('exit', function() {
  stop_child_processes();
});
var cleanExit = function() {
  process.exit(-2);
};
process.on('SIGINT', cleanExit); // catch ctrl-c
process.on('SIGTERM', cleanExit); // catch kill
function stop_child_processes() {
  if (all_child_processes.length > 0) {
    console.info(`killing ${all_child_processes.length} child processes`);
    all_child_processes.forEach(function(child) {
      try {
        child.kill();
      } catch (err) {

      }
    });
  }
}


let test_nodes_info = {nodes:[]};

function start_test_nodes(callback) {
  start_node('kbucket','hub','kbucket-hub.js',`${__dirname}/test/test_nodes/test_kbhub1`);
  setTimeout(function() {
    start_node('kbucket','leaf','kbucket-host.js',`${__dirname}/test/test_nodes/test_kbshare1`);
    setTimeout(function() {
      start_node('lari','hub','lari-hub.js',`${__dirname}/test/test_nodes/test_larihub1`);
      setTimeout(function() {
        start_node('lari','leaf','lari-host.js',`${__dirname}/test/test_nodes/test_larinode1`);
        setTimeout(function() {
          if (callback) {
            callback(test_nodes_info);
          }
        }, 4000);
      }, 1000);
    }, 1000);
  }, 1000);

  function start_node(network_type,node_type,command,dirpath) {
  	let cmd=`${__dirname}/src/${network_type}/${command} ${dirpath} --auto`;
    console.info(`Running ${cmd}`);
    let args = cmd.split(' ');
    let P = require('child_process').spawn(args[0], args.slice(1), {
      shell: false,
      stdio: ['pipe', process.stdout, process.stderr]
    });
    all_child_processes.push(P);
    test_nodes_info.nodes.push({
    	network_type:network_type,
    	node_type:node_type,
    	command:command,
    	dirpath:dirpath
    });
  }
}

function stop_test_nodes() {
  stop_child_processes();
}