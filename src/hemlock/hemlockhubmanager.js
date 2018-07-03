exports.HemlockHubManager = HemlockHubManager;

const async = require('async');
//const fs = require('fs');
//const request = require('request');
const logger = require(__dirname + '/logger.js').logger();

const HttpOverWebSocketClient = require(__dirname + '/httpoverwebsocket.js').HttpOverWebSocketClient;

var LIMITS = {
  max_connected_terminals: 1e3,
  max_connected_child_hubs: 10
};

function HemlockHubManager(config) {
  this.connectedTerminalManager = function() {
    return m_connected_terminal_manager;
  };
  this.connectedChildHubManager = function() {
    return m_connected_child_hub_manager;
  };
  this.setTopHubUrl = function(url) {
    m_connected_child_hub_manager.setTopHubUrl(url);
  };
  this.nodeDataForParent = function() {
    return nodeDataForParent();
  };
  this.routeHttpRequestToNode = function(hemlock_node_id, path, req, res) {
    routeHttpRequestToNode(hemlock_node_id, path, req, res);
  };

  // The terminal manager (see HemlockConnectedTerminalManager)
  var m_connected_terminal_manager = new HemlockConnectedTerminalManager(config);
  // The connected child hub manager (see HemlockConnectedChildHubManager)
  var m_connected_child_hub_manager = new HemlockConnectedChildHubManager(config);

  function nodeDataForParent() {
    var data = {
      hemlock_node_id: config.hemlockNodeId(),
      descendant_nodes: {}
    };
    var hemlock_terminal_ids = m_connected_terminal_manager.connectedTerminalIds();
    for (let ii in hemlock_terminal_ids) {
      var hemlock_terminal_id = hemlock_terminal_ids[ii];
      var SS = m_connected_terminal_manager.getConnectedTerminal(hemlock_terminal_id);
      data.descendant_nodes[hemlock_terminal_id] = {
        hemlock_node_id: hemlock_terminal_id,
        parent_hemlock_node_id: config.hemlockNodeId(),
        listen_url: SS.listenUrl(),
        hemlock_node_type: 'terminal'
      };
    }
    var hemlock_hub_ids = m_connected_child_hub_manager.connectedChildHubIds();
    for (let ii in hemlock_hub_ids) {
      var hemlock_hub_id = hemlock_hub_ids[ii];
      var HH = m_connected_child_hub_manager.getConnectedChildHub(hemlock_hub_id);
      data.descendant_nodes[hemlock_hub_id] = {
        hemlock_node_id: hemlock_hub_id,
        parent_hemlock_node_id: config.hemlockNodeId(),
        listen_url: HH.listenUrl(),
        hemlock_node_type: 'hub'
      };
      var data0 = HH.childNodeData();
      data0.descendant_nodes = data0.descendant_nodes || {};
      for (let id in data0.descendant_nodes) {
        data.descendant_nodes[id] = data0.descendant_nodes[id];
      }
    }
    return data;
  }

  function routeHttpRequestToNode(hemlock_node_id, path, req, res) {
    var SS = m_connected_terminal_manager.getConnectedTerminal(hemlock_node_id);
    if (SS) {
      SS.processHttpRequest(path, req, res);
      return;
    }
    const HH0 = m_connected_child_hub_manager.getConnectedChildHub(hemlock_node_id);
    if (HH0) {
      HH0.processHttpRequest(path, req, res);
      return;
    }
    var ids = m_connected_child_hub_manager.connectedChildHubIds();
    for (let ii in ids) {
      var id = ids[ii];
      const HH_child = m_connected_child_hub_manager.getConnectedChildHub(id);
      var data0 = HH_child.childNodeData();
      var dn0 = data0.descendant_nodes || {};
      if (hemlock_node_id in dn0) {
        HH_child.processHttpRequest(path, req, res);
        return;
      }
    }
    res.status(500).send({
      error: 'Unable to locate node with id: ' + hemlock_node_id
    });
  }
}

function HemlockConnectedTerminalManager(config) {
  // Manage a collection of HemlockConnectedTerminal objects, each representing a connected terminal
  this.addConnectedTerminal = function(connection_to_child_node, callback) {
    addConnectedTerminal(connection_to_child_node, callback);
  };
  this.connectedTerminalIds = function() {
    return Object.keys(m_connected_terminals);
  };
  this.getConnectedTerminal = function(hemlock_node_id) {
    return m_connected_terminals[hemlock_node_id] || null;
  };

  var m_connected_terminals = {};

  function addConnectedTerminal(connection_to_child_node, callback) {
    // Add a new connected terminal

    var num_connected_terminals = Object.keys(m_connected_terminals).length;
    if (num_connected_terminals >= LIMITS.max_connected_terminals) {
      callback('Exceeded maximum number of child terminal connections.');
      return;
    }

    var hemlock_node_id = connection_to_child_node.childNodeId();

    if (hemlock_node_id in m_connected_terminals) {
      callback(`A connected terminal with id=${hemlock_node_id} already exists.`);
      return;
    }

    connection_to_child_node.onClose(function() {
      remove_connected_terminal(hemlock_node_id);
    });

    // create a new HemlockConnectedTerminal object, and pass in the connection object
    m_connected_terminals[hemlock_node_id] = new HemlockConnectedTerminal(connection_to_child_node, config);
    callback(null);
  }

  function remove_connected_terminal(hemlock_node_id) {
    // remove the terminal from the manager
    if (!(hemlock_node_id in m_connected_terminals)) {
      // we don't have it anyway
      return;
    }
    // actually remove it
    var logmsg = `Removing child terminal: ${hemlock_node_id}`;
    logger.info(logmsg);
    console.info(logmsg);
    delete m_connected_terminals[hemlock_node_id];
  }
}

function HemlockConnectedTerminal(connection_to_child_node, config) {
  // Encapsulate a single terminal (child node)
  this.processHttpRequest = function(path, req, res) {
    processHttpRequest(path, req, res);
  };
  this.name = function() {
    var data = connection_to_child_node.childNodeRegistrationInfo();
    return data.name;
  };
  this.listenUrl = function() {
    var data = connection_to_child_node.childNodeRegistrationInfo();
    return data.listen_url;
  };
  this.connectionToChildNode=function() {
    return connection_to_child_node;
  }
  this.childNodeData = function() {
    return connection_to_child_node.childNodeData();
  };

  connection_to_child_node.onMessage(function(msg) {
    process_message_from_connected_terminal(msg, function(err, response) {
      if (err) {
        connection_to_child_node.reportErrorAndCloseSocket(err);
        return;
      }
      if (!response) {
        response = {
          message: 'ok'
        };
      }
      connection_to_child_node.sendMessage(response);
    });
  });

  // todo: move this http client to the connection_to_child_node and handle all http stuff there
  var m_http_over_websocket_client = new HttpOverWebSocketClient(send_message_to_terminal);
  m_http_over_websocket_client.onByteCount(function(num_bytes_in, num_bytes_out) {
    config.incrementMetric('http_bytes_in_from_child_terminal', num_bytes_in);
    config.incrementMetric('http_bytes_out_to_child_terminal', num_bytes_out);
  });

  function send_message_to_terminal(msg) {
    connection_to_child_node.sendMessage(msg);
  }

  ////////////////////////////////////////////////////////////////////////

  function process_message_from_connected_terminal(msg, callback) {
    // We got a message msg from the terminal computer

    // todo: move this http client to the connection_to_child_node and handle all http stuff there
    if (msg.message_type == 'http') {
      m_http_over_websocket_client.processMessageFromServer(msg, function(err) {
        if (err) {
          callback('Error in http over websocket: ' + err);
          return;
        }
        callback(null);
      });
      return;
    }

    {
      // Unrecognized command
      callback(`Unrecognized command: ${msg.command}`);
    }
  }

  function processHttpRequest(path, req, res) {
    // Forward a http request through the websocket to the terminal computer

    m_http_over_websocket_client.handleRequest(path, req, res);
  }
}

function HemlockConnectedChildHubManager(config) {
  // Manage a collection of HemlockConnectedChildHub objects, each representing a connected child hub
  this.addConnectedChildHub = function(connection_to_child_node, callback) {
    addConnectedChildHub(connection_to_child_node, callback);
  };
  this.connectedChildHubIds = function() {
    return Object.keys(m_connected_child_hubs);
  };
  this.getConnectedChildHub = function(hemlock_node_id) {
    return m_connected_child_hubs[hemlock_node_id] || null;
  };

  var m_connected_child_hubs = {};

  config.onTopHubUrlChanged(function() {
    send_message_to_all_child_hubs({
      command: 'set_top_hub_url',
      top_hub_url: config.topHubUrl()
    });
  });

  function send_message_to_all_child_hubs(msg) {
    for (let id in m_connected_child_hubs) {
      m_connected_child_hubs[id].sendMessage(msg);
    }
  }

  function addConnectedChildHub(connection_to_child_node, callback) {
    // Add a new connected terminal

    var num_connected_child_hubs = Object.keys(m_connected_child_hubs).length;
    if (num_connected_child_hubs >= LIMITS.max_connected_child_hubs) {
      callback('Exceeded maximum number of child hub connections.');
      return;
    }

    var hemlock_node_id = connection_to_child_node.childNodeId();

    if (hemlock_node_id in m_connected_child_hubs) {
      callback(`A child hub with id=${hemlock_node_id} already exists.`);
      return;
    }

    connection_to_child_node.onClose(function() {
      remove_connected_child_hub(hemlock_node_id);
    });

    // create a new HemlockConnectedChildHub object, and pass in the connection object
    m_connected_child_hubs[hemlock_node_id] = new HemlockConnectedChildHub(connection_to_child_node, config);

    m_connected_child_hubs[hemlock_node_id].sendMessage({
      command: 'set_top_hub_url',
      top_hub_url: config.topHubUrl()
    });

    callback(null);
  }

  function remove_connected_child_hub(hemlock_node_id) {
    // remove the terminal from the manager
    if (!(hemlock_node_id in m_connected_child_hubs)) {
      // we don't have it anyway
      return;
    }
    // actually remove it
    var logmsg = `Removing child hub: ${hemlock_node_id}`;
    logger.info(logmsg);
    console.info(logmsg);
    delete m_connected_child_hubs[hemlock_node_id];
  }
}

function HemlockConnectedChildHub(connection_to_child_node, config) {
  // Encapsulate a single child hub
  this.processHttpRequest = function(path, req, res) {
    processHttpRequest(path, req, res);
  };
  this.sendMessage = function(msg) {
    connection_to_child_node.sendMessage(msg);
  };
  this.childNodeData = function() {
    return connection_to_child_node.childNodeData();
  };
  this.name = function() {
    var data = connection_to_child_node.childNodeRegistrationInfo();
    return data.name;
  };
  this.listenUrl = function() {
    var data = connection_to_child_node.childNodeRegistrationInfo();
    return data.listen_url;
  };
  this.httpOverWebSocketClient=function() {
    return m_http_over_websocket_client;
  };

  connection_to_child_node.onMessage(function(msg) {
    process_message_from_connected_child_hub(msg, function(err, response) {
      if (err) {
        connection_to_child_node.reportErrorAndCloseSocket(err);
        return;
      }
      if (!response) {
        response = {
          message: 'ok'
        };
      }
      connection_to_child_node.sendMessage(response);
    });
  });

  //var m_response_handlers = {}; // handlers corresponding to requests we have sent to the child hub

  // todo: move this http client to the connection_to_child_node and handle all http stuff there
  var m_http_over_websocket_client = new HttpOverWebSocketClient(send_message_to_child_hub);
  m_http_over_websocket_client.onByteCount(function(num_bytes_in, num_bytes_out) {
    config.incrementMetric('http_bytes_in_from_child_hub', num_bytes_in);
    config.incrementMetric('http_bytes_out_to_child_hub', num_bytes_out);
  });

  function send_message_to_child_hub(msg) {
    connection_to_child_node.sendMessage(msg);
  }

  ////////////////////////////////////////////////////////////////////////

  function process_message_from_connected_child_hub(msg, callback) {
    // We got a message msg from the terminal computer

    // todo: move this http client to the connection_to_child_node and handle all http stuff there
    if (msg.message_type == 'http') {
      m_http_over_websocket_client.processMessageFromServer(msg, function(err) {
        if (err) {
          callback('Error in http over websocket: ' + err);
          return;
        }
        callback(null);
      });
      return;
    }
  }

  function processHttpRequest(path, req, res) {
    // Forward a http request through the websocket to the terminal computer

    m_http_over_websocket_client.handleRequest(path, req, res);
  }
}

function is_valid_sha1(sha1) {
  // check if this is a valid SHA-1 hash
  if (sha1.match(/\b([a-f0-9]{40})\b/))
    return true;
  return false;
}

