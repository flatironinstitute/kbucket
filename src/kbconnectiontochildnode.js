exports.KBConnectionToChildNode = KBConnectionToChildNode;

const crypto = require('crypto');

function KBConnectionToChildNode(config) {
  this.setWebSocket = function(polite_web_socket) {
    setWebSocket(polite_web_socket);
  };
  this.reportErrorAndCloseSocket = function(err) {
    report_error_and_close_socket(err);
  }
  this.childNodeId = function() {
    return m_child_node_id;
  };
  this.childNodeType = function() {
    if (!m_child_info) return '';
    return m_child_info.kbnode_type;
  }
  this.childNodeInfo = function() {
    if (!m_child_info) return {};
    return JSON.parse(JSON.stringify(m_child_info));
  }
  this.onRegistered = function(handler) {
    m_on_registered_handlers.push(handler);
  }
  this.onMessage = function(handler) {
    m_on_message_handlers.push(handler);
  }
  this.sendMessage = function(msg) {
    sendMessage(msg);
  }
  this.onError = function(handler) {
    m_on_error_handlers.push(handler);
  }
  this.onClose = function(handler) {
    m_on_close_handlers.push(handler);
  }

  var m_child_node_socket = null;
  var m_child_node_id = null; //should be received on first message
  var m_child_public_key = null;
  var m_child_info = null;
  var m_on_error_handlers = [];
  var m_on_close_handlers = [];
  var m_on_registered_handlers = [];
  var m_on_message_handlers = [];

  function setWebSocket(socket) {
    m_child_node_socket = socket;
    socket.onMessage(function(msg) {
      // the child has sent us a message
      process_message_from_child_node(msg);
    });
    socket.onClose(function() {
      for (var i in m_on_close_handlers) {
        m_on_close_handlers[i]();
      }
    });
    //todo: somewhere detect the close event and remove the child node from the collection of connected child nodes
  }

  function report_error_and_close_socket(err) {
    for (var i in m_on_error_handlers) {
      m_on_error_handlers[i](err);
    }
    if (m_child_node_socket)
      m_child_node_socket.sendErrorAndClose(err);
  }

  function process_message_from_child_node(msg) {
    if (!msg.kbnode_id) {
      report_error_and_close_socket('kbnode_id not found in message');
      return;
    }

    // Set the kbnode id (should be received on first message)
    if (!m_child_node_id) {
      m_child_node_id = msg.kbnode_id;
    }

    if (!is_valid_kbnode_id(m_child_node_id)) {
      // Not a valid share key. Close the connection.
      report_error_and_close_socket('Invalid share key');
      return;
    }

    if (msg.kbnode_id != m_child_node_id) {
      // The kbnode_id was set, but this message did not match. Close the connection.
      report_error_and_close_socket('kbnode_id in message does not match previous messages.');
      return;
    }

    // If we are given the public key, remember it, and compare it to the kbnode_id
    if ((msg.public_key) && (!m_child_public_key)) {
      m_child_public_key = msg.public_key;
      var list = msg.public_key.split('\n');
      var expected_kbnode_id = list[1].slice(0, 12);
      if (expected_kbnode_id != m_child_node_id) {
        PWS.sendErrorAndClose(`Child node id does not match public key (${m_child_node_id}<>${expected_kbnode_id})`);
        return;
      }
    }

    var X = msg.message;
    if (!X) {
      // The message is invalid. Let's close the connection.
      report_error_and_close_socket('Invalid message.');
      return;
    }

    if (!verify_message_signature(X, msg.signature || '', m_child_public_key)) {
      report_error_and_close_socket('Unable to verify message using signature');
      return;
    }

    if (X.command == 'register_child_node') {
      if (!X.info) {
        report_error_and_close_socket('No info field found in message');
        return;
      }

      // Check whether client (or user of client) can attest that the data is being shared for scientific research purposes
      if (X.info.scientific_research != 'yes') {
        report_error_and_close_socket('KBucket should only be used to share data for scientific research purposes');
        return;
      }
      // Check whether client (or user of client) agreed to share the data
      if (X.info.kbnode_type != 'hub') {
        if (X.info.confirm_share != 'yes') {
          report_error_and_close_socket('Sharing of directory has not been confirmed');
          return;
        }
      }

      m_child_info = X.info;

      for (var i in m_on_registered_handlers) {
        m_on_registered_handlers[i]();
      }

    } else {
      // Handle all other messages
      for (var i in m_on_message_handlers) {
        m_on_message_handlers[i](X);
      }
    }
  }

  function sendMessage(msg) {
    if (!m_child_node_socket) return;

    m_child_node_socket.sendMessage(msg);
  }
}

function is_valid_kbnode_id(key) {
  // check if a kbnode_id is valid
  // TODO: add detail and use regexp
  return ((8 <= key.length) && (key.length <= 64));
}

function verify_message_signature(msg, hex_signature, public_key) {
  const verifier = crypto.createVerify('sha256');
  verifier.update(JSON.stringify(msg));
  verifier.end();

  var signature = Buffer.from(hex_signature, 'hex');

  const verified = verifier.verify(public_key, signature);
  return verified;
}