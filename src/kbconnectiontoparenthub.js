exports.KBConnectionToParentHub = KBConnectionToParentHub;

const crypto = require('crypto');

const PoliteWebSocket = require(__dirname + '/politewebsocket.js').PoliteWebSocket;

function KBConnectionToParentHub(config) {
  this.setHttpOverWebSocketServer = function(X) {
    m_http_over_websocket_server = X;
  };
  this.initialize = function(parent_hub_url, callback) {
    initialize(parent_hub_url, callback);
  }
  this.sendMessage = function(msg) {
    sendMessage(msg);
  }
  this.onClose = function(handler) {
    m_on_close_handlers.push(handler);
  }

  var m_parent_hub_socket = null;
  var m_http_over_websocket_server = null;
  var m_on_close_handlers = [];

  function initialize(parent_hub_url, callback) {
    var parent_hub_ws_url = get_websocket_url_from_http_url(parent_hub_url);
    m_parent_hub_socket = new PoliteWebSocket({
      wait_for_response: true,
      enforce_remote_wait_for_response: false
    });
    m_parent_hub_socket.connectToRemote(parent_hub_ws_url, function(err) {
      if (err) {
        callback(err);
        return;
      }
      register_with_parent_hub(function(err) {
        if (err) {
          callback(err);
          return;
        }
        m_parent_hub_socket.onClose(function() {
		      console.info(`Websocket closed.`);
		      for (var i in m_on_close_handlers) {
		        m_on_close_handlers[i]();
		      }
		    });
        callback(null);
      });
    });
    m_parent_hub_socket.onMessage(function(msg) {
      process_message_from_parent_hub(msg);
    });
  }

  function register_with_parent_hub(callback) {
    var listen_url = config.listenUrl();
    var command = 'register_child_node';
    var info = {
      listen_url: `${listen_url}`,
      name: config.getConfig('name'),
      kbnode_type: config.kbNodeType(),
      scientific_research: config.getConfig('scientific_research'),
      description: config.getConfig('description'),
      owner: config.getConfig('owner'),
      owner_email: config.getConfig('owner_email')
    };
    if (config.kbNodeType() == 'share') {
      info['confirm_share'] = config.getConfig('confirm_share');
    }
    sendMessage({
      command: command,
      info: info
    });
    callback();
  }

  function process_message_from_parent_hub(msg) {
    /*
    console.log('==============================================================');
    console.log('==============================================================');
    console.log(msg);
    console.log('==============================================================');
    console.log('==============================================================');
    console.log('');
    console.log('');
    */

    if (msg.error) {
      console.error(`Error from hub: ${msg.error}`);
      return;
    }

    if (msg.message_type == 'http') {
      if (m_http_over_websocket_server) {
        m_http_over_websocket_server.processMessageFromClient(msg, sendMessage, function(err) {
          if (err) {
            console.error('http over websocket error: ' + err + '. Closing websocket.');
            m_parent_hub_socket.close();
          }
        });
      } else {
        console.error('no http over websocket server set. Closing websocket.');
        m_parent_hub_socket.close();
      }
      return;
    }
    if (msg.command == 'set_top_hub_url') {
      config.setTopHubUrl(msg.top_hub_url);
    } else if (msg.message == 'ok') {
      // just ok.
    } else {
      console.info(`Unexpected command: ${msg.command}. Closing websocket.`);
      m_parent_hub_socket.close();
      return;
    }
  }

  function sendMessage(msg) {
    msg.timestamp = (new Date()) - 0;
    msg.kbnode_id = config.kbNodeId();
    var signature = sign_message(msg, config.privateKey());
    var X = {
      message: msg,
      kbnode_id: config.kbNodeId(),
      signature: signature
    }
    if (msg.command.startsWith('register')) {
      // send the public key on the first message
      X.public_key = config.publicKey();
    }

    m_parent_hub_socket.sendMessage(X);
  }

  function sign_message(msg, private_key) {
    const signer = crypto.createSign('sha256');
    signer.update(JSON.stringify(msg));
    signer.end();

    const signature = signer.sign(private_key);
    const signature_hex = signature.toString('hex');

    return signature_hex;
  }
}

function get_websocket_url_from_http_url(url) {
  var URL = require('url').URL;
  var url_ws = new URL(url);
  if (url_ws.protocol == 'http:')
    url_ws.protocol = 'ws';
  else
    url_ws.protocol = 'wss';
  url_ws = url_ws.toString();
  return url_ws;
}