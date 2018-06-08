exports.PoliteWebSocket=PoliteWebSocket;

const WebSocket = require('ws');

function PoliteWebSocket(opts) {
  opts=opts||{
    wait_for_response:false,
    enforce_remote_wait_for_response:false
  };

  // use one of the following two methods to initialize
  this.connectToRemote=function(url,callback) {connectToRemote(url,callback);}
  this.setSocket=function(ws) {m_ws=ws; setup();};

  // operations
  this.sendMessage=function(X) {sendMessage(X);};
  this.close=function() {close();}
  this.sendErrorAndClose=function(err) {send_error_and_close_socket(err);};
  this.forwardHttpRequest=function(req,res) {forward_http_request(req,res);};

  // event handlers
  this.onMessage=function(handler) {m_on_message_handlers.push(handler);};
  this.onClose=function(handler) {m_on_close_handlers.push(handler);};

  var m_received_response_since_last_message=true;
  var m_sent_message_since_last_response=true;
  var m_queued_messages=[];
  var m_ws=null;
  var m_on_message_handlers=[];
  var m_on_close_handlers=[];
  var m_wait_to_send=true;

  function connectToRemote(url,callback) {
    m_ws = new WebSocket(url, {
      perMessageDeflate:false
    });
    m_ws.on('open',function() {
      if (callback) {
        callback(null);
        callback=null;
      }
    });
    m_ws.on('error',function(err) {
      if (callback) {
        callback('Websocket error: '+err.message);
        callback=null;
        return;
      }
    });
    setup();
  }

  function setup() {
    m_ws.on('close',function() {
      for (var i in m_on_close_handlers) {
        m_on_close_handlers[i]();
      }
    });
    m_ws.on('error',function(err) {
      console.error('Websocket error: '+err.message);
    });
    m_ws.on('unexpected-response',function(err) {
      console.error('Websocket unexpected response: '+err.message);
    });
    m_ws.on('message', (message_str) => {
      if ((opts.enforce_remote_wait_for_response)&&(!m_sent_message_since_last_response)) {
        send_error_and_close_socket('Received message before sending response to last message.');
        return;
      }
      var msg=parse_json(message_str);
      if (!msg) {
        send_error_and_close_socket('Error parsing json of message');
        return;
      }
      m_received_response_since_last_message=true;
      m_sent_message_since_last_response=false;
      call_on_message_handlers(msg);
      check_send_queued_message();
    });
  }

  function call_on_message_handlers(msg) {
    for (var i in m_on_message_handlers) {
      m_on_message_handlers[i](msg);
    }
  }

  function send_error_and_close_socket(err) {
    var errstr=err+'. Closing websocket.';
    console.error(errstr);
    if (m_ws.readyState==1) {
      // open
      actually_send_message({error:errstr});
    }
    close();
  }

  function close() {
    m_ws.close();
  }

  function sendMessage(X) {
    if ((opts.wait_for_response)&&(!m_received_response_since_last_message)) {
      m_queued_messages.push(X);
    }
    else {
      actually_send_message(X);
    }
  }
  function actually_send_message(X) {
    if (m_ws.readyState!=1) {
      // the socket is not open
      return;
    }
    var message_str=JSON.stringify(X);
    m_ws.send(message_str);
    m_received_response_since_last_message=false;
    m_sent_message_since_last_response=true;
  }
  function check_send_queued_message() {
    if ((m_received_response_since_last_message)&&(m_queued_messages.length>0)) {
      var msg=m_queued_messages[0];
      m_queued_messages=m_queued_messages.slice(1);
      actually_send_message(msg);
    }
  }
}

function parse_json(str) {
  try {
    return JSON.parse(str);
  }
  catch(err) {
    return null;
  }
}