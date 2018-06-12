var KBShareBrowser = require(__dirname + '/kbsharebrowser.js').KBShareBrowser;
var KBHubBrowser = require(__dirname + '/kbhubbrowser.js').KBHubBrowser;

var TOP_KBUCKET_HUB_URL = 'https://kbucket.flatironinstitute.org';

$(document).ready(function() {
  var query = parse_url_params();
  var kbnode_id = query.share || query.hub;
  if (!kbnode_id) {
    $('#main_window').append('Missing query parameter: share or hub');
    return;
  }

  find_lowest_accessible_hub_url(kbnode_id, function(err, hub_url) {
    if (err) {
      $('#main_window').append(err);
      return;
    }
    if (query.share) {
      var W = new KBShareBrowser();
      W.setKBHubUrl(hub_url);
      $('#main_window').append(W.element());
      W.setKBShareId(query.share);
    } else if (query.hub) {
      var W = new KBHubBrowser();
      W.setKBHubUrl(hub_url);
      $('#main_window').append(W.element());
      W.setKBHubId(query.hub);
    }
  })
});

function find_lowest_accessible_hub_url(kbnode_id, callback) {
  get_node_info(kbnode_id, function(err, info, accessible) {
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

function get_node_info(kbnode_id, callback) {
  var url = `${TOP_KBUCKET_HUB_URL}/${kbnode_id}/api/nodeinfo`;
  get_json(url, function(err, resp) {
    if (err) {
      callback('Error getting node info: ' + err);
      return;
    }
    if (!resp.info) {
      callback('Unexpected: response missing info field.');
      return;
    }
    //check accessible
    get_json(`${resp.info.listen_url}/${kbnode_id}/api/nodeinfo`, function(err, resp2) {
      var accessible = false;
      if ((!err) && (resp2.info) && (resp2.info.kbnode_id == kbnode_id))
        accessible = true;
      callback(null, resp.info, accessible);
    });
  });
}

function get_json(url, callback) {
  $.ajax({
    url: url,
    dataType: 'json',
    success: function(data) {
      callback(null,data);
    },
    error: function(err) {
      callback('Error getting: '+url);
    }
  });
}

function parse_url_params() {
  var match;
  var pl = /\+/g; // Regex for replacing addition symbol with a space
  var search = /([^&=]+)=?([^&]*)/g;
  var decode = function(s) {
    return decodeURIComponent(s.replace(pl, " "));
  };
  var query = window.location.search.substring(1);
  var url_params = {};
  while (match = search.exec(query))
    url_params[decode(match[1])] = decode(match[2]);
  return url_params;
}