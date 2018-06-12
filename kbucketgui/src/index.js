var KBShareBrowser = require(__dirname + '/kbsharebrowser.js').KBShareBrowser;
var KBHubBrowser = require(__dirname + '/kbhubbrowser.js').KBHubBrowser;

$(document).ready(function() {
  var config = {
    kbucket_hub_url: 'http://localhost:3240'
  };
  var query = parse_url_params();
  if (query.share) {
  	var W = new KBShareBrowser(config);
  	$('#main_window').append(W.element());
  	W.setKBShareId(query.share);
  }
  else if (query.hub) {
  	var W = new KBHubBrowser(config);
  	$('#main_window').append(W.element());
  	W.setKBHubId(query.hub);
  }
  else {
  	$('main_window').append('Missing query parameter: share or hub');
  }
});

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