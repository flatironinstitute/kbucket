exports.KBNodeHubApi = KBNodeHubApi;

const fs = require('fs');
const async = require('async');

function KBNodeHubApi(config, manager) {
  this.handleFind = handle_find;
  this.handleDownload = handle_download;
  this.handleProxyDownload = handle_proxy_download;
  this.handleForwardToConnectedShare = handle_forward_to_connected_share;
  this.handleForwardToConnectedChildHub = handle_forward_to_connected_child_hub;
  this.handleUpload = handle_upload;

  const HUB_DIRECTORY = config.kbNodeDirectory();

  // The raw local data files will be stored in RAW_DIRECTORY
  const RAW_DIRECTORY = require('path').join(HUB_DIRECTORY, 'raw');

  // Temporary location for files being uploaded
  const UPLOADS_IN_PROGRESS_DIRECTORY = require('path').join(HUB_DIRECTORY, 'uploads_in_progress');

  // The maximum size for individual uploads
  // TODO: allow configuration
  const MAX_UPLOAD_SIZE_MB = 1024;

  /*
  Handle API /find/:sha1/filename(*)
  What it does:
  	1. Checks on local disk for the file with the specified sha1
  	2. Also checks the connected shares for the file with the specified sha1
  	3. Returns whether it was found
  	4. If it was found, returns the direct urls and the proxy url for retrieving the file
  */
  function handle_find(sha1, filename, req, res) {
    allow_cross_domain_requests(req, res);
    // Note: In future we should only allow method=GET
    if ((req.method == 'GET') || (req.method == 'POST')) {
      // find the file
      manager.findFile({
        sha1: sha1,
        filename: filename //only used for convenience in appending the url, not for finding the file
      }, function(err, resp) {
        if (err) {
          // There was an error in trying to find the file
          res.status(500).send({
            error: err
          });
        } else {
          if (resp.found) {
            // The file was found!
            res.json({
              success: true,
              found: true,
              size: resp.size,
              urls: resp.urls || undefined
            });
          } else {
            // The file was not found
            var ret={
              success: true,
              found: false,
            };
            if (config.topHubUrl()!=config.listenUrl()) {
              ret['alt_hub_url']=config.topHubUrl();
            }
            res.json(ret);
          }
        }
      });
    } else {
      // Other request methods are not allowed
      res.status(405).send('Method not allowed');
    }
  }

  /*
  Handle API /:code/download/:sha1/filename(*)
  What it does:
  	Downloads the file (if present) from the local kbucket-hub disk
  	Used internally and should not be called directly
  	First use /find/:sha1/filename(*) to get the urls
  */
  function handle_download(sha1, filename, req, res) {
    allow_cross_domain_requests(req, res);
    if ((req.method == 'GET') || (req.method == 'HEAD')) {
      console.info(`download: sha1=${sha1}`)

      // check whether it is a valid sha1
      if (!is_valid_sha1(sha1)) {
        const errstr = `Invalid sha1 for download: ${sha1}`;
        console.error(errstr);
        res.status(500).send({
          error: errstr
        });
        return;
      }

      // The file name will always be equal to the sha1 hash
      var path_to_file = RAW_DIRECTORY + '/' + sha1;
      res.sendFile(path_to_file); // stream the file back to the client
    } else {
      // Other request methods are not allowed
      res.status(405).send('Method not allowed');
    }
  }

  /*
  Handle API /:code/proxy-download/:sha1/filename(*)
  What it does:
  	Provide a proxy (pass-through) for a file located on a connected share, via websocket
  	Used internally and should not be called directly
  	First use /find/:sha1/filename(*) to get a list of urls including this proxy url
  */
  function handle_proxy_download(sha1, filename, req, res) {
    allow_cross_domain_requests(req, res);
    if ((req.method == 'GET') || (req.method == 'HEAD')) {
      console.info(`proxy-download: sha1=${sha1}`)

      // First check whether it is a valid sha1
      if (!is_valid_sha1(sha1)) {
        const errstr = `Invalid sha1 for download: ${filename}`;
        console.error(errstr);
        res.end(errstr);
        return;
      }

      // If we have it on our own disk, it is best to send it that way
      // (this is the same as the /download API call)
      var path_to_file = RAW_DIRECTORY + '/' + sha1;
      if (require('fs').existsSync(path_to_file)) {
        res.sendFile(path_to_file);
      } else {
        var opts = {
          sha1: sha1,
          filename: filename // filename is only used for constructing the urls -- not used for finding the file
        };
        // Search for the file on all the connected shares and connected child hubs
        manager.findFile(opts, function(err, resp) {
          if (err) {
            // There was an unanticipated error
            res.status(500).send({
              error: 'Error in findFileOnConnectedShares'
            });
            return;
          }
          if ((!resp.internal_finds) || (resp.internal_finds.length == 0)) {
            // Unable to find file on any of the connected shares
            res.status(500).send({
              error: 'Unable to find file on hub or on shares.'
            });
            return;
          }
          // we will use the first, but if that doesn't work for some reason we will also try the others
          async.eachSeries(resp.internal_finds, function(internal_find, cb) {
            if (internal_find.share_id) {
              var SS = manager.connectedShareManager().getConnectedShare(internal_find.share_id);
              if (!SS) {
                // We just found it... it really should still exist.
                console.warn(`Unexpected problem (1) in handle_proxy_download. share_id=${internal_find.share_id}.`);
                cb(); //try the next one
                return;
              }
              // Forward the http request to the share through the websocket in order to handle the download
              SS.processHttpRequest(`${internal_find.share_id}/download/${internal_find.path}`, req, res);
            } else if (internal_find.child_hub_id) {
              var SS = manager.connectedChildHubManager().getConnectedChildHub(internal_find.child_hub_id);
              if (!SS) {
                // We just found it... it really should still exist.
                console.warn(`Unexpected problem (2) in handle_proxy_download. child_hub_id=${internal_find.child_hub_id}.`);
                cb(); //try the next one
                return;
              }
              // Forward the http request to the share through the websocket in order to handle the download
              var url0 = `${internal_find.child_hub_id}/proxy-download/${opts.sha1}`;
              if (opts.filename)
                url0 += '/' + opts.filename;
              SS.processHttpRequest(url0, req, res);
              return;
            } else {
              console.warn(`Unexpected problem (3) in handle_proxy_download`);
              cb(); //try the next one.
              return;
            }
          }, function() {
            // did not find it
            res.status(500).send({
              error: `Not able to proxy the download even though we had ${resp.internal_finds.length} internal finds.`
            });
            return;
          });

        });
      }
    } else {
      // Other request methods are not allowed
      res.status(405).send('Method not allowed');
    }
  }

  /*
  Handle API /share/:kbnode_id/:path(*)
  What it does:
  	Forward arbitrary http/https requests through the websocket to the share computer (computer running kbucket-share)
  	Note that the kbnode_id must be known to access the computer in this way
  */
  function handle_forward_to_connected_share(kbnode_id, path, req, res) {
    allow_cross_domain_requests(req, res);
    // find the share by kbnode_id
    var SS = manager.connectedShareManager().getConnectedShare(kbnode_id);
    if (!SS) {
      var errstr = `Unable to find share with key=${kbnode_id}`;
      console.error(errstr);
      res.status(500).send({
        error: errstr
      })
      return;
    }
    // Forward the request to the share through the websocket
    SS.processHttpRequest(path, req, res);
  }

  /*
  Handle API /:code/hub/:kbnode_id/:path(*)
  What it does:
    Forward arbitrary http/https requests through the websocket to the child hub
    Note that the kbnode_id must be known to access the child hub in this way
  */
  function handle_forward_to_connected_child_hub(kbnode_id, path, req, res) {
    allow_cross_domain_requests(req, res);
    // find the share by kbnode_id
    var SS = manager.connectedChildHubManager().getConnectedChildHub(kbnode_id);
    if (!SS) {
      var errstr = `Unable to find child hub with key=${kbnode_id}`;
      console.error(errstr);
      res.status(500).send({
        error: errstr
      })
      return;
    }
    // Forward the request to the child hub through the websocket
    SS.processHttpRequest(path, req, res);
  }

  function handle_upload(req, res) {
    allow_cross_domain_requests(req, res);
    // TODO: document this
    const send_response = function(obj) {
      if (res.headersSent)
        return;
      if (!obj)
        obj = {};
      obj.success = true;
      res.status(200).json(obj);
    };
    const send_error = function(err) {
      console.error('ERROR uploading' + (res.headersSent ? ' (too late)' : '') + ':', err);
      if (res.headersSent)
        return;
      res.status(400).send({
        status: 'error',
        message: err
      });
    };

    if (MAX_UPLOAD_SIZE_MB <= 0) {
      return send_error(`Uploads not allowed (MAX_UPLOAD_SIZE_MB=${MAX_UPLOAD_SIZE_MB})`);
    }

    const query = req.query;
    if (!(query.resumableIdentifier && query.resumableTotalSize >= 0)) {
      return send_error('Missing upload parameters');
    }

    const name = sanitize((query.identity ? query.identity + '-' : '') + query.resumableIdentifier);
    const size = +query.resumableTotalSize;

    if (query.max_size_bytes && size > +query.max_size_bytes)
      return send_error('File too large');

    if (size / (1024 * 1024) > MAX_UPLOAD_SIZE_MB) {
      return send_error(`File too large for upload: ${size/(1024*1024)}>${MAX_UPLOAD_SIZE_MB}`);
    }

    const file = require('path').join(UPLOADS_IN_PROGRESS_DIRECTORY, name);
    const stat = stat_file(file);

    if (query.resumableDone) {
      if (!stat) {
        return send_error('Unable to stat file: ' + file);
      }
      /* resumable upload complete */
      if (stat.size != size)
        return send_error('File size mismatch: upload may be incomplete -- ' + stat.size + ' <> ' + size);
      const input = fs.createReadStream(file);
      input.pipe(crypto.createHash(PRV_HASH).setEncoding('hex'))
        .on('finish', function() {
          assert.equal(input.bytesRead, stat.size, 'File changed size while reading: ' + file);
          commit_file(file, query.resumableFileName, input.bytesRead, this.read(), (err, prv) => {
            if (err)
              return send_error('Error committing file: ' + err.message);
            send_response({
              prv: prv
            });
          });
        });
      return;
    }

    if (query.resumableChunkSize >= 1 && query.resumableChunkNumber >= 1) {
      /* resumable chunk upload */
      console.info(`Handling upload for ${name} (chunk ${query.resumableChunkNumber})`);
      const offset = query.resumableChunkSize * (query.resumableChunkNumber - 1);
      const output = new fs.WriteStream(file, {
        flags: fs.constants.O_WRONLY | fs.constants.O_CREAT,
        start: offset
      });
      req.on('readable', () => {
        if (output.pos > size)
          send_error('File too large on upload');
      });
      req.pipe(output).on('finish', () => {
        send_response();
      });
      req.on('error', send_error);
      req.on('aborted', send_error);
    } else {
      return send_error('Missing resumable parameters');
    }
  }

  function allow_cross_domain_requests(req, res) {
    // Allow browsers to access this server
    if (req.method == 'OPTIONS') {
      res.set('Access-Control-Allow-Origin', '*');
      res.set("Access-Control-Allow-Methods", "POST, GET, HEAD, OPTIONS");
      res.set("Access-Control-Allow-Credentials", true);
      res.set("Access-Control-Max-Age", '86400'); // 24 hours
      res.set("Access-Control-Allow-Headers", "X-Requested-With, X-HTTP-Method-Override, Content-Type, Accept, Authorization, Range");
      res.status(200).send();
      return;
    } else {
      res.header("Access-Control-Allow-Origin", "*");
      res.set("Access-Control-Allow-Headers", "X-Requested-With, X-HTTP-Method-Override, Content-Type, Accept, Authorization, Range");
    }
  }
}

function is_valid_sha1(sha1) {
  // check if this is a valid SHA-1 hash
  if (sha1.match(/\b([a-f0-9]{40})\b/))
    return true;
  return false;
}