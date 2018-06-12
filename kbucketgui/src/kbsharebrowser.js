exports.KBShareBrowser = KBShareBrowser;

var FileBrowserWidget = require(__dirname + '/filebrowserwidget.js').FileBrowserWidget;
var KBNodeInfoWidget = require(__dirname+'/kbnodeinfowidget.js').KBNodeInfoWidget;

function KBShareBrowser(config) {
  this.element = function() {
    return m_element;
  };
  this.setKBShareId = function(id) {
    setKBShareId(id);
  };

  var m_kbshare_id = '';
  var m_left_panel_width = 600;

  var m_element = $(`
		<span>
			<div class="ml-vlayout">
				<div class="ml-vlayout-item" style="flex:20px 0 0">
					<span id=top_bar style="padding-left:20px">

					</span>
				</div>
				<div class="ml-vlayout-item" style="flex:1">
					<div class="ml-hlayout">
						<div class="ml-hlayout-item" style="flex:${m_left_panel_width}px 0 0">
							<div class="ml-item-content" id="left_panel" style="margin:10px; background:">

							</div>
						</div>
						<div class="ml-hlayout-item" style="flex:1">
							<div class="ml-item-content" id="file_browser" style="margin:10px; background:">
							</div>
						</div>
					</div>
				</div
			</div>
		</span>
	`);

  var m_file_browser_widget = new FileBrowserWidget();
  var m_info_widget = new KBNodeInfoWidget(config);
  m_info_widget.setMaxWidth(m_left_panel_width);

  m_element.find('#file_browser').append(m_file_browser_widget.element());
  m_element.find('#left_panel').append(m_info_widget.element());

  function setKBShareId(id) {
    m_kbshare_id = id;
    m_file_browser_widget.setBaseUrl(`${config.kbucket_hub_url}/${m_kbshare_id}`);
    m_info_widget.setKBNodeId(id);
  }
}

