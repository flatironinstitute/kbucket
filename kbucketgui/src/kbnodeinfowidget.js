exports.KBNodeInfoWidget = KBNodeInfoWidget;

function KBNodeInfoWidget() {
  this.element = function() {
    return m_element;
  };
  this.setKBNodeId = function(id) {
    setKBNodeId(id);
  };
  this.setKBHubUrl = function(url) {
    setKBHubUrl(url);
  };
  this.setMaxWidth = function(max_width) {
    m_max_width = max_width;
    refresh();
  }

  var m_element = $(`
		<span>
			<table class="table">
			</table>
		</span>
	`);

  var m_kbhub_url='';
  var m_kbnode_id = '';
  var m_info = null;
  var m_max_width = 500;

  function setKBHubUrl(url) {
    if (m_kbhub_url == url) return;
    m_kbhub_url = url;
    update_info();
  }

  function setKBNodeId(id) {
    if (m_kbnode_id == id) return;
    m_kbnode_id = id;
    update_info();
  }

  function update_info() {
    m_info=null;
    refresh();
    if ((!m_kbnode_id)||(!m_kbhub_url)) {
      return;
    }
    var url = `${m_kbhub_url}/${m_kbnode_id}/api/nodeinfo`;
    $.getJSON(url, {}, function(resp) {
      m_info = resp.info || {};
      refresh();
    });
  }

  function refresh() {
    var table = m_element.find('table');
    table.empty();

    if (!m_info) return;

    var parent_info = m_info.parent_hub_info || null;

    var tablerows = [];
    tablerows.push({
      label: 'Name',
      value: `${m_info.name} (${m_info.kbnode_id})`
    });
    tablerows.push({
      label: 'owner',
      value: `${m_info.owner} (${m_info.owner_email})`
    });
    tablerows.push({
      label: 'Type',
      value: m_info.kbnode_type
    });
    if (parent_info) {
      tablerows.push({
        label: 'Parent hub',
        value: `${parent_info.name} (<a href=# id=open_parent_hub>${parent_info.kbnode_id}</a>)`
      });
    } else {
      tablerows.push({
        label: 'Parent hub',
        value: `[None]`
      });
    }

    tablerows.push({
      label: 'Description',
      value: m_info.description
    });

    tablerows.push({
      label: 'Other',
      value: JSON.stringify(m_info, null, 4).split('\n').join('<br>').split(' ').join('&nbsp;')
    });

    for (var i in tablerows) {
      var row = tablerows[i];
      var tr = $('<tr></tr>');
      tr.append(`<th id=label">${row.label}</th>`);
      tr.append(`<td id=value>${row.value}</td>`);
      tr.find('#label').css({
        "max-width": 100
      });
      tr.find('#value').css({
        "max-width": m_max_width - 100 - 50
      });
      table.append(tr);
    }

    table.find('#open_parent_hub').click(function() {
      window.location.href = '?hub=' + parent_info.kbnode_id;
    });
  }
}