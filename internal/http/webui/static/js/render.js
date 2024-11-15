
const  startPage = 0;
const  paginationSize = 5;

function renderNodesMetrics(data) {
    if (!data || !data.nodes_info || data.nodes_info.length === 0) {
        document.getElementById("components").innerHTML = "No Found Components information"
        return
    }

    let tableHTML = '<thead class="thead-light"><tr>' +
        '<th scope="col">Node Name</th>' +
        '<th scope="col">CPU Usage</th>' +
        '<th scope="col">Usage/Memory(GB)</th>' +
        '<th scope="col">Usage/Disk(GB)</th> '+
        '<th scope="col">IO Wait</th>' +
        '<th scope="col">RPC Ops/s</th>' +
        '<th scope="col">Network Throughput(MB/s)</th>' +
        '<th scope="col">Disk Throughput(MB/s)</th>' +
        '</tr></thead>';

    tableHTML += '<tbody>';
    data.nodes_info.forEach(node => {
        tableHTML += '<tr>';
        tableHTML += `<td>${node.infos['name']}</td>`;
        let hardwareInfo = node.infos['hardware_infos']
        let cpuUsage = parseFloat(`${hardwareInfo['cpu_core_usage']}`).toFixed(2)
        tableHTML += `<td>${cpuUsage}%</td>`;
        let memoryUsage = (parseInt(`${hardwareInfo['memory_usage']}`)  / 1024 / 1024 / 1024).toFixed(2)
        let memory = (parseInt(`${hardwareInfo['memory']}`)  / 1024 / 1024 / 1024).toFixed(2)
        tableHTML += `<td>${memoryUsage}/${memory}</td>`;
        let diskUsage = (parseInt(`${hardwareInfo['disk_usage']}`) / 1024 / 1024 / 1024).toFixed(2)
        let disk = (parseInt(`${hardwareInfo['disk']}`)  / 1024 / 1024 / 1024).toFixed(2)
        tableHTML += `<td>${diskUsage}/${disk}</td>`;
        tableHTML += `<td>0.00</td>`;
        tableHTML += `<td>100</td>`;
        tableHTML += `<td>5</td>`;
        tableHTML += `<td>20</td>`;
        tableHTML += '</tr>';
    });
    tableHTML += '</tbody>';
    document.getElementById('nodeMetrics').innerHTML = tableHTML;
}

function renderComponentInfo(data) {
    if (!data || !data.nodes_info || data.nodes_info.length === 0) {
        document.getElementById("components").innerHTML = "No Found Components information"
        return
    }

    let tableHTML = `
    <thead class="thead-light">
        <tr>
            <th scope="col">Node Name</th>
            <th scope="col">Node IP</th>
            <th scope="col">Start Time</th>
            <th scope="col">Node Status</th>
        </tr>
    </thead><tbody>`;

    // Process each node's information
    data.nodes_info.forEach(node => {
        const hardwareInfo = node.infos['hardware_infos'];
        const tr = `
            <tr>
                <td>${node.infos['name']}</td>
                <td>${hardwareInfo['ip']}</td>
                <td>${node.infos['created_time']}</td>
                <td>${node.infos['has_error']? node.infos['error_reason'] : 'Healthy'}</td>
            </tr>`;
        tableHTML += tr
    });

    tableHTML += '</tbody>'
    document.getElementById("components").innerHTML = tableHTML;
}

function renderNodeRequests(data) {
    let tableHTML = '<thead class="thead-light"><tr>' +
        '<th scope="col">Node Name</th>' +
        '<th scope="col">QPS</th>' +
        '<th scope="col">Read Request Count</th>' +
        '<th scope="col">Write Request Count</th>' +
        '<th scope="col">Delete Request Count</th>' +
        '</tr></thead>';

    tableHTML += '<tbody>';
    data.forEach(node => {
        tableHTML += '<tr>';
        tableHTML += `<td>${node.node_name}</td>`;
        tableHTML += `<td>${node.QPS}</td>`;
        tableHTML += `<td>${node.read_request_count}</td>`;
        tableHTML += `<td>${node.write_request_count}</td>`;
        tableHTML += `<td>${node.delete_request_count}</td>`;
        tableHTML += '</tr>';
    });
    tableHTML += '</tbody>';

    document.getElementById('nodeRequests').innerHTML = tableHTML;
}


let databaseData = null; // Global variable to store fetched data
function renderDatabases(currentPage, rowsPerPage) {
    if (!databaseData) {
        console.error('No database data available');
        return;
    }

    // Generate the HTML for the table with Bootstrap classes
    let tableHTML = '<thead class="thead-light"><tr>' +
        '<th scope="col">Name</th>' +
        '<th scope="col">ID</th>' +
        '<th scope="col">Created Timestamp</th>' +
        '</tr></thead>';

    tableHTML += '<tbody>';

    // Calculate start and end indices for pagination
    const start = currentPage * rowsPerPage;
    const end = start + rowsPerPage;

    const totalCount = databaseData.db_names.length;
    // Loop over page data to render only rows for the current page
    for (let i = start; i < end && i < totalCount; i++) {
        tableHTML += '<tr>';
        tableHTML += `<td><a href="#" onclick="describeDatabase('${databaseData.db_names[i]}', ${i}, 'list-db')">${databaseData.db_names[i]}</a></td>`;
        tableHTML += `<td>${databaseData.db_ids? databaseData.db_ids[i] : 0}</td>`;
        tableHTML += `<td>${databaseData.created_timestamps[i]}</td>`;
        tableHTML += '</tr>';

        // Hidden row for displaying collection details as JSON
        tableHTML += `<tr id="list-db-details-row-${i}" class="list-db-details-row" style="display: none;">
                        <td colspan="3"><pre id="list-db-json-details-${i}">Loading...</pre></td>
                      </tr>`;
    }
    tableHTML += '</tbody>';

    // Insert table HTML into the DOM
    document.getElementById('database').innerHTML = tableHTML;

    // Display the total count
    document.getElementById('db_totalCount').innerText = `Total Databases: ${totalCount}`;

    // Create pagination controls
    const totalPages = Math.ceil(totalCount / rowsPerPage);
    let paginationHTML = '<nav><ul class="pagination justify-content-center">';

    // Previous button (disabled if on the first page)
    paginationHTML += `
        <li class="page-item ${currentPage === 0 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="renderDatabases(${currentPage - 1}, ${rowsPerPage})">Previous</a>
        </li>`;

    // Next button (disabled if on the last page)
    paginationHTML += `
        <li class="page-item ${currentPage >= totalPages - 1 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="renderDatabases(${currentPage + 1}, ${rowsPerPage})">Next</a>
        </li>`;

    paginationHTML += '</ul></nav>';

    // Insert pagination HTML into the DOM
    document.getElementById('dbPaginationControls').innerHTML = paginationHTML;
}

function describeDatabase(databaseName, rowIndex, type) {
    fetchData(`${MILVUS_URI}/_db/desc?db_name=${databaseName}`, describeDatabaseData)
        .then(data => {
            // Format data as JSON and insert into the designated row
            const jsonFormattedData = JSON.stringify(data, null, 2);
            document.getElementById(`${type}-json-details-${rowIndex}`).textContent = jsonFormattedData;

            // Toggle the visibility of the details row
            const detailsRow = document.getElementById(`${type}-details-row-${rowIndex}`);
            detailsRow.style.display = detailsRow.style.display === 'none' ? 'table-row' : 'none';
        })
        .catch(error => {
            console.error('Error fetching collection details:', error);
            document.getElementById(`${type}-json-details-${rowIndex}`).textContent = 'Failed to load collection details.';
        });
}

function describeCollection(databaseName, collectionName, rowIndex, type) {
    fetchData(`${MILVUS_URI}/_collection/desc?db_name=${databaseName}&&collection_name=${collectionName}`, describeCollectionData)
        .then(data => {
            // Format data as JSON and insert into the designated row
            const jsonFormattedData = JSON.stringify(data, null, 2);
            document.getElementById(`${type}-json-details-${rowIndex}`).textContent = jsonFormattedData;

            // Toggle the visibility of the details row
            const detailsRow = document.getElementById(`${type}-details-row-${rowIndex}`);
            detailsRow.style.display = detailsRow.style.display === 'none' ? 'table-row' : 'none';
        })
        .catch(error => {
            console.error('Error fetching collection details:', error);
            document.getElementById(`${type}-json-details-${rowIndex}`).textContent = 'Failed to load collection details.';
        });
}

function fetchCollections(databaseName) {
    fetchData(MILVUS_URI + `/_collection/list?db_name=${databaseName}`, listCollectionData )
        .then(data => {
            collectionsData = data;
            renderCollections(databaseName, startPage, paginationSize)
        })
        .catch(error => {
            handleError(error);
        });
}

let collectionsData = null; // Global variable to store fetched data
function renderCollections(databaseName, currentPage, rowsPerPage) {
    let data = collectionsData;
    if (!data || !data.collection_names) {
        console.error('No collections data available');
        return;
    }
    let tableHTML = '<thead class="thead-light"><tr>' +
        '<th scope="col">Name</th>' +
        '<th scope="col">Collection ID</th>' +
        '<th scope="col">Created Timestamp</th>' +
        '<th scope="col">Loaded Percentages</th>' +
        '<th scope="col">Queryable</th>' +
        '</tr></thead>';

    tableHTML += '<tbody>';

    const start = currentPage * rowsPerPage;
    const end = start + rowsPerPage;
    const totalCount = data.collection_names.length;
    for (let i = start; i < end && i < totalCount; i++) {
        tableHTML += '<tr>';
        tableHTML += `<td><a href="#" onclick="describeCollection('${databaseName}', '${data.collection_names[i]}', ${i}, 'list-coll')">${data.collection_names[i]}</a></td>`;
        tableHTML += `<td>${data.collection_ids[i]}</td>`;
        tableHTML += `<td>${data.created_utc_timestamps[i]}</td>`;
        tableHTML += `<td>${data.inMemory_percentages?  data.inMemory_percentages[i]: 'unknown'}</td>`;
        tableHTML += `<td>${data.query_service_available? data.query_service_available[i] ? 'Yes' : 'No' : 'No'}</td>`;
        tableHTML += '</tr>';

        // Hidden row for displaying collection details as JSON
        tableHTML += `<tr id="list-coll-details-row-${i}" class="list-coll-details-row" style="display: none;">
                        <td colspan="5"><pre id="list-coll-json-details-${i}">Loading...</pre></td>
                      </tr>`;
    }
    tableHTML += '</tbody>';

    document.getElementById('collectionList').innerHTML = tableHTML;
    document.getElementById('collection_totalCount').innerText = `Total Collections: ${totalCount}`;

    const totalPages = Math.ceil(totalCount / rowsPerPage);
    let paginationHTML = '<nav><ul class="pagination justify-content-center">';

    paginationHTML += `
        <li class="page-item ${currentPage === 0 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="renderCollections(${currentPage - 1}, ${rowsPerPage})">Previous</a>
        </li>`;

    paginationHTML += `
        <li class="page-item ${currentPage >= totalPages - 1 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="renderCollections(${currentPage + 1}, ${rowsPerPage})">Next</a>
        </li>`;

    paginationHTML += '</ul></nav>';
    document.getElementById('collectionPaginationControls').innerHTML = paginationHTML;
}

let collectionRequestsData = null; // Global variable to store fetched data
function renderCollectionRequests(database, currentRequestPage, requestRowsPerPage) {
    if (!collectionRequestsData) {
        console.error('No collection requests data available');
        return;
    }

    const data = collectionRequestsData;
    let tableHTML = '<thead class="thead-light"><tr>' +
        '<th scope="col">Collection Name</th>' +
        '<th scope="col">Search QPS</th>' +
        '<th scope="col">Query QPS</th>' +
        '<th scope="col">Insert Throughput(MB/s)</th>' +
        '<th scope="col">Delete QPS</th>' +
        '</tr></thead>';

    tableHTML += '<tbody>';

    const start = currentRequestPage * requestRowsPerPage;
    const end = start + requestRowsPerPage;
    const totalCount = data.length;

    for (let i = start; i < end && i < totalCount; i++) {
        tableHTML += '<tr>';
        tableHTML += `<td><a href="#" onclick="describeCollection('${database}','${data[i].collection_name}', ${i}, 'req')">${data[i].collection_name}</a></td>`;
        tableHTML += `<td>${data[i].search_QPS}</td>`;
        tableHTML += `<td>${data[i].query_QPS}</td>`;
        tableHTML += `<td>${data[i].write_throughput}</td>`;
        tableHTML += `<td>${data[i].delete_QPS}</td>`;
        tableHTML += '</tr>';

        // Hidden row for displaying collection details as JSON
        tableHTML += `<tr id="req-details-row-${i}" class="req-details-row" style="display: none;">
                        <td colspan="5"><pre id="req-json-details-${i}">Loading...</pre></td>
                      </tr>`;
    }
    tableHTML += '</tbody>';

    document.getElementById('collectionRequests').innerHTML = tableHTML;
    document.getElementById('collection_totalCount').innerText = `Total Collections: ${totalCount}`;

    const totalPages = Math.ceil(totalCount / requestRowsPerPage);
    let paginationHTML = '<nav><ul class="pagination justify-content-center">';

    paginationHTML += `
        <li class="page-item ${currentRequestPage === 0 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="renderCollectionRequests(${currentRequestPage - 1}, ${requestRowsPerPage})">Previous</a>
        </li>`;

    paginationHTML += `
        <li class="page-item ${currentRequestPage >= totalPages - 1 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="renderCollectionRequests(${currentRequestPage + 1}, ${requestRowsPerPage})">Next</a>
        </li>`;

    paginationHTML += '</ul></nav>';
    document.getElementById('collectionPaginationControls').innerHTML = paginationHTML;
}

function renderSysInfo(data) {
    if (!data || data.length === 0 ) {
        document.getElementById("sysInfo").innerHTML = "No Found Sys information"
        return
    }
    let tableHTML = '<thead class="thead-light"><tr>' +
        ' <th scope="col">Attribute</th>' +
        ' <th scope="col">Value</th>' +
        ' <th scope="col">Description</th>' +
        '</tr></thead>';
    tableHTML += readSysInfo(data.nodes_info[0].infos['system_info'])

    tableHTML += '<tr>';
    tableHTML += `<td>Started Time</td>`;
    tableHTML += `<td>${data.nodes_info[0].infos['created_time']}</td>`;
    tableHTML += `<td>The time when the system was started</td></tr>`;
    tableHTML += '</tbody>';

    // Display table in the div
    document.getElementById('sysInfo').innerHTML = tableHTML;
}

function renderClientsInfo(data) {
    if (!data || data.length === 0 ) {
        document.getElementById("clients").innerHTML = "No clients connected"
        return
    }

    let tableHTML = '<thead class="thead-light"><tr>' +
        '   <th scope="col">Host</th>' +
        '   <th scope="col">User</th>' +
        '   <th scope="col">SdkType</th>' +
        '   <th scope="col">SdkVersion</th>' +
        '   <th scope="col">LocalTime</th> ' +
        '   <th scope="col">LastActiveTime</th> ' +
         '</tr></thead>';

    data.forEach(client => {
        const tr = `
            <tr>
                <td>${client['host']}</td>
                <td>${client['user'] || "default"}</td>
                <td>${client['sdk_type']}</td>
                <td>${client['sdk_version']}</td>
                <td>${client['local_time']}</td>
                <td>${client['reserved'] ? client['reserved']['last_active_time']: ""}</td>
            </tr>`;
        tableHTML += tr
    });

    tableHTML += '</tbody>'
    document.getElementById("clients").innerHTML = tableHTML;
}

function readSysInfo(systemInfo) {
    let row = ''
    row += '<tr>';
    row += `<td>GitCommit</td>`;
    row += `<td>${systemInfo.system_version}</td>`;
    row += `<td>Git commit SHA that the current build of the system is based on</td>`;
    row += '</tr>';

    row += '<tr>';
    row += `<td>Deploy Mode</td>`;
    row += `<td>${systemInfo.deploy_mode}</td>`;
    row += `<td>the mode in which the system is deployed</td>`;
    row += '</tr>';

    row += '<tr>';
    row += `<td>Build Version</td>`;
    row += `<td>${systemInfo.build_version}</td>`;
    row += `<td>the version of the system that was built</td>`;
    row += '</tr>';

    row += '<tr>';
    row += `<td>Build Time</td>`;
    row += `<td>${systemInfo.build_time}</td>`;
    row += `<td>the exact time when the system was built</td>`;
    row += '</tr>';

    row += '<tr>';
    row += `<td>Go Version</td>`;
    row += `<td>${systemInfo.used_go_version}</td>`;
    row += `<td>the version of the Golang that was used to build the system</td>`;
    row += '</tr>';
    return row
}

let configData = null;  // Global variable to store the config object
let currentPage = 0;
const rowsPerPage = 10;

function renderConfigs(obj, searchTerm = '') {
    configData = obj;

    // Filter the data based on the search term
    const filteredData = Object.keys(obj).filter(key =>
        key.toLowerCase().includes(searchTerm.toLowerCase()) ||
        String(obj[key]).toLowerCase().includes(searchTerm.toLowerCase())
    ).reduce((acc, key) => {
        acc[key] = obj[key];
        return acc;
    }, {});

    // Calculate pagination variables
    const totalCount = Object.keys(filteredData).length;
    const totalPages = Math.ceil(totalCount / rowsPerPage);
    const start = currentPage * rowsPerPage;
    const end = start + rowsPerPage;

    // Generate table header
    let tableHTML = '<thead class="thead-light"><tr>' +
        ' <th scope="col">Attribute</th>' +
        ' <th scope="col">Value</th>' +
        '</tr></thead>';

    // Generate table rows based on current page and filtered data
    tableHTML += '<tbody>';
    const entries = Object.entries(filteredData).slice(start, end);
    entries.forEach(([prop, value]) => {
        tableHTML += '<tr>';
        tableHTML += `<td>${prop}</td>`;
        tableHTML += `<td>${value}</td>`;
        tableHTML += '</tr>';
    });
    tableHTML += '</tbody>';

    // Display the table
    document.getElementById('mConfig').innerHTML = tableHTML;

    // Display total count and pagination controls
    let paginationHTML = '<div class="d-flex justify-content-between align-items-center">';

    // Total count display
    paginationHTML += `<span>Total Configs: ${totalCount}</span>`;

    // Pagination controls
    paginationHTML += '<nav><ul class="pagination mb-0">';

    // Previous button
    paginationHTML += `
        <li class="page-item ${currentPage === 0 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="changePage(${currentPage - 1}, '${searchTerm}')">Previous</a>
        </li>`;

    // Page numbers
    // for (let i = 0; i < totalPages; i++) {
    //     paginationHTML += `
    //         <li class="page-item ${currentPage === i ? 'active' : ''}">
    //             <a class="page-link" href="#" onclick="changePage(${i}, '${searchTerm}')">${i + 1}</a>
    //         </li>`;
    // }

    // Next button
    paginationHTML += `
        <li class="page-item ${currentPage >= totalPages - 1 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="changePage(${currentPage + 1}, '${searchTerm}')">Next</a>
        </li>`;

    paginationHTML += '</ul></nav></div>';
    document.getElementById('paginationControls').innerHTML = paginationHTML;
}

function changePage(page, searchTerm = '') {
    currentPage = page;
    renderConfigs(configData, searchTerm);
}

function renderDependencies(data) {
    if (!data || data.length === 0 ) {
        document.getElementById("3rdDependency").innerHTML = "No Found "
        return
    }

    let tableHTML = '<thead class="thead-light"><tr>' +
        '   <th scope="col">Sys Name</th>' +
        '   <th scope="col">Cluster Status</th>' +
        '   <th scope="col">Members Status</th>' +
        '</tr></thead>';

    Object.keys(data).forEach(key => {
        row = data[key]
        const tr = `
            <tr>
                <td><strong>${key === 'metastore'? 'metastore [' + row['meta_type'] + ']' : 'mq [' + row['mq_type'] + ']'} </strong> </td>
                <td>${row['health_status']? 'Healthy' :  row['unhealthy_reason']}</td>
                <td>${row['members_health']? row['members_health'].map(member => `
                    <ul>
                      <li>Endpoint: ${member.endpoint}, Health: ${member.health ? "Healthy" : "Unhealthy"}</li>
                    </ul>`).join('') :
                    `<ul>
                        <li>No members</li>
                    </ul>`}
                </td>
            </tr>`;
        tableHTML += tr
    });

    tableHTML += '</tbody>'
    document.getElementById("3rdDependency").innerHTML = tableHTML;
}

function renderQCTasks(data) {
    if (!data || data.length === 0 ) {
        document.getElementById("qcTasks").innerHTML = "No Found QC Tasks"
        return
    }
    let tableHTML = '<thead class="thead-light"><tr>' +
        '<th scope="col">Task Name</th>' +
        '<th scope="col">Collection ID</th>' +
        '<th scope="col">Task Type</th>' +
        '<th scope="col">Task Status</th>' +
        '<th scope="col">Actions</th>' +
        '</tr></thead>';

    tableHTML += '<tbody>';

    data.forEach(task => {
        let taskStatus = task.task_status;
        if (taskStatus === 'failed') {
            taskStatus = task.reason;
        }

        tableHTML += '<tr>';
        tableHTML += `<td>${task.task_name}</td>`;
        tableHTML += `<td>${task.collection_id}</td>`;
        tableHTML += `<td>${task.task_type}</td>`;
        tableHTML += `<td>${taskStatus}</td>`;
        tableHTML += `<td>${task.actions.join(', ')}</td>`;
        tableHTML += '</tr>';
    });

    tableHTML += '</tbody>';

    document.getElementById('qcTasks').innerHTML = tableHTML;
}

function renderBuildIndexTasks(data) {
    if (!data || data.length === 0 ) {
        document.getElementById("buildIndexTasks").innerHTML = "No Found Build Index Tasks"
        return
    }
    let tableHTML = '<thead class="thead-light"><tr>' +
        '<th scope="col">Index ID</th>' +
        '<th scope="col">Collection ID</th>' +
        '<th scope="col">Segment ID</th>' +
        '<th scope="col">Build ID</th>' +
        '<th scope="col">Index State</th>' +
        '<th scope="col">Index Size</th>' +
        '<th scope="col">Index Version</th>' +
        '<th scope="col">Create Time</th>' +
        '</tr></thead>';

    tableHTML += '<tbody>';

    data.forEach(task => {
        let indexState = task.index_state;
        if (indexState === 'Failed') {
            indexState = task.fail_reason;
        }

        tableHTML += '<tr>';
        tableHTML += `<td>${task.index_id}</td>`;
        tableHTML += `<td>${task.collection_id}</td>`;
        tableHTML += `<td>${task.segment_id}</td>`;
        tableHTML += `<td>${task.build_id}</td>`;
        tableHTML += `<td>${indexState}</td>`;
        tableHTML += `<td>${task.index_size}</td>`;
        tableHTML += `<td>${task.index_version}</td>`;
        tableHTML += `<td>${task.create_time}</td>`;
        tableHTML += '</tr>';
    });

    tableHTML += '</tbody>';

    document.getElementById('buildIndexTasks').innerHTML = tableHTML;
}

function renderCompactionTasks(data) {
    if (!data || data.length === 0 ) {
        document.getElementById("compactionTasks").innerHTML = "No Found Compaction Tasks"
        return
    }

    let tableHTML = '<thead class="thead-light"><tr>' +
        '<th scope="col">Plan ID</th>' +
        '<th scope="col">Collection ID</th>' +
        '<th scope="col">Type</th>' +
        '<th scope="col">State</th>' +
        '<th scope="col">Start Time</th>' +
        '<th scope="col">End Time</th>' +
        '<th scope="col">Total Rows</th>' +
        '<th scope="col">Input Segments</th>' +
        '<th scope="col">Result Segments</th>' +
        '</tr></thead>';

    tableHTML += '<tbody>';

    data.forEach(task => {
        let state = task.state;
        if (state === 'Failed') {
            state = task.fail_reason;
        }

        tableHTML += '<tr>';
        tableHTML += `<td>${task.plan_id}</td>`;
        tableHTML += `<td>${task.collection_id}</td>`;
        tableHTML += `<td>${task.type}</td>`;
        tableHTML += `<td>${state}</td>`;
        tableHTML += `<td>${new Date(task.start_time * 1000).toLocaleString()}</td>`;
        tableHTML += `<td>${new Date(task.end_time * 1000).toLocaleString()}</td>`;
        tableHTML += `<td>${task.total_rows}</td>`;
        tableHTML += `<td>${task.input_segments? task.input_segments.join(', '): ''}</td>`;
        tableHTML += `<td>${task.result_segments? task.result_segments.join(', '): ''}</td>`;
        tableHTML += '</tr>';
    });

    tableHTML += '</tbody>';

    document.getElementById('compactionTasks').innerHTML = tableHTML;
}

function renderImportTasks(data) {
    if (!data  || data.length === 0 ) {
        document.getElementById("importTasks").innerHTML = "No Found Import Tasks"
        return
    }
    let tableHTML = '<thead class="thead-light"><tr>' +
        '<th scope="col">Job ID</th>' +
        '<th scope="col">Task ID</th>' +
        '<th scope="col">Collection ID</th>' +
        '<th scope="col">Node ID</th>' +
        '<th scope="col">State</th>' +
        '<th scope="col">Task Type</th>' +
        '<th scope="col">Created Time</th>' +
        '<th scope="col">Complete Time</th>' +
        '</tr></thead>';

    tableHTML += '<tbody>';

    data.forEach(task => {
        let state = task.state;
        if (state === 'Failed') {
            state = task.reason;
        }

        tableHTML += '<tr>';
        tableHTML += `<td>${task.job_id}</td>`;
        tableHTML += `<td>${task.task_id}</td>`;
        tableHTML += `<td>${task.collection_id}</td>`;
        tableHTML += `<td>${task.node_id}</td>`;
        tableHTML += `<td>${state}</td>`;
        tableHTML += `<td>${task.task_type}</td>`;
        tableHTML += `<td>${task.created_time}</td>`;
        tableHTML += `<td>${task.complete_time}</td>`;
        tableHTML += '</tr>';
    });

    tableHTML += '</tbody>';

    document.getElementById('importTasks').innerHTML = tableHTML;
}

function renderSyncTasks(data) {
    if (!data || data.length === 0 ) {
        document.getElementById("syncTasks").innerHTML = "No Found Sync Tasks"
        return
    }
    let tableHTML = '<thead class="thead-light"><tr>' +
        '<th scope="col">Segment ID</th>' +
        '<th scope="col">Batch Rows</th>' +
        '<th scope="col">Segment Level</th>' +
        '<th scope="col">Timestamp From</th>' +
        '<th scope="col">Timestamp To</th>' +
        '<th scope="col">Delta Row Count</th>' +
        '<th scope="col">Flush Size</th>' +
        '<th scope="col">Running Time</th>' +
        '<th scope="col">Node ID</th>' +
        '</tr></thead>';

    tableHTML += '<tbody>';

    data.forEach(task => {
        tableHTML += '<tr>';
        tableHTML += `<td>${task.segment_id}</td>`;
        tableHTML += `<td>${task.batch_rows}</td>`;
        tableHTML += `<td>${task.segment_level}</td>`;
        tableHTML += `<td>${task.ts_from}</td>`;
        tableHTML += `<td>${task.ts_to}</td>`;
        tableHTML += `<td>${task.delta_row_count}</td>`;
        tableHTML += `<td>${task.flush_size}</td>`;
        tableHTML += `<td>${task.running_time}</td>`;
        tableHTML += `<td>${task.node_id}</td>`;
        tableHTML += '</tr>';
    });

    tableHTML += '</tbody>';

    document.getElementById('syncTasks').innerHTML = tableHTML;
}

let dataChannels = null; // Store fetched merged dn_channels data and dc_dist data
function fetchAndRenderDataChannels() {
    let dcDist = fetchData(MILVUS_URI + "/_dc/dist", dc_dist);
    let dnChannels = fetchData(MILVUS_URI + "/_dn/channels", dn_channels);
    Promise.all([dnChannels, dcDist])
        .then(([dnChannelsData, dcDistData]) => {
            if (dnChannelsData && dcDistData) {
                const { mergedChannels, notifications } = mergeChannels(dnChannelsData, dcDistData);
                dataChannels = mergedChannels
                renderChannels(mergedChannels, currentPage, rowsPerPage);
                renderNotifications(notifications, 'notificationsDChannels');
            }
        })
        .catch(error => {
            handleError(error);
        });;
}

// Merge channels by matching names from dn_channels and dc_dist
function mergeChannels(dnChannels, dcDist) {
    const merged = {};
    const notifications = []; // Store notifications for unmatched channels
    // const dnChannelNames = new Set(dnChannels.map(channel => channel.name));
    const dcChannelNames = new Set(dcDist.dm_channels.map(dcChannel => dcChannel.channel_name));

    // Add dn_channels to merged and mark channels missing in dc_dist
    dnChannels.forEach(channel => {
        channel.node_id = "datanode-" + channel.node_id;
        const channelData = { ...channel };
        if (!dcChannelNames.has(channel.name)) {
            channelData.notification = 'Not found in DataCoord';
            notifications.push(channelData); // Add to notifications
        }
        merged[channel.name] = channelData;
    });

    // Merge dc_dist channels or add new ones with notification if missing in dn_channels
    dcDist.dm_channels.forEach(dcChannel => {
        dcChannel.node_id = "datacoord-" + dcChannel.node_id;
        const name = dcChannel.channel_name;
        if (merged[name]) {
            if (merged[name].watch_state !== "Healthy") {
                dcChannel.watch_state = merged[name].watch_state
            }
            merged[name] = { ...merged[name], ...dcChannel };
        } else {
            const channelData = { ...dcChannel, notification: 'Not found in DataNode' };
            notifications.push(channelData); // Add to notifications
            merged[name] = channelData;
        }
    });

    return { mergedChannels: Object.values(merged), notifications };
}

// Render the merged channels in a paginated table
function renderChannels(channels, currentPage, rowsPerPage) {
    const table = document.getElementById("dataChannelsTableBody");
    table.innerHTML = ""; // Clear previous rows

    const start = currentPage * rowsPerPage;
    const end = start + rowsPerPage;
    const paginatedData = channels.slice(start, end);

    paginatedData.forEach(channel => {
        const row = document.createElement("tr");

        row.innerHTML = `
            <td>${channel.name || channel.channel_name}</td>
            <td>${channel.watch_state || "N/A"}</td>
            <td>${channel.node_id}</td>
            <td>${channel.latest_time_tick || "N/A"}</td>
            <td>${channel.start_watch_ts || "N/A"}</td>
            <td>${channel.check_point_ts || "N/A"}</td>
        `;
        table.appendChild(row);
    });

    // Update pagination info
    const totalPages = Math.ceil(channels.length / rowsPerPage);
    let paginationHTML = '<nav><ul class="pagination justify-content-center">';

    paginationHTML += `
        <li class="page-item ${currentPage === 0 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick=" renderChannels(dataChannels, ${currentPage - 1}, ${rowsPerPage})">Previous</a>
        </li>`;

    paginationHTML += `
        <li class="page-item ${currentPage >= totalPages - 1 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="renderChannels(dataChannels, ${currentPage + 1}, ${rowsPerPage})">Next</a>
        </li>`;

    paginationHTML += '</ul></nav>';
    document.getElementById('dchannelPaginationControls').innerHTML = paginationHTML;

    document.getElementById('dchannel_totalCount').innerText = `Total Channels: ${channels.length}`;
}

// Render notifications below the table
function renderNotifications(notifications, id) {
    if (!notifications || notifications.length === 0) {
        return
    }
    const notificationsContainer = document.getElementById(id);
    notificationsContainer.innerHTML = ""; // Clear previous notifications

    notifications.forEach(channel => {
        const notificationAlert = document.createElement("div");
        notificationAlert.className = "alert alert-warning";
        notificationAlert.role = "alert";

        // Generate detailed information for each channel
        const details = `
            <strong>Channel:</strong> ${channel.name || channel.channel_name} <strong> ${channel.notification}</strong><br>
        `;
        notificationAlert.innerHTML = details;

        notificationsContainer.appendChild(notificationAlert);
    });
}

let dataSegments = null; // Store fetched merged dn_segments data and dc_dist data
const  dataSegmentsStartPage = 0;
const  dataSegmentsPaginationSize = 5;

function fetchAndRenderDataSegments() {
    let dcDist = fetchData(MILVUS_URI + "/_dc/dist", dc_dist);
    let dnSegments = fetchData(MILVUS_URI + "/_dn/segments", dn_segments);
    Promise.all([dnSegments, dcDist])
        .then(([dnSegmentsData, dcDistData]) => {
            if (dnSegmentsData && dcDistData) {
                const { mergedSegments, notifications } = mergeSegments(dnSegmentsData, dcDistData);
                dataSegments = mergedSegments;
                renderSegments(mergedSegments, currentPage, rowsPerPage);
                renderNotifications(notifications, 'notificationsDSegments');
            }
        })
        .catch(error => {
            handleError(error);
        });
}

// Merge segments by matching segment IDs from dn_segments and dc_dist
function mergeSegments(dnSegments, dcDist) {
    const merged = {};
    const notifications = []; // Store notifications for unmatched segments
    const dcSegmentIds = new Set(dcDist.segments.map(dcSegment => dcSegment.segment_id));

    // Add dn_segments to merged and mark segments missing in dc_dist
    dnSegments.forEach(segment => {
        segment.node_id = "datanode-" + segment.node_id;
        const segmentData = { ...segment };
        if (!dcSegmentIds.has(segment.segment_id)) {
            segmentData.notification = 'Not found in DataCoord';
            notifications.push(segmentData); // Add to notifications
        }
        merged[segment.segment_id] = segmentData;
    });

    // Merge dc_dist segments or add new ones with notification if missing in dn_segments
    dcDist.segments.forEach(dcSegment => {
        dcSegment.node_id = "datacoord-" + dcSegment.node_id;
        const id = dcSegment.segment_id;
        if (merged[id]) {
            merged[id] = { ...merged[id], ...dcSegment };
        } else {
            const segmentData = { ...dcSegment, notification: 'Not found in DataNode' };
            if (dcSegment.state !== 'Dropped' && dcSegment.state !== 'Flushed'){
                notifications.push(segmentData); // Add to notifications
            }
            merged[id] = segmentData;
        }
    });

    return { mergedSegments: Object.values(merged), notifications };
}

// Render the merged segments in a paginated table
function renderSegments(segments, currentPage, rowsPerPage) {
    const table = document.getElementById("dataSegmentsTableBody");
    table.innerHTML = ""; // Clear previous rows

    const start = currentPage * rowsPerPage;
    const end = start + rowsPerPage;
    const paginatedData = segments.slice(start, end);

    paginatedData.forEach(segment => {
        const row = document.createElement("tr");

        const numRows = segment.state === "Growing" ? segment.flushed_rows : segment.num_of_rows;
        row.innerHTML = `
            <td>${segment.segment_id}</td>
            <td>${segment.collection_id}</td>
            <td>${segment.partition_id}</td>
            <td>${segment.channel}</td>
            <td>${numRows || 'unknown'}</td>
            <td>${segment.state}</td>
            <td>${segment.level}</td>
        `;
        table.appendChild(row);
    });

    // Update pagination info
    const totalPages = Math.ceil(segments.length / rowsPerPage);
    let paginationHTML = '<nav><ul class="pagination justify-content-center">';

    paginationHTML += `
        <li class="page-item ${currentPage === 0 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="renderSegments(dataSegments, ${currentPage - 1}, ${rowsPerPage})">Previous</a>
        </li>`;

    paginationHTML += `
        <li class="page-item ${currentPage >= totalPages - 1 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="renderSegments(dataSegments, ${currentPage + 1}, ${rowsPerPage})">Next</a>
        </li>`;

    paginationHTML += '</ul></nav>';
    document.getElementById('dsegmentPaginationControls').innerHTML = paginationHTML;

    document.getElementById('dsegment_totalCount').innerText = `Total Segments: ${segments.length}`;
}

function fetchAndRenderTargets(currentPage = startPage, rowsPerPage = rowsPerPage) {
    let currentTargets = fetchData(MILVUS_URI + "/qc/target", qcCurrentTargets);
    let nextTargets = fetchData(MILVUS_URI + "/_qc/target?target_scope=2", qcNextTargets);

    Promise.all([currentTargets, nextTargets])
        .then(([currentTargetsData, nextTargetsData]) => {
            if (currentTargetsData && nextTargetsData) {
                const mergedSegments = mergeTargetSegments(currentTargetsData, nextTargetsData);
                const mergedChannels = mergeTargetChannels(currentTargetsData, nextTargetsData);

                renderTargetSegments(mergedSegments, currentPage, rowsPerPage);
                renderTargetChannels(mergedChannels, currentPage, rowsPerPage);
            }
        })
        .catch(error => {
            handleError(error);
        });
}

function mergeTargetSegments(currentTargets, nextTargets) {
    const merged = [];

    currentTargets.forEach(target => {
        target.segments.forEach(segment => {
            segment.targetScope = 'current';
            merged.push(segment);
        });
    });

    nextTargets.forEach(target => {
        target.segments.forEach(segment => {
            segment.targetScope = 'next';
            merged.push(segment);
        });
    });

    return merged;
}

function mergeTargetChannels(currentTargets, nextTargets) {
    const merged = [];

    currentTargets.forEach(target => {
        if (target.dm_channels) {
            target.dm_channels.forEach(channel => {
                channel.targetScope = 'current';
                merged.push(channel);
            });
        }
    });

    nextTargets.forEach(target => {
        if (target.dm_channels) {
            target.dm_channels.forEach(channel => {
                channel.targetScope = 'next';
                merged.push(channel);
            });
        }
    });

    return merged;
}

function renderTargetSegments(segments, currentPage, rowsPerPage) {
    const table = document.getElementById("dataSegmentsTableBody");
    table.innerHTML = ""; // Clear previous rows

    const start = currentPage * rowsPerPage;
    const end = start + rowsPerPage;
    const paginatedData = segments.slice(start, end);

    paginatedData.forEach(segment => {
        const row = document.createElement("tr");

        row.innerHTML = `
            <td>${segment.segment_id}</td>
            <td>${segment.collection_id}</td>
            <td>${segment.partition_id}</td>
            <td>${segment.channel}</td>
            <td>${segment.num_of_rows}</td>
            <td>${segment.state}</td>
            <td>${segment.targetScope}</td>
        `;
        table.appendChild(row);
    });

    // Update pagination info
    const totalPages = Math.ceil(segments.length / rowsPerPage);
    let paginationHTML = '<nav><ul class="pagination justify-content-center">';

    paginationHTML += `
        <li class="page-item ${currentPage === 0 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="renderTargetSegments(segments, ${currentPage - 1}, ${rowsPerPage})">Previous</a>
        </li>`;

    paginationHTML += `
        <li class="page-item ${currentPage >= totalPages - 1 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="renderTargetSegments(segments, ${currentPage + 1}, ${rowsPerPage})">Next</a>
        </li>`;

    paginationHTML += '</ul></nav>';
    document.getElementById('segmentPaginationControls').innerHTML = paginationHTML;

    document.getElementById('segment_totalCount').innerText = `Total Segments: ${segments.length}`;
}

function renderTargetChannels(channels, currentPage, rowsPerPage) {
    const table = document.getElementById("dataChannelsTableBody");
    table.innerHTML = ""; // Clear previous rows

    const start = currentPage * rowsPerPage;
    const end = start + rowsPerPage;
    const paginatedData = channels.slice(start, end);

    paginatedData.forEach(channel => {
        const row = document.createElement("tr");

        row.innerHTML = `
            <td>${channel.channel_name}</td>
            <td>${channel.collection_id}</td>
            <td>${channel.node_id}</td>
            <td>${channel.version}</td>
            <td>${channel.unflushed_segment_ids.join(', ')}</td>
            <td>${channel.flushed_segment_ids.join(', ')}</td>
            <td>${channel.dropped_segment_ids.join(', ')}</td>
            <td>${channel.targetScope}</td>
        `;
        table.appendChild(row);
    });

    // Update pagination info
    const totalPages = Math.ceil(channels.length / rowsPerPage);
    let paginationHTML = '<nav><ul class="pagination justify-content-center">';

    paginationHTML += `
        <li class="page-item ${currentPage === 0 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="renderTargetChannels(channels, ${currentPage - 1}, ${rowsPerPage})">Previous</a>
        </li>`;

    paginationHTML += `
        <li class="page-item ${currentPage >= totalPages - 1 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="renderTargetChannels(channels, ${currentPage + 1}, ${rowsPerPage})">Next</a>
        </li>`;

    paginationHTML += '</ul></nav>';
    document.getElementById('channelPaginationControls').innerHTML = paginationHTML;
    document.getElementById('channel_totalCount').innerText = `Total Channels: ${channels.length}`;
}

function renderSlowQueries(data) {
    let tableHTML = '<thead class="thead-light"><tr>' +
        '<th scope="col">Time</th>' +
        '<th scope="col">Trace ID</th>' +
        '<th scope="col">Request</th>' +
        '<th scope="col">User</th>' +
        '<th scope="col">Database</th>' +
        '<th scope="col">Collection</th>' +
        '<th scope="col">Parameters</th>' +
        '<th scope="col">Duration</th>' +
        '</tr></thead>';

    tableHTML += '<tbody>';
    data.forEach(query => {
        tableHTML += '<tr>';
        tableHTML += `<td>${query.time}</td>`;
        tableHTML += `<td>${query.trace_id}</td>`;
        tableHTML += `<td>${query.type}</td>`;
        tableHTML += `<td>${query.user || 'unknown'}</td>`;
        tableHTML += `<td>${query.database}</td>`;
        tableHTML += `<td>${query.collection}</td>`;
        tableHTML += `<td>${JSON.stringify(query.query_params)}</td>`;
        tableHTML += `<td>${query.duration}</td>`;
        tableHTML += '</tr>';
    });
    tableHTML += '</tbody>';

    document.getElementById('slowQueries').innerHTML = tableHTML;
    document.getElementById('slowQueriesTotalCount').innerText = `Total Slow Requests: ${data.length}`;
}

const querySegmentsStartPage = 0;
const querySegmentsPaginationSize = 5;
var querySegments = null;
var queryChannels = null;

function fetchAndRenderQuerySegmentsAndChannels(currentPage = querySegmentsStartPage, rowsPerPage = querySegmentsPaginationSize) {
    let qcDistData =  fetchData(MILVUS_URI + "/_qc/dist", qcDist);
    let qcCurrentTargetsData  =  fetchData(MILVUS_URI + "/_qc/target", qcCurrentTargets);
    let qcNextTargetsData = fetchData(MILVUS_URI + "/_qc/target?target_scope=2", qcNextTargets);
    let qnSegmentsData = fetchData(MILVUS_URI + "/_qn/segments", qnSegments);
    let qnChannelsData = fetchData(MILVUS_URI + "/_qn/channels", qnChannels);

    Promise.all([qcDistData, qcCurrentTargetsData, qcNextTargetsData, qnSegmentsData,qnChannelsData])
        .then(([qcDistData, qcCurrentTargetsData, qcNextTargetsData, qnSegmentsData,qnChannelsData]) => {
            const mergedSegments = mergeQuerySegments(qcDistData, qcCurrentTargetsData, qcNextTargetsData, qnSegmentsData);
            querySegments = mergedSegments
            renderQuerySegments(mergedSegments, currentPage, rowsPerPage);

            document.getElementById('querySegmentsTotalCount').innerText = `Total Segments: ${mergedSegments.length}`;
            renderQuerySegmentsPaginationControls(currentPage, Math.ceil(mergedSegments.length / rowsPerPage));


            const mergedChannels = mergeQueryChannels(qcDistData, qcCurrentTargetsData, qcNextTargetsData, qnChannelsData);
            queryChannels = mergedChannels
            renderQueryChannels(mergedChannels, currentPage, rowsPerPage);

            document.getElementById('queryChannelsTotalCount').innerText = `Total Channels: ${mergedChannels.length}`;
            renderQueryChannelsPaginationControls(currentPage, Math.ceil(mergedChannels.length / rowsPerPage));
        })
        .catch(error => {
            handleError(error);
        });
}

function mergeQuerySegments(qcDist, qcCurrentTargets, qcNextTargets, qnSegments) {
    const segments = [];

    const addSegment = (segment, from, leaderId = null) => {
        const existingSegment = segments.find(s => s.segment_id === segment.segment_id);
        if (existingSegment) {
            if (from) {
                existingSegment.from += ` | ${from}`;
            }
            if (leaderId) {
                existingSegment.leader_id = leaderId;
            }
            if (segment.state && segment.state !== 'SegmentStateNone') {
                existingSegment.state = segment.state;
            }
        } else {
            segments.push({ ...segment, from, leader_id: leaderId });
        }
    };

    if (qcDist && qcDist.segments && qcDist.segments.length > 0) {
        qcDist.segments.forEach(segment => {
            addSegment(segment, 'DIST');
        });
    }

    if (qcCurrentTargets && qcCurrentTargets.length >0 ) {
        qcCurrentTargets.forEach(target => {
            target.segments.forEach(segment => {
                addSegment(segment, '<a href="../../query_target.html">CT</a>');
            });
        });
    }

    if (qcCurrentTargets && qcCurrentTargets.length >0 ) {
        qcCurrentTargets.forEach(target => {
            target.segments.forEach(segment => {
                addSegment(segment, 'NT');
            });
        });
    }

    if (qnSegments && qnSegments.length > 0) {
        qnSegments.forEach(segment => {
            addSegment(segment, 'QN');
        });
    }

    if (qcDist && qcDist.leader_views && qcDist.leader_views.length > 0) {
        qcDist.leader_views.forEach(view => {
            if (view.sealed_segments && view.sealed_segments.length > 0) {
                view.sealed_segments.forEach(segment => {
                    addSegment(segment, null, view.leader_id);
                });
            }

            if (view.growing_segments && view.growing_segments.length > 0) {
                view.growing_segments.forEach(segment => {
                    addSegment(segment, null, view.leader_id);
                });
            }
        });
    }

    return segments;
}

function renderQuerySegments(segments, currentPage, rowsPerPage) {
    const start = currentPage * rowsPerPage;
    const end = start + rowsPerPage;
    const paginatedSegments = segments.slice(start, end);

    let tableHTML = `
        <table class="table table-bordered table-hover">
            <thead>
                <tr>
                    <th>Segment ID</th>
                    <th>Collection ID</th>
                    <th>Leader ID</th>
                    <th>Node ID</th>
                    <th>State</th>
                    <th>Rows</th>
                    <th>From</th>
                </tr>
            </thead>
            <tbody>
    `;

    paginatedSegments.forEach(segment => {
        tableHTML += `
            <tr>
                <td>${segment.segment_id}</td>
                <td>${segment.collection_id}</td>
                <td>${segment.leader_id || 'Not Found'}</td>
                <td>${segment.node_id}</td>
                <td>${segment.state}</td>
                <td>${segment.num_of_rows}</td>
                <td>${segment.from}</td>
            </tr>
        `;
    });

    tableHTML += `
            </tbody>
        </table>
    `;

    document.getElementById('querySegmentsTable').innerHTML = tableHTML;
}


function renderQuerySegmentsPaginationControls(currentPage, totalPages) {
    let paginationHTML = '<nav><ul class="pagination">';
    // Previous button
    paginationHTML += `
        <li class="page-item ${currentPage === 0 ? 'disabled' : ''}">
                <a class="page-link" href="#" aria-label="Previous" onclick="renderQuerySegments(querySegments, ${querySegmentsStartPage - 1}, ${querySegmentsPaginationSize})">Previous </a>
        </li>
    `;

    // Next button
    paginationHTML += `
        <li class="page-item ${currentPage === totalPages - 1 ? 'disabled' : ''}">
                <a class="page-link" href="#" aria-label="Previous" onclick="renderQuerySegments(querySegments, ${querySegmentsStartPage + 1}, ${querySegmentsPaginationSize})">Next</a>
        </li>
    `;

    paginationHTML += '</ul></nav>';
    document.getElementById('querySegmentsPagination').innerHTML = paginationHTML;
}


const queryChannelsStartPage = 0;
const queryChannelsPaginationSize = 5;
function renderQueryChannelsPaginationControls(currentPage, totalPages) {
    let paginationHTML = '<nav><ul class="pagination">';
    // Previous button
    paginationHTML += `
        <li class="page-item ${currentPage === 0 ? 'disabled' : ''}">
                <a class="page-link" href="#" aria-label="Previous" onclick="renderQueryChannels(queryChannels, ${queryChannelsStartPage- 1}, ${queryChannelsPaginationSize})">Previous </a>
        </li>
    `;

    // Next button
    paginationHTML += `
        <li class="page-item ${currentPage === totalPages - 1 ? 'disabled' : ''}">
                <a class="page-link" href="#" aria-label="Previous" onclick="renderQueryChannels(queryChannelsStartPage, ${currentPage + 1}, ${queryChannelsPaginationSize})">Next</a>
        </li>
    `;

    paginationHTML += '</ul></nav>';
    document.getElementById('queryChannelsPagination').innerHTML = paginationHTML;
}

function mergeQueryChannels(qcDist, qcCurrentTargets, qcNextTargets, qnChannels) {
    const channels = [];

    const addChannel = (channel, from, leaderId = null) => {
        const existingChannel = channels.find(c => c.name === channel.name || c.name === channel.channel_name);
        if (existingChannel) {
            if (from) {
                existingChannel.from += ` | ${from}`;
            }
            if (leaderId) {
                existingChannel.leader_id = leaderId;
            }

            if (channel.watch_state && channel.watch_state !== 'Healthy' ) {
                existingChannel.watch_state = channel.watch_state;
            }
        } else {
            channels.push({ ...channel, from, leader_id: leaderId });
        }
    };

    if (qcDist && qcDist.dm_channels && qcDist.dm_channels.length > 0) {
        qcDist.dm_channels.forEach(channel => {
            addChannel(channel, 'DIST');
        });
    }

    if (qcCurrentTargets && qcCurrentTargets.length > 0) {
        qcCurrentTargets.forEach(target => {
            if (target.dm_channels) {
                target.dm_channels.forEach(channel => {
                    addChannel(channel, '<a href="../../query_target.html">CT</a>');
                });
            }
        });
    }

    if (qcNextTargets && qcNextTargets.length > 0) {
        qcNextTargets.forEach(target => {
            if (target.dm_channels) {
                target.dm_channels.forEach(channel => {
                    addChannel(channel, 'NT');
                });
            }
        });
    }

    if (qnChannels && qnChannels.length > 0) {
        qnChannels.forEach(channel => {
            addChannel(channel, 'QN');
        });
    }

    if (qcDist && qcDist.leader_views && qcDist.leader_views.length > 0) {
        qcDist.leader_views.forEach(view => {
            if (view.channel) {
                addChannel({name: view.channel}, null, view.leader_id);
            }
        });
    }

    return channels;
}

function renderQueryChannels(channels, currentPage, rowsPerPage) {
    const start = currentPage * rowsPerPage;
    const end = start + rowsPerPage;
    const paginatedChannels = channels.slice(start, end);

    let tableHTML = `
        <table class="table table-bordered table-hover">
            <thead>
                <tr>
                    <th>Channel Name</th>
                    <th>Collection ID</th>
                    <th>Leader ID</th>
                    <th>Node ID</th>
                    <th>Watch State</th>
                    <th>From</th>
                </tr>
            </thead>
            <tbody>
    `;

    paginatedChannels.forEach(channel => {
        tableHTML += `
            <tr>
                <td>${channel.name || channel.channel_name}</td>
                <td>${channel.collection_id}</td>
                <td>${channel.leader_id || 'Not Found'}</td>
                <td>${channel.node_id}</td>
                <td>${channel.watch_state||''}</td>
                <td>${channel.from}</td>
            </tr>
        `;
    });

    tableHTML += `
            </tbody>
        </table>
    `;

    document.getElementById('queryChannelsTable').innerHTML = tableHTML;
}


function fetchAndRenderReplicas() {
    fetchData(MILVUS_URI + "/_qc/replica", qcReplica)
        .then(data => {
            renderReplicas(data);
        })
        .catch(error => {
            handleError(error);
        });
}

function renderReplicas(replicas) {
    if (!replicas || replicas.length === 0) {
        return
    }

    const table = document.getElementById('replicasTable');
    table.innerHTML = ''; // Clear the table

    // Render table headers
    const headers = ['ID', 'Collection ID', 'RW Nodes', 'RO Nodes', 'Resource Group'];
    const headerRow = document.createElement('tr');
    headers.forEach(header => {
        const th = document.createElement('th');
        th.textContent = header;
        headerRow.appendChild(th);
    });
    table.appendChild(headerRow);

    // Render table rows
    replicas.forEach(replica => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${replica.ID}</td>
            <td>${replica.collectionID}</td>
            <td>${replica.rw_nodes? replica.rw_nodes.join(', '): ''}</td>
            <td>${replica.ro_nodes? replica.ro_nodes.join(', '): ''}</td>
            <td>${replica.resource_group}</td>
        `;
        table.appendChild(row);
    });
}

function fetchAndRenderResourceGroup() {
    fetchData(MILVUS_URI + "/_qc/resource_group", qcResourceGroup)
        .then(data => {
            renderResourceGroup(data);
        })
        .catch(error => {
            console.error('Error fetching resource group data:', error);
        });
}

function renderResourceGroup(data) {
    if (!data || data.length === 0) {
        return
    }

    const table = document.getElementById('resourceGroupTable');
    table.innerHTML = ''; // Clear existing table content

    // Create table headers
    const headerRow = document.createElement('tr');
    const headers = ['Name', 'Nodes', 'Cfg'];
    headers.forEach(headerText => {
        const header = document.createElement('th');
        header.textContent = headerText;
        headerRow.appendChild(header);
    });
    table.appendChild(headerRow);

    // Populate table rows with data
    data.forEach(resourceGroup => {
        const row = document.createElement('tr');

        const nameCell = document.createElement('td');
        nameCell.textContent = resourceGroup.name;
        row.appendChild(nameCell);

        const nodesCell = document.createElement('td');
        nodesCell.textContent = resourceGroup.nodes.join(', ');
        row.appendChild(nodesCell);

        const cfgCell = document.createElement('td');
        cfgCell.textContent = resourceGroup.cfg? JSON.stringify(resourceGroup.cfg) : '';
        row.appendChild(cfgCell)

        table.appendChild(row);
    });
}