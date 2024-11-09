var DEBUG_MODE = false; // Set this to false to disable debug mode
var MILVUS_URI = "http://127.0.0.1:9091/api/v1"

// Function to check URL for "debug" parameter and switch debug mode
function toggleDebugMode() {
    // Get the current URL's query parameters
    const urlParams = new URLSearchParams(window.location.search);

    // Check if "debug" parameter is present and its value
    if (urlParams.has('debug')) {
        if (urlParams.get('debug') === 'true') {
            console.log("Debug mode is ON");
            // Enable debug mode: Add any additional debug functionality here
            localStorage.setItem('debug', 'true');
        } else {
            console.log("Debug mode is OFF");
            localStorage.setItem('debug', 'false');
        }
    }

    // Check if debug mode is enabled
    DEBUG_MODE = localStorage.getItem('debug') === 'true';
}

// Call the function to check the URL and apply debug mode
toggleDebugMode();

const handleError = (error) => {
    console.error('Error fetching data:', error);
    // const errorMessage = encodeURIComponent(error.message || 'Unknown error');
    // window.location.href = `5xx.html?error=${errorMessage}`;
};

const fetchData = (url, localData, kvParams) => {
    if (DEBUG_MODE) {
        return new Promise((resolve) => {
            resolve(JSON.parse(localData));
        });
    } else if (kvParams && kvParams.length !== 0) {
        return fetch(url, {
            method: 'POST',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            },
            mode: 'no-cors',
            body: JSON.stringify(kvParams)
        }).then(response => response.json())
    } else {
        return fetch(url).then(response => {
            return response.json();
        })
    }
};

function getQueryParams() {
    const params = {};
    const queryString = window.location.search.substring(1);
    const queryArray = queryString.split('&');
    queryArray.forEach(param => {
        const [key, value] = param.split('=');
        params[decodeURIComponent(key)] = decodeURIComponent(value || '');
    });
    return params;
}

function formatTimestamp(timestamp) {
    const date = new Date(timestamp); // Convert timestamp to a Date object
    // Format the date components
    const year = date.getFullYear();
    const month = ('0' + (date.getMonth() + 1)).slice(-2); // Months are zero-indexed
    const day = ('0' + date.getDate()).slice(-2);
    const hours = ('0' + date.getHours()).slice(-2);
    const minutes = ('0' + date.getMinutes()).slice(-2);
    const seconds = ('0' + date.getSeconds()).slice(-2);

    // Return formatted date string
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`
}