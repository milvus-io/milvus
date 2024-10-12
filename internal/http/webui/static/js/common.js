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
    const errorMessage = encodeURIComponent(error.message || 'Unknown error');
    window.location.href = `5xx.html?error=${errorMessage}`;
};

const fetchData = (url, localData) => {
    if (DEBUG_MODE) {
        return new Promise((resolve) => {
            resolve(JSON.parse(localData));
        });
    } else {
        return fetch(url)
            .then(response => response.json())
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