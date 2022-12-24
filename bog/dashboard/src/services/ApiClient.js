/**
 * ApiClient.
 *
 * Functions for retrieving data from REST API endpoints.
 *
*/

let baseUrl = process.env.REACT_APP_BASE_API_URL

export async function SearchForBuoys(searchParams){
    let data = {
        min_time: searchParams.startDate,
        max_time: searchParams.endDate,
        buoy_ids: searchParams.buoyIds,
        fisheries: searchParams.fisheries
    }
    let uri = encodeURI(`${baseUrl}/landingpage/buoysearch/`);
    return await PostToApi(uri, data);
}

export async function FetchLandingPageFilters(){
    let uri = encodeURI(`${baseUrl}/landingpage/filters/`);
    return await GetFromApi(uri);
}

export async function FetchStations(){
    let uri = encodeURI(`${baseUrl}/landingpage/stations/`);
    return await GetFromApi(uri);
}

export async function FetchBuoyReport(buoyId){
    let uri = encodeURI(`${baseUrl}/buoyreports/${buoyId}/`);
    return await GetFromApi(uri);
}

async function GetFromApi(url) {
    try {
        const response = await fetch(url, {"method": "GET" });
        return await response.json();
    } 
    catch (err) {
        console.log(err);
    }
}

async function PostToApi(url, data) {
    try {
        const response = await fetch(url, {
            "headers": {
                "Content-Type": "application/json"
            },
            "body": JSON.stringify(data),
            "method": "POST"
        });
        return await response.json();
    } 
    catch (err) {
        console.log(err);
    }
}