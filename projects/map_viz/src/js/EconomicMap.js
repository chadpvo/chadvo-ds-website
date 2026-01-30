// Sticky Header scroll effect (Kept for immediate feedback, though UI Manager handles most styling)
window.addEventListener('scroll', () => {
    const stickyHeader = document.getElementById('stickyHeader');
    if (stickyHeader) {
        if (window.scrollY > 50) {
            stickyHeader.classList.add('floating');
        } else {
            stickyHeader.classList.remove('floating');
        }
    }
});

// Global data storage
let usData, countiesData, cbsaData,zipData;
let currentLevel = 'states';
let allRegions = [];
let highlightedRegions = new Set();
let redfinData = {};
let redfinStateData = {};
let redfinCountyData = {};
let redfinCbsaData = {};

let globalYearMetadata = {};

// Color settings
let colorSettings = {
    border: '#999999',
    selected: '#627BC1',
    dimmed: '#CCCCCC',
    selectedNoShade: false,
    dimmedNoShade: false,
    choroLow: '#eff3ff',
    choroHigh: '#08519c',
    choroOpacity: 1.0,
    enhanceLabels: true
};

// ... (ALL YOUR ORIGINAL METRIC MAPS & HELPERS REMAIN HERE) ...
const metricYearMap = {
    'medianHouseholdIncome': 'householdEconomics',
    'povertyRate': 'householdEconomics',
    'medianHomeValue': 'housingValues',
    'medianGrossRent': 'housingValues',
    'medianOwnerCostsWithMortgage': 'housingValues',
    'medianOwnerCostsNoMortgage': 'housingValues',
    'totalHousingUnits': 'housingCharacteristics',
    'homeownershipRate': 'housingCharacteristics',
    'vacancyRate': 'housingCharacteristics',
    'medianYearBuilt': 'housingCharacteristics',
    'totalPopulation': 'demographics',
    'medianAge': 'demographics',
    'employmentRate': 'demographics',
    'unemploymentRate': 'demographics',
    'gdpTotal': 'gdp',
    'fmr0Bedroom': 'hudFMR',
    'fmr1Bedroom': 'hudFMR',
    'fmr2Bedroom': 'hudFMR',
    'fmr3Bedroom': 'hudFMR',
    'fmr4Bedroom': 'hudFMR',
    'medianFamilyIncome': 'hudIncomeLimits'
};

const metricLabels = {
    'medianHouseholdIncome': 'Median Household Income',
    'medianFamilyIncome': 'HUD Median Family Income',
    'povertyRate': 'Poverty Rate',
    'gdpTotal': 'Total GDP',
    'totalPopulation': 'Total Population',
    'medianAge': 'Median Age',
    'employmentRate': 'Employment Rate',
    'unemploymentRate': 'Unemployment Rate',
    'medianHomeValue': 'Median Home Value',
    'medianGrossRent': 'Median Gross Rent',
    'medianOwnerCostsWithMortgage': 'Owner Costs (Mortgage)',
    'medianOwnerCostsNoMortgage': 'Owner Costs (No Mortgage)',
    'totalHousingUnits': 'Total Housing Units',
    'occupiedUnits': 'Occupied Units',
    'vacantUnits': 'Vacant Units',
    'homeownershipRate': 'Homeownership Rate',
    'vacancyRate': 'Vacancy Rate',
    'medianYearBuilt': 'Median Year Built',
    'fmr0Bedroom': 'Studio FMR',
    'fmr1Bedroom': '1-Bed FMR',
    'fmr2Bedroom': '2-Bed FMR',
    'fmr3Bedroom': '3-Bed FMR',
    'fmr4Bedroom': '4-Bed FMR',
    'MEDIAN_SALE_PRICE': 'Median Sale Price',
    'MEDIAN_SALE_PRICE_YOY': 'Price YoY Change',
    'HOMES_SOLD': 'Homes Sold',
    'INVENTORY': 'Inventory',
    'MEDIAN_DOM': 'Days on Market'        
};

function formatMetricValue(metric, value) {
    if (value === null || value === undefined) return 'N/A';
    switch(metric) {
        case 'medianHouseholdIncome':
        case 'medianHomeValue':
        case 'medianGrossRent':
        case 'medianFamilyIncome':
        case 'medianOwnerCostsWithMortgage':
        case 'medianOwnerCostsNoMortgage':
        case 'fmr0Bedroom':
        case 'fmr1Bedroom':
        case 'fmr2Bedroom':
        case 'fmr3Bedroom':
        case 'fmr4Bedroom':
            return `$${value.toLocaleString()}`;
        case 'MEDIAN_SALE_PRICE':
            return `$${value.toLocaleString()}`;
        case 'MEDIAN_SALE_PRICE_YOY':
            return `${(value * 100).toFixed(1)}%`;
        case 'HOMES_SOLD':
        case 'INVENTORY':
        case 'MEDIAN_DOM':
            return value.toLocaleString();    
        case 'gdpTotal':
            return `$${value.toLocaleString()}M`;
        case 'employmentRate':
        case 'unemploymentRate':
        case 'homeownershipRate':
        case 'vacancyRate':
        case 'povertyRate':
            return `${value}%`;
        case 'totalPopulation':
        case 'totalHousingUnits':
            return value.toLocaleString();
        default:
            return value.toLocaleString();
    }
}

function interpolateColor(color1, color2, factor) {
    const c1 = parseInt(color1.slice(1), 16);
    const c2 = parseInt(color2.slice(1), 16);
    const r1 = (c1 >> 16) & 0xff;
    const g1 = (c1 >> 8) & 0xff;
    const b1 = c1 & 0xff;
    const r2 = (c2 >> 16) & 0xff;
    const g2 = (c2 >> 8) & 0xff;
    const b2 = c2 & 0xff;
    const r = Math.round(r1 + factor * (r2 - r1));
    const g = Math.round(g1 + factor * (g2 - g1));
    const b = Math.round(b1 + factor * (b2 - b1));
    return `#${((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1)}`;
}

// Notification Logic
let notificationTimeout;
function showNotification(message) {
    const toast = document.getElementById('data-notification');
    const msgEl = document.getElementById('notification-message');
    if (toast && msgEl) {
        msgEl.innerHTML = message;
        toast.classList.add('show');
        clearTimeout(notificationTimeout);
        notificationTimeout = setTimeout(() => { closeNotification(); }, 4000);
    }
}
function closeNotification() {
    const toast = document.getElementById('data-notification');
    if (toast) toast.classList.remove('show');
}

function enhanceLabels() {
    if (!map || !map.getStyle()) return;
    const layers = map.getStyle().layers;
    layers.forEach(layer => {
        if (layer.type === 'symbol' && layer.layout && layer.layout['text-field']) {
            if (colorSettings.enhanceLabels) {
                map.setPaintProperty(layer.id, 'text-halo-color', '#ffffff');
                map.setPaintProperty(layer.id, 'text-halo-width', 2);
                map.setPaintProperty(layer.id, 'text-halo-blur', 1);
            } else {
                map.setPaintProperty(layer.id, 'text-halo-width', 0);
            }
        }
    });
}

const stateFipsToAbbr = {
    1: 'AL', 2: 'AK', 4: 'AZ', 5: 'AR', 6: 'CA', 8: 'CO', 9: 'CT', 10: 'DE', 11: 'DC', 12: 'FL',
    13: 'GA', 15: 'HI', 16: 'ID', 17: 'IL', 18: 'IN', 19: 'IA', 20: 'KS', 21: 'KY', 22: 'LA', 23: 'ME',
    24: 'MD', 25: 'MA', 26: 'MI', 27: 'MN', 28: 'MS', 29: 'MO', 30: 'MT', 31: 'NE', 32: 'NV', 33: 'NH',
    34: 'NJ', 35: 'NM', 36: 'NY', 37: 'NC', 38: 'ND', 39: 'OH', 40: 'OK', 41: 'OR', 42: 'PA', 44: 'RI',
    45: 'SC', 46: 'SD', 47: 'TN', 48: 'TX', 49: 'UT', 50: 'VT', 51: 'VA', 53: 'WA', 54: 'WV', 55: 'WI',
    56: 'WY', 60: 'AS', 66: 'GU', 69: 'MP', 72: 'PR', 78: 'VI'
};

let economicData = { states: {}, counties: {}, cbsas: {} };
let currentMetric = 'none';

async function loadEconomicData() {
    try {
        // Ensure this path matches where you put the JSON files relative to this HTML
        // Add cache busting to force reload of data files
        const cacheBuster = `?v=${Date.now()}`;
        const [statesResponse, countiesResponse, cbsasResponse] = await Promise.all([
            fetch(`data/states_economic_data.json${cacheBuster}`).then(r => r.json()),
            fetch(`data/counties_economic_data.json${cacheBuster}`).then(r => r.json()),
            fetch(`data/cbsas_economic_data.json${cacheBuster}`).then(r => r.json())
        ]);

        console.log('=== ECONOMIC DATA LOADING DEBUG ===');
        console.log('States response structure:', Object.keys(statesResponse));
        console.log('States has data key?', 'data' in statesResponse);

        economicData.states = statesResponse.data || statesResponse;
        economicData.counties = countiesResponse.data || countiesResponse;
        economicData.cbsas = cbsasResponse.data || cbsasResponse;

        console.log('States loaded count:', Object.keys(economicData.states).length);
        console.log('Counties loaded count:', Object.keys(economicData.counties).length);
        console.log('CBSAs loaded count:', Object.keys(economicData.cbsas).length);

        // Check if first state has Redfin data
        const firstStateKey = Object.keys(economicData.states)[0];
        if (firstStateKey) {
            const firstState = economicData.states[firstStateKey];
            console.log('First state sample:', firstStateKey, firstState.name);
            console.log('Has MEDIAN_SALE_PRICE?', 'MEDIAN_SALE_PRICE' in firstState, firstState.MEDIAN_SALE_PRICE);
            console.log('Has MEDIAN_SALE_PRICE_RANK?', 'MEDIAN_SALE_PRICE_RANK' in firstState, firstState.MEDIAN_SALE_PRICE_RANK);
        }
        
        [economicData.states, economicData.counties, economicData.cbsas].forEach(dataset => {
            Object.values(dataset).forEach(item => {
                if (item.vacancyRate === undefined && item.vacantUnits && item.totalHousingUnits) {
                    item.vacancyRate = parseFloat(((item.vacantUnits / item.totalHousingUnits) * 100).toFixed(1));
                }
                if (item.homeownershipRate === undefined && item.ownerOccupied && item.occupiedUnits) {
                    item.homeownershipRate = parseFloat(((item.ownerOccupied / item.occupiedUnits) * 100).toFixed(1));
                }
            });
        });

        if (statesResponse.metadata && statesResponse.metadata.yearsUsed) {
            globalYearMetadata = statesResponse.metadata.yearsUsed;
        }

        const allYears = Object.values(globalYearMetadata).filter(y => typeof y === 'number');
        const maxYear = allYears.length > 0 ? Math.max(...allYears) : new Date().getFullYear();
        
        const yearDisplay = document.getElementById('data-year-display');
        if (yearDisplay) yearDisplay.textContent = maxYear;

        console.log('✓ Economic data loaded and rates calculated');
        updateMetricDescriptions();
        
    } catch (error) {
        console.error('Error loading economic data:', error);
        const overlay = document.getElementById('loadingOverlay');
        if(overlay) overlay.innerHTML = '<div class="text-danger">Error loading data.</div>';
    }
}

async function mergeRedfinIntoEconomicData() {
    console.log('\n=== MERGING REDFIN DATA INTO ECONOMIC DATA ===');
    
    const sampleRedfinCounty = Object.entries(redfinCountyData)[0];
    const sampleEconCounty = Object.entries(economicData.counties)[0];
    console.log('Sample Redfin County key:', sampleRedfinCounty?.[0], 'data:', sampleRedfinCounty?.[1]);
    console.log('Sample Econ County key:', sampleEconCounty?.[0], 'fips:', sampleEconCounty?.[1]?.fips);
    // Merge State data
    if (Object.keys(redfinStateData).length > 0) {
        let stateMatches = 0;
        Object.values(economicData.states).forEach(state => {
            const stateCode = state.stateCode || state.STATE_CODE;
            if (stateCode && redfinStateData[stateCode]) {
                const redfinRow = redfinStateData[stateCode];
                // Copy all Redfin properties into the state object
                Object.keys(redfinRow).forEach(key => {
                    state[key] = redfinRow[key];
                });
                stateMatches++;
            }
        });
        console.log(`✓ Merged Redfin data into ${stateMatches} states`);
    }
    
    // Merge County data
    if (Object.keys(redfinCountyData).length > 0) {
        let countyMatches = 0;
        Object.values(economicData.counties).forEach(county => {
            const fips = county.fips || county.FIPS || county.geoid || county.GEOID;
            if (fips && redfinCountyData[fips]) {
                const redfinRow = redfinCountyData[fips];
                // Copy all Redfin properties into the county object
                Object.keys(redfinRow).forEach(key => {
                    county[key] = redfinRow[key];
                });
                countyMatches++;
            }
        });
        console.log(`✓ Merged Redfin data into ${countyMatches} counties`);
    }

    // Merge CBSA data
    if (Object.keys(redfinCbsaData).length > 0) {
        let cbsaMatches = 0;
        Object.values(economicData.cbsas).forEach(cbsa => {
            const cbsaCode = cbsa.cbsaCode || cbsa.CBSA_CODE || cbsa.geoid || cbsa.GEOID;
            if (cbsaCode && redfinCbsaData[cbsaCode]) {
                const redfinRow = redfinCbsaData[cbsaCode];
                // Copy all Redfin properties into the CBSA object
                Object.keys(redfinRow).forEach(key => {
                    cbsa[key] = redfinRow[key];
                });
                cbsaMatches++;
            }
        });
        console.log(`✓ Merged Redfin data into ${cbsaMatches} CBSAs`);
    }

    console.log('=== REDFIN MERGE COMPLETE ===\n');
}

function updateMetricDescriptions() {
    const getYear = (key) => globalYearMetadata[metricYearMap[key]] || 'Latest';
    const metricDescriptions = {
        'medianHouseholdIncome': `Average household income (${getYear('medianHouseholdIncome')} ACS)`,
        'medianFamilyIncome': `HUD Median Family Income (${getYear('medianFamilyIncome')} HUD)`,
        'employmentRate': `Employed / Labor Force (${getYear('employmentRate')} ACS)`,
        'unemploymentRate': `Unemployed / Labor Force (${getYear('unemploymentRate')} ACS)`,
        'gdpTotal': `Total economic output (${getYear('gdpTotal')} BEA)`,
        'medianHomeValue': `Median owner-occupied value (${getYear('medianHomeValue')} ACS)`,
        'medianGrossRent': `Median gross rent (${getYear('medianGrossRent')} ACS)`,
        'homeownershipRate': `Owner-occupied / Total occupied (${getYear('homeownershipRate')} ACS)`,
        'vacancyRate': `Vacant units / Total units (${getYear('vacancyRate')} ACS)`,
        'fmr2Bedroom': `HUD Fair Market Rent (2-Bed) (${getYear('fmr2Bedroom')})`,
        'povertyRate': `Percentage below poverty line (${getYear('povertyRate')} ACS)`
    };
    
    window.metricDescriptions = metricDescriptions;
    const helpEl = document.getElementById('metric-help');
    if (helpEl && currentMetric !== 'none') {
        helpEl.textContent = metricDescriptions[currentMetric] || '';
    }
}

mapboxgl.accessToken = 'pk.eyJ1IjoiY2hhZHZvIiwiYSI6ImNtZWdhcjUwMDEycm8ybm9lOTRqeHZqdjAifQ.ol7GJKQKjRAQZ33trNvWWA';

const usBounds = [[-125.0, 24.0], [-66.0, 49.5]];

const map = new mapboxgl.Map({
    container: 'map',
    style: 'mapbox://styles/mapbox/light-v11',
    center: [-98.5795, 39.8283],
    zoom: 4.5,
    minZoom: 2.5,
    maxZoom: 12,
    renderWorldCopies: false,
    projection: 'mercator'
});

map.scrollZoom.enable();
map.dragPan.enable();
map.doubleClickZoom.enable();
map.touchZoomRotate.enable();

async function loadGeoData() {
    try {
        const [us, counties, cbsas] = await Promise.all([
            d3.json('https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json'),
            d3.json('https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json'),
            d3.json('assets/us-cbsa.json')
        ]);

        usData = us;
        countiesData = counties;
        cbsaData = topojson.feature(cbsas, cbsas.objects.cb_2021_us_cbsa_500k);
        
        // Load Redfin ZIP data
        try {
            const redfinResponse = await fetch('data/redfin/processed/redfin_latest_optimized.json');
            if (!redfinResponse.ok) {
                throw new Error('Failed to load Redfin JSON');
            }
            const redfinJson = await redfinResponse.json();
            redfinJson.data.forEach(row => {
                if (row.ZIP != null) {
                    const zip = String(row.ZIP).padStart(5, '0');
                    redfinData[zip] = row;
                }
            });
            console.log('✓ Redfin ZIP data loaded:', Object.keys(redfinData).length, 'ZIPs');
        } catch (e) {
            console.warn('Redfin ZIP loading error:', e);
        }

        
        try {
            const redfinStateResponse = await fetch('data/redfin/processed/redfin_state_aggregated.json');
            if (redfinStateResponse.ok) {
                const redfinStateJson = await redfinStateResponse.json();
                
                // Find latest period
                let latestPeriod = null;
                redfinStateJson.data.forEach(row => {
                    if (row.PERIOD_END && (!latestPeriod || row.PERIOD_END > latestPeriod)) {
                        latestPeriod = row.PERIOD_END;
                    }
                });
                
                // Load only latest period + "All Residential" property type
                redfinStateJson.data.forEach(row => {
                    if (row.PERIOD_END === latestPeriod && 
                        row.PROPERTY_TYPE === 'All Residential' &&
                        row.STATE_CODE) {
                        redfinStateData[row.STATE_CODE] = row;
                    }
                });
                console.log('✓ Redfin State data loaded:', Object.keys(redfinStateData).length, 'states (latest period, All Residential)');
            }
        } catch (e) {
            console.warn('Redfin State loading error:', e);
        }

        // Load Redfin County data
        try {
            const redfinCountyResponse = await fetch('data/redfin/processed/redfin_county_aggregated.json');
            if (redfinCountyResponse.ok) {
                const redfinCountyJson = await redfinCountyResponse.json();

                // Find latest period
                let latestPeriod = null;
                redfinCountyJson.data.forEach(row => {
                    if (row.PERIOD_END && (!latestPeriod || row.PERIOD_END > latestPeriod)) {
                        latestPeriod = row.PERIOD_END;
                    }
                });

                // Load only latest period + "All Residential" property type
                redfinCountyJson.data.forEach(row => {
                    if (row.PERIOD_END === latestPeriod &&
                        row.PROPERTY_TYPE === 'All Residential' &&
                        row.COUNTY_FIPS) {  // ✅ NOW USING COUNTY_FIPS
                        redfinCountyData[row.COUNTY_FIPS] = row;
                    }
                });
                console.log('✓ Redfin County data loaded:', Object.keys(redfinCountyData).length, 'counties (latest period, All Residential)');
            }
        } catch (e) {
            console.warn('Redfin County loading error:', e);
        }

        // Load Redfin CBSA data
        try {
            const redfinCbsaResponse = await fetch('data/redfin/processed/redfin_cbsa_aggregated.json');
            if (redfinCbsaResponse.ok) {
                const redfinCbsaJson = await redfinCbsaResponse.json();

                // Find latest period
                let latestPeriod = null;
                redfinCbsaJson.data.forEach(row => {
                    if (row.PERIOD_END && (!latestPeriod || row.PERIOD_END > latestPeriod)) {
                        latestPeriod = row.PERIOD_END;
                    }
                });

                // Load only latest period + "All Residential" property type
                redfinCbsaJson.data.forEach(row => {
                    if (row.PERIOD_END === latestPeriod &&
                        row.PROPERTY_TYPE === 'All Residential' &&
                        row.TABLE_ID) {
                        redfinCbsaData[row.TABLE_ID] = row;
                    }
                });
                console.log('✓ Redfin CBSA data loaded:', Object.keys(redfinCbsaData).length, 'CBSAs (latest period, All Residential)');
            }
        } catch (e) {
            console.warn('Redfin CBSA loading error:', e);
        }

        await mergeRedfinIntoEconomicData();

        addBoundariesToMap();
        populateRegionList();
        setupColorControls();
        
        const overlay = document.getElementById('loadingOverlay');
        if(overlay) overlay.classList.add('hidden');
        
        return Promise.resolve();
    } catch (error) {
        console.error('Error:', error);
        const overlay = document.getElementById('loadingOverlay');
        if(overlay) overlay.innerHTML = '<div class="text-danger">Error loading data.</div>';
        return Promise.reject(error);
    }
}

function createZipPointsFromRedfin() {
    // Create GeoJSON from Redfin data with coordinates
    const features = Object.entries(redfinData)
        .map(([zip, data]) => {
            if (!data.LATITUDE || !data.LONGITUDE) return null;
            
            return {
                type: 'Feature',
                properties: {
                    ZIP: zip,
                    CITY_NAME: data.CITY_NAME,
                    STATE_ABBREV: data.STATE_ABBREV
                },
                geometry: {
                    type: 'Point',
                    coordinates: [data.LONGITUDE, data.LATITUDE]
                }
            };
        })
        .filter(f => f !== null);

    zipData = {
        type: 'FeatureCollection',
        features: features
    };
    
    console.log('✓ ZIP points created from Redfin:', features.length);
}

function addBoundariesToMap() {
    const layers = map.getStyle().layers;
    let firstLabelLayerId;

    for (const layer of layers) {
        if (layer.type === 'symbol' && layer.layout['text-field']) {
            firstLabelLayerId = layer.id;
            break;
        }
    }

    /* =======================
    STATES
    ======================= */
    map.addSource('states', {
        type: 'geojson',
        data: topojson.feature(usData, usData.objects.states)
    });

    map.addLayer({
        id: 'states-fill',
        type: 'fill',
        source: 'states',
        paint: {
            'fill-color': colorSettings.dimmed,
            'fill-opacity': 0
        }
    }, firstLabelLayerId);

    map.addLayer({
        id: 'states-layer',
        type: 'line',
        source: 'states',
        paint: {
            'line-color': colorSettings.border,
            'line-width': 0.3,
            'line-opacity': 0
        }
    }, firstLabelLayerId);

    /* =======================
    COUNTIES
    ======================= */
    map.addSource('counties', {
        type: 'geojson',
        data: topojson.feature(countiesData, countiesData.objects.counties)
    });

    map.addLayer({
        id: 'counties-fill',
        type: 'fill',
        source: 'counties',
        paint: {
            'fill-color': colorSettings.dimmed,
            'fill-opacity': 0
        }
    }, firstLabelLayerId);

    map.addLayer({
        id: 'counties-layer',
        type: 'line',
        source: 'counties',
        paint: {
            'line-color': colorSettings.border,
            'line-width': 0.3,
            'line-opacity': 0
        }
    }, firstLabelLayerId);

    /* =======================
    CBSAs
    ======================= */
    if (cbsaData) {
        map.addSource('cbsas', {
            type: 'geojson',
            data: cbsaData,
            promoteId: 'GEOID'
        });

        map.addLayer({
            id: 'cbsas-fill',
            type: 'fill',
            source: 'cbsas',
            paint: {
                'fill-color': colorSettings.dimmed,
                'fill-opacity': 0
            }
        }, firstLabelLayerId);

        map.addLayer({
            id: 'cbsas-layer',
            type: 'line',
            source: 'cbsas',
            paint: {
                'line-color': colorSettings.border,
                'line-width': 0.4,
                'line-opacity': 0
            }
        }, firstLabelLayerId);
    }

    /* =======================
    ZIP CODES (VECTOR TILES)
    ======================= */
    map.addSource('zips', {
        type: 'vector',
        url: 'mapbox://chadvo.90yt5vnd',
        promoteId: 'GEOID'  
    });

    map.addLayer({
        id: 'zips-fill',
        type: 'fill',
        source: 'zips',
        'source-layer': 'us-zips-simplified-ahb14j',
        paint: {
            'fill-color': colorSettings.dimmed,
            'fill-opacity': 0
        },
    }, firstLabelLayerId);

    map.addLayer({
        id: 'zips-layer',
        type: 'line',
        source: 'zips',
        'source-layer': 'us-zips-simplified-ahb14j',
        paint: {
            'line-color': colorSettings.border,
            'line-width': [
                'interpolate',
                ['linear'],
                ['zoom'],
                4, 0.1,
                7, 0.4,
                10, 1
            ],
            'line-opacity': 0
        }
    }, firstLabelLayerId);
}

// Store whether we've set up the dynamic feature state handler
let zipFeatureStateHandlerSetup = false;

async function applyRedfinDataToZips() {
    // Determine which Redfin dataset to use based on current level
    console.log('🔵 applyRedfinDataToZips() called, currentLevel:', currentLevel);
    console.log('🔵 redfinData keys:', Object.keys(redfinData).length);
    console.log('🔵 redfinStateData keys:', Object.keys(redfinStateData).length);
    console.log('🔵 redfinCountyData keys:', Object.keys(redfinCountyData).length);
    let activeRedfinData = {};
    let redfinKeyField = null;
    let sourceLayer = null;
    let sourceId = null;
    
    if (currentLevel === 'zips' && Object.keys(redfinData).length > 0) {
        activeRedfinData = redfinData;
        redfinKeyField = 'ZIP';
        sourceLayer = 'us-zips-simplified-ahb14j';
        sourceId = 'zips';
    } else if (currentLevel === 'states' && Object.keys(redfinStateData).length > 0) {
        activeRedfinData = redfinStateData;
        redfinKeyField = 'STATE_CODE';
        sourceLayer = 'us-states-simplified';
        sourceId = 'states';
    } else if (currentLevel === 'counties' && Object.keys(redfinCountyData).length > 0) {
        activeRedfinData = redfinCountyData;
        redfinKeyField = 'TABLE_ID';
        sourceLayer = 'us-counties-simplified-80n5rl';
        sourceId = 'counties';
    } else if (currentLevel === 'cbsa' && Object.keys(redfinCbsaData).length > 0) {
        activeRedfinData = redfinCbsaData;
        redfinKeyField = 'TABLE_ID';
        sourceLayer = 'us-cbsa-simplified-6uc2hv';
        sourceId = 'cbsa';
    }
    
    if (Object.keys(activeRedfinData).length === 0) {
        console.warn(`No Redfin data available for ${currentLevel}`);
        return;
    }

    return new Promise((resolve) => {
        if (!map.isSourceLoaded(sourceId)) {
            map.once('sourcedata', (e) => {
                if (e.sourceId === sourceId && e.isSourceLoaded) {
                    applyRedfinDataToZips().then(resolve);
                }
            });
            return;
        }

        console.log(`Applying Redfin data to ${currentLevel} features...`);

        // Apply feature states to currently loaded features
        function applyToLoadedFeatures() {
            const features = map.querySourceFeatures(sourceId, { sourceLayer });

            if (features.length === 0) {
                console.log(`No ${currentLevel} features loaded yet`);
                return 0;
            }

            let successCount = 0;

            features.forEach(feature => {
                const featureId = feature.id;
                let matchingData = null;

                // Different matching logic based on level
                if (currentLevel === 'zips') {
                    // ZIP matching logic
                    const geoidProp = feature.properties?.GEOID;
                    
                    if (activeRedfinData[featureId]) {
                        matchingData = activeRedfinData[featureId];
                    } else if (activeRedfinData[String(featureId).padStart(5, '0')]) {
                        matchingData = activeRedfinData[String(featureId).padStart(5, '0')];
                    } else if (activeRedfinData[parseInt(String(featureId), 10)]) {
                        matchingData = activeRedfinData[parseInt(String(featureId), 10)];
                    } else if (geoidProp && activeRedfinData[geoidProp]) {
                        matchingData = activeRedfinData[geoidProp];
                    } else if (geoidProp && activeRedfinData[String(geoidProp).padStart(5, '0')]) {
                        matchingData = activeRedfinData[String(geoidProp).padStart(5, '0')];
                    }
                } else if (currentLevel === 'states') {
                    // State matching logic
                    const stateCode = feature.properties?.STUSPS;
                    if (stateCode && activeRedfinData[stateCode]) {
                        matchingData = activeRedfinData[stateCode];
                    }
                } else if (currentLevel === 'counties') {
                    // County matching logic
                    const geoid = feature.properties?.GEOID;
                    if (geoid && activeRedfinData[geoid]) {
                        matchingData = activeRedfinData[geoid];
                    }
                } else if (currentLevel === 'cbsa') {
                    // CBSA matching logic
                    const geoid = feature.properties?.GEOID;
                    if (geoid && activeRedfinData[geoid]) {
                        matchingData = activeRedfinData[geoid];
                    }
                }

                if (matchingData && featureId !== undefined) {
                    try {
                        map.setFeatureState(
                            { source: sourceId, sourceLayer: sourceLayer, id: featureId },
                            {
                                // Original values (for tooltips)
                                MEDIAN_SALE_PRICE: matchingData.MEDIAN_SALE_PRICE,
                                MEDIAN_SALE_PRICE_YOY: matchingData.MEDIAN_SALE_PRICE_YOY,
                                HOMES_SOLD: matchingData.HOMES_SOLD,
                                INVENTORY: matchingData.INVENTORY,
                                MEDIAN_DOM: matchingData.MEDIAN_DOM,

                                // Log-transformed values (optional)
                                MEDIAN_SALE_PRICE_LOG: matchingData.MEDIAN_SALE_PRICE_LOG,
                                HOMES_SOLD_LOG: matchingData.HOMES_SOLD_LOG,
                                INVENTORY_LOG: matchingData.INVENTORY_LOG,
                                MEDIAN_DOM_LOG: matchingData.MEDIAN_DOM_LOG,

                                // RANK values (for smooth gradients)
                                MEDIAN_SALE_PRICE_RANK: matchingData.MEDIAN_SALE_PRICE_RANK,
                                MEDIAN_SALE_PRICE_YOY_RANK: matchingData.MEDIAN_SALE_PRICE_YOY_RANK,
                                HOMES_SOLD_RANK: matchingData.HOMES_SOLD_RANK,
                                INVENTORY_RANK: matchingData.INVENTORY_RANK,
                                MEDIAN_DOM_RANK: matchingData.MEDIAN_DOM_RANK
                            }
                        );
                        successCount++;
                    } catch (e) {
                        // Feature state failed
                    }
                }
            });

            return successCount;
        }

        // Initial application
        const successCount = applyToLoadedFeatures();
        console.log(`✓ Applied Redfin data to ${successCount} loaded ${currentLevel} features`);

        // Set up dynamic handler to apply data as new tiles load (only for zips)
        if (currentLevel === 'zips' && !zipFeatureStateHandlerSetup) {
            zipFeatureStateHandlerSetup = true;

            map.on('sourcedata', (e) => {
                if (e.sourceId === 'zips' && e.isSourceLoaded && !e.tile) {
                    if (currentLevel === 'zips' && Object.keys(redfinData).length > 0) {
                        applyToLoadedFeatures();
                    }
                }
            });

            console.log('✓ Dynamic ZIP feature state handler installed');
        }

        resolve();
    });
}

function debugZipFeatureState() {
    console.log('=== ZIP CODE CHOROPLETH DEBUG ===');
    console.log('Current level:', currentLevel);
    console.log('Current metric:', currentMetric);
    console.log('Zoom level:', map.getZoom());
    console.log('Redfin data entries:', Object.keys(redfinData).length);

    // Query ZIP features at the current map view
    const renderedFeatures = map.queryRenderedFeatures({ layers: ['zips-fill'] });
    const sourceFeatures = map.querySourceFeatures('zips', {
        sourceLayer: 'us-zips-simplified-ahb14j'
    });

    console.log('ZIP features rendered:', renderedFeatures.length);
    console.log('ZIP features in source (loaded tiles):', sourceFeatures.length);

    // Check layer visibility
    console.log('\n--- Layer Status ---');
    console.log('zips-fill opacity:', map.getPaintProperty('zips-fill', 'fill-opacity'));
    console.log('zips-fill color:', map.getPaintProperty('zips-fill', 'fill-color'));

    if (sourceFeatures.length > 0) {
        // Test first 3 features
        const samplesToTest = Math.min(3, sourceFeatures.length);
        console.log(`\n--- Testing ${samplesToTest} sample features ---`);

        for (let i = 0; i < samplesToTest; i++) {
            const feature = sourceFeatures[i];
            console.log(`\nSample ${i + 1}:`);
            console.log('  Feature ID:', feature.id, `(type: ${typeof feature.id})`);
            console.log('  GEOID property:', feature.properties.GEOID);

            // Check feature state
            const state = map.getFeatureState({
                source: 'zips',
                sourceLayer: 'us-zips-simplified-ahb14j',
                id: feature.id
            });
            console.log('  Feature state:', state);
            console.log('  Has Redfin data:', state.MEDIAN_SALE_PRICE !== undefined);

            // Check if Redfin data exists for this ZIP in various formats
            const formats = [
                feature.id,
                String(feature.id),
                String(feature.id).padStart(5, '0'),
                parseInt(String(feature.id), 10),
                feature.properties.GEOID,
                String(feature.properties.GEOID).padStart(5, '0')
            ];

            const matches = formats.filter(fmt => redfinData[fmt]);
            if (matches.length > 0) {
                console.log('  ✓ Found in Redfin data with format:', matches[0], `(type: ${typeof matches[0]})`);
            } else {
                console.log('  ✗ NOT found in Redfin data (tried', formats.length, 'formats)');
            }
        }

        // Summary
        console.log('\n--- Summary ---');
        console.log('Redfin data key types:', typeof Object.keys(redfinData)[0]);
        console.log('Redfin data sample keys:', Object.keys(redfinData).slice(0, 5));

        // Check how many features have data applied
        let withData = 0;
        sourceFeatures.forEach(f => {
            const state = map.getFeatureState({
                source: 'zips',
                sourceLayer: 'us-zips-simplified-ahb14j',
                id: f.id
            });
            if (state.MEDIAN_SALE_PRICE !== undefined) withData++;
        });
        console.log(`Features with data applied: ${withData} / ${sourceFeatures.length} (${(withData/sourceFeatures.length*100).toFixed(1)}%)`);

    } else if (map.getZoom() < 4) {
        console.log('⚠ Zoom in closer (zoom level 4+) to load ZIP tiles');
    } else {
        console.log('⚠ No ZIP features loaded. Check if tiles are loading.');
    }

    console.log('=== END DEBUG ===\n');
}

// Helper function to manually re-apply everything (for debugging)
window.forceZipChoropleth = function() {
    console.log('🔄 Force re-applying ZIP choropleth...');
    if (currentLevel !== 'zips') {
        console.warn('Not at ZIP level. Switch to ZIP level first.');
        return;
    }
    if (currentMetric === 'none') {
        console.warn('No metric selected. Select a Redfin metric first.');
        return;
    }

    applyRedfinDataToZips().then(() => {
        console.log('✓ Feature states re-applied');
        applyChoropleth();
        console.log('✓ Choropleth re-applied');
        console.log('Done! Run debugZipFeatureState() to verify.');
    });
};

// Make debug function available globally
window.debugZipFeatureState = debugZipFeatureState;

function populateRegionList() {
    const container = document.getElementById('region-checkboxes');
    if (!container) return;
    container.innerHTML = '';
    
    let features = [];
    if (currentLevel === 'states') features = topojson.feature(usData, usData.objects.states).features;
    else if (currentLevel === 'counties') features = topojson.feature(countiesData, countiesData.objects.counties).features;
    else if (currentLevel === 'cbsas') features = cbsaData.features;
    else if (currentLevel === 'zips') {
        // For ZIPs, create features from Redfin data
        features = Object.entries(redfinData).map(([zip, data]) => ({
            id: zip,
            properties: {
                ZIP: zip,
                GEOID: zip,
                CITY_NAME: data.CITY_NAME,
                STATE_ABBREV: data.STATE_ABBREV
            }
        }));
    }       
    allRegions = features.map(f => {
        let id, name;
        if (currentLevel === 'states') {
            id = f.id;
            name = f.properties.name;
        } else if (currentLevel === 'counties') {
            id = f.id;
            name = f.properties.name;
            const countyFipsStr = String(id).padStart(5, '0');
            const stateFips = parseInt(countyFipsStr.substring(0, 2));
            const stateAbbr = stateFipsToAbbr[stateFips];
            if (stateAbbr) name = `${name}, ${stateAbbr}`;
        } else if (currentLevel === 'cbsas') {
            id = f.properties.GEOID || f.properties.CBSAFP || f.properties.AFFGEOID;
            name = f.properties.NAME || f.properties.name || 'Unknown';
        } else if (currentLevel === 'zips') {
            id = f.properties.ZIP || f.properties.GEOID;
            name = `ZIP ${id}`;
            const cityName = f.properties.CITY_NAME;
            const stateAbbr = f.properties.STATE_ABBREV;
            if (cityName && stateAbbr) name = `${id} - ${cityName}, ${stateAbbr}`;
        }
        return { id, name, feature: f };
    }).filter(r => r.id && r.name).sort((a, b) => a.name.localeCompare(b.name));
    
    allRegions.forEach(region => {
        const div = document.createElement('div');
        div.className = 'checkbox-item';
        div.dataset.regionName = region.name.toLowerCase();
        div.innerHTML = `<input type="checkbox" id="region-${region.id}" data-region-id="${region.id}"><label for="region-${region.id}">${region.name}</label>`;
        container.appendChild(div);
    });
    
    container.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.addEventListener('change', handleRegionCheckbox));
}

function handleRegionCheckbox(e) {
    const regionId = e.target.dataset.regionId;
    if (e.target.checked) highlightedRegions.add(regionId);
    else highlightedRegions.delete(regionId);
    updateHighlights();
}

function updateHighlights() {
    const sourceId = currentLevel === 'states' ? 'states' :
                    currentLevel === 'counties' ? 'counties' : 'cbsas';

    allRegions.forEach(region => {
        map.setFeatureState({ source: sourceId, id: region.id }, { highlighted: false, dimmed: false });
    });
    
    if (highlightedRegions.size > 0) {
        allRegions.forEach(region => {
            map.setFeatureState({ source: sourceId, id: region.id }, { dimmed: true });
        });
        
        highlightedRegions.forEach(regionId => {
            map.setFeatureState({ source: sourceId, id: regionId }, { highlighted: true, dimmed: false });
        });
        
        const bounds = new mapboxgl.LngLatBounds();
        highlightedRegions.forEach(regionId => {
            const region = allRegions.find(r => r.id == regionId);
            if (region && region.feature.geometry) {
                const coords = getFeatureCoordinates(region.feature);
                coords.forEach(coord => bounds.extend(coord));
            }
        });
        map.fitBounds(bounds, { padding: 50, duration: 1000 });
    }
}

function getFeatureCoordinates(feature) {
    const coords = [];
    const geometry = feature.geometry;
    if (geometry.type === 'Polygon') geometry.coordinates[0].forEach(coord => coords.push(coord));
    else if (geometry.type === 'MultiPolygon') geometry.coordinates.forEach(poly => poly[0].forEach(coord => coords.push(coord)));
    return coords;
}

function getColorForValue(value, metric, allValues) {
    if (value === null || value === undefined) return colorSettings.dimmed;
    if (allValues.length === 0) return colorSettings.dimmed;
    const sortedValues = [...allValues].sort((a, b) => a - b);
    const min = sortedValues[0];
    const max = sortedValues[sortedValues.length - 1];
    if (max === min) return interpolateColor(colorSettings.choroLow, colorSettings.choroHigh, 0.5);
    const normalized = (value - min) / (max - min);
    return interpolateColor(colorSettings.choroLow, colorSettings.choroHigh, normalized);
}

function applyChoropleth() {
    const choroplethSettings = document.getElementById('choropleth-settings');
    if (choroplethSettings) choroplethSettings.style.display = currentMetric !== 'none' ? 'block' : 'none';
    
    if (currentMetric === 'none') {
        const layerFill = `${currentLevel}-fill`;
        if (map.getLayer(layerFill)) {
            map.setPaintProperty(layerFill, 'fill-color', ['case', ['boolean', ['feature-state', 'highlighted'], false], colorSettings.selected, colorSettings.dimmed]);
            map.setPaintProperty(layerFill, 'fill-opacity', ['case', ['boolean', ['feature-state', 'highlighted'], false], colorSettings.selectedNoShade ? 0 : 0.6, ['boolean', ['feature-state', 'dimmed'], false], colorSettings.dimmedNoShade ? 0 : 0.15, 0]);
        }
        highlightedRegions.clear();
        document.querySelectorAll('.checkbox-item input').forEach(cb => cb.checked = false);
        return;
    }

    // Check if this is a Redfin metric
    const redfinMetrics = ['MEDIAN_SALE_PRICE', 'MEDIAN_SALE_PRICE_YOY', 'HOMES_SOLD', 'INVENTORY', 'MEDIAN_DOM'];
    const isRedfinMetric = redfinMetrics.includes(currentMetric);

    // For ZIP level with Redfin metrics, use feature-state interpolation
    if (currentLevel === 'zips' && isRedfinMetric) {
        console.log('=== APPLYING ZIP CHOROPLETH ===');
        console.log('Current metric:', currentMetric);
        console.log('Zoom level:', map.getZoom());

        // Check how many features have data
        const sourceFeatures = map.querySourceFeatures('zips', {
            sourceLayer: 'us-zips-simplified-ahb14j'
        });
        console.log('ZIP features currently loaded:', sourceFeatures.length);

        // Define which metrics have rank versions (for smooth gradients)
        const RANK_METRICS = [
            'MEDIAN_SALE_PRICE',
            'MEDIAN_SALE_PRICE_YOY',
            'MEDIAN_DOM',
            'INVENTORY',
            'HOMES_SOLD'
        ];

        // Use RANK version for smooth color distribution (0.0 to 1.0)
        const useRankVersion = RANK_METRICS.includes(currentMetric);
        const visualizationMetric = useRankVersion ? `${currentMetric}_RANK` : currentMetric;

        // For rank metrics, min is always 0.0 and max is always 1.0
        let minViz, maxViz;

        if (useRankVersion) {
            // Percentile ranks always range from 0.0 to 1.0
            minViz = 0.0;
            maxViz = 1.0;
            console.log(`Using RANK version: ${visualizationMetric} (range: 0.0 to 1.0)`);
        } else {
            // For non-rank metrics, calculate actual min/max
            const allValuesForViz = Object.values(redfinData)
                .map(d => d[visualizationMetric])
                .filter(v => v !== null && v !== undefined);

            minViz = allValuesForViz.length > 0 ? Math.min(...allValuesForViz) : 0;
            maxViz = allValuesForViz.length > 0 ? Math.max(...allValuesForViz) : 1;
            console.log(`Using raw metric: ${visualizationMetric} (range: ${minViz} to ${maxViz})`);
        }

        // Sample check: verify feature states are set
        if (sourceFeatures.length > 0) {
            const sampleFeature = sourceFeatures[0];
            const sampleState = map.getFeatureState({
                source: 'zips',
                sourceLayer: 'us-zips-simplified-ahb14j',
                id: sampleFeature.id
            });
            console.log('Sample feature state:', sampleState);
            console.log('Sample has metric data:', sampleState[visualizationMetric] !== undefined);
        }

        // Apply color using the visualization metric (_RANK for smooth gradients)
        const fillColorExpression = [
            'case',
            ['!=', ['feature-state', visualizationMetric], null],
            [
                'interpolate',
                ['linear'],
                ['feature-state', visualizationMetric],
                minViz, colorSettings.choroLow,
                maxViz, colorSettings.choroHigh
            ],
            colorSettings.dimmed  // Color for ZIPs without data
        ];

        const fillOpacityExpression = [
            'case',
            ['!=', ['feature-state', visualizationMetric], null],
            colorSettings.choroOpacity,
            0  // Hide ZIPs without data
        ];

        console.log('Setting paint properties...');
        console.log('Fill color expression:', JSON.stringify(fillColorExpression));
        console.log('Fill opacity:', colorSettings.choroOpacity);

        map.setPaintProperty('zips-fill', 'fill-color', fillColorExpression);
        map.setPaintProperty('zips-fill', 'fill-opacity', fillOpacityExpression);

        // Verify paint properties were set
        console.log('Paint property set successfully');
        console.log('Current fill-opacity paint property:', map.getPaintProperty('zips-fill', 'fill-opacity'));

        if (map.getZoom() < 4) {
            showNotification(`Zoom in to see ZIP code data (tiles load at zoom level 4+)`);
        }

        console.log('=== ZIP CHOROPLETH APPLIED ===\n');
        updateLegend();
        return;
    }

    // For ZIP level with non-Redfin metrics, show notification
    if (currentLevel === 'zips') {
        showNotification(`Economic indicators are not available for ZIP codes. Try <b>Redfin metrics</b> or switch to States/Counties/CBSAs.`);
        return;
    }

    // Handle states/counties/cbsas
    let dataset, layerId;

    if (currentLevel === 'states') {
        dataset = economicData.states;
        layerId = 'states-fill';
    }
    else if (currentLevel === 'counties') {
        dataset = economicData.counties;
        layerId = 'counties-fill';
    }
    else if (currentLevel === 'cbsas') {
        dataset = economicData.cbsas;
        layerId = 'cbsas-fill';
    }

    highlightedRegions.clear();
    document.querySelectorAll('.checkbox-item input').forEach(cb => cb.checked = false);

    // For Redfin metrics at states/counties/cbsas, use RANK version for smooth gradients
    if (isRedfinMetric) {
        const RANK_METRICS = [
            'MEDIAN_SALE_PRICE',
            'MEDIAN_SALE_PRICE_YOY',
            'MEDIAN_DOM',
            'INVENTORY',
            'HOMES_SOLD'
        ];

        const useRankVersion = RANK_METRICS.includes(currentMetric);
        const visualizationMetric = useRankVersion ? `${currentMetric}_RANK` : currentMetric;

        console.log(`=== REDFIN METRIC DEBUG ===`);
        console.log(`Applying Redfin metric ${currentMetric} at ${currentLevel} level`);
        console.log(`Using ${useRankVersion ? 'RANK' : 'raw'} version: ${visualizationMetric}`);
        console.log(`Dataset keys count:`, Object.keys(dataset).length);

        // Sample first entry to see structure
        const firstKey = Object.keys(dataset)[0];
        if (firstKey) {
            console.log(`Sample entry (${firstKey}):`, dataset[firstKey]);
            console.log(`Has ${visualizationMetric}?`, dataset[firstKey][visualizationMetric]);
        }

        // Get all rank values for color mapping
        const allRankValues = Object.values(dataset)
            .map(d => d[visualizationMetric])
            .filter(v => v !== null && v !== undefined);

        console.log(`Found ${allRankValues.length} values for ${visualizationMetric}`);
        console.log(`Sample values:`, allRankValues.slice(0, 5));

        if (allRankValues.length === 0) {
            console.log(`ERROR: No values found for ${visualizationMetric}`);

            // Provide context-specific message
            let message = `<b>${currentMetric}</b> data is not available for <b>${currentLevel}</b>.`;
            if (currentLevel === 'cbsas') {
                message += '<br><small>Note: Redfin data is currently available for <b>ZIP Codes</b>, <b>States</b>, and <b>Counties</b> only.</small>';
            }

            showNotification(message);
            return;
        }

        // For RANK metrics, min is ~0.0 and max is ~1.0
        const minRank = useRankVersion ? 0.0 : Math.min(...allRankValues);
        const maxRank = useRankVersion ? 1.0 : Math.max(...allRankValues);

        console.log(`Rank range: ${minRank} to ${maxRank}, ${allRankValues.length} regions with data`);

        // Build color expression using RANK values for smooth gradients
        let colorExpression;

        if (currentLevel === 'cbsas') {
            colorExpression = ['match', ['get', 'GEOID']];
        } else {
            colorExpression = ['match', ['id']];
        }

        Object.keys(dataset).forEach(key => {
            const data = dataset[key];
            const rankValue = data ? data[visualizationMetric] : null;

            if (rankValue !== null && rankValue !== undefined) {
                // Normalize rank to 0-1 range
                const normalized = (rankValue - minRank) / (maxRank - minRank);
                const color = interpolateColor(colorSettings.choroLow, colorSettings.choroHigh, normalized);

                let matchKey = currentLevel === 'cbsas' ? key : parseInt(key, 10);
                colorExpression.push(matchKey, color);
            }
        });

        colorExpression.push('rgba(0, 0, 0, 0)');
        map.setPaintProperty(layerId, 'fill-color', colorExpression);
        map.setPaintProperty(layerId, 'fill-opacity', colorSettings.choroOpacity);

        updateLegend();
        return;
    }

    // For non-Redfin metrics (Census/BEA/HUD data), use original approach
    const allValues = Object.values(dataset)
        .map(d => d[currentMetric])
        .filter(v => v !== null && v !== undefined);

    if (allValues.length === 0) {
        const selector = document.getElementById('metric-selector');
        const metricName = selector.options[selector.selectedIndex].text;
        showNotification(`<b>${metricName}</b> data is not available for <b>${currentLevel}</b>.`);
        return;
    }

    // Build color expression (original working code for states/counties/cbsas)
    let colorExpression;

    if (currentLevel === 'cbsas') {
        colorExpression = ['match', ['get', 'GEOID']];
    } else {
        colorExpression = ['match', ['id']];
    }

    Object.keys(dataset).forEach(key => {
        const data = dataset[key];
        const value = data ? data[currentMetric] : null;
        const color = getColorForValue(value, currentMetric, allValues);

        let matchKey = currentLevel === 'cbsas' ? key : parseInt(key, 10);
        colorExpression.push(matchKey, color);
    });

    colorExpression.push('rgba(0, 0, 0, 0)');
    map.setPaintProperty(layerId, 'fill-color', colorExpression);
    map.setPaintProperty(layerId, 'fill-opacity', colorSettings.choroOpacity);

    updateLegend(); 
}

function updateLegend() {
    const legend = document.getElementById('map-legend');
    const legendTitle = document.getElementById('legend-title');
    const legendGradient = document.getElementById('legend-gradient');
    if (!legend || !legendTitle || !legendGradient) return;
    
    if (currentMetric !== 'none') {
        legend.style.display = 'block';
        const selector = document.getElementById('metric-selector');
        const selectedOption = selector.options[selector.selectedIndex];
        legendTitle.textContent = selectedOption ? selectedOption.text : currentMetric;
        legendGradient.style.background = `linear-gradient(to right, ${colorSettings.choroLow}, ${colorSettings.choroHigh})`;
        
        // Calculate min/max values for current metric and level
        let allValues = [];
        const redfinMetrics = ['MEDIAN_SALE_PRICE', 'MEDIAN_SALE_PRICE_YOY', 'HOMES_SOLD', 'INVENTORY', 'MEDIAN_DOM'];
        const isRedfinMetric = redfinMetrics.includes(currentMetric);
        
        // Get the ORIGINAL values (not RANK) for legend display
        if (currentLevel === 'zips' && isRedfinMetric) {
            allValues = Object.values(redfinData)
                .map(d => d[currentMetric])  // Use original value, NOT _RANK
                .filter(v => v !== null && v !== undefined);
        } else {
            let dataset;
            if (currentLevel === 'states') dataset = economicData.states;
            else if (currentLevel === 'counties') dataset = economicData.counties;
            else if (currentLevel === 'cbsas') dataset = economicData.cbsas;
            
            if (dataset) {
                allValues = Object.values(dataset)
                    .map(d => d[currentMetric])  // Use original value, NOT _RANK
                    .filter(v => v !== null && v !== undefined);
            }
        }
        
        // Update legend labels with actual values
        if (allValues.length > 0) {
            const minValue = Math.min(...allValues);
            const maxValue = Math.max(...allValues);
            
            const legendLow = document.getElementById('legend-low');
            const legendHigh = document.getElementById('legend-high');
            
            if (legendLow) legendLow.textContent = formatMetricValue(currentMetric, minValue);
            if (legendHigh) legendHigh.textContent = formatMetricValue(currentMetric, maxValue);
        }
        
        // Add/Update metric definition in legend
        updateLegendMetricInfo();
    } else {
        legend.style.display = 'none';
    }
}

// Updated function to insert metric definition between title and gradient
function updateLegendMetricInfo() {
    const legend = document.getElementById('map-legend');
    const legendTitle = document.getElementById('legend-title');
    if (!legend || !legendTitle) return;
    // Check if metric info container exists, if not create it
    let metricInfoContainer = legend.querySelector('.legend-metric-info');
    if (!metricInfoContainer) {
        metricInfoContainer = document.createElement('div');
        metricInfoContainer.className = 'legend-metric-info';
        // Insert AFTER the title (which is the first child)
        legendTitle.insertAdjacentElement('afterend', metricInfoContainer);
    }
    // Get metric definition
    const metricInfo = metricDefinitions[currentMetric];
    
    if (metricInfo) {
        metricInfoContainer.innerHTML = `
            <div class="legend-metric-definition">${metricInfo.definition}</div>
            <div class="legend-metric-source">${metricInfo.source}</div>
        `;
    } else {
        metricInfoContainer.innerHTML = '';
    }
}

const searchInput = document.getElementById('search-input');
if (searchInput) {
    searchInput.addEventListener('input', (e) => {
        const query = e.target.value.toLowerCase().trim();
        document.querySelectorAll('.checkbox-item').forEach(item => {
            if (query === '' || item.dataset.regionName.includes(query)) item.classList.remove('hidden');
            else item.classList.add('hidden');
        });
    });
}

const geoLevelSelect = document.getElementById('geo-level');

if (geoLevelSelect) {
    geoLevelSelect.addEventListener('change', (e) => {
        currentLevel = e.target.value;
        highlightedRegions.clear();

        // 1. Turn everything OFF
        ['states', 'counties', 'cbsas', 'zips'].forEach(lvl => {
            if (map.getLayer(`${lvl}-layer`)) {
                map.setPaintProperty(`${lvl}-layer`, 'line-opacity', 0);
            }
            if (map.getLayer(`${lvl}-fill`)) {
                map.setPaintProperty(`${lvl}-fill`, 'fill-opacity', 0);
            }
        });

        // 2. Turn the selected geography ON
        if (map.getLayer(`${currentLevel}-layer`)) {
            map.setPaintProperty(`${currentLevel}-layer`, 'line-opacity', 1);
        }

        if (map.getLayer(`${currentLevel}-fill`)) {
            map.setPaintProperty(
                `${currentLevel}-fill`,
                'fill-opacity',
                [
                    'case',
                    ['boolean', ['feature-state', 'highlighted'], false],
                    colorSettings.selectedNoShade ? 0 : 0.6,
                    ['boolean', ['feature-state', 'dimmed'], false],
                    colorSettings.dimmedNoShade ? 0 : 0.15,
                    0
                ]
            );
        }

        // 3. Refresh dependent UI + interactions
        populateRegionList();
        setupHoverTooltip();

        // 4. If switching to ZIPs, ensure Redfin data is applied
        if (currentLevel === 'zips' && Object.keys(redfinData).length > 0) {
            console.log('Switched to ZIP level, applying Redfin data...');

            // Wait for tiles to load, then apply data and choropleth
            const waitForTilesAndApply = () => {
                const features = map.querySourceFeatures('zips', {
                    sourceLayer: 'us-zips-simplified-ahb14j'
                });

                if (features.length > 0) {
                    console.log('ZIP tiles loaded, applying data...');
                    applyRedfinDataToZips().then(() => {
                        if (currentMetric !== 'none') {
                            console.log('Applying choropleth after data loaded...');
                            applyChoropleth();
                        }
                    });
                } else {
                    console.log('Waiting for ZIP tiles to load...');
                    // If no features yet, wait for tiles to load
                    const checkInterval = setInterval(() => {
                        const features = map.querySourceFeatures('zips', {
                            sourceLayer: 'us-zips-simplified-ahb14j'
                        });
                        if (features.length > 0) {
                            clearInterval(checkInterval);
                            console.log('ZIP tiles loaded, applying data...');
                            applyRedfinDataToZips().then(() => {
                                if (currentMetric !== 'none') {
                                    console.log('Applying choropleth after data loaded...');
                                    applyChoropleth();
                                }
                            });
                        }
                    }, 100);

                    // Timeout after 5 seconds
                    setTimeout(() => {
                        clearInterval(checkInterval);
                        if (map.getZoom() < 4) {
                            showNotification('Zoom in to zoom level 4+ to see ZIP code data');
                        }
                    }, 5000);
                }
            };

            waitForTilesAndApply();
        } else {
            if (currentMetric !== 'none') applyChoropleth();
        }
    });
}


const metricSelector = document.getElementById('metric-selector');
if (metricSelector) {
    metricSelector.addEventListener('change', (e) => {
        currentMetric = e.target.value;
        updateMetricDescriptions();

        // For ZIP level with Redfin metrics, ensure feature states are applied first
        const redfinMetrics = ['MEDIAN_SALE_PRICE', 'MEDIAN_SALE_PRICE_YOY', 'HOMES_SOLD', 'INVENTORY', 'MEDIAN_DOM'];
        const isRedfinMetric = redfinMetrics.includes(currentMetric);

        if (currentLevel === 'zips' && isRedfinMetric && Object.keys(redfinData).length > 0) {
            console.log('Applying Redfin feature states before choropleth...');
            applyRedfinDataToZips().then(() => {
                applyChoropleth();
                updateLegend();
                setupHoverTooltip(); // Refresh tooltip handlers after choropleth
            });
        } else {
            applyChoropleth();
            updateLegend();
            setupHoverTooltip(); // Refresh tooltip handlers after choropleth
        }
    });
}

function initializeDefaultGeography() {
    // Turn on the initial geography level (states) visibility
    if (map.getLayer('states-layer')) {
        map.setPaintProperty('states-layer', 'line-opacity', 1);
    }

    if (map.getLayer('states-fill')) {
        map.setPaintProperty(
            'states-fill',
            'fill-opacity',
            [
                'case',
                ['boolean', ['feature-state', 'highlighted'], false],
                colorSettings.selectedNoShade ? 0 : 0.6,
                ['boolean', ['feature-state', 'dimmed'], false],
                colorSettings.dimmedNoShade ? 0 : 0.15,
                0
            ]
        );
    }

    // Populate the region list for the initial level
    populateRegionList();

    console.log('Default geography (states) initialized');
}

map.on('load', async () => {
    map.fitBounds(usBounds, { padding: 20 });
    await loadEconomicData();
    await loadGeoData();


    // Wait for Redfin data to be applied before continuing
    await applyRedfinDataToZips();
        // Also pre-apply ZIP data so it's ready when user switches
    if (Object.keys(redfinData).length > 0) {
        const originalLevel = currentLevel;
        currentLevel = 'zips';  // Temporarily switch to zips
        await applyRedfinDataToZips();
        currentLevel = originalLevel;  // Switch back
        console.log('✓ Pre-loaded ZIP Redfin data for faster switching');
    }


    // DEBUG: Check if feature-state is working
    setTimeout(() => {
        debugZipFeatureState();
    }, 1000);

    enhanceLabels();
    setTimeout(() => {
        // Initialize the default geography level (states)
        initializeDefaultGeography();
        setupHoverTooltip();
        console.log('Tooltip setup complete');
    }, 500);
});

const btnZoomIn = document.getElementById('zoom-in');
const btnZoomOut = document.getElementById('zoom-out');
const btnZoomReset = document.getElementById('zoom-reset');

if (btnZoomIn) btnZoomIn.addEventListener('click', () => map.zoomIn());
if (btnZoomOut) btnZoomOut.addEventListener('click', () => map.zoomOut());
if (btnZoomReset) btnZoomReset.addEventListener('click', () => map.fitBounds(usBounds, { padding: 20, duration: 1000 }));

function setupColorControls() {
    const updateColor = (key, hex) => {
        colorSettings[key] = hex;
        updateMapColors();
        if (currentMetric !== 'none' && (key === 'choroLow' || key === 'choroHigh')) applyChoropleth();
    };

    ['border', 'selected', 'dimmed', 'choro-low', 'choro-high'].forEach(type => {
        const input = document.getElementById(`${type}-color`);
        const hex = document.getElementById(`${type}${type.includes('color')?'':'-color'}-hex`) || document.getElementById(`${type}-hex`);
        
        if (input && hex) {
            input.addEventListener('input', e => {
                const key = type.replace(/-([a-z])/g, g => g[1].toUpperCase()).replace('Color', '');
                hex.value = e.target.value.toUpperCase();
                updateColor(key, e.target.value);
            });
            hex.addEventListener('input', e => {
                if (/^#[0-9A-F]{6}$/i.test(e.target.value)) {
                    const key = type.replace(/-([a-z])/g, g => g[1].toUpperCase()).replace('Color', '');
                    input.value = e.target.value;
                    updateColor(key, e.target.value);
                }
            });
        }
    });

    const opacitySlider = document.getElementById('choro-opacity');
    if (opacitySlider) opacitySlider.addEventListener('input', e => {
        const valSpan = document.getElementById('opacity-value');
        if (valSpan) valSpan.textContent = e.target.value;
        colorSettings.choroOpacity = e.target.value / 100;
        if (currentMetric !== 'none') applyChoropleth();
    });

    const enhanceCheck = document.getElementById('enhance-labels');
    if (enhanceCheck) enhanceCheck.addEventListener('change', e => {
        colorSettings.enhanceLabels = e.target.checked;
        enhanceLabels();
    });
}

function updateMapColors() {
    if (!map || !map.getLayer('states-layer')) return;
    ['states', 'counties', 'cbsas'].forEach(layer => {
        try {
            if (map.getLayer(`${layer}-layer`)) map.setPaintProperty(`${layer}-layer`, 'line-color', colorSettings.border);
            if (map.getLayer(`${layer}-fill`) && currentMetric === 'none') {
                map.setPaintProperty(`${layer}-fill`, 'fill-color', ['case', ['boolean', ['feature-state', 'highlighted'], false], colorSettings.selected, colorSettings.dimmed]);
            }
        } catch(e) {}
    });
}

function getFeatureName(feature, geoLevel) {
    if (geoLevel === 'states') return feature.properties.name || 'Unknown State';
    if (geoLevel === 'cbsas') return feature.properties.NAME || feature.properties.name || 'Unknown CBSA';
    if (geoLevel === 'zips') {  
        const zipCode = feature.properties.GEOID || feature.properties.ZIP;
        
        // Get city/state from Redfin data using the GEOID
        const redfinInfo = redfinData[String(zipCode).padStart(5, '0')];
        const cityName = redfinInfo?.CITY_NAME;
        const stateAbbr = redfinInfo?.STATE_ABBREV;
        
        if (cityName && stateAbbr) return `ZIP ${zipCode} - ${cityName}, ${stateAbbr}`;
        return `ZIP ${zipCode}`;
    }
    const countyName = feature.properties.name || 'Unknown County';
    const countyFipsStr = String(feature.id).padStart(5, '0');
    const stateFips = parseInt(countyFipsStr.substring(0, 2));
    const stateAbbr = stateFipsToAbbr[stateFips];
    return stateAbbr ? `${countyName}, ${stateAbbr}` : countyName;
}

let isTooltipLocked = false;

window.closeTooltip = function(event) {
    if (event) event.stopPropagation(); // Stop click from passing to map
    isTooltipLocked = false;
    
    const tooltip = document.getElementById('mapTooltip');
    if (tooltip) {
        tooltip.classList.remove('locked');
        tooltip.classList.remove('show');
    }
    
    if (typeof map !== 'undefined') map.getCanvas().style.cursor = '';
};

function setupHoverTooltip() {
    const tooltip = document.getElementById('mapTooltip');
    if (!tooltip) return;

    map.off('mousemove', onMouseMove);
    map.off('mouseleave', onMouseLeave);
    map.off('click', onMapClick);

    function updateTooltipContent(feature) {
            const name = getFeatureName(feature, currentLevel);
            
            let tooltipHtml = `
                <div class="tooltip-header">
                    ${name}
                    <button class="tooltip-close-btn" onclick="window.closeTooltip(event)">×</button>
                </div>`;
            
            let regionData;
            if (currentLevel === 'states') regionData = economicData.states[String(feature.id).padStart(2, '0')];
            else if (currentLevel === 'counties') regionData = economicData.counties[String(feature.id).padStart(5, '0')];
            else if (currentLevel === 'cbsas') regionData = economicData.cbsas[feature.properties.GEOID];
            else if (currentLevel === 'zips') regionData = economicData.counties[String(feature.id).padStart(5, '0')];
            
            // Get Redfin data for ZIPs
            let redfinZipData = null;
            if (currentLevel === 'zips') {
                const zipCode = String(feature.properties.GEOID || feature.properties.ZIP).padStart(5, '0');
                redfinZipData = redfinData[zipCode];
            }

            if (regionData) {
                const years = regionData.years || {};
                const y = (key) => years[metricYearMap[key]] || '';

                tooltipHtml += '<div class="tooltip-grid">';

                Object.entries(metricLabels).forEach(([key, label]) => {
                    const value = regionData[key];
                    
                    if (value !== undefined && value !== null) {
                        const formattedValue = formatMetricValue(key, value);
                        const yearStr = y(key);
                        
                        const isSelected = key === currentMetric;
                        const labelStyle = isSelected ? 'color: #509ee3; font-weight:700;' : ''; 
                        const valueStyle = isSelected ? 'color: #ffffff; font-weight:700;' : '';

                        tooltipHtml += `
                            <div class="tooltip-label" style="${labelStyle}">${label}</div>
                            <div class="tooltip-value" style="${valueStyle}">
                                ${formattedValue} 
                                <span style="opacity:0.5; font-size:10px; margin-left:6px; color:#ffffff; font-weight:400;">${yearStr}</span>
                            </div>
                        `;
                    }
                });
                
                tooltipHtml += '</div>'; // End Grid
            }
                // ADD REDFIN DATA SECTION
            if (redfinZipData) {
                tooltipHtml += '<div style="border-top: 1px solid #3a424a; margin-top: 12px; padding-top: 12px;">';
                tooltipHtml += '<div style="color: #509ee3; font-weight: 600; font-size: 13px; margin-bottom: 8px;">Redfin Housing Data (2025)</div>';
                tooltipHtml += '<div class="tooltip-grid">';
                
                if (redfinZipData.MEDIAN_SALE_PRICE) {
                    tooltipHtml += `<div class="tooltip-label">Median Sale Price</div>`;
                    tooltipHtml += `<div class="tooltip-value">$${redfinZipData.MEDIAN_SALE_PRICE.toLocaleString()}</div>`;
                }
                if (redfinZipData.MEDIAN_SALE_PRICE_YOY !== null && redfinZipData.MEDIAN_SALE_PRICE_YOY !== undefined) {
                    const yoyPercent = (redfinZipData.MEDIAN_SALE_PRICE_YOY * 100).toFixed(1);
                    const yoyColor = redfinZipData.MEDIAN_SALE_PRICE_YOY >= 0 ? '#4ade80' : '#f87171';
                    tooltipHtml += `<div class="tooltip-label">Price YoY Change</div>`;
                    tooltipHtml += `<div class="tooltip-value" style="color: ${yoyColor}">${yoyPercent > 0 ? '+' : ''}${yoyPercent}%</div>`;
                }
                if (redfinZipData.HOMES_SOLD) {
                    tooltipHtml += `<div class="tooltip-label">Homes Sold</div>`;
                    tooltipHtml += `<div class="tooltip-value">${redfinZipData.HOMES_SOLD}</div>`;
                }
                if (redfinZipData.INVENTORY) {
                    tooltipHtml += `<div class="tooltip-label">Inventory</div>`;
                    tooltipHtml += `<div class="tooltip-value">${redfinZipData.INVENTORY}</div>`;
                }
                if (redfinZipData.MEDIAN_DOM) {
                    tooltipHtml += `<div class="tooltip-label">Median Days on Market</div>`;
                    tooltipHtml += `<div class="tooltip-value">${redfinZipData.MEDIAN_DOM} days</div>`;
                }
                
                tooltipHtml += '</div></div>';
            }
            // Only show "No data available" if BOTH are missing
            if (!regionData && !redfinZipData) {
                tooltipHtml += '<div style="color:#949a9e; font-style:italic; padding:10px;">No data available</div>';
            }
            tooltip.innerHTML = tooltipHtml;
        }
    
    function positionTooltip(x, y) {
        const tooltipEl = document.getElementById('mapTooltip');
        if (!tooltipEl) return;

        const tooltipWidth = tooltipEl.offsetWidth; 
        const tooltipHeight = tooltipEl.offsetHeight; 
        const mapWidth = map.getCanvas().width;
        const mapHeight = map.getCanvas().height;
        
        const offset = 20;

        let left = x + offset;
        if (left + tooltipWidth > mapWidth) {
            left = x - tooltipWidth - offset;
        }
        if (left < 0) {
            left = 10;
        }

        let top = y + offset;
        if (top + tooltipHeight > mapHeight) {
            top = mapHeight - tooltipHeight - 10;
        }
        if (top < 10) {
            top = 10;
        }

        tooltipEl.style.left = left + 'px';
        tooltipEl.style.top = top + 'px';
        tooltipEl.classList.add('show');
    }

    function onMouseMove(e) {
        if (isTooltipLocked) return; 

        const fillLayerId = `${currentLevel}-fill`;
        if (!map.getLayer(fillLayerId)) return;

        const features = map.queryRenderedFeatures(e.point, { layers: [fillLayerId] });

        if (features.length > 0) {
            map.getCanvas().style.cursor = 'pointer';
            updateTooltipContent(features[0]);
            positionTooltip(e.point.x, e.point.y);
        } else {
            map.getCanvas().style.cursor = '';
            tooltip.classList.remove('show');
        }
    }

    function onMouseLeave() {
        if (isTooltipLocked) return;
        tooltip.classList.remove('show');
        map.getCanvas().style.cursor = '';
    }

    function onMapClick(e) {
        const fillLayerId = `${currentLevel}-fill`;
        let features = [];
        if (map.getLayer(fillLayerId)) {
            features = map.queryRenderedFeatures(e.point, { layers: [fillLayerId] });
        }

        if (features.length > 0) {
            isTooltipLocked = true;
            tooltip.classList.add('locked');
            updateTooltipContent(features[0]);
            positionTooltip(e.point.x, e.point.y);
        } else {
            window.closeTooltip();
        }
    }

    map.on('mousemove', onMouseMove);
    map.on('mouseleave', onMouseLeave);
    map.on('click', onMapClick);
}

// --- Theme Logic Integration ---
function setTheme(mode) {
    document.body.classList.toggle('dark-mode', mode === 'dark');
    localStorage.setItem('theme', mode);

    map.setStyle(mode === 'dark' ? 'mapbox://styles/mapbox/dark-v11' : 'mapbox://styles/mapbox/light-v11');

    map.once('style.load', () => {
        map.setProjection('mercator');
        if (usData && countiesData) {
            addBoundariesToMap();
            enhanceLabels();

            // Re-apply Redfin data to ZIP features after style change (feature states are cleared)
            if (Object.keys(redfinData).length > 0) {
                // Reset the handler flag so it can be set up again
                zipFeatureStateHandlerSetup = false;
                applyRedfinDataToZips().then(() => {
                    console.log('✓ Redfin data re-applied after style change');
                });
            }

            setTimeout(() => {
                const geoSelect = document.getElementById('geo-level');
                if (geoSelect) geoSelect.dispatchEvent(new Event('change'));
            }, 100);
        }
    });
}

// Attach setTheme to window so UI Manager can find it
window.setTheme = setTheme;

const savedTheme = localStorage.getItem('theme') || 'light';
// Apply immediate visual changes (body class) to prevent flash
document.body.classList.toggle('dark-mode', savedTheme === 'dark');

// Let UI Manager handle the button creation and click events.
// We only initialize the Map style here.
map.setStyle(savedTheme === 'dark' ? 'mapbox://styles/mapbox/dark-v11' : 'mapbox://styles/mapbox/light-v11');

// ==================== METRIC TOOLTIP SYSTEM ====================

const metricDefinitions = {
    // Economic Indicators
    medianHouseholdIncome: {
        title: "Median Household Income",
        definition: "The middle value when all households are ranked by income. Half earn more, half earn less.",
        source: "Census ACS 5-Year Estimates (2023)"
    },
    povertyRate: {
        title: "Poverty Rate",
        definition: "Percentage of people living below the federal poverty threshold.",
        source: "Census ACS 5-Year Estimates (2023)"
    },
    gdpTotal: {
        title: "GDP Total",
        definition: "Total economic output in millions of dollars. Not available for ZIP codes.",
        source: "Bureau of Economic Analysis (2023)"
    },
    
    // Employment & Demographics
    employmentRate: {
        title: "Employment Rate",
        definition: "Percentage of the civilian labor force that is currently employed.",
        source: "Census ACS 5-Year Estimates (2023)"
    },
    unemploymentRate: {
        title: "Unemployment Rate",
        definition: "Percentage of the civilian labor force actively seeking work but unable to find employment.",
        source: "Census ACS 5-Year Estimates (2023)"
    },
    totalPopulation: {
        title: "Total Population",
        definition: "Total number of residents in the area.",
        source: "Census ACS 5-Year Estimates (2023)"
    },
    medianAge: {
        title: "Median Age",
        definition: "The age that divides the population into two equal groups—half younger, half older.",
        source: "Census ACS 5-Year Estimates (2023)"
    },
    
    // Housing Market
    medianHomeValue: {
        title: "Median Home Value",
        definition: "The middle value of owner-occupied homes. Half are worth more, half less.",
        source: "Census ACS 5-Year Estimates (2023)"
    },
    medianGrossRent: {
        title: "Median Gross Rent",
        definition: "The middle monthly rent including utilities. Half pay more, half pay less.",
        source: "Census ACS 5-Year Estimates (2023)"
    },
    medianOwnerCostsWithMortgage: {
        title: "Monthly Owner Costs (With Mortgage)",
        definition: "Median monthly housing costs for homeowners with a mortgage, including mortgage payments, taxes, and insurance.",
        source: "Census ACS 5-Year Estimates (2023)"
    },
    medianOwnerCostsNoMortgage: {
        title: "Monthly Owner Costs (No Mortgage)",
        definition: "Median monthly housing costs for homeowners without a mortgage, including taxes and insurance only.",
        source: "Census ACS 5-Year Estimates (2023)"
    },
    
    // Housing Characteristics
    homeownershipRate: {
        title: "Homeownership Rate",
        definition: "Percentage of occupied housing units that are owner-occupied rather than rented.",
        source: "Census ACS 5-Year Estimates (2023)"
    },
    vacancyRate: {
        title: "Vacancy Rate",
        definition: "Percentage of all housing units that are currently vacant.",
        source: "Census ACS 5-Year Estimates (2023)"
    },
    medianYearBuilt: {
        title: "Median Year Built",
        definition: "The middle year when housing units were constructed. Half were built earlier, half later.",
        source: "Census ACS 5-Year Estimates (2023)"
    },
    totalHousingUnits: {
        title: "Total Housing Units",
        definition: "Total count of all housing units, both occupied and vacant.",
        source: "Census ACS 5-Year Estimates (2023)"
    },
    
    // HUD Fair Market Rents
    fmr0Bedroom: {
        title: "Studio/Efficiency Fair Market Rent",
        definition: "HUD-determined rent at the 40th percentile for studio apartments in the area.",
        source: "HUD Fair Market Rents (2025/26)"
    },
    fmr1Bedroom: {
        title: "1-Bedroom Fair Market Rent",
        definition: "HUD-determined rent at the 40th percentile for 1-bedroom units in the area.",
        source: "HUD Fair Market Rents (2025/26)"
    },
    fmr2Bedroom: {
        title: "2-Bedroom Fair Market Rent",
        definition: "HUD-determined rent at the 40th percentile for 2-bedroom units in the area.",
        source: "HUD Fair Market Rents (2025/26)"
    },
    fmr3Bedroom: {
        title: "3-Bedroom Fair Market Rent",
        definition: "HUD-determined rent at the 40th percentile for 3-bedroom units in the area.",
        source: "HUD Fair Market Rents (2025/26)"
    },
    fmr4Bedroom: {
        title: "4-Bedroom Fair Market Rent",
        definition: "HUD-determined rent at the 40th percentile for 4-bedroom units in the area.",
        source: "HUD Fair Market Rents (2025/26)"
    },
    medianFamilyIncome: {
        title: "Median Family Income (HUD)",
        definition: "HUD-calculated median income for families, used to determine housing assistance eligibility.",
        source: "HUD Income Limits (2025)"
    },
    
    // Redfin Housing Market
    MEDIAN_SALE_PRICE: {
        title: "Median Sale Price",
        definition: "The middle sale price of homes sold. Half sold for more, half for less.",
        source: "Redfin Market Data (2025)"
    },
    MEDIAN_SALE_PRICE_YOY: {
        title: "Price Year-over-Year Change",
        definition: "Percentage change in median sale price compared to the same period last year.",
        source: "Redfin Market Data (2025)"
    },
    HOMES_SOLD: {
        title: "Homes Sold",
        definition: "Total number of homes that sold in the most recent reporting period.",
        source: "Redfin Market Data (2025)"
    },
    INVENTORY: {
        title: "Current Inventory",
        definition: "Number of homes actively listed for sale on the market.",
        source: "Redfin Market Data (2025)"
    },
    MEDIAN_DOM: {
        title: "Median Days on Market",
        definition: "The middle number of days homes spend listed before selling. Half sell faster, half slower.",
        source: "Redfin Market Data (2025)"
    }
};

// Initialize tooltip functionality
function initMetricTooltip() {
    const icon = document.getElementById('metric-info-icon');
    const tooltip = document.getElementById('metric-tooltip');
    const metricSelector = document.getElementById('metric-selector');
    
    if (!icon || !tooltip || !metricSelector) return;
    
    // Update tooltip content when metric changes
    function updateTooltipContent() {
        const selectedMetric = metricSelector.value;
        
        if (selectedMetric === 'none') {
            icon.style.opacity = '0.3';
            icon.style.cursor = 'not-allowed';
            return;
        }
        
        icon.style.opacity = '1';
        icon.style.cursor = 'pointer';
        
        const metricInfo = metricDefinitions[selectedMetric];
        if (metricInfo) {
            tooltip.querySelector('.metric-tooltip-title').textContent = metricInfo.title;
            tooltip.querySelector('.metric-tooltip-definition').textContent = metricInfo.definition;
            tooltip.querySelector('.metric-tooltip-source').textContent = 'Source: ' + metricInfo.source;
        }
    }
    
    // Position tooltip relative to icon
    function positionTooltip() {
        const iconRect = icon.getBoundingClientRect();
        const tooltipRect = tooltip.getBoundingClientRect();
        
        // Position to the right of the icon
        tooltip.style.left = (iconRect.right + 10) + 'px';
        tooltip.style.top = (iconRect.top - (tooltipRect.height / 2) + (iconRect.height / 2)) + 'px';
    }
    
    // Show tooltip on hover
    icon.addEventListener('mouseenter', () => {
        if (metricSelector.value !== 'none') {
            updateTooltipContent();
            tooltip.classList.add('show');
            positionTooltip();
        }
    });
    
    // Hide tooltip on mouse leave
    icon.addEventListener('mouseleave', () => {
        tooltip.classList.remove('show');
    });
    
    // Update when metric selection changes
    metricSelector.addEventListener('change', updateTooltipContent);
    
    // Initial state
    updateTooltipContent();
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initMetricTooltip);
} else {
    initMetricTooltip();
}