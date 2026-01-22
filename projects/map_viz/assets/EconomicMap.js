/**
 * EconomicMap.js
 * Specialized logic for the Economic Map Tool.
 * independent of the global UI Manager.
 */

class EconomicMap {
    constructor(config) {
        this.containerId = config.containerId;
        this.mapboxToken = config.mapboxToken;
        // Adjust path based on where this script is called from
        this.dataBasePath = config.dataBasePath || '../../projects/map_viz/data/'; 
        
        // --- State Variables ---
        this.map = null;
        this.usData = null; // TopoJSON for States
        this.countiesData = null; // TopoJSON for Counties
        this.cbsaData = null; // GeoJSON for Metros
        this.currentLevel = 'states'; // 'states', 'counties', 'cbsas'
        this.currentMetric = 'none';
        
        this.economicData = { states: {}, counties: {}, cbsas: {} };
        this.globalYearMetadata = {};
        
        this.highlightedRegions = new Set();
        this.isTooltipLocked = false;

        // --- Settings & Mappings ---
        this.colorSettings = {
            border: '#999999',
            selected: '#627BC1',
            dimmed: '#CCCCCC',
            choroLow: '#eff3ff',
            choroHigh: '#08519c',
            choroOpacity: 1.0,
            enhanceLabels: true,
            selectedNoShade: false,
            dimmedNoShade: false
        };

        // Maps HTML select values to JSON data keys
        this.metricYearMap = {
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
            'fmr2Bedroom': 'hudFMR',
            'medianFamilyIncome': 'hudIncomeLimits'
        };

        // FIPS Code Helper
        this.stateFipsToAbbr = {
            1: 'AL', 2: 'AK', 4: 'AZ', 5: 'AR', 6: 'CA', 8: 'CO', 9: 'CT', 10: 'DE', 11: 'DC', 12: 'FL',
            13: 'GA', 15: 'HI', 16: 'ID', 17: 'IL', 18: 'IN', 19: 'IA', 20: 'KS', 21: 'KY', 22: 'LA', 23: 'ME',
            24: 'MD', 25: 'MA', 26: 'MI', 27: 'MN', 28: 'MS', 29: 'MO', 30: 'MT', 31: 'NE', 32: 'NV', 33: 'NH',
            34: 'NJ', 35: 'NM', 36: 'NY', 37: 'NC', 38: 'ND', 39: 'OH', 40: 'OK', 41: 'OR', 42: 'PA', 44: 'RI',
            45: 'SC', 46: 'SD', 47: 'TN', 48: 'TX', 49: 'UT', 50: 'VT', 51: 'VA', 53: 'WA', 54: 'WV', 55: 'WI',
            56: 'WY'
        };

        this.init();
    }

    init() {
        mapboxgl.accessToken = this.mapboxToken;

        this.map = new mapboxgl.Map({
            container: this.containerId,
            style: 'mapbox://styles/mapbox/light-v11',
            center: [-98.5795, 39.8283],
            zoom: 3.5,
            minZoom: 2.5,
            maxZoom: 10,
            projection: 'mercator'
        });

        this.map.addControl(new mapboxgl.NavigationControl({ showCompass: false }), 'top-right');

        this.map.on('load', async () => {
            await this.loadGeoData();
            await this.loadEconomicData();
            this.setupInteractions(); // Setup Tooltips & Clicks
            this.setupUIControls();   // Setup Sidebar inputs
            this.populateRegionList();
            this.updateYearDisplay();
            
            console.log('Economic Map Initialized');
        });
    }

    // --- Data Fetching ---
    async loadGeoData() {
        try {
            const [us, counties] = await Promise.all([
                d3.json('https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json'),
                d3.json('https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json')
            ]);
            
            this.usData = us;
            this.countiesData = counties;

            // Try fetching CBSA (Metro) data
            try {
                const cbsaRes = await fetch('../../projects/map_viz/assets/us-cbsa.json');
                if (cbsaRes.ok) {
                    const data = await cbsaRes.json();
                    // Handle if it's TopoJSON or GeoJSON
                    this.cbsaData = (data.type === 'Topology') 
                        ? topojson.feature(data, data.objects[Object.keys(data.objects)[0]]) 
                        : data;
                }
            } catch (e) { console.warn('CBSA Layer skipped:', e); }

            this.addBoundariesToMap();
            
            // Hide Loader
            const overlay = document.getElementById('loadingOverlay');
            if(overlay) overlay.classList.add('hidden');

        } catch (error) {
            console.error('GeoData Error:', error);
        }
    }

    async loadEconomicData() {
        try {
            const [statesRes, countiesRes, cbsasRes] = await Promise.all([
                fetch(`${this.dataBasePath}states_economic_data.json`).then(r => r.json()),
                fetch(`${this.dataBasePath}counties_economic_data.json`).then(r => r.json()),
                fetch(`${this.dataBasePath}cbsas_economic_data.json`).then(r => r.json())
            ]);

            this.economicData.states = statesRes.data || statesRes;
            this.economicData.counties = countiesRes.data || countiesRes;
            this.economicData.cbsas = cbsasRes.data || cbsasRes;

            if (statesRes.metadata?.yearsUsed) {
                this.globalYearMetadata = statesRes.metadata.yearsUsed;
            }

            // Calculate derived percentages on the fly
            this.calculateDerivedRates();

        } catch (error) {
            console.error('EconomicData Error:', error);
        }
    }

    calculateDerivedRates() {
        [this.economicData.states, this.economicData.counties, this.economicData.cbsas].forEach(dataset => {
            Object.values(dataset).forEach(item => {
                if (item.vacantUnits && item.totalHousingUnits) item.vacancyRate = parseFloat(((item.vacantUnits / item.totalHousingUnits) * 100).toFixed(1));
                if (item.ownerOccupied && item.occupiedUnits) item.homeownershipRate = parseFloat(((item.ownerOccupied / item.occupiedUnits) * 100).toFixed(1));
            });
        });
    }

    // --- Map Rendering ---
    addBoundariesToMap() {
        const layers = this.map.getStyle().layers;
        let firstLabelLayerId = layers.find(layer => layer.type === 'symbol' && layer.layout['text-field'])?.id;

        // Add Sources & Layers (Hidden by default using opacity 0)
        // States
        this.map.addSource('states', { type: 'geojson', data: topojson.feature(this.usData, this.usData.objects.states) });
        this.map.addLayer({ id: 'states-fill', type: 'fill', source: 'states', paint: { 'fill-color': this.colorSettings.dimmed, 'fill-opacity': 0 } }, firstLabelLayerId);
        this.map.addLayer({ id: 'states-layer', type: 'line', source: 'states', paint: { 'line-color': this.colorSettings.border, 'line-width': 0.3 } }, firstLabelLayerId);

        // Counties
        this.map.addSource('counties', { type: 'geojson', data: topojson.feature(this.countiesData, this.countiesData.objects.counties) });
        this.map.addLayer({ id: 'counties-fill', type: 'fill', source: 'counties', paint: { 'fill-color': this.colorSettings.dimmed, 'fill-opacity': 0 } }, firstLabelLayerId);
        this.map.addLayer({ id: 'counties-layer', type: 'line', source: 'counties', paint: { 'line-color': this.colorSettings.border, 'line-width': 0.3, 'line-opacity': 0 } }, firstLabelLayerId);

        // CBSAs
        if (this.cbsaData) {
            this.map.addSource('cbsas', { type: 'geojson', data: this.cbsaData, promoteId: 'GEOID' });
            this.map.addLayer({ id: 'cbsas-fill', type: 'fill', source: 'cbsas', paint: { 'fill-color': this.colorSettings.dimmed, 'fill-opacity': 0 } }, firstLabelLayerId);
            this.map.addLayer({ id: 'cbsas-layer', type: 'line', source: 'cbsas', paint: { 'line-color': this.colorSettings.border, 'line-width': 0.4, 'line-opacity': 0 } }, firstLabelLayerId);
        }
        
        // Activate default level
        this.updateVisibleLevel();
    }

    updateVisibleLevel() {
        // Hide all layers
        ['states', 'counties', 'cbsas'].forEach(lvl => {
            if (this.map.getLayer(`${lvl}-layer`)) this.map.setPaintProperty(`${lvl}-layer`, 'line-opacity', 0);
            if (this.map.getLayer(`${lvl}-fill`)) this.map.setPaintProperty(`${lvl}-fill`, 'fill-opacity', 0);
        });

        // Show current level
        if (this.map.getLayer(`${this.currentLevel}-layer`)) this.map.setPaintProperty(`${this.currentLevel}-layer`, 'line-opacity', 1);
        if (this.map.getLayer(`${this.currentLevel}-fill`)) this.map.setPaintProperty(`${this.currentLevel}-fill`, 'fill-opacity', 0.15); // Default dim opacity

        this.populateRegionList();
        if (this.currentMetric !== 'none') this.applyChoropleth();
    }

    // --- Visualization Logic (Choropleth) ---
    applyChoropleth() {
        const layerId = `${this.currentLevel}-fill`;
        if (!this.map.getLayer(layerId)) return;

        // Toggle Legend visibility
        const legend = document.getElementById('map-legend');
        if(legend) legend.style.display = this.currentMetric !== 'none' ? 'block' : 'none';

        if (this.currentMetric === 'none') {
            // Reset to default styling (highlight selected only)
            this.map.setPaintProperty(layerId, 'fill-color', ['case', ['boolean', ['feature-state', 'highlighted'], false], this.colorSettings.selected, this.colorSettings.dimmed]);
            return;
        }

        let dataset;
        if (this.currentLevel === 'states') dataset = this.economicData.states;
        else if (this.currentLevel === 'counties') dataset = this.economicData.counties;
        else if (this.currentLevel === 'cbsas') dataset = this.economicData.cbsas;
        
        // 1. Get all values for color scale
        const allValues = Object.values(dataset).map(d => d[this.currentMetric]).filter(v => v !== null && v !== undefined);
        
        // 2. Build Mapbox Expression
        // ['match', ['get', 'id'], 1001, '#color', 1002, '#color', defaultColor]
        let colorExpression = (this.currentLevel === 'cbsas') ? ['match', ['get', 'GEOID']] : ['match', ['id']];
        
        Object.keys(dataset).forEach(key => {
            const val = dataset[key][this.currentMetric];
            const color = this.getColorForValue(val, allValues);
            const matchKey = (this.currentLevel === 'cbsas') ? key : parseInt(key, 10);
            colorExpression.push(matchKey, color);
        });

        colorExpression.push(this.colorSettings.dimmed); // Default color

        this.map.setPaintProperty(layerId, 'fill-color', colorExpression);
        this.map.setPaintProperty(layerId, 'fill-opacity', parseFloat(this.colorSettings.choroOpacity));
        
        this.updateLegendUI();
    }

    getColorForValue(value, allValues) {
        if (value == null || allValues.length === 0) return this.colorSettings.dimmed;
        // Simple Min/Max interpolation (Can be improved with D3 quantiles later)
        const sorted = [...allValues].sort((a,b) => a-b);
        const min = sorted[0];
        const max = sorted[sorted.length-1];
        if (max === min) return this.colorSettings.dimmed;
        const normalized = (value - min) / (max - min);
        return this.interpolateColor(this.colorSettings.choroLow, this.colorSettings.choroHigh, normalized);
    }

    interpolateColor(c1, c2, factor) {
        const hex = (c) => parseInt(c.slice(1), 16);
        const r1 = (hex(c1) >> 16) & 0xff, g1 = (hex(c1) >> 8) & 0xff, b1 = hex(c1) & 0xff;
        const r2 = (hex(c2) >> 16) & 0xff, g2 = (hex(c2) >> 8) & 0xff, b2 = hex(c2) & 0xff;
        return `#${((1<<24)+(Math.round(r1+factor*(r2-r1))<<16)+(Math.round(g1+factor*(g2-g1))<<8)+Math.round(b1+factor*(b2-b1))).toString(16).slice(1)}`;
    }

    // --- UI Interactions ---
    setupUIControls() {
        // Geo Level Selector
        const geoSelect = document.getElementById('geo-level');
        if (geoSelect) geoSelect.addEventListener('change', (e) => {
            this.currentLevel = e.target.value;
            this.updateVisibleLevel();
        });

        // Metric Selector
        const metricSelect = document.getElementById('metric-selector');
        if (metricSelect) metricSelect.addEventListener('change', (e) => {
            this.currentMetric = e.target.value;
            this.applyChoropleth();
        });
        
        // Search Input
        const searchInput = document.getElementById('search-input');
        if (searchInput) searchInput.addEventListener('input', (e) => {
            const term = e.target.value.toLowerCase();
            document.querySelectorAll('.checkbox-item').forEach(item => {
                item.style.display = item.dataset.regionName.includes(term) ? 'flex' : 'none';
            });
        });
        
        // Color Pickers (Simplified Binding)
        ['choro-low', 'choro-high'].forEach(id => {
            const input = document.getElementById(`${id}-color`);
            if(input) input.addEventListener('input', (e) => {
                const key = id === 'choro-low' ? 'choroLow' : 'choroHigh';
                this.colorSettings[key] = e.target.value;
                if(this.currentMetric !== 'none') this.applyChoropleth();
            });
        });
    }

    populateRegionList() {
        const container = document.getElementById('region-checkboxes');
        if (!container) return;
        container.innerHTML = '';
        
        let features = [];
        if (this.currentLevel === 'states') features = topojson.feature(this.usData, this.usData.objects.states).features;
        else if (this.currentLevel === 'counties') features = topojson.feature(this.countiesData, this.countiesData.objects.counties).features;
        else if (this.currentLevel === 'cbsas') features = this.cbsaData.features;

        // Sort and Create List
        const regions = features.map(f => {
            let id = f.id;
            let name = f.properties.name;
            if (this.currentLevel === 'cbsas') { id = f.properties.GEOID; name = f.properties.NAME; }
            return { id, name };
        }).sort((a,b) => a.name.localeCompare(b.name));

        regions.forEach(region => {
            const div = document.createElement('div');
            div.className = 'checkbox-item';
            div.dataset.regionName = region.name.toLowerCase();
            div.innerHTML = `<input type="checkbox" id="region-${region.id}" data-region-id="${region.id}"><label for="region-${region.id}">${region.name}</label>`;
            container.appendChild(div);
            
            // Interaction: Clicking checkbox highlights region
            div.querySelector('input').addEventListener('change', (e) => {
                this.map.setFeatureState(
                    { source: this.currentLevel, id: region.id },
                    { highlighted: e.target.checked }
                );
            });
        });
    }
    
    // --- Helper UI Updates ---
    updateYearDisplay() {
        const years = Object.values(this.globalYearMetadata).filter(y => typeof y === 'number');
        const maxYear = years.length ? Math.max(...years) : new Date().getFullYear();
        const display = document.getElementById('data-year-display');
        if(display) display.textContent = maxYear;
    }

    updateLegendUI() {
        const title = document.getElementById('legend-title');
        const grad = document.getElementById('legend-gradient');
        if(title) title.textContent = this.currentMetric;
        if(grad) grad.style.background = `linear-gradient(to right, ${this.colorSettings.choroLow}, ${this.colorSettings.choroHigh})`;
    }

    setupInteractions() {
        // Basic hover effect hook (Tooltip logic can be added here)
        this.map.on('mousemove', (e) => {
            const layer = `${this.currentLevel}-fill`;
            const features = this.map.queryRenderedFeatures(e.point, { layers: [layer] });
            this.map.getCanvas().style.cursor = features.length ? 'pointer' : '';
        });
    }
}