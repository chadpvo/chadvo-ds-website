// Configuration
const API_BASE_URL = 'http://localhost:5000/api'; // Change this to your Railway URL after deployment
// For deployment, use: const API_BASE_URL = 'https://your-app.railway.app/api';

// State
let currentChart = null;
let currentData = [];
let schema = null;

// DOM Elements
const vizButtons = document.querySelectorAll('.viz-btn');
const stateSelect = document.getElementById('state-select');
const metricSelect = document.getElementById('metric-select');
const groupBySelect = document.getElementById('group-by-select');
const startDateInput = document.getElementById('start-date');
const endDateInput = document.getElementById('end-date');
const limitInput = document.getElementById('limit');
const loadDataBtn = document.getElementById('load-data-btn');
const statusMessage = document.getElementById('status-message');
const dataInfo = document.getElementById('data-info');
const chartCanvas = document.getElementById('main-chart');
const dataTableDiv = document.getElementById('data-table');
const showAverageCheckbox = document.getElementById('show-average');
const showGridCheckbox = document.getElementById('show-grid');

// Initialize
async function init() {
    setupEventListeners();
    await loadSchema();
    await loadStates();
    setStatus('Ready to visualize data', 'success');
}

// Event Listeners
function setupEventListeners() {
    vizButtons.forEach(btn => {
        btn.addEventListener('click', () => {
            vizButtons.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            if (currentData.length > 0) {
                renderChart();
            }
        });
    });

    loadDataBtn.addEventListener('click', loadData);
    
    showAverageCheckbox.addEventListener('change', () => {
        if (currentData.length > 0) renderChart();
    });
    
    showGridCheckbox.addEventListener('change', () => {
        if (currentData.length > 0) renderChart();
    });
}

// API Calls
async function apiCall(endpoint, options = {}) {
    try {
        const response = await fetch(`${API_BASE_URL}${endpoint}`, options);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error('API call failed:', error);
        throw error;
    }
}

async function loadSchema() {
    try {
        setStatus('Loading schema...', 'loading');
        schema = await apiCall('/schema');
        
        // Populate metric and group-by selects with column names
        schema.columns.forEach(col => {
            // Add to metric select
            const metricOption = document.createElement('option');
            metricOption.value = col;
            metricOption.textContent = col;
            metricSelect.appendChild(metricOption);
            
            // Add to group-by select
            const groupOption = document.createElement('option');
            groupOption.value = col;
            groupOption.textContent = col;
            groupBySelect.appendChild(groupOption);
        });
        
        setStatus('Schema loaded', 'success');
    } catch (error) {
        setStatus('Failed to load schema. Make sure the API is running!', 'error');
        console.error('Schema load error:', error);
    }
}

async function loadStates() {
    try {
        const response = await apiCall('/states');
        response.states.forEach(state => {
            const option = document.createElement('option');
            option.value = state;
            option.textContent = state;
            stateSelect.appendChild(option);
        });
    } catch (error) {
        console.error('States load error:', error);
    }
}

async function loadData() {
    try {
        setStatus('Loading data...', 'loading');
        loadDataBtn.disabled = true;
        
        // Build query parameters
        const params = new URLSearchParams();
        
        if (stateSelect.value) params.append('state', stateSelect.value);
        if (startDateInput.value) params.append('start_date', startDateInput.value);
        if (endDateInput.value) params.append('end_date', endDateInput.value);
        if (metricSelect.value) params.append('metric', metricSelect.value);
        if (groupBySelect.value) params.append('group_by', groupBySelect.value);
        params.append('limit', limitInput.value);
        
        const response = await apiCall(`/data?${params.toString()}`);
        currentData = response.data;
        
        setStatus(`Loaded ${response.count} rows`, 'success');
        dataInfo.textContent = `${response.count} records`;
        
        renderChart();
        renderDataTable();
        
    } catch (error) {
        setStatus('Failed to load data', 'error');
        console.error('Data load error:', error);
    } finally {
        loadDataBtn.disabled = false;
    }
}

// Visualization
function renderChart() {
    if (currentData.length === 0) {
        setStatus('No data to visualize', 'error');
        return;
    }
    
    const chartType = document.querySelector('.viz-btn.active').dataset.type;
    const metric = metricSelect.value;
    const groupBy = groupBySelect.value;
    
    if (!metric || !groupBy) {
        setStatus('Please select both metric and group-by columns', 'error');
        return;
    }
    
    // Destroy existing chart
    if (currentChart) {
        currentChart.destroy();
    }
    
    // Prepare data for Chart.js
    const labels = currentData.map(row => row[groupBy]);
    const metricKey = Object.keys(currentData[0]).find(key => 
        key.includes(metric) && (key.includes('avg_') || key.includes('median_') || key === metric)
    ) || metric;
    
    const values = currentData.map(row => row[metricKey]);
    
    // Calculate average if needed
    const average = values.reduce((a, b) => a + b, 0) / values.length;
    
    // Chart configuration
    const config = {
        type: chartType === 'scatter' ? 'scatter' : chartType,
        data: {
            labels: labels,
            datasets: [{
                label: metricKey,
                data: chartType === 'scatter' 
                    ? currentData.map((row, i) => ({ x: i, y: row[metricKey] }))
                    : values,
                backgroundColor: chartType === 'pie' 
                    ? generateColors(values.length)
                    : 'rgba(37, 99, 235, 0.5)',
                borderColor: 'rgba(37, 99, 235, 1)',
                borderWidth: 2,
                fill: chartType === 'line' ? false : true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: chartType === 'pie',
                    position: 'right'
                },
                title: {
                    display: true,
                    text: `${metricKey} by ${groupBy}`,
                    font: {
                        size: 16,
                        weight: 'bold'
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            let label = context.dataset.label || '';
                            if (label) label += ': ';
                            if (context.parsed.y !== null) {
                                label += new Intl.NumberFormat('en-US').format(context.parsed.y);
                            }
                            return label;
                        }
                    }
                }
            },
            scales: chartType !== 'pie' ? {
                x: {
                    grid: {
                        display: showGridCheckbox.checked
                    }
                },
                y: {
                    beginAtZero: false,
                    grid: {
                        display: showGridCheckbox.checked
                    },
                    ticks: {
                        callback: function(value) {
                            return new Intl.NumberFormat('en-US').format(value);
                        }
                    }
                }
            } : {}
        }
    };
    
    // Add average line if requested
    if (showAverageCheckbox.checked && chartType !== 'pie') {
        config.options.plugins.annotation = {
            annotations: {
                line1: {
                    type: 'line',
                    yMin: average,
                    yMax: average,
                    borderColor: 'rgb(255, 99, 132)',
                    borderWidth: 2,
                    borderDash: [5, 5],
                    label: {
                        display: true,
                        content: `Average: ${average.toFixed(2)}`,
                        position: 'end'
                    }
                }
            }
        };
    }
    
    currentChart = new Chart(chartCanvas, config);
}

function renderDataTable() {
    if (currentData.length === 0) return;
    
    const columns = Object.keys(currentData[0]);
    const maxRows = 10; // Show first 10 rows
    
    let html = '<table><thead><tr>';
    columns.forEach(col => {
        html += `<th>${col}</th>`;
    });
    html += '</tr></thead><tbody>';
    
    currentData.slice(0, maxRows).forEach(row => {
        html += '<tr>';
        columns.forEach(col => {
            const value = row[col];
            const formatted = typeof value === 'number' 
                ? new Intl.NumberFormat('en-US').format(value)
                : value;
            html += `<td>${formatted}</td>`;
        });
        html += '</tr>';
    });
    
    html += '</tbody></table>';
    
    if (currentData.length > maxRows) {
        html += `<p style="margin-top: 1rem; color: var(--text-secondary);">Showing ${maxRows} of ${currentData.length} rows</p>`;
    }
    
    dataTableDiv.innerHTML = html;
}

// Utilities
function setStatus(message, type = '') {
    statusMessage.textContent = message;
    statusMessage.className = type;
}

function generateColors(count) {
    const colors = [];
    for (let i = 0; i < count; i++) {
        const hue = (i * 360 / count) % 360;
        colors.push(`hsla(${hue}, 70%, 60%, 0.7)`);
    }
    return colors;
}

// Start the app
init();