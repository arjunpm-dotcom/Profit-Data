// ========== BUSINESS INTELLIGENCE DASHBOARD - JAVASCRIPT ==========
// Premium Flask Dashboard with Performance Optimization

// ========== STATE MANAGEMENT ==========
let dataLoaded = false;
let filterOptions = {};
let currentMode = 'single';
let activeChart = 'monthly';
let filterDebounceTimer = null;
let pendingRequest = null; // For request cancellation
let indiaMap = null; // Leaflet map instance

// ========== INITIALIZATION ==========
$(document).ready(function () {
    console.log('[INIT] Initializing Business Intelligence Dashboard...');

    // Initialize Select2 for multi-select
    $('.multi-select').select2({
        placeholder: 'Select options...',
        allowClear: true,
        width: '100%',
        dropdownParent: $('body'),
        theme: 'default'
    });

    // Event listeners
    $('#refreshBtn').click(() => loadData(true));
    $('#exportBtn').click(exportData);
    $('#periodType').change(onPeriodTypeChange);
    $('#comparisonType').change(onComparisonTypeChange);

    // Filter change events - USE DEBOUNCED VERSION for fast performance!
    $('#states, #districts, #rbms, #bdms, #branches, #brands').on('change', debouncedApplyFilters);
    $('#year, #fy, #quarter').on('change', debouncedApplyFilters);
    $('#period1, #period2, #comparisonDimension').on('change', debouncedLoadComparison);
    $('#compRbms, #compStates, #compBrands, #compBranches').on('change', debouncedLoadComparison);

    // AUTO-LOAD DATA ON PAGE LOAD
    console.log('[DATA] Auto-loading data from Google Sheets...');
    updateLoadingMessage('Connecting to Supabase...', 'Fetching records from database...');
    loadData();
});

// ========== LOADING MESSAGES ==========
function updateLoadingMessage(title, tip) {
    $('#loadingTitle').text(title);
    $('#loadingTip').html('<i class="fas fa-lightbulb"></i> ' + tip);
}

function hideLoadingOverlay() {
    $('#loadingOverlay').addClass('hidden');
}

function showLoadingOverlay() {
    $('#loadingOverlay').removeClass('hidden');
}

// ========== DEBOUNCING FOR FAST FILTERS ==========
function debouncedApplyFilters() {
    clearTimeout(filterDebounceTimer);
    showFilterLoading();
    filterDebounceTimer = setTimeout(() => {
        applyFilters();
    }, 300); // 300ms debounce as requested
}

function debouncedLoadComparison() {
    clearTimeout(filterDebounceTimer);
    showComparisonLoading();
    filterDebounceTimer = setTimeout(() => {
        loadComparison();
    }, 300);
}

// ... (existing code) ...

function renderComparisonKPIs(data) {
    const $kpis = $('#comparisonKpis').empty();

    const kpiData = [
        {
            title: 'Revenue Growth',
            base_label: `Base: ${formatCurrency(data.period1_kpis.revenue)} (${data.period1_label})`,
            diff: formatCurrency(data.revenue_diff),
            growth: data.revenue_growth
        },
        {
            title: 'Profit Growth',
            base_label: `Base: ${formatCurrency(data.period1_kpis.profit)} (${data.period1_label})`,
            diff: formatCurrency(data.profit_diff),
            growth: data.profit_growth
        },
        {
            title: 'Quantity Growth',
            base_label: `Base: ${data.period1_kpis.quantity_formatted} (${data.period1_label})`,
            diff: data.qty_diff.toLocaleString(),
            growth: data.qty_growth
        },
        {
            title: 'Margin Change',
            base_label: `Base: ${data.period1_kpis.margin}% (${data.period1_label})`,
            diff: `${data.margin_change > 0 ? '+' : ''}${data.margin_change.toFixed(1)} pts`,
            growth: data.margin_change,
            is_margin: true
        }
    ];

    kpiData.forEach(kpi => {
        const isPositive = kpi.growth >= 0;
        const cls = isPositive ? 'positive' : 'negative';
        const arrow = isPositive ? '<i class="fas fa-arrow-up"></i>' : '<i class="fas fa-arrow-down"></i>';

        // Format: 
        // Title
        // +Rs.921.83 Cr (Main)
        // (+26.7%) (Sub)
        // Base: Rs.3,457.19 Cr (2024) (Footer)

        let growthDisplay = '';
        if (kpi.is_margin) {
            growthDisplay = ''; // Margin change is already absolute points
        } else {
            growthDisplay = `(${isPositive ? '+' : ''}${kpi.growth.toFixed(1)}%)`;
        }

        $kpis.append(`
            <div class="comparison-card ${cls}">
                <div class="comparison-label">${kpi.title}</div>
                <div class="comparison-main-value ${cls}">
                    ${kpi.diff}
                </div>
                <div class="comparison-sub-value ${cls}">
                    ${arrow} ${growthDisplay}
                </div>
                <div class="comparison-footer">
                    ${kpi.base_label}
                </div>
            </div>
        `);
    });
}

function showFilterLoading() {
    $('#filterLoadingIndicator').removeClass('hidden');
    $('.chart-container').addClass('loading');
    $('.kpi-card').addClass('loading');
}

function hideFilterLoading() {
    $('#filterLoadingIndicator').addClass('hidden');
    $('.chart-container').removeClass('loading');
    $('.kpi-card').removeClass('loading');
}

function showComparisonLoading() {
    $('#comparisonChartDisplay').parent().addClass('loading');
    $('#comparisonKpis').addClass('loading');
}

function hideComparisonLoading() {
    $('#comparisonChartDisplay').parent().removeClass('loading');
    $('#comparisonKpis').removeClass('loading');
}

// ========== DATA LOADING ==========
async function loadData(forceRefresh = false, retryCount = 0) {
    if (!forceRefresh) {
        showLoadingOverlay();
    }

    $('#refreshBtn').prop('disabled', true);

    const messages = [
        { title: 'Connecting to Supabase...', tip: 'Fetching live data from database...' },
        { title: 'Loading Records...', tip: 'This takes ~1-2 minutes for 8 lakh records...' },
        { title: 'Processing Data...', tip: 'Preparing dashboard visualizations...' }
    ];

    const msg = messages[Math.min(retryCount, messages.length - 1)];
    updateLoadingMessage(msg.title, msg.tip);

    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 300000); // 5 min timeout

        const response = await fetch('/api/load', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ force: forceRefresh }),
            signal: controller.signal
        });

        clearTimeout(timeoutId);
        const data = await response.json();

        if (data.success) {
            dataLoaded = true;
            filterOptions = data.options;
            populateFilters(data.options);

            $('#welcomeScreen').addClass('hidden');
            $('#dashboardContent').removeClass('hidden');
            hideLoadingOverlay();

            await applyFilters();

            console.log(`[SUCCESS] ${data.message}`);
            showToast(`Data loaded successfully! ${data.message}`, 'success');
        } else {
            throw new Error(data.error || 'Unknown error loading data');
        }
    } catch (error) {
        console.error('[ERROR] Load error:', error);

        if (retryCount < 3 && error.name === 'AbortError') {
            updateLoadingMessage('Retrying...', `Attempt ${retryCount + 2}/4 - Large dataset processing...`);
            await new Promise(r => setTimeout(r, 2000));
            return loadData(forceRefresh, retryCount + 1);
        }

        hideLoadingOverlay();
        $('#welcomeScreen').removeClass('hidden');
        showToast(`Load failed: ${error.message}. Click Refresh to retry.`, 'error');
    } finally {
        $('#refreshBtn').prop('disabled', false);
    }
}

// ========== TOAST NOTIFICATIONS ==========
function showToast(message, type = 'info') {
    const toast = $(`<div class="toast ${type}"><i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'exclamation-circle' : 'info-circle'}"></i> ${message}</div>`);
    $('body').append(toast);

    setTimeout(() => toast.addClass('show'), 100);
    setTimeout(() => {
        toast.removeClass('show');
        setTimeout(() => toast.remove(), 400);
    }, 4000);
}

// ========== FILTER POPULATION ==========
function populateFilters(options) {
    // Helper to preserve selection
    const preserveSelection = (id, newOptions) => {
        const $el = $(id);
        const currentVal = $el.val();
        $el.empty();
        newOptions.forEach(opt => $el.append(new Option(opt, opt)));
        if (currentVal && currentVal.length > 0) {
            $el.val(currentVal);
        }
    };

    // Years
    const $year = $('#year');
    const currentYear = $year.val();
    $year.empty().append('<option value="">All Years</option>');
    options.years.forEach(y => $year.append(`<option value="${y}">${y}</option>`));
    if (currentYear) $year.val(currentYear);

    // Financial Years
    const $fy = $('#fy');
    const currentFy = $fy.val();
    $fy.empty().append('<option value="">All Financial Years</option>');
    options.financial_years.forEach(fy => $fy.append(`<option value="${fy}">${fy}</option>`));
    if (currentFy) $fy.val(currentFy);

    // Quarters
    const $quarter = $('#quarter');
    const currentQuarter = $quarter.val();
    $quarter.empty().append('<option value="">All Quarters</option>');
    options.quarters.forEach(q => $quarter.append(`<option value="${q}">${q}</option>`));
    if (currentQuarter) $quarter.val(currentQuarter);

    // Multi-selects with preservation
    preserveSelection('#states', options.states);
    preserveSelection('#districts', options.districts);
    preserveSelection('#rbms', options.rbms);
    preserveSelection('#bdms', options.bdms);
    preserveSelection('#branches', options.branches);
    preserveSelection('#brands', options.brands);

    // Comparison filters
    preserveSelection('#compStates', options.states);
    preserveSelection('#compRbms', options.rbms);
    preserveSelection('#compBranches', options.branches);
    preserveSelection('#compBrands', options.brands);

    // Reinitialize Select2 but don't trigger change to avoid double-fetch
    $('.multi-select').trigger('change.select2');

    // Comparison periods
    populateComparisonPeriods();
}

function populateComparisonPeriods() {
    const type = $('#comparisonType').val();
    const $period1 = $('#period1').empty();
    const $period2 = $('#period2').empty();

    if (type === 'year') {
        filterOptions.years.forEach(y => {
            $period1.append(`<option value="${y}">${y}</option>`);
            $period2.append(`<option value="${y}">${y}</option>`);
        });
        if (filterOptions.years.length >= 2) {
            $period1.val(filterOptions.years[1]);
            $period2.val(filterOptions.years[0]);
        }
    } else if (type === 'fy') {
        filterOptions.financial_years.forEach(fy => {
            $period1.append(`<option value="${fy}">${fy}</option>`);
            $period2.append(`<option value="${fy}">${fy}</option>`);
        });
        if (filterOptions.financial_years.length >= 2) {
            $period1.val(filterOptions.financial_years[1]);
            $period2.val(filterOptions.financial_years[0]);
        }
    } else if (type === 'quarter') {
        filterOptions.years.forEach(y => {
            filterOptions.quarters.forEach(q => {
                const val = `${y}-${q}`;
                const label = `${q} ${y}`;
                $period1.append(`<option value="${val}">${label}</option>`);
                $period2.append(`<option value="${val}">${label}</option>`);
            });
        });
    }
}

// ========== FILTER EVENTS ==========
function onPeriodTypeChange() {
    const type = $('#periodType').val();
    $('#yearSelect, #fySelect, #quarterSelect').addClass('hidden');

    if (type === 'year' || type === 'quarter') {
        $('#yearSelect').removeClass('hidden');
    }
    if (type === 'fy') {
        $('#fySelect').removeClass('hidden');
    }
    if (type === 'quarter') {
        $('#quarterSelect').removeClass('hidden');
    }

    applyFilters();
}

function onComparisonTypeChange() {
    populateComparisonPeriods();
    loadComparison();
}

// ========== MODE SWITCHING ==========
function setMode(mode) {
    currentMode = mode;
    $('#singleModeBtn, #comparisonModeBtn').removeClass('active');
    $(`#${mode}ModeBtn`).addClass('active');

    if (mode === 'single') {
        $('#singleFilters').removeClass('hidden');
        $('#comparisonFilters').addClass('hidden');
        $('#singleView').removeClass('hidden');
        $('#comparisonView').addClass('hidden');
        applyFilters();
    } else {
        $('#singleFilters').addClass('hidden');
        $('#comparisonFilters').removeClass('hidden');
        $('#singleView').addClass('hidden');
        $('#comparisonView').removeClass('hidden');
        loadComparison();
    }
}

// ========== APPLY FILTERS ==========
function getFilters() {
    return {
        period_type: $('#periodType').val(),
        year: $('#year').val(),
        fy: $('#fy').val(),
        quarter: $('#quarter').val(),
        states: $('#states').val() || [],
        districts: $('#districts').val() || [],
        rbms: $('#rbms').val() || [],
        bdms: $('#bdms').val() || [],
        branches: $('#branches').val() || [],
        brands: $('#brands').val() || []
    };
}

// Global cache for chart data
let cachedChartData = {};

async function applyFilters() {
    if (!dataLoaded) return;

    // Cancel pending request
    if (pendingRequest) {
        pendingRequest.abort();
    }

    const controller = new AbortController();
    pendingRequest = controller;

    const filters = getFilters();

    try {
        const response = await fetch('/api/dashboard', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(filters),
            signal: controller.signal
        });
        const data = await response.json();

        if (data.success) {
            // Update KPIs
            const kpis = data.kpis;
            $('#kpiRevenue').text(kpis.revenue_formatted);
            $('#kpiProfit').text(kpis.profit_formatted);
            $('#kpiQuantity').text(kpis.quantity_formatted);
            $('#kpiMargin').text(kpis.margin + '%');
            $('#kpiDiscount').text(kpis.discount_pct + '%');
            $('#kpiCounts').text(`${kpis.states} / ${kpis.districts} / ${kpis.stores}`);

            $('#kpiProfitChange').text(`${kpis.margin}% profit margin`);
            $('#kpiMarginStatus').text(kpis.margin > 20 ? 'Excellent' : kpis.margin > 10 ? 'Good' : 'Needs Improvement');
            $('#kpiDiscountAmount').text(`${kpis.discount_formatted} total discount`);

            $('#totalRecords').text(kpis.records.toLocaleString());
            $('#totalRevenue').text(kpis.revenue_formatted);
            $('#recordsBadge').text(`${kpis.records.toLocaleString()} Records`);

            // Update insights
            if (data.insights) {
                updateInsights(data.insights);
            }

            // Cache ALL chart data for instant switching!
            cachedChartData = data.charts;

            // Render current active chart from cache (no API call needed!)
            if (activeChart === 'map') {
                renderIndiaMap(cachedChartData.map);
            } else if (cachedChartData[activeChart]) {
                renderChart(activeChart, cachedChartData[activeChart]);
            }

            // Update table
            updateTableFromData(data.table.data, data.table.total_records);

            // Update status
            updateStatus(filters);

            // Hide loading indicator
            hideFilterLoading();
        }
    } catch (error) {
        if (error.name !== 'AbortError') {
            console.error('[ERROR] Error loading dashboard data:', error);
        }
        hideFilterLoading();
    } finally {
        pendingRequest = null;
    }
}

// ========== INSIGHTS UPDATE ==========
function updateInsights(insights) {
    $('#insightTopPerformer').text(insights.top_performer || 'No data');
    $('#insightGrowth').text(insights.growth_trend || 'No trend data');
    $('#insightHighlight').text(insights.highlight || 'No highlights');
    $('#insightAlert').text(insights.alert || 'No alerts');
}

function refreshInsights() {
    const filters = getFilters();
    fetch('/api/insights', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(filters)
    })
        .then(res => res.json())
        .then(data => {
            if (data.success) {
                updateInsights(data.insights);
                showToast('Insights refreshed!', 'success');
            }
        })
        .catch(err => console.error('Error refreshing insights:', err));
}

function updateTableFromData(tableData, totalRecords) {
    const $tbody = $('#dataTableBody').empty();

    tableData.forEach(row => {
        const date = row.Date ? new Date(row.Date).toLocaleDateString() : '-';
        $tbody.append(`
            <tr>
                <td>${date}</td>
                <td>${row.RBM || '-'}</td>
                <td>${row.BDM || '-'}</td>
                <td>${row.Branch || '-'}</td>
                <td>${row.State || '-'}</td>
                <td>${row.District || '-'}</td>
                <td>${row.Brand || '-'}</td>
                <td>${row.Product ? row.Product.substring(0, 30) + '...' : '-'}</td>
                <td>${row.QTY || 0}</td>
                <td>Rs.${(row.Sold_Price || 0).toLocaleString()}</td>
                <td>Rs.${(row.Profit || 0).toLocaleString()}</td>
            </tr>
        `);
    });

    $('#tableCount').text(`Showing ${tableData.length} of ${totalRecords.toLocaleString()} records`);
}

// ========== CHARTS ==========
function showChart(chartType) {
    $('.chart-tab').removeClass('active');
    $(`.chart-tab`).each(function () {
        const text = $(this).text().toLowerCase();
        if (chartType === 'map' && text.includes('map')) {
            $(this).addClass('active');
        } else if (text.includes(chartType.substring(0, 4))) {
            $(this).addClass('active');
        }
    });
    activeChart = chartType;

    // Handle map separately
    if (chartType === 'map') {
        $('#chartDisplay').addClass('hidden');
        $('#mapContainer').removeClass('hidden');
        if (cachedChartData && cachedChartData.map) {
            renderIndiaMap(cachedChartData.map);
        } else {
            loadMapData(getFilters());
        }
        return;
    }

    // Show chart, hide map
    $('#chartDisplay').removeClass('hidden');
    $('#mapContainer').addClass('hidden');

    // Use CACHED data for INSTANT chart switching (no API call!)
    if (cachedChartData && cachedChartData[chartType]) {
        renderChart(chartType, cachedChartData[chartType]);
    } else {
        loadChart(chartType, getFilters());
    }
}

// Plotly LIGHT theme layout
const chartLayout = {
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: 'rgba(0,0,0,0)',
    font: { color: '#475569', family: 'Inter, sans-serif' },
    xaxis: {
        gridcolor: '#e2e8f0',
        linecolor: '#e2e8f0',
        tickfont: { color: '#475569' }
    },
    yaxis: {
        gridcolor: '#e2e8f0',
        linecolor: '#e2e8f0',
        tickfont: { color: '#475569' }
    },
    legend: { font: { color: '#1e293b' } },
    margin: { t: 40, r: 20, b: 60, l: 60 }
};

async function loadChart(chartType, filters) {
    $('#chartLoading').removeClass('hidden');
    $('#chartDisplay').empty();

    try {
        const response = await fetch(`/api/charts/${chartType}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(filters)
        });
        const data = await response.json();

        if (data.success && data.data) {
            renderChart(chartType, data.data);
        } else {
            $('#chartDisplay').html('<p style="text-align: center; color: #64748b; padding: 3rem;">No data available for this chart</p>');
        }
    } catch (error) {
        console.error('[ERROR] Error loading chart:', error);
        $('#chartDisplay').html('<p style="text-align: center; color: #ef4444; padding: 3rem;">Error loading chart</p>');
    } finally {
        $('#chartLoading').addClass('hidden');
    }
}

function renderChart(chartType, chartData) {
    let traces = [];
    // Deep copy axis objects to avoid mutating global chartLayout
    let layout = {
        ...chartLayout,
        height: 450,
        xaxis: { ...chartLayout.xaxis },
        yaxis: { ...chartLayout.yaxis }
    };

    if (chartType === 'monthly' && chartData.labels) {
        traces = [
            {
                x: chartData.labels,
                y: chartData.revenue,
                name: 'Revenue (Rs. Cr)',
                type: 'bar',
                marker: { color: '#3b82f6', borderRadius: 4 }
            },
            {
                x: chartData.labels,
                y: chartData.profit,
                name: 'Profit (Rs. Cr)',
                type: 'scatter',
                mode: 'lines+markers',
                line: { color: '#10b981', width: 3 },
                marker: { size: 8 },
                yaxis: 'y2'
            }
        ];
        layout.title = { text: 'Monthly Revenue & Profit Trend', font: { color: '#1e293b' } };
        layout.yaxis2 = {
            title: 'Profit (Rs. Cr)',
            overlaying: 'y',
            side: 'right',
            gridcolor: '#e2e8f0',
            tickfont: { color: '#059669' }
        };
        layout.yaxis.title = 'Revenue (Rs. Cr)';
    }
    else if (chartType === 'hierarchy' && chartData.rbm) {
        traces = [
            {
                x: chartData.rbm.labels,
                y: chartData.rbm.revenue,
                name: 'Revenue (Rs. Cr)',
                type: 'bar',
                marker: {
                    color: chartData.rbm.margin.map(m => m > 15 ? '#10b981' : m > 10 ? '#f59e0b' : '#ef4444')
                }
            }
        ];
        layout.title = { text: 'RBM Performance by Revenue', font: { color: '#1e293b' } };
    }
    else if (chartType === 'geographic' && chartData.states) {
        traces = [{
            labels: chartData.states.labels,
            values: chartData.states.revenue,
            type: 'pie',
            hole: 0.4,
            marker: {
                colors: ['#3b82f6', '#10b981', '#8b5cf6', '#f59e0b', '#ef4444', '#06b6d4', '#ec4899', '#84cc16']
            },
            textinfo: 'label+percent',
            textfont: { color: '#1e293b' }
        }];
        layout.title = { text: 'Revenue Distribution by State', font: { color: '#1e293b' } };
    }
    else if (chartType === 'product' && chartData.labels) {
        traces = [{
            x: chartData.profit, // Changed to Profit as requested
            y: chartData.labels,
            type: 'bar',
            orientation: 'h',
            name: 'Profit (Rs. Lakh)',
            marker: {
                color: chartData.profit.map(p => p > 0 ? '#10b981' : '#ef4444') // Green for profit, Red for loss
            },
            text: chartData.profit.map(p => `Rs.${p} L`),
            textposition: 'auto'
        }];
        layout.title = { text: 'Top Products by Profit (Rs. Lakh)', font: { color: '#1e293b' } };
        layout.xaxis = { ...layout.xaxis, title: 'Profit (Rs. Lakh)' };
        layout.yaxis = { ...layout.yaxis, type: 'category', automargin: true };
        layout.height = 500;
        layout.margin.l = 200;
    }
    else if (chartType === 'rbm' && chartData.labels) {
        traces = [
            {
                x: chartData.labels,
                y: chartData.revenue,
                name: 'Revenue (Rs. Cr)',
                type: 'bar',
                marker: { color: '#3b82f6' }
            },
            {
                x: chartData.labels,
                y: chartData.profit_margin,
                name: 'Profit Margin %',
                type: 'scatter',
                mode: 'lines+markers',
                line: { color: '#10b981', width: 3 },
                marker: { size: 10 },
                yaxis: 'y2'
            }
        ];
        layout.title = { text: 'RBM Performance: Revenue vs Margin', font: { color: '#1e293b' } };
        layout.yaxis2 = {
            title: 'Profit Margin %',
            overlaying: 'y',
            side: 'right',
            gridcolor: '#e2e8f0',
            tickfont: { color: '#059669' }
        };
    }
    else {
        $('#chartDisplay').html('<p style="text-align: center; color: #64748b; padding: 3rem;">No data available</p>');
        return;
    }

    Plotly.newPlot('chartDisplay', traces, layout, { responsive: true, displayModeBar: false });
}

// ========== INDIA MAP ==========
async function loadMapData(filters) {
    try {
        const response = await fetch('/api/map', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(filters)
        });
        const data = await response.json();

        if (data.success && data.data) {
            renderIndiaMap(data.data);
        }
    } catch (error) {
        console.error('[ERROR] Error loading map data:', error);
    }
}

function renderIndiaMap(mapData) {
    const container = document.getElementById('mapContainer');
    if (!container) return;

    // Initialize map if not already done
    if (!indiaMap) {
        indiaMap = L.map('mapContainer').setView([10.8505, 76.2711], 7); // Kerala center
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap contributors'
        }).addTo(indiaMap);
    }

    // Clear existing markers
    indiaMap.eachLayer(layer => {
        if (layer instanceof L.CircleMarker) {
            indiaMap.removeLayer(layer);
        }
    });

    if (!mapData || !mapData.districts || mapData.districts.length === 0) {
        return;
    }

    // Find max revenue for scaling
    const maxRevenue = Math.max(...mapData.districts.map(d => d.revenue));

    // Add circle markers for each district
    mapData.districts.forEach(district => {
        const radius = Math.max(10, Math.min(50, (district.revenue / maxRevenue) * 50));
        const color = district.margin > 15 ? '#10b981' : district.margin > 10 ? '#f59e0b' : '#ef4444';

        const marker = L.circleMarker([district.lat, district.lng], {
            radius: radius,
            fillColor: color,
            color: '#fff',
            weight: 2,
            opacity: 1,
            fillOpacity: 0.7
        }).addTo(indiaMap);

        marker.bindPopup(`
            <div style="text-align: center; padding: 10px;">
                <strong style="font-size: 14px;">${district.name}</strong><br>
                <hr style="margin: 8px 0;">
                <div style="text-align: left;">
                    <strong>Revenue:</strong> ${district.revenue_formatted}<br>
                    <strong>Margin:</strong> ${district.margin}%<br>
                    <strong>Branches:</strong> ${district.branches}
                </div>
            </div>
        `);
    });

    // Fit bounds
    if (mapData.districts.length > 0) {
        const bounds = mapData.districts.map(d => [d.lat, d.lng]);
        indiaMap.fitBounds(bounds, { padding: [20, 20] });
    }
}

// ========== DATA TABLE ==========
async function loadTable(filters) {
    try {
        const response = await fetch('/api/table', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(filters)
        });
        const data = await response.json();

        if (data.success) {
            const $tbody = $('#dataTableBody').empty();

            data.data.forEach(row => {
                const date = row.Date ? new Date(row.Date).toLocaleDateString() : '-';
                $tbody.append(`
                    <tr>
                        <td>${date}</td>
                        <td>${row.RBM || '-'}</td>
                        <td>${row.BDM || '-'}</td>
                        <td>${row.Branch || '-'}</td>
                        <td>${row.State || '-'}</td>
                        <td>${row.District || '-'}</td>
                        <td>${row.Brand || '-'}</td>
                        <td>${row.Product ? row.Product.substring(0, 30) + '...' : '-'}</td>
                        <td>${row.QTY || 0}</td>
                        <td>Rs.${(row.Sold_Price || 0).toLocaleString()}</td>
                        <td>Rs.${(row.Profit || 0).toLocaleString()}</td>
                    </tr>
                `);
            });

            $('#tableCount').text(`Showing ${data.data.length} of ${data.total_records.toLocaleString()} records`);
        }
    } catch (error) {
        console.error('[ERROR] Error loading table:', error);
    }
}

// ========== COMPARISON ==========
async function loadComparison() {
    if (!dataLoaded) return;

    const compType = $('#comparisonType').val();
    const period1 = $('#period1').val();
    const period2 = $('#period2').val();
    const dimension = $('#comparisonDimension').val();

    $('#comparisonTitle').text(`${period1} vs ${period2}`);
    $('#comparisonSubtitle').text(`${compType.toUpperCase()} Comparison | ${dimension}`);

    const params = {
        comparison_type: compType,
        dimension: dimension,
        filters: {
            rbms: $('#compRbms').val() || [],
            states: $('#compStates').val() || [],
            brands: $('#compBrands').val() || [],
            branches: $('#compBranches').val() || []
        }
    };

    // Parse period values
    if (compType === 'year') {
        params.period1_year = period1;
        params.period2_year = period2;
    } else if (compType === 'fy') {
        params.period1_fy = period1;
        params.period2_fy = period2;
    } else if (compType === 'quarter') {
        const [y1, q1] = period1.split('-');
        const [y2, q2] = period2.split('-');
        params.period1_year = y1;
        params.period1_quarter = q1;
        params.period2_year = y2;
        params.period2_quarter = q2;
    }

    try {
        const response = await fetch('/api/comparison', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(params)
        });
        const data = await response.json();

        if (data.success) {
            renderComparisonKPIs(data);
            renderComparisonChart(data);
            renderComparisonTable(data.comparisons);
            hideComparisonLoading();
        }
    } catch (error) {
        console.error('[ERROR] Error loading comparison:', error);
        hideComparisonLoading();
    }
}

function renderComparisonKPIs(data) {
    const $kpis = $('#comparisonKpis').empty();

    const kpiData = [
        { label: `Revenue (${data.period2_label})`, value: formatCurrency(data.period2_kpis.revenue), growth: data.revenue_growth, diff: formatCurrency(data.revenue_diff) },
        { label: `Profit (${data.period2_label})`, value: formatCurrency(data.period2_kpis.profit), growth: data.profit_growth, diff: formatCurrency(data.profit_diff) },
        { label: `Quantity (${data.period2_label})`, value: data.period2_kpis.quantity_formatted, growth: data.qty_growth, diff: data.qty_diff.toLocaleString() },
        { label: 'Margin Change', value: `${data.period2_kpis.margin}%`, growth: data.margin_change, suffix: 'pts', diff: `${data.margin_change > 0 ? '+' : ''}${data.margin_change.toFixed(1)}%` }
    ];

    kpiData.forEach(kpi => {
        const cls = kpi.growth >= 0 ? 'positive' : 'negative';
        const arrow = kpi.growth >= 0 ? '<i class="fas fa-arrow-up"></i>' : '<i class="fas fa-arrow-down"></i>';
        const suffix = kpi.suffix || '%';

        // Show absolute difference for financial metrics
        const diffDisplay = kpi.diff;

        $kpis.append(`
            <div class="comparison-card ${cls}">
                <div class="comparison-label">${kpi.label}</div>
                <div class="comparison-value">${kpi.value}</div>
                <div class="comparison-growth ${cls}">
                    ${arrow} ${diffDisplay} <span style="font-size: 0.8em; opacity: 0.8;">(${Math.abs(kpi.growth).toFixed(1)}${suffix})</span>
                </div>
            </div>
        `);
    });
}

function renderComparisonChart(data) {
    if (!data.chart || !data.chart.labels.length) {
        $('#comparisonChartDisplay').html('<p style="text-align: center; color: #64748b; padding: 3rem;">No comparison data</p>');
        return;
    }

    const traces = [
        {
            x: data.chart.labels,
            y: data.chart.period1,
            name: data.period1_label,
            type: 'bar',
            marker: { color: '#3b82f6' }
        },
        {
            x: data.chart.labels,
            y: data.chart.period2,
            name: data.period2_label,
            type: 'bar',
            marker: { color: '#10b981' }
        }
    ];

    const layout = {
        ...chartLayout,
        height: 400,
        title: { text: 'Period Comparison', font: { color: '#1e293b' } },
        barmode: 'group',
        yaxis: { ...chartLayout.yaxis, title: 'Revenue (Rs. Cr)' }
    };

    Plotly.newPlot('comparisonChartDisplay', traces, layout, { responsive: true, displayModeBar: false });
}

function renderComparisonTable(comparisons) {
    const $tbody = $('#comparisonTableBody').empty();

    comparisons.forEach(row => {
        const cls = row.growth >= 0 ? 'positive' : 'negative';
        const icon = row.growth >= 0 ? '<i class="fas fa-chart-line text-green"></i>' : '<i class="fas fa-chart-line text-red"></i>';

        $tbody.append(`
            <tr>
                <td>${row.dimension}</td>
                <td>${row.period1_formatted}</td>
                <td>${row.period2_formatted}</td>
                <td class="text-${cls}">${row.growth >= 0 ? '+' : ''}${row.growth.toFixed(1)}%</td>
                <td>${icon}</td>
            </tr>
        `);
    });
}

function formatCurrency(value) {
    if (!value && value !== 0) return 'Rs.0';
    const sign = value < 0 ? '-' : (value > 0 ? '+' : '');
    value = Math.abs(value);

    if (value >= 10000000) return `${sign}Rs.${(value / 10000000).toFixed(2)} Cr`;
    if (value >= 100000) return `${sign}Rs.${(value / 100000).toFixed(2)} Lakh`;
    if (value >= 1000) return `${sign}Rs.${(value / 1000).toFixed(2)} K`;
    return `${sign}Rs.${value.toFixed(0)}`;
}

// ========== STATUS UPDATE ==========
function updateStatus(filters) {
    let period = 'All Time';
    if (filters.year && filters.period_type === 'year') period = filters.year;
    if (filters.fy) period = filters.fy;
    if (filters.quarter && filters.year) period = `${filters.quarter} ${filters.year}`;

    $('#periodBadge').text(period);

    let activeFiltersText = 'All Data';
    const appliedFilters = [];

    if (filters.states && filters.states.length) appliedFilters.push(`${filters.states.length} States`);
    if (filters.districts && filters.districts.length) appliedFilters.push(`${filters.districts.length} Districts`);
    if (filters.rbms && filters.rbms.length) appliedFilters.push(`${filters.rbms.length} RBMs`);
    if (filters.bdms && filters.bdms.length) appliedFilters.push(`${filters.bdms.length} BDMs`);
    if (filters.branches && filters.branches.length) appliedFilters.push(`${filters.branches.length} Branches`);
    if (filters.brands && filters.brands.length) appliedFilters.push(`${filters.brands.length} Brands`);

    if (appliedFilters.length > 0) {
        activeFiltersText = 'Filtered by: ' + appliedFilters.join(', ');
    }

    $('#activeFilters').text(activeFiltersText);
}

// ========== RESET FILTERS ==========
function resetFilters() {
    // Reset time period
    $('#periodType').val('');
    $('#year').val('');
    $('#fy').val('');
    $('#quarter').val('');
    $('#yearSelect, #fySelect, #quarterSelect').addClass('hidden');

    // Reset multi-selects
    $('#states, #districts, #rbms, #bdms, #branches, #brands').val(null).trigger('change');

    // Reset comparison filters
    $('#comparisonType').val('year');
    $('#period1, #period2').val('');
    $('#comparisonDimension').val('Overall');
    $('#compRbms, #compStates, #compBrands, #compBranches').val(null).trigger('change');

    applyFilters();
    showToast('Filters reset successfully!', 'success');
}

// ========== EXPORT ==========
async function exportData() {
    const filters = getFilters();

    try {
        const response = await fetch('/api/export', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(filters)
        });

        if (response.ok) {
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `business_data_${new Date().toISOString().slice(0, 10)}.csv`;
            document.body.appendChild(a);
            a.click();
            a.remove();
            window.URL.revokeObjectURL(url);
            showToast('Data exported successfully!', 'success');
        } else {
            showToast('Export failed. Please try again.', 'error');
        }
    } catch (error) {
        console.error('[ERROR] Export error:', error);
        showToast('Export failed. Please try again.', 'error');
    }
}
