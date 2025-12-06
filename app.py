"""
Flask Business Intelligence Dashboard
Main application with API routes
Optimized for large datasets with caching
"""

from flask import Flask, render_template, jsonify, request, Response
from flask_caching import Cache
import pandas as pd
from datetime import datetime
import json
import data_service

app = Flask(__name__)

# Configure caching with longer TTL
cache = Cache(app, config={
    'CACHE_TYPE': 'simple', 
    'CACHE_DEFAULT_TIMEOUT': 600,
    'CACHE_THRESHOLD': 100
})

# Global data store
data_loaded = False

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/api/load', methods=['POST'])
def load_data():
    """Load data from Google Sheets"""
    global data_loaded
    try:
        force = request.json.get('force', False) if request.json else False
        df = data_service.load_data(force_refresh=force)
        
        if df.empty:
            return jsonify({'success': False, 'error': 'Failed to load data'})
        
        data_loaded = True
        options = data_service.get_filter_options(df)
        
        # Get data summary
        summary = {
            'total_records': len(df),
            'assigned_branches': len(df[df['RBM'] != 'NOT ASSIGNED']['Branch'].unique()),
            'total_branches': df['Branch'].nunique(),
            'date_range': {
                'min': df['Date'].min().strftime('%Y-%m-%d') if 'Date' in df.columns else None,
                'max': df['Date'].max().strftime('%Y-%m-%d') if 'Date' in df.columns else None
            }
        }
        
        return jsonify({
            'success': True,
            'message': f'Loaded {len(df):,} records!',
            'options': options,
            'summary': summary
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/kpis', methods=['POST'])
def get_kpis():
    """Get KPI metrics based on filters"""
    try:
        df = data_service.load_data()
        if df.empty:
            return jsonify({'success': False, 'error': 'No data loaded'})
        
        filters = request.json or {}
        filtered_df = data_service.apply_filters(df, filters)
        kpis = data_service.calculate_kpis(filtered_df)
        
        return jsonify({'success': True, 'kpis': kpis})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/insights', methods=['POST'])
def get_insights():
    """Get AI-style insights based on filters"""
    try:
        df = data_service.load_data()
        if df.empty:
            return jsonify({'success': False, 'error': 'No data loaded'})
        
        filters = request.json or {}
        filtered_df = data_service.apply_filters(df, filters)
        insights = data_service.generate_insights(filtered_df)
        
        return jsonify({'success': True, 'insights': insights})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/map', methods=['POST'])
def get_map_data():
    """Get geographic data for India map visualization"""
    try:
        df = data_service.load_data()
        if df.empty:
            return jsonify({'success': False, 'error': 'No data loaded'})
        
        filters = request.json or {}
        filtered_df = data_service.apply_filters(df, filters)
        map_data = data_service.get_map_data(filtered_df)
        
        return jsonify({'success': True, 'data': map_data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/dashboard', methods=['POST'])
def get_all_dashboard_data():
    """Get ALL dashboard data in ONE request for fast loading!"""
    try:
        filters = request.json or {}
        
        # Use the cached data service function
        result = data_service.get_dashboard_data(filters)
        
        if result is None:
            return jsonify({'success': False, 'error': 'No data loaded'})
            
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/charts/<chart_type>', methods=['POST'])
def get_chart_data(chart_type):
    """Get chart data based on type and filters"""
    try:
        df = data_service.load_data()
        if df.empty:
            return jsonify({'success': False, 'error': 'No data loaded'})
        
        filters = request.json or {}
        filtered_df = data_service.apply_filters(df, filters)
        
        chart_data = None
        
        if chart_type == 'monthly':
            chart_data = data_service.get_monthly_trend_data(filtered_df)
        elif chart_type == 'hierarchy':
            chart_data = data_service.get_hierarchy_data(filtered_df)
        elif chart_type == 'geographic':
            chart_data = data_service.get_geographic_data(filtered_df)
        elif chart_type == 'product':
            chart_data = data_service.get_product_data(filtered_df)
        elif chart_type == 'rbm':
            chart_data = data_service.get_rbm_performance_data(filtered_df)
        elif chart_type == 'map':
            chart_data = data_service.get_map_data(filtered_df)
        
        if chart_data is None:
            return jsonify({'success': False, 'error': f'No data for {chart_type} chart'})
        
        return jsonify({'success': True, 'data': chart_data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/comparison', methods=['POST'])
def get_comparison():
    """Get comparison data between two periods"""
    try:
        df = data_service.load_data()
        if df.empty:
            return jsonify({'success': False, 'error': 'No data loaded'})
        
        params = request.json or {}
        comparison_type = params.get('comparison_type', 'year')
        dimension = params.get('dimension', 'Overall')
        filters = params.get('filters', {})
        
        # Apply base filters first
        filtered_df = data_service.apply_filters(df, filters)
        
        # Get period data
        if comparison_type == 'year':
            period1_year = params.get('period1_year')
            period2_year = params.get('period2_year')
            period1_data = filtered_df[filtered_df['Year'] == int(period1_year)]
            period2_data = filtered_df[filtered_df['Year'] == int(period2_year)]
            period1_label = f"Year {period1_year}"
            period2_label = f"Year {period2_year}"
            
        elif comparison_type == 'fy':
            period1_fy = params.get('period1_fy')
            period2_fy = params.get('period2_fy')
            period1_data = filtered_df[filtered_df['FY_Label'] == period1_fy]
            period2_data = filtered_df[filtered_df['FY_Label'] == period2_fy]
            period1_label = period1_fy
            period2_label = period2_fy
            
        elif comparison_type == 'quarter':
            period1_year = params.get('period1_year')
            period1_quarter = params.get('period1_quarter')
            period2_year = params.get('period2_year')
            period2_quarter = params.get('period2_quarter')
            period1_data = filtered_df[(filtered_df['Year'] == int(period1_year)) & 
                                       (filtered_df['Quarter'] == period1_quarter)]
            period2_data = filtered_df[(filtered_df['Year'] == int(period2_year)) & 
                                       (filtered_df['Quarter'] == period2_quarter)]
            period1_label = f"{period1_quarter} {period1_year}"
            period2_label = f"{period2_quarter} {period2_year}"
        else:
            return jsonify({'success': False, 'error': 'Invalid comparison type'})
        
        # Calculate comparison metrics
        comparisons = data_service.get_comparison_data(filtered_df, period1_data, period2_data, dimension)
        
        # Calculate overview KPIs
        period1_kpis = data_service.calculate_kpis(period1_data)
        period2_kpis = data_service.calculate_kpis(period2_data)
        
        revenue_growth = data_service.calculate_growth(period2_kpis['revenue'], period1_kpis['revenue'])
        profit_growth = data_service.calculate_growth(period2_kpis['profit'], period1_kpis['profit'])
        qty_growth = data_service.calculate_growth(period2_kpis['quantity'], period1_kpis['quantity'])
        margin_change = period2_kpis['margin'] - period1_kpis['margin']
        
        # Calculate absolute differences
        revenue_diff = period2_kpis['revenue'] - period1_kpis['revenue']
        profit_diff = period2_kpis['profit'] - period1_kpis['profit']
        qty_diff = period2_kpis['quantity'] - period1_kpis['quantity']
        
        # Chart data for comparison
        chart_labels = [c['dimension'] for c in comparisons]
        chart_period1 = [c['period1_value'] / 10000000 for c in comparisons]
        chart_period2 = [c['period2_value'] / 10000000 for c in comparisons]
        
        return jsonify({
            'success': True,
            'period1_label': period1_label,
            'period2_label': period2_label,
            'period1_kpis': period1_kpis,
            'period2_kpis': period2_kpis,
            'revenue_growth': round(revenue_growth, 1),
            'profit_growth': round(profit_growth, 1),
            'qty_growth': round(qty_growth, 1),
            'margin_change': round(margin_change, 1),
            'revenue_diff': revenue_diff,
            'profit_diff': profit_diff,
            'qty_diff': qty_diff,
            'comparisons': comparisons,
            'chart': {
                'labels': chart_labels,
                'period1': chart_period1,
                'period2': chart_period2
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/table', methods=['POST'])
def get_table_data():
    """Get filtered data for table display"""
    try:
        df = data_service.load_data()
        if df.empty:
            return jsonify({'success': False, 'error': 'No data loaded'})
        
        filters = request.json or {}
        filtered_df = data_service.apply_filters(df, filters)
        
        # Limit to 100 rows for display
        table_data = data_service.get_data_for_export(filtered_df.head(100))
        
        return jsonify({
            'success': True,
            'data': table_data,
            'total_records': len(filtered_df)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/export', methods=['POST'])
def export_data():
    """Export filtered data as CSV"""
    try:
        df = data_service.load_data()
        if df.empty:
            return jsonify({'success': False, 'error': 'No data loaded'})
        
        filters = request.json or {}
        filtered_df = data_service.apply_filters(df, filters)
        
        # Get export columns
        display_cols = ['Date', 'RBM', 'BDM', 'Branch', 'State', 'District', 'Brand', 'Product', 'QTY', 'Sold_Price', 'Profit']
        available_cols = [col for col in display_cols if col in filtered_df.columns]
        
        csv_data = filtered_df[available_cols].to_csv(index=False)
        
        return Response(
            csv_data,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment;filename=business_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'}
        )
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/filter-options', methods=['POST'])
def get_dynamic_filter_options():
    """Get dynamic filter options based on current selections"""
    try:
        df = data_service.load_data()
        if df.empty:
            return jsonify({'success': False, 'error': 'No data loaded'})
        
        params = request.json or {}
        filtered_df = df.copy()
        
        # Apply partial filters to get cascading options
        if params.get('states'):
            filtered_df = filtered_df[filtered_df['State'].isin(params['states'])]
        
        if params.get('rbms'):
            filtered_df = filtered_df[filtered_df['RBM'].isin(params['rbms'])]
        
        options = data_service.get_filter_options(filtered_df)
        
        return jsonify({'success': True, 'options': options})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

if __name__ == '__main__':
    print("Starting Business Intelligence Dashboard...")
    print("Open http://127.0.0.1:5000 in your browser")
    app.run(debug=True, host='0.0.0.0', port=5000)
