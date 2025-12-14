"""
Data Service Module
Handles all data loading, processing, filtering, calculations, and insights
Optimized for large datasets (1-5 lakh rows)
"""

import pandas as pd
import numpy as np
from datetime import datetime
import hashlib
import json
from config import BRANCH_RBM_BDM_MAPPING, BRANCH_DISTRICT_MAPPING, DISTRICT_STATE_MAPPING, SUPABASE_URL, SUPABASE_KEY, SUPABASE_TABLE

# Cache for data
_cached_data = None
_cache_time = None
CACHE_TTL = 600  # 10 minutes

# Filter result cache (for fast repeated queries)
_filter_cache = {}
_filter_cache_max_size = 50

# Local file cache paths
import os
LOCAL_CACHE_FILE = os.path.join(os.path.dirname(__file__), 'data_cache.csv')
PROCESSED_CACHE_FILE = os.path.join(os.path.dirname(__file__), 'data_processed.pkl')
LOCAL_RAW_CSV = os.path.join(os.path.dirname(__file__), 'raw_data.csv')

# Kerala district coordinates for map
KERALA_DISTRICT_COORDS = {
    "Thiruvananthapuram": [8.5241, 76.9366],
    "Kollam": [8.8932, 76.6141],
    "Pathanamthitta": [9.2648, 76.7870],
    "Alappuzha": [9.4981, 76.3388],
    "Kottayam": [9.5916, 76.5222],
    "Idukki": [9.8494, 76.9714],
    "Ernakulam": [9.9312, 76.2673],
    "Thrissur": [10.5276, 76.2144],
    "Palakkad": [10.7867, 76.6548],
    "Malappuram": [11.0509, 76.0711],
    "Kozhikode": [11.2588, 75.7804],
    "Wayanad": [11.6854, 76.1320],
    "Kannur": [11.8745, 75.3704],
    "Kasaragod": [12.4996, 75.0004],
    "Corporate": [10.8505, 76.2711]
}


def format_indian_currency(value):
    """Format number in Indian currency format with proper UTF-8 Rs. symbol"""
    if pd.isna(value) or value == 0:
        return "Rs.0"
    
    value = float(value)
    sign = "" if value >= 0 else "-"
    abs_value = abs(value)
    
    if abs_value >= 10000000:  # 1 Crore
        formatted = f"{abs_value/10000000:,.2f}"
        return f"{sign}Rs.{formatted} Cr"
    elif abs_value >= 100000:  # 1 Lakh
        formatted = f"{abs_value/100000:,.2f}"
        return f"{sign}Rs.{formatted} Lakh"
    elif abs_value >= 1000:  # 1 Thousand
        formatted = f"{abs_value/1000:,.2f}"
        return f"{sign}Rs.{formatted} K"
    else:
        return f"{sign}Rs.{abs_value:,.2f}"


def format_indian_number(value):
    """Format any number in Indian format"""
    if pd.isna(value) or value == 0:
        return "0"
    
    value = float(value)
    sign = "" if value >= 0 else "-"
    abs_value = abs(value)
    
    if abs_value >= 10000000:  # 1 Crore
        return f"{sign}{abs_value/10000000:,.2f} Cr"
    elif abs_value >= 100000:  # 1 Lakh
        return f"{sign}{abs_value/100000:,.2f} Lakh"
    elif abs_value >= 1000:  # 1 Thousand
        return f"{sign}{abs_value/1000:,.2f} K"
    else:
        return f"{sign}{abs_value:,.0f}"


def calculate_growth(current, previous):
    """Calculate percentage growth"""
    if previous == 0:
        if current > 0:
            return 100
        elif current == 0:
            return 0
        else:
            return -100
    return ((current - previous) / abs(previous)) * 100


def get_filter_hash(filters):
    """Generate hash key for filter combination"""
    filter_str = json.dumps(filters, sort_keys=True, default=str)
    return hashlib.md5(filter_str.encode()).hexdigest()


def get_financial_year(date):
    """Determine financial year (April to March)"""
    if date.month >= 4:
        return f"FY {date.year}-{str(date.year + 1)[-2:]}"
    else:
        return f"FY {date.year - 1}-{str(date.year)[-2:]}"


def get_quarter(date):
    """Get quarter from date"""
    quarter = ((date.month - 1) // 3) + 1
    return f"Q{quarter}"


def get_financial_quarter(date):
    """Get financial quarter (April-June = Q1)"""
    if date.month >= 4:
        adjusted_month = date.month - 3
    else:
        adjusted_month = date.month + 9
    financial_quarter = ((adjusted_month - 1) // 3) + 1
    return f"FQ{financial_quarter}"


def add_rbm_bdm_columns(df):
    """Add RBM and BDM columns to dataframe - OPTIMIZED with vectorized operations"""
    if 'Branch' in df.columns:
        # Create lookup dictionaries for fast vectorized mapping
        rbm_map = {branch: info['RBM'] for branch, info in BRANCH_RBM_BDM_MAPPING.items()}
        bdm_map = {branch: info['BDM'] for branch, info in BRANCH_RBM_BDM_MAPPING.items()}
        
        # Use vectorized map() instead of slow iterrows() - 100x faster!
        df['RBM'] = df['Branch'].map(rbm_map).fillna('NOT ASSIGNED')
        df['BDM'] = df['Branch'].map(bdm_map).fillna('NOT ASSIGNED')
    
    return df


def add_location_columns(df):
    """Add District and State columns to dataframe"""
    if 'Branch' in df.columns:
        df['District'] = df['Branch'].map(BRANCH_DISTRICT_MAPPING)
        df['State'] = df['District'].map(DISTRICT_STATE_MAPPING)
        
        df['District'] = df['District'].fillna('NOT ASSIGNED')
        df['State'] = df['State'].fillna('NOT ASSIGNED')
    
    return df


def load_data(force_refresh=False):
    """Load data from Google Sheets with memory caching for fast filters!
    
    Strategy:
    1. Memory cache (fastest, used for filter operations within same session)
    2. Google Sheets (fetch fresh data on first load or force refresh)
    """
    global _cached_data, _cache_time
    
    # Use memory cache if available (for fast filter operations within same session)
    if not force_refresh and _cached_data is not None and _cache_time is not None:
        # Use longer TTL for session-based caching (30 minutes)
        if (datetime.now() - _cache_time).total_seconds() < 1800:
            print("[CACHE] Loading from memory cache (instant!)")
            return _cached_data
    
    
    # Fetch from Supabase using REST API (requests)
    try:
        import requests
        
        print("[DATA] Fetching data from Supabase (REST API)...")
        print(f"[INFO] Connecting to {SUPABASE_URL}...")
        
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }
        
        # Base URL for the table
        base_url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?select=*"
        
        all_data = []
        offset = 0
        batch_size = 1000
        
        print("[DATA] Starting data fetch sequence...")
        
        while True:
            # Add range header for pagination
            # Range: bytes=0-9 (inclusive)
            range_header = {"Range": f"{offset}-{offset + batch_size - 1}"}
            
            # Merge headers
            req_headers = {**headers, **range_header}
            
            response = requests.get(base_url, headers=req_headers, timeout=60)
            
            if response.status_code != 200:
                print(f"[ERROR] Failed to fetch data: {response.status_code} - {response.text}")
                break
                
            rows = response.json()
            
            if not rows:
                break
                
            all_data.extend(rows)
            
            if len(rows) < batch_size:
                # We reached the end
                break
                
            offset += batch_size
            
            # Print progress every 10k records
            if len(all_data) % 10000 == 0:
                print(f"[DATA] Fetched {len(all_data):,} records so far...")
                
        print(f"[DATA] Loaded {len(all_data):,} raw records from Supabase!")
        
        if not all_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(all_data)
        
        print("[PROCESS] Processing data...")
        df.columns = df.columns.str.strip()
        
        # Process date column
        # Handle various date formats potentially coming from Supabase (often ISO string YYYY-MM-DD or similar)
        if 'Month' in df.columns:
            df['Date'] = pd.to_datetime(df['Month'], errors='coerce')
        elif 'date' in df.columns: # Common lowercase in DB
             df['Date'] = pd.to_datetime(df['date'], errors='coerce')
        elif 'Date' in df.columns:
             df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
             
        if 'Date' in df.columns:
            if df['Date'].isna().all():
                 # Try other fallbacks if needed, or fail gracefully
                 pass
            else:
                df = df.dropna(subset=['Date'])
                df['Year'] = df['Date'].dt.year.astype(int)
                df['Month_Num'] = df['Date'].dt.month.astype(int)
                df['Month_Short'] = df['Date'].dt.strftime('%b')
                df['Month_Full'] = df['Date'].dt.strftime('%B')
                df['Month_Year'] = df['Date'].dt.strftime('%b %Y')
                
                # Financial Year Calculations - OPTIMIZED with vectorized operations
                years = df['Date'].dt.year
                months = df['Date'].dt.month
                
                # Vectorized financial year calculation
                fy_start_year = np.where(months >= 4, years, years - 1)
                fy_end_year_short = (fy_start_year + 1) % 100
                
                # Use Pandas Series for safe string concatenation
                s_start = pd.Series(fy_start_year).astype(str)
                s_end = pd.Series(fy_end_year_short).astype(str).str.zfill(2)
                
                df['Financial_Year'] = 'FY ' + s_start + '-' + s_end
                
                # Vectorized quarter calculation
                df['Quarter'] = 'Q' + (((months - 1) // 3) + 1).astype(str)
                
                # Vectorized financial quarter calculation
                adjusted_month = np.where(months >= 4, months - 3, months + 9)
                # Use Pandas Series for safe string concatenation
                df['Financial_Quarter'] = 'FQ' + pd.Series(((adjusted_month - 1) // 3) + 1).astype(str)
                
                df['FY_Label'] = df['Financial_Year']
        
        # Clean numeric columns
        numeric_cols = ['QTY', 'Taxable Value', 'Sold Price', 'Direct Discount', 'Profit']
        
        for col in df.columns:
            for possible_col in numeric_cols:
                # Case insensitive match
                if possible_col.lower() in col.lower():
                    try:
                        # Handle if it's already numeric (DB usually provides numbers)
                        if pd.api.types.is_numeric_dtype(df[col]):
                            df[col] = df[col].fillna(0).astype(float)
                        else:
                            df[col] = df[col].astype(str).str.replace(',', '')
                            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(float)
                    except:
                        pass
        
        # Rename columns to match internal schema
        column_mapping = {}
        for col in df.columns:
            col_lower = col.lower()
            if 'qty' in col_lower:
                column_mapping[col] = 'QTY'
            elif 'sold' in col_lower or 'price' in col_lower:
                column_mapping[col] = 'Sold_Price'
            elif 'profit' in col_lower:
                column_mapping[col] = 'Profit'
            elif 'discount' in col_lower:
                column_mapping[col] = 'Discount'
            elif 'branch' in col_lower:
                column_mapping[col] = 'Branch'
            elif 'brand' in col_lower:
                column_mapping[col] = 'Brand'
            elif 'product' in col_lower and 'code' in col_lower:
                column_mapping[col] = 'Product_Code'
            elif 'product' in col_lower and not 'code' in col_lower:
                column_mapping[col] = 'Product'
        
        if column_mapping:
            df = df.rename(columns=column_mapping)
        
        # Ensure Product_Code exists
        if 'Product_Code' not in df.columns:
            df['Product_Code'] = 'N/A'
        
        # Add RBM/BDM and Location columns
        print("[MAPPING] Adding RBM/BDM mappings...")
        df = add_rbm_bdm_columns(df)
        df = add_location_columns(df)
        
        # Cache in memory for fast filter operations
        _cached_data = df
        _cache_time = datetime.now()
        
        # Clear filter cache when new data is loaded
        _filter_cache.clear()
        
        print(f"[SUCCESS] Loaded and cached {len(df):,} records from Supabase!")
        return df
        
    except Exception as e:
        print(f"[ERROR] Error loading data from Supabase: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def get_filter_options(df):
    """Get all available filter options from data"""
    options = {
        'years': sorted([int(y) for y in df['Year'].unique()], reverse=True) if 'Year' in df.columns else [],
        'financial_years': sorted(df['FY_Label'].unique().tolist(), reverse=True) if 'FY_Label' in df.columns else [],
        'quarters': sorted(df['Quarter'].unique().tolist()) if 'Quarter' in df.columns else [],
        'states': sorted([s for s in df['State'].dropna().unique().tolist() if s != 'NOT ASSIGNED']) if 'State' in df.columns else [],
        'districts': sorted([d for d in df['District'].dropna().unique().tolist() if d != 'NOT ASSIGNED']) if 'District' in df.columns else [],
        'rbms': sorted([r for r in df['RBM'].unique().tolist() if r != 'NOT ASSIGNED']) if 'RBM' in df.columns else [],
        'bdms': sorted([b for b in df['BDM'].unique().tolist() if b != 'NOT ASSIGNED']) if 'BDM' in df.columns else [],
        'branches': sorted(df['Branch'].unique().tolist()) if 'Branch' in df.columns else [],
        'brands': sorted(df['Brand'].dropna().unique().tolist()) if 'Brand' in df.columns else [],
        'products': sorted(df['Product'].dropna().unique().tolist()) if 'Product' in df.columns else []
    }
    return options


def apply_filters(df, filters, use_cache=True):
    """Apply multi-select filters to dataframe with optional caching"""
    global _filter_cache
    
    # Check cache first
    if use_cache:
        filter_hash = get_filter_hash(filters)
        if filter_hash in _filter_cache:
            return _filter_cache[filter_hash]
    
    filtered = df
    
    # Time period filters - Use boolean indexing for speed
    if filters.get('period_type') == 'year' and filters.get('year'):
        filtered = filtered[filtered['Year'] == int(filters['year'])]
    elif filters.get('period_type') == 'fy' and filters.get('fy'):
        filtered = filtered[filtered['FY_Label'] == filters['fy']]
    elif filters.get('period_type') == 'quarter' and filters.get('year') and filters.get('quarter'):
        filtered = filtered[(filtered['Year'] == int(filters['year'])) & 
                           (filtered['Quarter'] == filters['quarter'])]
    
    # Multi-select filters - Use isin() for fast filtering
    if filters.get('states'):
        filtered = filtered[filtered['State'].isin(filters['states'])]
    
    if filters.get('districts'):
        filtered = filtered[filtered['District'].isin(filters['districts'])]
    
    if filters.get('rbms'):
        filtered = filtered[filtered['RBM'].isin(filters['rbms'])]
    
    if filters.get('bdms'):
        filtered = filtered[filtered['BDM'].isin(filters['bdms'])]
    
    if filters.get('branches'):
        filtered = filtered[filtered['Branch'].isin(filters['branches'])]
    
    if filters.get('brands'):
        filtered = filtered[filtered['Brand'].isin(filters['brands'])]
    
    if filters.get('products'):
        filtered = filtered[filtered['Product'].isin(filters['products'])]
    
    # Price range filter
    if filters.get('price_min') is not None and filters.get('price_max') is not None:
        filtered = filtered[(filtered['Sold_Price'] >= filters['price_min']) & 
                           (filtered['Sold_Price'] <= filters['price_max'])]
    
    # Cache the result
    if use_cache:
        if len(_filter_cache) >= _filter_cache_max_size:
            # Remove oldest entry
            _filter_cache.pop(next(iter(_filter_cache)))
        _filter_cache[filter_hash] = filtered
    
    return filtered


def calculate_kpis(df):
    """Calculate KPI metrics from filtered data"""
    revenue = float(df['Sold_Price'].sum()) if 'Sold_Price' in df.columns else 0.0
    profit = float(df['Profit'].sum()) if 'Profit' in df.columns else 0.0
    quantity = float(df['QTY'].sum()) if 'QTY' in df.columns else 0.0
    discount = float(df['Discount'].sum()) if 'Discount' in df.columns else 0.0
    
    margin = (profit / revenue * 100) if revenue > 0 else 0.0
    discount_pct = (discount / revenue * 100) if revenue > 0 else 0.0
    avg_price = revenue / quantity if quantity > 0 else 0
    
    stores = df['Branch'].nunique() if 'Branch' in df.columns else 0
    brands = df['Brand'].nunique() if 'Brand' in df.columns else 0
    products = df['Product'].nunique() if 'Product' in df.columns else 0
    states = df['State'].nunique() if 'State' in df.columns else 0
    districts = df['District'].nunique() if 'District' in df.columns else 0
    
    return {
        'revenue': revenue,
        'revenue_formatted': format_indian_currency(revenue),
        'profit': profit,
        'profit_formatted': format_indian_currency(profit),
        'quantity': quantity,
        'quantity_formatted': format_indian_number(quantity),
        'discount': discount,
        'discount_formatted': format_indian_currency(discount),
        'margin': round(margin, 1),
        'discount_pct': round(discount_pct, 1),
        'avg_price': round(avg_price, 0),
        'stores': stores,
        'brands': brands,
        'products': products,
        'states': states,
        'districts': districts,
        'records': len(df)
    }


def generate_insights(df):
    """Generate AI-style insights from the data"""
    insights = {
        'top_performer': '',
        'growth_trend': '',
        'highlight': '',
        'alert': ''
    }
    
    if df.empty:
        return {
            'top_performer': 'No data available',
            'growth_trend': 'Load data to see trends',
            'highlight': 'Apply filters to view insights',
            'alert': 'No alerts'
        }
    
    try:
        # Top Performer - Branch by Revenue
        if 'Branch' in df.columns and 'Sold_Price' in df.columns:
            branch_revenue = df.groupby('Branch')['Sold_Price'].sum().sort_values(ascending=False)
            if len(branch_revenue) > 0:
                top_branch = branch_revenue.index[0]
                top_revenue = format_indian_currency(branch_revenue.iloc[0])
                insights['top_performer'] = f"{top_branch} leads with {top_revenue} in revenue"
        
        # Growth Trend - Compare months if available
        if 'Month_Year' in df.columns and 'Sold_Price' in df.columns:
            monthly = df.groupby('Month_Year')['Sold_Price'].sum()
            if len(monthly) >= 2:
                # Sort by date
                last_month_val = monthly.iloc[-1]
                prev_month_val = monthly.iloc[-2]
                growth = calculate_growth(last_month_val, prev_month_val)
                direction = "up" if growth > 0 else "down"
                insights['growth_trend'] = f"Revenue is {direction} {abs(growth):.1f}% compared to previous month"
        
        # Highlight - Best performing RBM
        if 'RBM' in df.columns and 'Profit' in df.columns:
            rbm_profit = df.groupby('RBM').agg({
                'Profit': 'sum',
                'Sold_Price': 'sum'
            })
            rbm_profit['Margin'] = (rbm_profit['Profit'] / rbm_profit['Sold_Price'] * 100).round(1)
            rbm_profit = rbm_profit[rbm_profit['Sold_Price'] > 0].sort_values('Margin', ascending=False)
            if len(rbm_profit) > 0:
                best_rbm = rbm_profit.index[0]
                best_margin = rbm_profit['Margin'].iloc[0]
                insights['highlight'] = f"RBM {best_rbm} has the best margin at {best_margin}%"
        
        # Alert - Low margin products or branches
        if 'Branch' in df.columns and 'Profit' in df.columns and 'Sold_Price' in df.columns:
            branch_perf = df.groupby('Branch').agg({
                'Profit': 'sum',
                'Sold_Price': 'sum'
            })
            branch_perf['Margin'] = (branch_perf['Profit'] / branch_perf['Sold_Price'] * 100).round(1)
            low_margin = branch_perf[branch_perf['Margin'] < 5]
            if len(low_margin) > 0:
                count = len(low_margin)
                insights['alert'] = f"{count} branches have profit margin below 5%"
            else:
                insights['alert'] = "All branches performing above minimum margin threshold"
    
    except Exception as e:
        print(f"[ERROR] Error generating insights: {str(e)}")
        insights = {
            'top_performer': 'Analysis in progress...',
            'growth_trend': 'Calculating trends...',
            'highlight': 'Processing data...',
            'alert': 'Checking metrics...'
        }
    
    return insights


def get_map_data(df):
    """Get geographic data for India map visualization"""
    if df.empty or 'District' not in df.columns:
        return {'districts': []}
    
    district_data = df.groupby('District').agg({
        'Sold_Price': 'sum',
        'Profit': 'sum',
        'Branch': 'nunique'
    }).reset_index()
    
    district_data['Margin'] = (district_data['Profit'] / district_data['Sold_Price'] * 100).round(1)
    
    # Add coordinates
    map_data = []
    for _, row in district_data.iterrows():
        district = row['District']
        if district in KERALA_DISTRICT_COORDS:
            coords = KERALA_DISTRICT_COORDS[district]
            map_data.append({
                'name': district,
                'lat': coords[0],
                'lng': coords[1],
                'revenue': float(row['Sold_Price']),
                'revenue_formatted': format_indian_currency(row['Sold_Price']),
                'profit': float(row['Profit']),
                'margin': float(row['Margin']),
                'branches': int(row['Branch'])
            })
    
    return {'districts': map_data}


def get_monthly_trend_data(df):
    """Get monthly trend chart data"""
    if 'Month_Year' not in df.columns or 'Sold_Price' not in df.columns:
        return None
    
    monthly_data = df.groupby('Month_Year').agg({
        'Sold_Price': 'sum',
        'Profit': 'sum',
        'QTY': 'sum'
    }).reset_index()
    
    if monthly_data.empty or len(monthly_data) < 2:
        return None
    
    # Sort by date
    monthly_data['Date'] = pd.to_datetime(monthly_data['Month_Year'], format='%b %Y', errors='coerce')
    monthly_data = monthly_data.sort_values('Date')
    
    return {
        'labels': monthly_data['Month_Year'].tolist(),
        'revenue': (monthly_data['Sold_Price'] / 10000000).round(2).tolist(),
        'profit': (monthly_data['Profit'] / 10000000).round(2).tolist(),
        'quantity': monthly_data['QTY'].tolist()
    }


def get_hierarchy_data(df):
    """Get hierarchy (RBM/BDM) chart data"""
    if 'RBM' not in df.columns or 'BDM' not in df.columns:
        return None
    
    # RBM performance
    rbm_data = df.groupby('RBM').agg({
        'Sold_Price': 'sum',
        'Profit': 'sum',
        'QTY': 'sum'
    }).reset_index()
    
    rbm_data = rbm_data.sort_values('Sold_Price', ascending=False)
    rbm_data['Profit_Margin'] = (rbm_data['Profit'] / rbm_data['Sold_Price'] * 100).round(1)
    
    # Sunburst data
    hierarchy_data = df.groupby(['RBM', 'BDM', 'Branch']).agg({
        'Sold_Price': 'sum',
        'Profit': 'sum'
    }).reset_index()
    
    return {
        'rbm': {
            'labels': rbm_data['RBM'].tolist(),
            'revenue': (rbm_data['Sold_Price'] / 10000000).round(2).tolist(),
            'profit': (rbm_data['Profit'] / 10000000).round(2).tolist(),
            'margin': rbm_data['Profit_Margin'].tolist()
        },
        'hierarchy': hierarchy_data.to_dict('records')
    }


def get_geographic_data(df):
    """Get geographic (State/District) chart data"""
    if 'State' not in df.columns or 'District' not in df.columns:
        return None
    
    # State-wise
    state_data = df.groupby('State').agg({
        'Sold_Price': 'sum',
        'Profit': 'sum',
        'Branch': 'nunique'
    }).reset_index()
    
    state_data = state_data.sort_values('Sold_Price', ascending=False)
    state_data['Profit_Margin'] = (state_data['Profit'] / state_data['Sold_Price'] * 100).round(1)
    
    # District-wise
    district_data = df.groupby(['State', 'District']).agg({
        'Sold_Price': 'sum',
        'Profit': 'sum',
        'Branch': 'nunique'
    }).reset_index()
    
    district_data = district_data.sort_values('Sold_Price', ascending=False).head(15)
    
    return {
        'states': {
            'labels': state_data['State'].tolist(),
            'revenue': (state_data['Sold_Price'] / 10000000).round(2).tolist(),
            'profit_margin': state_data['Profit_Margin'].tolist(),
            'branches': state_data['Branch'].tolist()
        },
        'districts': district_data.to_dict('records')
    }


# Result cache for aggregated chart data
_result_cache = {}
_result_cache_max_size = 100

def get_dashboard_data(filters):
    """Get ALL dashboard data with result caching"""
    global _result_cache
    
    # Generate hash for these filters
    filter_hash = get_filter_hash(filters)
    
    # Check cache
    if filter_hash in _result_cache:
        print(f"[CACHE] Serving dashboard data from cache ({filter_hash[:8]})")
        return _result_cache[filter_hash]
    
    # Load and filter data
    df = load_data()
    if df.empty:
        return None
        
    filtered_df = apply_filters(df, filters)
    
    # Calculate all metrics
    kpis = calculate_kpis(filtered_df)
    monthly_data = get_monthly_trend_data(filtered_df)
    hierarchy_data = get_hierarchy_data(filtered_df)
    geographic_data = get_geographic_data(filtered_df)
    product_data = get_product_data(filtered_df)
    rbm_data = get_rbm_performance_data(filtered_df)
    insights = generate_insights(filtered_df)
    map_data = get_map_data(filtered_df)
    
    # Get table data (limited to 100 rows)
    table_data = get_data_for_export(filtered_df.head(100))
    
    result = {
        'success': True,
        'kpis': kpis,
        'insights': insights,
        'charts': {
            'monthly': monthly_data,
            'hierarchy': hierarchy_data,
            'geographic': geographic_data,
            'product': product_data,
            'rbm': rbm_data,
            'map': map_data
        },
        'table': {
            'data': table_data,
            'total_records': len(filtered_df)
        }
    }
    
    # Update cache
    if len(_result_cache) >= _result_cache_max_size:
        _result_cache.pop(next(iter(_result_cache)))
    _result_cache[filter_hash] = result
    
    return result

def get_product_data(df):
    """Get product chart data with robust handling for empty/null values"""
    if df.empty or 'Product' not in df.columns or 'Sold_Price' not in df.columns:
        return {
            'labels': [],
            'revenue': [],
            'profit_margin': [],
            'quantity': []
        }
    
    try:
        # Filter out invalid products first
        valid_df = df[df['Product'].notna() & (df['Product'].astype(str).str.strip() != '')]
        
        if valid_df.empty:
             return {
                'labels': [],
                'revenue': [],
                'profit_margin': [],
                'quantity': []
            }

        product_data = valid_df.groupby('Product').agg({
            'Sold_Price': 'sum',
            'Profit': 'sum',
            'QTY': 'sum'
        }).reset_index()
        
        # Sort by Profit to get Top 20 most profitable
        product_data = product_data.sort_values('Profit', ascending=False).head(20)
        
        # Sort by Profit Ascending for Plotly Horizontal Bar (so largest is at top)
        product_data = product_data.sort_values('Profit', ascending=True)
        
        # Safe division for margin
        product_data['Profit_Margin'] = np.where(
            product_data['Sold_Price'] > 0,
            (product_data['Profit'] / product_data['Sold_Price'] * 100).round(1),
            0.0
        )
        
        # Truncate long product names
        product_data['Product_Short'] = product_data['Product'].apply(
            lambda x: str(x)[:30] + '...' if len(str(x)) > 30 else str(x)
        )
        
        return {
            'labels': product_data['Product_Short'].tolist(),
            'revenue': (product_data['Sold_Price'] / 10000000).round(2).tolist(),
            'profit': (product_data['Profit'] / 100000).round(2).tolist(), # Profit in Lakhs
            'profit_margin': product_data['Profit_Margin'].tolist(),
            'quantity': product_data['QTY'].tolist()
        }
    except Exception as e:
        print(f"[ERROR] Error in get_product_data: {str(e)}")
        return {
            'labels': [],
            'revenue': [],
            'profit_margin': [],
            'quantity': []
        }


def get_rbm_performance_data(df):
    """Get RBM performance chart data"""
    if 'RBM' not in df.columns or 'Sold_Price' not in df.columns:
        return None
    
    rbm_data = df.groupby('RBM').agg({
        'Sold_Price': 'sum',
        'Profit': 'sum',
        'QTY': 'sum'
    }).reset_index()
    
    rbm_data = rbm_data.sort_values('Sold_Price', ascending=False)
    rbm_data['Profit_Margin'] = (rbm_data['Profit'] / rbm_data['Sold_Price'] * 100).round(1)
    
    return {
        'labels': rbm_data['RBM'].tolist(),
        'revenue': (rbm_data['Sold_Price'] / 10000000).round(2).tolist(),
        'profit': (rbm_data['Profit'] / 10000000).round(2).tolist(),
        'profit_margin': rbm_data['Profit_Margin'].tolist(),
        'quantity': rbm_data['QTY'].tolist()
    }


def get_comparison_data(df, period1_data, period2_data, comparison_dimension):
    """Get comparison data between two periods"""
    comparisons = []
    
    if comparison_dimension == "Overall":
        period1_revenue = period1_data['Sold_Price'].sum() if 'Sold_Price' in period1_data.columns else 0
        period2_revenue = period2_data['Sold_Price'].sum() if 'Sold_Price' in period2_data.columns else 0
        revenue_growth = calculate_growth(period2_revenue, period1_revenue)
        
        period1_profit = period1_data['Profit'].sum() if 'Profit' in period1_data.columns else 0
        period2_profit = period2_data['Profit'].sum() if 'Profit' in period2_data.columns else 0
        profit_growth = calculate_growth(period2_profit, period1_profit)
        
        period1_qty = period1_data['QTY'].sum() if 'QTY' in period1_data.columns else 0
        period2_qty = period2_data['QTY'].sum() if 'QTY' in period2_data.columns else 0
        qty_growth = calculate_growth(period2_qty, period1_qty)
        
        comparisons = [
            {'dimension': 'Revenue', 'period1_value': period1_revenue, 'period2_value': period2_revenue, 'growth': revenue_growth},
            {'dimension': 'Profit', 'period1_value': period1_profit, 'period2_value': period2_profit, 'growth': profit_growth},
            {'dimension': 'Quantity', 'period1_value': period1_qty, 'period2_value': period2_qty, 'growth': qty_growth}
        ]
    
    elif comparison_dimension in ["RBM", "BDM", "State", "District", "Brand", "Branch"]:
        col = comparison_dimension
        
        period1_grouped = period1_data.groupby(col).agg({'Sold_Price': 'sum'}).reset_index()
        period2_grouped = period2_data.groupby(col).agg({'Sold_Price': 'sum'}).reset_index()
        
        all_values = set(period1_grouped[col].unique()) | set(period2_grouped[col].unique())
        
        # Sort by total revenue (both periods combined) and show ALL items (max 200 for performance)
        value_totals = []
        for value in all_values:
            p1_val = period1_grouped[period1_grouped[col] == value]['Sold_Price'].sum()
            p2_val = period2_grouped[period2_grouped[col] == value]['Sold_Price'].sum()
            value_totals.append((value, p1_val + p2_val))
        
        # Sort by total revenue descending, show all (up to 200)
        sorted_values = sorted(value_totals, key=lambda x: x[1], reverse=True)[:200]
        
        for value, _ in sorted_values:
            p1_val = period1_grouped[period1_grouped[col] == value]['Sold_Price'].sum()
            p2_val = period2_grouped[period2_grouped[col] == value]['Sold_Price'].sum()
            growth = calculate_growth(p2_val, p1_val)
            
            # Show full branch names (no truncation for Branch column)
            display_name = str(value) if comparison_dimension == 'Branch' else (value[:30] + ('...' if len(str(value)) > 30 else ''))
            
            comparisons.append({
                'dimension': display_name,
                'period1_value': p1_val,
                'period2_value': p2_val,
                'growth': round(growth, 1)
            })
    
    # Format values
    for c in comparisons:
        c['period1_formatted'] = format_indian_currency(c['period1_value'])
        c['period2_formatted'] = format_indian_currency(c['period2_value'])
        # Use text indicators instead of emojis
        if c['growth'] > 20:
            c['growth_indicator'] = 'Strong Growth'
        elif c['growth'] > 10:
            c['growth_indicator'] = 'Good Growth'
        elif c['growth'] > 0:
            c['growth_indicator'] = 'Slight Growth'
        elif c['growth'] < -10:
            c['growth_indicator'] = 'Decline'
        elif c['growth'] < 0:
            c['growth_indicator'] = 'Slight Decline'
        else:
            c['growth_indicator'] = 'No Change'
    
    return comparisons


def get_data_for_export(df):
    """Get data formatted for CSV export"""
    display_cols = ['Date', 'RBM', 'BDM', 'Branch', 'State', 'District', 'Brand', 'Product', 'QTY', 'Sold_Price', 'Profit']
    available_cols = [col for col in display_cols if col in df.columns]
    return df[available_cols].to_dict('records')
