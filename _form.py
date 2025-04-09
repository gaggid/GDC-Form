import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import hashlib
import os
import uuid
import json
from contextlib import contextmanager


# Page configuration
st.set_page_config(
    page_title="GDC Dashboard Data Management",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Initialize session state variables
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'current_hub' not in st.session_state:
    st.session_state.current_hub = None
if 'current_view' not in st.session_state:
    st.session_state.current_view = "hub_metrics"  # Default view

@st.cache_resource
def init_connection():
    """Initialize and cache a database connection."""
    return sqlite3.connect('gdc_data.db', check_same_thread=False, timeout=30)

# Modified context manager that uses the connection pool
@contextmanager
def get_db_connection():
    """
    Context manager for SQLite database connections.
    Ensures connections are properly closed after use.
    """
    conn = None
    try:
        conn = init_connection()
        yield conn
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
        raise
    finally:
        # Don't close the connection - just commit changes
        if conn:
            try:
                conn.commit()
            except:
                pass  # Ignore errors during commit

# Database setup function
def setup_database():
    """Set up the database schema and initial data if needed."""
    with get_db_connection() as conn:
        c = conn.cursor()
        
        # Set pragmas for better concurrency
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA busy_timeout=5000")
        c.execute("PRAGMA synchronous=NORMAL")
        
        # Create users table for hub login
        c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            hub_name TEXT NOT NULL,
            is_admin INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Create hubs table
        c.execute('''
        CREATE TABLE IF NOT EXISTS hubs (
            id INTEGER PRIMARY KEY,
            hub_name TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Create hub metrics table (aggregated data)
        c.execute('''
        CREATE TABLE IF NOT EXISTS hub_metrics (
            id INTEGER PRIMARY KEY,
            hub_id INTEGER NOT NULL,
            total_headcount INTEGER,
            total_seats INTEGER,
            total_clients INTEGER,
            services_offered INTEGER,
            female_percent REAL,
            male_percent REAL,
            other_gender_percent REAL,
            campus_type TEXT,
            sez_status TEXT,
            location TEXT,
            coverage_hours TEXT,
            transport_facilities TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT,
            bench_count INTEGER DEFAULT 0,
            location_headcounts TEXT,
            certifications TEXT,
            FOREIGN KEY (hub_id) REFERENCES hubs(id)
        )
        ''')
        
        # Check if we need to update existing hub_metrics table with the new columns
        # Get column info from hub_metrics table
        c.execute("PRAGMA table_info(hub_metrics)")
        columns = [column[1] for column in c.fetchall()]
        
        # Add new columns if they don't exist
        if "bench_count" not in columns:
            c.execute("ALTER TABLE hub_metrics ADD COLUMN bench_count INTEGER DEFAULT 0")
        
        if "location_headcounts" not in columns:
            c.execute("ALTER TABLE hub_metrics ADD COLUMN location_headcounts TEXT")
        
        if "certifications" not in columns:
            c.execute("ALTER TABLE hub_metrics ADD COLUMN certifications TEXT")
        
        # Create hub capabilities table (aggregated data)
        c.execute('''
        CREATE TABLE IF NOT EXISTS hub_capabilities (
            id INTEGER PRIMARY KEY,
            hub_id INTEGER NOT NULL,
            capability_name TEXT NOT NULL,
            capability_category TEXT NOT NULL,
            headcount INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT,
            capability_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (hub_id) REFERENCES hubs(id),
            UNIQUE(hub_id, capability_name)
        )
        ''')
        
        # Create client metrics table (aggregated data)
        c.execute('''
        CREATE TABLE IF NOT EXISTS client_metrics (
            id INTEGER PRIMARY KEY,
            hub_id INTEGER NOT NULL,
            client_name TEXT NOT NULL,
            engagement_status TEXT,
            commercial_model TEXT,
            capability_category TEXT,
            capability_name TEXT,
            relationship_duration REAL,
            scope_summary TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT,
            FOREIGN KEY (hub_id) REFERENCES hubs(id),
            UNIQUE(hub_id, client_name)
        )
        ''')
        
        # Create people metrics table (aggregated data for HR charts)
        c.execute('''
        CREATE TABLE IF NOT EXISTS people_metrics (
            id INTEGER PRIMARY KEY,
            hub_id INTEGER NOT NULL,
            metric_name TEXT NOT NULL,
            metric_value REAL,
            metric_category TEXT,
            time_period TEXT,
            hiring_reason TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT,
            people_metric_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (hub_id) REFERENCES hubs(id),
            UNIQUE(hub_id, metric_name, time_period, hiring_reason)
        )
        ''')
        
        # Insert default capabilities with categories
        capabilities_data = [
            # MEDIA+
            ('Ad Operations', 'MEDIA+'),
            ('Media Reporting', 'MEDIA+'),
            ('SEO', 'MEDIA+'),
            ('Media Activation', 'MEDIA+'),
            ('Retail Media', 'MEDIA+'),
            ('Paid Search', 'MEDIA+'),
            ('Commerce', 'MEDIA+'),
            ('Programmatic', 'MEDIA+'),
            ('Analytics & Insights', 'MEDIA+'),
            
            # CONTENT+
            ('Language Services', 'CONTENT+'),
            ('Post-production', 'CONTENT+'),
            ('Transcreation', 'CONTENT+'),
            ('Adaptation', 'CONTENT+'),
            ('Content for Commerce', 'CONTENT+'),
            
            # CX+
            ('Experience Platforms', 'CX+'),
            ('Commerce Platforms', 'CX+'),
            ('Marketing Automation', 'CX+'),
            ('Engineering Services', 'CX+'),
            ('Creative Technology', 'CX+'),
            ('CRM', 'CX+'),
            ('DevOps', 'CX+'),
            ('Quality Engineering', 'CX+')
        ]
        
        # Insert default hubs if table is empty
        c.execute("SELECT COUNT(*) FROM hubs")
        if c.fetchone()[0] == 0:
            default_hubs = [
                "AKQA",
                "Mirum Digital Pvt Ltd",
                "GroupM Nexus Global Team",
                "Hogarth Worldwide",
                "Hogarth Studios",
                "Verticurl",
                "VML-Tech Commerce"
            ]
            
            for hub_name in default_hubs:
                c.execute('INSERT INTO hubs (hub_name) VALUES (?)', (hub_name,))
                
                # Get the hub id
                c.execute('SELECT id FROM hubs WHERE hub_name = ?', (hub_name,))
                hub_id = c.fetchone()[0]
                
                # Add initial default hub metrics
                c.execute('''
                INSERT INTO hub_metrics (
                    hub_id, total_headcount, total_seats, total_clients, 
                    services_offered, female_percent, male_percent, other_gender_percent,
                    campus_type, sez_status, location, coverage_hours, transport_facilities
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    hub_id, 95, 76, 12, 8, 
                    35.0, 63.0, 2.0,
                    "In-Campus" if "AKQA" in hub_name or "GroupM" in hub_name or "VML-Tech Commerce" in hub_name or "Hogarth Studios" in hub_name else "Outside-Campus",
                    "Yes" if "Hogarth Worldwide" in hub_name else "No",
                    "Chennai, Hyderabad, Gurugram" if "Hogarth Worldwide" in hub_name else 
                    "Coimbatore, Hyderabad, Gurugram" if "Verticurl" in hub_name else
                    "Mumbai and Gurugram" if "Mirum" in hub_name else "Gurugram",
                    "24x5",
                    "Yes" if "Hogarth" in hub_name or "Verticurl" in hub_name else "No"
                ))
                
                # Add default capabilities based on hub name
                primary_category = "CX+" if "AKQA" in hub_name or "Mirum" in hub_name or "VML-Tech Commerce" in hub_name or "Verticurl" in hub_name else \
                                 "MEDIA+" if "GroupM" in hub_name else \
                                 "CONTENT+" if "Hogarth" in hub_name else "CX+"
                
                # Add capabilities matching the primary category
                for capability_name, category in capabilities_data:
                    if category == primary_category:
                        c.execute('''
                        INSERT INTO hub_capabilities (hub_id, capability_name, capability_category, headcount)
                        VALUES (?, ?, ?, ?)
                        ''', (hub_id, capability_name, category, 0))
                
                # Add sample client metrics
                sample_clients = [f"Client {i+1}" for i in range(5)]
                for client in sample_clients:
                    c.execute('''
                    INSERT INTO client_metrics (hub_id, client_name, engagement_status, commercial_model, capability_category, scope_summary)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''', (hub_id, client, "Active", "FTE", primary_category, "Sample scope details"))
                
                # Add sample people metrics
                people_metric_types = [
                    # Turnover metrics
                    # ("Overall Turnover Rate", "Turnover", "2025", 15.5),
                    # ("Voluntary Turnover", "Turnover", "2025", 10.2),
                    # ("Involuntary Turnover", "Turnover", "2025", 5.3),
                    
                    # Tenure data
                    ("Tenure <1 year", "Tenure", "2025", 25),
                    ("Tenure 1-2 years", "Tenure", "2025", 22),
                    ("Tenure 2-3 years", "Tenure", "2025", 18),
                    ("Tenure 3-5 years", "Tenure", "2025", 15),
                    ("Tenure 5-7 years", "Tenure", "2025", 10),
                    ("Tenure 7-10 years", "Tenure", "2025", 5),
                    ("Tenure 10+ years", "Tenure", "2025", 3),
                    
                    # Employee type distribution
                    ("Permanent Employees", "Employment Type", "2025", 75),
                    ("Contract Employees", "Employment Type", "2025", 25),
                    
                    # Marital status
                    ("Single", "Marital Status", "2025", 55),
                    ("Married", "Marital Status", "2025", 40),
                    ("Other Marital Status", "Marital Status", "2025", 5)
                ]
                
                for metric_name, category, period, value in people_metric_types:
                    c.execute('''
                    INSERT INTO people_metrics (hub_id, metric_name, metric_value, metric_category, time_period)
                    VALUES (?, ?, ?, ?, ?)
                    ''', (hub_id, metric_name, value, category, period))
        
        # Create default admin user if no users exist
        c.execute("SELECT COUNT(*) FROM users")
        if c.fetchone()[0] == 0:
            # Creating admin user with password "admin123"
            admin_password_hash = hashlib.sha256("admin123".encode()).hexdigest()
            c.execute('''
            INSERT INTO users (username, password_hash, hub_name, is_admin)
            VALUES (?, ?, ?, ?)
            ''', ("admin", admin_password_hash, "ALL", 1))
            
            # Create a user for each hub with the hub name as username and password
            c.execute("SELECT id, hub_name FROM hubs")
            hubs = c.fetchall()
            
            for _, hub_name in hubs:
                username = hub_name.lower().replace(" ", "_")
                password = username + "123"
                password_hash = hashlib.sha256(password.encode()).hexdigest()
                
                c.execute('''
                INSERT INTO users (username, password_hash, hub_name, is_admin)
                VALUES (?, ?, ?, ?)
                ''', (username, password_hash, hub_name, 0))
        
        # For hub_metrics table
        c.execute("PRAGMA table_info(hub_metrics)")
        hub_metrics_columns = [column[1] for column in c.fetchall()]
        
        hub_metrics_timestamp_columns = [
            "metrics_updated_at", 
            "location_updated_at", 
            "certifications_updated_at"
        ]
        
        for column in hub_metrics_timestamp_columns:
            if column not in hub_metrics_columns:
                c.execute(f"ALTER TABLE hub_metrics ADD COLUMN {column} TIMESTAMP")
        
        # For hub_capabilities table
        c.execute("PRAGMA table_info(hub_capabilities)")
        capabilities_columns = [column[1] for column in c.fetchall()]
        
        if "capability_updated_at" not in capabilities_columns:
            c.execute("ALTER TABLE hub_capabilities ADD COLUMN capability_updated_at TIMESTAMP")
        if "headcount" not in capabilities_columns:
            add_column_if_not_exists(c, "hub_capabilities", "headcount", "INTEGER DEFAULT 0")
        
        # For client_metrics table
        c.execute("PRAGMA table_info(client_metrics)")
        client_columns = [column[1] for column in c.fetchall()]

        # Add employee_count column if it doesn't exist
        if "employee_count" not in client_columns:
            c.execute("ALTER TABLE client_metrics ADD COLUMN employee_count INTEGER DEFAULT 0")

        # Add new columns if they don't exist
        if "client_updated_at" not in client_columns:
            c.execute("ALTER TABLE client_metrics ADD COLUMN client_updated_at TIMESTAMP")
            
        # Add capability_name column if it doesn't exist
        if "capability_name" not in client_columns:
            c.execute("ALTER TABLE client_metrics ADD COLUMN capability_name TEXT")
            
        # Add relationship_duration column if it doesn't exist
        if "relationship_duration" not in client_columns:
            c.execute("ALTER TABLE client_metrics ADD COLUMN relationship_duration REAL")
        
        # For people_metrics table
        c.execute("PRAGMA table_info(people_metrics)")
        people_columns = [column[1] for column in c.fetchall()]

        # Add date_created column if it doesn't exist (without default constraint)
        if "date_created" not in people_columns:
            # First, add the column without a default value
            c.execute("ALTER TABLE people_metrics ADD COLUMN date_created TIMESTAMP")
            
            # Then, update existing records to set date_created equal to updated_at
            c.execute("UPDATE people_metrics SET date_created = updated_at WHERE date_created IS NULL")
        
        if "people_metric_updated_at" not in people_columns:
            c.execute("ALTER TABLE people_metrics ADD COLUMN people_metric_updated_at TIMESTAMP")
        
        # Initialize timestamps for existing records if needed
        tables_to_update = [
            ("hub_metrics", "metrics_updated_at", "location_updated_at", "certifications_updated_at"),
            ("hub_capabilities", "capability_updated_at"),
            ("client_metrics", "client_updated_at"),
            ("people_metrics", "people_metric_updated_at")
        ]
        
        for table_info in tables_to_update:
            table_name = table_info[0]
            timestamp_columns = table_info[1:]
            
            # Prepare column list for the SQL query
            columns_check = " OR ".join([f"{col} IS NULL" for col in timestamp_columns])
            
            # Get records with NULL timestamps
            c.execute(f"SELECT id FROM {table_name} WHERE {columns_check}")
            null_timestamp_records = c.fetchall()
            
            if null_timestamp_records:
                for record_id in null_timestamp_records:
                    # Set initial timestamps to current time for each column
                    updates = ", ".join([f"{col} = CURRENT_TIMESTAMP" for col in timestamp_columns])
                    c.execute(f"UPDATE {table_name} SET {updates} WHERE id = ?", (record_id[0],))

def add_column_if_not_exists(cursor, table, column, definition):
    """Safely add a column to an existing table if it doesn't already exist."""
    try:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    except sqlite3.OperationalError:
        # Column likely already exists, just continue
        pass



# Authentication functions
def login_user(username, password):
    """Authenticate user with database and set session state."""
    # Hash the provided password for comparison
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Look up user in database
        cursor.execute("SELECT hub_name, is_admin FROM users WHERE username = ? AND password_hash = ?", 
                     (username, password_hash))
        result = cursor.fetchone()
    
    if result:
        st.session_state.logged_in = True
        st.session_state.current_hub = result[0]
        st.session_state.is_admin = bool(result[1])
        return True
    return False

def logout_user():
    st.session_state.logged_in = False
    st.session_state.current_hub = None
    st.session_state.is_admin = False

# Data management functions
def get_hub_metrics(hub_name):
    """Get hub metrics with proper connection management."""
    with get_db_connection() as conn:
        if hub_name == "ALL":
            # Admin sees all hubs
            query = """
            SELECT h.hub_name, m.*
            FROM hub_metrics m
            JOIN hubs h ON m.hub_id = h.id
            ORDER BY h.hub_name
            """
            metrics = pd.read_sql(query, conn)
        else:
            # Get metrics for specific hub
            query = """
            SELECT h.hub_name, m.*
            FROM hub_metrics m
            JOIN hubs h ON m.hub_id = h.id
            WHERE h.hub_name = ?
            """
            metrics = pd.read_sql(query, conn, params=[hub_name])
        
        return metrics

# For capabilities data
def get_hub_capabilities(hub_name):
    """Get hub capabilities data from the database."""
    with get_db_connection() as conn:
        if hub_name == "ALL":
            # Admin sees all hubs
            query = """
            SELECT h.hub_name, c.*, h.id as hub_id
            FROM hub_capabilities c
            JOIN hubs h ON c.hub_id = h.id
            ORDER BY h.hub_name, c.capability_category, c.capability_name
            """
            capabilities = pd.read_sql(query, conn)
        else:
            # Get capabilities for specific hub
            query = """
            SELECT h.hub_name, c.*, h.id as hub_id
            FROM hub_capabilities c
            JOIN hubs h ON c.hub_id = h.id
            WHERE h.hub_name = ?
            ORDER BY c.capability_category, c.capability_name
            """
            capabilities = pd.read_sql(query, conn, params=[hub_name])
        
        return capabilities

# For client metrics
def get_client_metrics(hub_name):
    """Get client metrics data from the database."""
    with get_db_connection() as conn:
        if hub_name == "ALL":
            # Admin sees all client relationships
            query = """
            SELECT h.hub_name, c.*, h.id as hub_id
            FROM client_metrics c
            JOIN hubs h ON c.hub_id = h.id
            ORDER BY h.hub_name, c.client_name
            """
            clients = pd.read_sql(query, conn)
        else:
            # Get clients for specific hub
            query = """
            SELECT h.hub_name, c.*, h.id as hub_id
            FROM client_metrics c
            JOIN hubs h ON c.hub_id = h.id
            WHERE h.hub_name = ?
            ORDER BY c.client_name
            """
            clients = pd.read_sql(query, conn, params=[hub_name])
        
        return clients

def get_people_metrics(hub_name, category=None):
    """Get people metrics data from the database."""
    with get_db_connection() as conn:
        if category:
            # Filter by category if provided
            if hub_name == "ALL":
                query = """
                SELECT h.hub_name, p.*, h.id as hub_id
                FROM people_metrics p
                JOIN hubs h ON p.hub_id = h.id
                WHERE p.metric_category = ? AND p.metric_category != 'Hiring'
                ORDER BY h.hub_name, p.metric_name, p.time_period
                """
                metrics = pd.read_sql(query, conn, params=[category])
            else:
                query = """
                SELECT h.hub_name, p.*, h.id as hub_id
                FROM people_metrics p
                JOIN hubs h ON p.hub_id = h.id
                WHERE h.hub_name = ? AND p.metric_category = ? AND p.metric_category != 'Hiring'
                ORDER BY p.metric_name, p.time_period
                """
                metrics = pd.read_sql(query, conn, params=[hub_name, category])
        else:
            # Get all metrics if no category specified
            if hub_name == "ALL":
                query = """
                SELECT h.hub_name, p.*, h.id as hub_id
                FROM people_metrics p
                JOIN hubs h ON p.hub_id = h.id
                ORDER BY h.hub_name, p.metric_category, p.metric_name, p.time_period
                """
                metrics = pd.read_sql(query, conn)
            else:
                query = """
                SELECT h.hub_name, p.*, h.id as hub_id
                FROM people_metrics p
                JOIN hubs h ON p.hub_id = h.id
                WHERE h.hub_name = ? AND p.metric_category != 'Hiring'
                ORDER BY p.metric_category, p.metric_name, p.time_period
                """
                metrics = pd.read_sql(query, conn, params=[hub_name])
        
        return metrics

def get_time_difference(timestamp_str):
    """Convert a timestamp string to a human-readable time difference"""
    if pd.isna(timestamp_str):
        return "Never"
    
    try:
        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        now = datetime.now()
        diff = now - timestamp
        
        days = diff.days
        
        if days == 0:
            hours = diff.seconds // 3600
            if hours == 0:
                minutes = diff.seconds // 60
                return f"{minutes} minutes ago"
            return f"{hours} hours ago"
        elif days == 1:
            return "Yesterday"
        elif days < 30:
            return f"{days} days ago"
        elif days < 365:
            months = days // 30
            return f"{months} months ago"
        else:
            years = days // 365
            return f"{years} years ago"
    except:
        return "Unknown"

def is_outdated(timestamp_str):
    """Check if a timestamp is more than 30 days old"""
    if pd.isna(timestamp_str):
        return True
    
    try:
        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        now = datetime.now()
        diff = now - timestamp
        return diff.days > 30
    except:
        return True

def apply_outdated_style(df, timestamp_col):
    """Apply styling to a dataframe based on timestamp age"""
    import pandas as pd
    
    # Function to apply background color based on timestamp age
    def style_outdated(val):
        try:
            timestamp = datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
            now = datetime.now()
            diff = now - timestamp
            if diff.days > 30:
                return 'background-color: #FFCCCC'  # Light red for outdated
            return ''
        except:
            return 'background-color: #FFCCCC'  # Light red for invalid dates
    
    # Apply style to the timestamp column only
    return df.style.applymap(style_outdated, subset=[timestamp_col])


def show_dashboard_view():
    st.header("Hub Dashboard")
    st.write("Overview of all hub data with last update information")
    
    # For admin users, add a hub selection dropdown
    if st.session_state.is_admin and st.session_state.current_hub == "ALL":
        conn = sqlite3.connect('gdc_data.db')
        hub_df = pd.read_sql("SELECT hub_name FROM hubs ORDER BY hub_name", conn)
        conn.close()
        
        selected_hub = st.selectbox(
            "Select Hub to View",
            options=["ALL"] + hub_df['hub_name'].tolist(),
            key="admin_hub_selection"
        )
        
        # If a specific hub is selected, use that instead of ALL
        if selected_hub != "ALL":
            metrics_df = get_hub_metrics(selected_hub)
        else:
            metrics_df = get_hub_metrics(st.session_state.current_hub)
    else:
        metrics_df = get_hub_metrics(st.session_state.current_hub)
    
    # Get current date for comparison
    current_date = datetime.now()
    
    if not metrics_df.empty:
        # Process each hub's data
        for _, row in metrics_df.iterrows():
            hub_name = row['hub_name']
            hub_id = row['hub_id']
            
            st.markdown(f"## {hub_name} Dashboard")
            st.markdown("---")
            
            # Create tabs for different data categories
            tabs = st.tabs(["Hub Overview", "Client Data", "People Data"])
            
            # Tab 1: Hub Overview (Core Metrics, Locations, Certifications)
            with tabs[0]:
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.markdown("### Core Metrics")
                    
                    # Check if metrics are outdated
                    metrics_updated = datetime.strptime(row['metrics_updated_at'], '%Y-%m-%d %H:%M:%S') if not pd.isna(row['metrics_updated_at']) else None
                    metrics_outdated = False
                    if metrics_updated:
                        days_since_update = (current_date - metrics_updated).days
                        metrics_outdated = days_since_update > 30
                    
                    # Display metrics data
                    metrics_data = [
                        ["Total Headcount", row['total_headcount'], get_time_difference(row['metrics_updated_at'])],
                        ["Total Seats", row['total_seats'], get_time_difference(row['metrics_updated_at'])],
                        ["Bench Count", row['bench_count'] if 'bench_count' in row and not pd.isna(row['bench_count']) else 0, get_time_difference(row['metrics_updated_at'])],
                        ["Total Clients", row['total_clients'], get_time_difference(row['metrics_updated_at'])],
                        ["Services Offered", row['services_offered'], get_time_difference(row['metrics_updated_at'])]
                    ]
                    
                    # Create metrics dataframe
                    metrics_table = pd.DataFrame(metrics_data, columns=["Metric", "Value", "Last Updated"])
                    
                    # Display with styling for outdated data
                    if metrics_outdated:
                        st.warning(f"⚠️ Core metrics last updated {days_since_update} days ago")
                        st.dataframe(metrics_table, use_container_width=True)
                    else:
                        st.dataframe(metrics_table, use_container_width=True)
                    
                    # Facility information
                    st.markdown("### Facility Information")
                    facility_data = [
                        ["Campus Type", row['campus_type'], get_time_difference(row['metrics_updated_at'])],
                        ["SEZ Status", row['sez_status'], get_time_difference(row['metrics_updated_at'])],
                        ["Coverage Hours", row['coverage_hours'], get_time_difference(row['metrics_updated_at'])],
                        ["Transport Available", row['transport_facilities'], get_time_difference(row['metrics_updated_at'])]
                    ]
                    
                    facility_table = pd.DataFrame(facility_data, columns=["Information", "Value", "Last Updated"])
                    st.dataframe(facility_table, use_container_width=True)
                
                with col2:
                    # Gender distribution visualization
                    st.markdown("### Gender Distribution")
                    
                    # Calculate gender counts from percentages
                    total_head = int(row['total_headcount']) if not pd.isna(row['total_headcount']) else 0
                    female_pct = float(row['female_percent']) if not pd.isna(row['female_percent']) else 0
                    male_pct = float(row['male_percent']) if not pd.isna(row['male_percent']) else 0
                    other_pct = float(row['other_gender_percent']) if not pd.isna(row['other_gender_percent']) else 0
                    
                    female_count = int(round(total_head * female_pct / 100)) if female_pct > 0 else 0
                    male_count = int(round(total_head * male_pct / 100)) if male_pct > 0 else 0
                    other_count = total_head - female_count - male_count
                    
                    # Create data for pie chart
                    if total_head > 0:
                        gender_data = {
                            'Gender': ['Female', 'Male', 'Other'],
                            'Count': [female_count, male_count, other_count]
                        }
                        gender_df = pd.DataFrame(gender_data)
                        
                        # Show gender distribution chart
                        st.bar_chart(gender_df.set_index('Gender'))
                        
                        # Show gender counts table
                        st.dataframe(gender_df)
                    else:
                        st.info("No gender distribution data available")
                
                # Locations section
                st.markdown("### Hub Locations")
                
                # Check if location data is outdated
                location_updated = datetime.strptime(row['location_updated_at'], '%Y-%m-%d %H:%M:%S') if not pd.isna(row['location_updated_at']) else None
                location_outdated = False
                if location_updated:
                    days_since_update = (current_date - location_updated).days
                    location_outdated = days_since_update > 30
                
                # Display warning for outdated data
                if location_outdated:
                    st.warning(f"⚠️ Location data last updated {days_since_update} days ago")
                
                # Display locations and headcounts
                if 'location' in row and not pd.isna(row['location']):
                    locations = [loc.strip() for loc in row['location'].split(',') if loc.strip()]
                    
                    # Get location headcounts
                    location_headcounts = {}
                    if 'location_headcounts' in row and not pd.isna(row['location_headcounts']):
                        try:
                            import json
                            location_headcounts = json.loads(row['location_headcounts'])
                        except:
                            pass
                    
                    # Create location data for display
                    location_data = []
                    for loc in locations:
                        headcount = location_headcounts.get(loc, 0)
                        location_data.append([loc, headcount, get_time_difference(row['location_updated_at'])])
                    
                    if location_data:
                        location_df = pd.DataFrame(location_data, columns=["Location", "Headcount", "Last Updated"])
                        st.dataframe(location_df, use_container_width=True)
                    else:
                        st.info("No location data available.")
                else:
                    st.info("No locations recorded.")
                
                # Certifications section
                st.markdown("### Certifications")
                
                # Check if certification data is outdated
                cert_updated = datetime.strptime(row['certifications_updated_at'], '%Y-%m-%d %H:%M:%S') if not pd.isna(row['certifications_updated_at']) else None
                cert_outdated = False
                if cert_updated:
                    days_since_update = (current_date - cert_updated).days
                    cert_outdated = days_since_update > 30
                
                # Display warning for outdated data
                if cert_outdated:
                    st.warning(f"⚠️ Certification data last updated {days_since_update} days ago")
                
                # Display certifications
                if 'certifications' in row and not pd.isna(row['certifications']):
                    try:
                        import json
                        certifications = json.loads(row['certifications'])
                        
                        if certifications:
                            cert_data = [[cert, count, get_time_difference(row['certifications_updated_at'])] 
                                       for cert, count in certifications.items()]
                            cert_df = pd.DataFrame(cert_data, columns=["Certification", "Employees", "Last Updated"])
                            st.dataframe(cert_df, use_container_width=True)
                            
                            # Visualize certifications
                            st.markdown("#### Certification Distribution")
                            if len(cert_data) > 0:
                                cert_chart_df = pd.DataFrame(cert_data, columns=["Certification", "Employees", "Last Updated"])
                                st.bar_chart(cert_chart_df.set_index('Certification')['Employees'])
                        else:
                            st.info("No certifications recorded.")
                    except:
                        st.info("No certifications data available.")
                else:
                    st.info("No certifications recorded.")
            
            # Tab 2: Client Data
            with tabs[1]:
                st.markdown("### Client Relationships")
                
                # Get client data for this hub
                clients_df = get_client_metrics(hub_name)
                
                if not clients_df.empty:
                    # Check for outdated client data
                    clients_df['is_outdated'] = clients_df['client_updated_at'].apply(is_outdated)
                    has_outdated = clients_df['is_outdated'].any()
                    
                    if has_outdated:
                        st.warning("⚠️ Some client information has not been updated in over 30 days")
                    
                    # Prepare data for display
                    display_df = clients_df[['client_name', 'engagement_status', 'commercial_model', 
                                            'capability_category', 'client_updated_at']].copy()
                    display_df['Last Updated'] = display_df['client_updated_at'].apply(get_time_difference)
                    display_df = display_df[['client_name', 'engagement_status', 'commercial_model', 
                                           'capability_category', 'Last Updated']]
                    display_df.columns = ['Client', 'Status', 'Model', 'Capability', 'Last Updated']
                    
                    # Display the data
                    st.dataframe(display_df, use_container_width=True)
                    
                    # Client summary
                    st.markdown("#### Client Summary")
                    
                    # Count by engagement status
                    st.markdown("**Clients by Status**")
                    status_counts = clients_df['engagement_status'].value_counts().reset_index()
                    status_counts.columns = ['Status', 'Count']
                    col1, col2 = st.columns([3, 2])
                    with col1:
                        st.dataframe(status_counts, use_container_width=True)
                    with col2:
                        st.bar_chart(status_counts.set_index('Status'))
                    
                    # Count by capability category
                    st.markdown("**Clients by Capability**")
                    capability_counts = clients_df['capability_category'].value_counts().reset_index()
                    capability_counts.columns = ['Capability', 'Count']
                    col1, col2 = st.columns([3, 2])
                    with col1:
                        st.dataframe(capability_counts, use_container_width=True)
                    with col2:
                        st.bar_chart(capability_counts.set_index('Capability'))
                else:
                    st.info("No client data available for this hub.")
            
            # Tab 3: People Data
            with tabs[2]:
                # First, show capabilities
                st.markdown("### Hub Capabilities")
                
                # Get capabilities data for this hub
                capabilities_df = get_hub_capabilities(hub_name)
                
                if not capabilities_df.empty:
                    # Group capabilities by category
                    categories = capabilities_df['capability_category'].unique()
                    
                    for category in categories:
                        st.markdown(f"#### {category}")
                        
                        # Filter capabilities for this category
                        category_df = capabilities_df[capabilities_df['capability_category'] == category].copy()
                        
                        # Check for outdated capabilities
                        category_df['is_outdated'] = category_df['capability_updated_at'].apply(is_outdated)
                        has_outdated = category_df['is_outdated'].any()
                        
                        if has_outdated:
                            st.warning(f"⚠️ Some {category} capabilities have not been updated in over 30 days")
                        
                        # Prepare data for display
                        display_df = category_df[['capability_name', 'headcount', 'capability_updated_at']].copy()
                        display_df['Last Updated'] = display_df['capability_updated_at'].apply(get_time_difference)
                        display_df = display_df[['capability_name', 'headcount', 'Last Updated']]
                        display_df.columns = ['Capability', 'Headcount', 'Last Updated']

                        # Display the data
                        st.dataframe(display_df, use_container_width=True)
                else:
                    st.info("No capabilities data available for this hub.")
                
                # Then, show people analytics
                st.markdown("### People Analytics")
                
                # Get people metrics data for this hub
                people_df = get_people_metrics(hub_name)
                
                if not people_df.empty:
                    # Group by metric category
                    categories = people_df['metric_category'].unique()
                    
                    for category in categories:
                        st.markdown(f"#### {category}")
                        
                        # Filter metrics for this category
                        category_df = people_df[people_df['metric_category'] == category].copy()
                        
                        # Check for outdated metrics
                        category_df['is_outdated'] = category_df['people_metric_updated_at'].apply(is_outdated)
                        has_outdated = category_df['is_outdated'].any()
                        
                        if has_outdated:
                            st.warning(f"⚠️ Some {category} metrics have not been updated in over 30 days")
                        
                        # Handle different types of metrics differently
                        if 'Monthly' in category_df['metric_name'].iloc[0]:
                            # Time series data (like monthly hires)
                            st.markdown(f"**{category_df['metric_name'].iloc[0]}**")
                            
                            # Sort by time period
                            time_series_df = category_df.sort_values('time_period')
                            
                            # Prepare for display
                            display_df = time_series_df[['time_period', 'metric_value', 'people_metric_updated_at']].copy()
                            display_df['Last Updated'] = display_df['people_metric_updated_at'].apply(get_time_difference)
                            display_df = display_df[['time_period', 'metric_value', 'Last Updated']]
                            display_df.columns = ['Period', 'Value', 'Last Updated']
                            
                            # Display data
                            st.dataframe(display_df, use_container_width=True)
                        else:
                            # Prepare for display
                            display_df = category_df[['metric_name', 'metric_value', 'people_metric_updated_at']].copy()
                            display_df['Last Updated'] = display_df['people_metric_updated_at'].apply(get_time_difference)
                            display_df = display_df[['metric_name', 'metric_value', 'Last Updated']]
                            display_df.columns = ['Metric', 'Value', 'Last Updated']
                            
                            # Display data
                            st.dataframe(display_df, use_container_width=True)
                else:
                    st.info("No people analytics data available for this hub.")
            
            # Summary section - Last update information
            st.markdown("---")
            st.markdown("### Last Update Summary")
            
            # Create a summary table with update information for each data category
            summary_data = [
                ["Core Metrics", get_time_difference(row['metrics_updated_at']), 
                 "Outdated" if is_outdated(row['metrics_updated_at']) else "Current"],
                ["Locations", get_time_difference(row['location_updated_at']), 
                 "Outdated" if is_outdated(row['location_updated_at']) else "Current"],
                ["Certifications", get_time_difference(row['certifications_updated_at']), 
                 "Outdated" if is_outdated(row['certifications_updated_at']) else "Current"]
            ]
            
            # Get capability update status
            capabilities_df = get_hub_capabilities(hub_name)
            if not capabilities_df.empty:
                capability_updated = capabilities_df['capability_updated_at'].max()
                summary_data.append(["Capabilities", get_time_difference(capability_updated),
                                    "Outdated" if is_outdated(capability_updated) else "Current"])
            
            # Get client update status
            clients_df = get_client_metrics(hub_name)
            if not clients_df.empty:
                client_updated = clients_df['client_updated_at'].max()
                summary_data.append(["Client Relationships", get_time_difference(client_updated),
                                    "Outdated" if is_outdated(client_updated) else "Current"])
            
            # Get people metrics update status
            people_df = get_people_metrics(hub_name)
            if not people_df.empty:
                people_updated = people_df['people_metric_updated_at'].max()
                summary_data.append(["People Analytics", get_time_difference(people_updated),
                                    "Outdated" if is_outdated(people_updated) else "Current"])
            
            # Create and display the summary table
            summary_df = pd.DataFrame(summary_data, columns=["Data Category", "Last Updated", "Status"])
            
            # Calculate overall status
            outdated_categories = summary_df[summary_df["Status"] == "Outdated"].shape[0]
            total_categories = summary_df.shape[0]
            
            # Create columns for status display
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.dataframe(summary_df, use_container_width=True)
            
            with col2:
                # Display overall data health status
                st.markdown("### Data Health")
                
                if outdated_categories == 0:
                    st.success("✅ All data is current")
                elif outdated_categories < total_categories / 2:
                    st.warning(f"⚠️ {outdated_categories} categories need update")
                else:
                    st.error(f"🚨 {outdated_categories} categories are outdated")
                
                # Calculate and display data health percentage
                health_percentage = 100 * (total_categories - outdated_categories) / total_categories
                st.metric("Data Health Score", f"{health_percentage:.0f}%")
                
                # Last updated by
                st.markdown(f"**Last Updated By:** {row['updated_by']}")
            
            # Add some spacing between hubs if admin view and ALL is selected
            if st.session_state.is_admin and 'admin_hub_selection' in st.session_state and st.session_state.admin_hub_selection == "ALL":
                st.markdown("---")
    else:
        st.info("No hub metrics available. Please contact an administrator.")
def update_hub_metrics(metrics_data):
    """Update hub metrics in the database."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get current metrics to preserve gender percentages if not provided
            if 'female_percent' not in metrics_data or 'male_percent' not in metrics_data or 'other_gender_percent' not in metrics_data:
                cursor.execute("SELECT female_percent, male_percent, other_gender_percent FROM hub_metrics WHERE id = ?", 
                             (metrics_data['id'],))
                current_metrics = cursor.fetchone()
                
                # If we found current metrics, use them
                if current_metrics:
                    female_percent, male_percent, other_gender_percent = current_metrics
                    
                    # Only set these if they're not in the metrics_data
                    if 'female_percent' not in metrics_data:
                        metrics_data['female_percent'] = female_percent
                    if 'male_percent' not in metrics_data:
                        metrics_data['male_percent'] = male_percent
                    if 'other_gender_percent' not in metrics_data:
                        metrics_data['other_gender_percent'] = other_gender_percent
                else:
                    # Default values if no current metrics
                    if 'female_percent' not in metrics_data:
                        metrics_data['female_percent'] = 0
                    if 'male_percent' not in metrics_data:
                        metrics_data['male_percent'] = 0
                    if 'other_gender_percent' not in metrics_data:
                        metrics_data['other_gender_percent'] = 0
            
            # Build a list of column=? pairs for SET clause
            set_parts = []
            params = []
            
            # Core fields (always included)
            set_parts.extend([
                "total_headcount = ?",
                "total_seats = ?",
                "total_clients = ?",
                "services_offered = ?",
                "female_percent = ?",
                "male_percent = ?",
                "other_gender_percent = ?",
                "campus_type = ?",
                "sez_status = ?",
                "location = ?",
                "coverage_hours = ?",
                "transport_facilities = ?",
                "updated_at = CURRENT_TIMESTAMP",
                "updated_by = ?"
            ])
            
            params.extend([
                metrics_data['total_headcount'],
                metrics_data['total_seats'],
                metrics_data['total_clients'],
                metrics_data['services_offered'],
                metrics_data['female_percent'],
                metrics_data['male_percent'],
                metrics_data['other_gender_percent'],
                metrics_data['campus_type'],
                metrics_data['sez_status'],
                metrics_data['location'],
                metrics_data['coverage_hours'],
                metrics_data['transport_facilities'],
                metrics_data['updated_by']
            ])
            
            # Optional fields
            if 'bench_count' in metrics_data:
                set_parts.append("bench_count = ?")
                params.append(metrics_data['bench_count'])
            
            if 'location_headcounts' in metrics_data:
                set_parts.append("location_headcounts = ?")
                params.append(metrics_data['location_headcounts'])
            
            if 'certifications' in metrics_data:
                set_parts.append("certifications = ?")
                params.append(metrics_data['certifications'])
            
            # Build the complete query
            query = f"UPDATE hub_metrics SET {', '.join(set_parts)} WHERE id = ?"
            params.append(metrics_data['id'])
            
            # Execute update
            cursor.execute(query, tuple(params))
        return True
    except Exception as e:
        st.error(f"Error updating hub metrics: {e}")
        return False

# Update capabilities timestamp when updating capabilities
# Example of a function with proper error handling
def update_hub_capability(capability_data):
    """Update hub capability with error handling."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Update the SQL query to include headcount field
            if 'headcount' in capability_data:
                cursor.execute("""
                UPDATE hub_capabilities SET
                    headcount = ?,
                    updated_at = CURRENT_TIMESTAMP,
                    updated_by = ?,
                    capability_updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """, (
                    capability_data['headcount'],
                    capability_data['updated_by'],
                    capability_data['id']
                ))
            else:
                # For backward compatibility
                cursor.execute("""
                UPDATE hub_capabilities SET
                    updated_at = CURRENT_TIMESTAMP,
                    updated_by = ?,
                    capability_updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """, (
                    capability_data['percentage'],
                    capability_data['updated_by'],
                    capability_data['id']
                ))
            
            return True
    except Exception as e:
        st.error(f"Error updating hub capability: {e}")
        import traceback
        st.error(traceback.format_exc())
        return False

def update_client_metric(client_data):
    """Update client metrics with proper connection management."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Update client metric with timestamp
            cursor.execute("""
            UPDATE client_metrics SET
                client_name = ?,
                engagement_status = ?,
                commercial_model = ?,
                capability_category = ?,
                capability_name = ?,
                relationship_duration = ?,
                scope_summary = ?,
                employee_count = ?,
                updated_at = CURRENT_TIMESTAMP,
                updated_by = ?,
                client_updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """, (
                client_data['client_name'],
                client_data['engagement_status'],
                client_data['commercial_model'],
                client_data['capability_category'],
                client_data['capability_name'],
                client_data['relationship_duration'],
                client_data['scope_summary'],
                client_data.get('employee_count', 0),  # Default to 0 if not provided
                client_data['updated_by'],
                client_data['id']
            ))
            
            return True
    except Exception as e:
        st.error(f"Error updating client metric: {e}")
        import traceback
        st.error(traceback.format_exc())
        return False

# Update people metrics timestamp when updating
def update_people_metric(metric_data):
    """Update people metrics with proper connection management."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Check if we're updating a hiring reason
            if 'hiring_reason' in metric_data:
                # Update people metric with timestamp and hiring reason
                cursor.execute("""
                UPDATE people_metrics SET
                    metric_value = ?,
                    hiring_reason = ?,
                    updated_at = CURRENT_TIMESTAMP,
                    updated_by = ?,
                    people_metric_updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """, (
                    metric_data['metric_value'],
                    metric_data['hiring_reason'],
                    metric_data['updated_by'],
                    metric_data['id']
                ))
            else:
                # Standard update without hiring reason
                cursor.execute("""
                UPDATE people_metrics SET
                    metric_value = ?,
                    updated_at = CURRENT_TIMESTAMP,
                    updated_by = ?,
                    people_metric_updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """, (
                    metric_data['metric_value'],
                    metric_data['updated_by'],
                    metric_data['id']
                ))
            
            return True
    except Exception as e:
        st.error(f"Error updating people metric: {e}")
        import traceback
        st.error(traceback.format_exc())
        return False

# Also update the add_client_metric function to initialize the timestamp
def add_client_metric(client_data):
    """Add a new client metric with proper connection management."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Add new client metric with timestamp
            cursor.execute("""
            INSERT INTO client_metrics (
                hub_id, client_name, engagement_status, commercial_model,
                capability_category, capability_name, relationship_duration,
                scope_summary, employee_count, updated_by, client_updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                client_data['hub_id'],
                client_data['client_name'],
                client_data['engagement_status'],
                client_data['commercial_model'],
                client_data['capability_category'],
                client_data['capability_name'],  # This should be JSON-encoded string
                client_data['relationship_duration'],
                client_data['scope_summary'],
                client_data.get('employee_count', 0),  # Default to 0 if not provided
                client_data['updated_by']
            ))
            
            return True
    except Exception as e:
        st.error(f"Error adding client metric: {e}")
        # Print more detailed error for debugging
        import traceback
        st.error(traceback.format_exc())
        return False
# UI Components
def show_login_screen():
    st.title("GDC Dashboard Data Validation Portal")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.subheader("Login to your Hub Account")
        
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        
        if st.button("Login"):
            if login_user(username, password):
                st.success("Login successful!")
                st.rerun()
            else:
                st.error("Invalid username or password")
                
        st.markdown("---")
        st.write("Default admin login: username 'admin', password 'admin123'")
        st.write("Default hub login: username '[hub_name_lowercase]', password '[hub_name_lowercase]123'")
        st.write("Example: For 'AKQA', username is 'akqa', password is 'akqa123'")

def show_main_interface():
    # Logout button in top right corner
    col1, col2 = st.columns([6, 1])
    
    with col2:
        if st.button("Logout"):
            logout_user()
            st.rerun()
    
    with col1:
        st.title(f"GDC Dashboard Data Validation - {st.session_state.current_hub}")
    
    # Different navigation options for admin vs. hub users
    if st.session_state.is_admin:
        # Admin-specific navigation
        tab_options = ["Dashboard", "Admin Tools"]
        
        selected_tab = st.radio(
            "Select View",
            tab_options,
            horizontal=True,
            key="selected_tab"
        )
        
        if selected_tab == "Dashboard":
            show_dashboard_view()
        else:  # Admin Tools
            show_admin_tools()
    else:
        # Hub user navigation - full access to data entry
        tab_options = ["Dashboard", "Hub Metrics", "Capabilities", "Client Relationships", "People Analytics"]
        
        selected_tab = st.radio(
            "Select Data Category",
            tab_options,
            horizontal=True,
            key="selected_tab"
        )

        # Map the selected tab to the view
        view_mapping = {
            "Dashboard": "dashboard",
            "Hub Metrics": "hub_metrics", 
            "Capabilities": "capabilities",
            "Client Relationships": "client_relationships",
            "People Analytics": "people_analytics"
        }

        # Set the current view based on selection
        if selected_tab in view_mapping:
            st.session_state.current_view = view_mapping[selected_tab]

        # Display selected data view
        if st.session_state.current_view == "dashboard":
            show_dashboard_view()
        elif st.session_state.current_view == "hub_metrics":
            show_hub_metrics_view()
        elif st.session_state.current_view == "capabilities":
            show_capabilities_view()
        elif st.session_state.current_view == "client_relationships":
            show_client_relationships_view()
        elif st.session_state.current_view == "people_analytics":
            show_people_analytics_view()

def apply_update_styling(df, timestamp_col):
    """Apply color coding based on last update time
    - Green: < 30 days
    - Yellow: 30-35 days
    - Red: > 35 days
    """
    def color_by_days(val):
        if pd.isna(val):
            return 'background-color: #FF9999'  # Red for missing dates
        
        try:
            timestamp = datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
            now = datetime.now()
            diff = now - timestamp
            days = diff.days
            
            if days < 30:
                return 'background-color: #99FF99'  # Green for < 30 days
            elif days < 35:
                return 'background-color: #FFFF99'  # Yellow for 30-35 days
            else:
                return 'background-color: #FF9999'  # Red for > 35 days
        except:
            return 'background-color: #FF9999'  # Red for invalid dates
    
    # Apply style to the timestamp column
    return df.style.applymap(color_by_days, subset=[timestamp_col])

def show_hub_metrics_view():
    st.header("Hub Metrics")
    st.write("These metrics are displayed in the Hub Overview dashboard.")
    
    # Get hub metrics
    metrics_df = get_hub_metrics(st.session_state.current_hub)
    
    # Get additional data needed for calculated fields
    current_hub = st.session_state.current_hub
    
    # Get employment type data to calculate total headcount
    people_metrics_df = get_people_metrics(current_hub)
    employment_type_df = people_metrics_df[people_metrics_df['metric_category'] == 'Employment Type']
    
    # Get latest time period for employment type data
    latest_time_period = None
    if not employment_type_df.empty:
        time_periods = sorted(employment_type_df['time_period'].unique(), 
                            key=lambda x: pd.to_datetime(x, format="%b %Y", errors='coerce'), 
                            reverse=True)  # Newest first
        if time_periods:
            latest_time_period = time_periods[0]
    
    # Calculate total headcount from latest employment type metrics
    total_headcount_calculated = 0
    if latest_time_period:
        latest_employment_data = employment_type_df[employment_type_df['time_period'] == latest_time_period]
        
        permanent_row = latest_employment_data[latest_employment_data['metric_name'] == 'Permanent Employees']
        contract_row = latest_employment_data[latest_employment_data['metric_name'] == 'Contract Employees']
        
        permanent_count = permanent_row['metric_value'].iloc[0] if not permanent_row.empty else 0
        contract_count = contract_row['metric_value'].iloc[0] if not contract_row.empty else 0
        
        total_headcount_calculated = permanent_count + contract_count
    
    # Get client metrics to calculate total clients
    clients_df = get_client_metrics(current_hub)
    total_clients_calculated = len(clients_df)
    
    # Get capabilities data to calculate services offered
    capabilities_df = get_hub_capabilities(current_hub)
    total_services_calculated = len(capabilities_df)
    
    if not metrics_df.empty:
        # Display prominent metrics tiles before the form
        st.subheader("Key Metrics Overview")
        
        # Create three columns for the prominent metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(label="Total Headcount", 
                    value=f"{total_headcount_calculated}")
            if latest_time_period:
                st.caption(f"Data from {latest_time_period}")
        
        with col2:
            st.metric(label="Total Clients", 
                    value=f"{total_clients_calculated}")
        
        with col3:
            st.metric(label="Services Offered", 
                    value=f"{total_services_calculated}")
        
        st.markdown("---")
        
        # Display metrics in an editable form
        for _, row in metrics_df.iterrows():
            # Initialize session states for this row
            row_id = row['id']
            
            # CRITICAL FIX: Initialize ALL session state variables AT ONCE
            # before we access them in any UI elements
            
            # 1. Initialize location data
            if f"locations_{row_id}" not in st.session_state:
                if 'location' in row and not pd.isna(row['location']):
                    locations = [loc.strip() for loc in row['location'].split(',') if loc.strip()]
                else:
                    locations = []
                st.session_state[f"locations_{row_id}"] = locations
            
            # 2. Initialize location headcounts
            if f"location_headcounts_{row_id}" not in st.session_state:
                try:
                    if 'location_headcounts' in row and not pd.isna(row['location_headcounts']):
                        st.session_state[f"location_headcounts_{row_id}"] = json.loads(row['location_headcounts'])
                    else:
                        st.session_state[f"location_headcounts_{row_id}"] = {}
                except:
                    st.session_state[f"location_headcounts_{row_id}"] = {}
            
            # 3. Initialize certifications
            if f"certifications_{row_id}" not in st.session_state:
                try:
                    if 'certifications' in row and not pd.isna(row['certifications']):
                        certifications = json.loads(row['certifications'])
                        st.session_state[f"certifications_{row_id}"] = list(certifications.items()) if certifications else []
                    else:
                        st.session_state[f"certifications_{row_id}"] = []
                except:
                    st.session_state[f"certifications_{row_id}"] = []
            
            # 4. Parse and initialize coverage hours/days
            coverage_days = 5
            coverage_hours = 24
            if 'coverage_hours' in row and not pd.isna(row['coverage_hours']):
                try:
                    hours_str = str(row['coverage_hours'])
                    if 'x' in hours_str.lower():
                        parts = hours_str.lower().split('x')
                        if len(parts) == 2:
                            coverage_hours = int(parts[0])
                            coverage_days = int(parts[1])
                except:
                    pass
            
            # 5. Initialize form data COMPLETELY with ALL fields - but without bench count and gender fields
            if f"form_data_{row_id}" not in st.session_state:
                st.session_state[f"form_data_{row_id}"] = {
                    'total_headcount': total_headcount_calculated,
                    'total_seats': int(row['total_seats']) if not pd.isna(row['total_seats']) else 0,
                    'total_clients': total_clients_calculated,
                    'services_offered': total_services_calculated,
                    'campus_type': row['campus_type'] if not pd.isna(row['campus_type']) else "In-Campus",
                    'sez_status': row['sez_status'] if not pd.isna(row['sez_status']) else "No",
                    'coverage_days': coverage_days,
                    'coverage_hours': coverage_hours,
                    'transport_facilities': row['transport_facilities'] if not pd.isna(row['transport_facilities']) else "No"
                }
            
            # Now we can safely proceed with the UI, all session state is initialized
            
            # Section 1: Location Management
            st.subheader("Hub Locations Management")
            st.write("Add, edit, or remove locations before submitting the form")
            
            # Add location button
            if st.button("➕ Add Location", key=f"add_location_{row_id}"):
                st.session_state[f"locations_{row_id}"].append("")
                st.rerun()
            
            # Display all locations with remove buttons
            for i, location in enumerate(st.session_state[f"locations_{row_id}"]):
                cols = st.columns([3, 2, 1])
                
                with cols[0]:
                    new_location = st.text_input(f"Location {i+1}", 
                                               value=location,
                                               key=f"loc_{row_id}_{i}")
                    # Update the location in session state
                    st.session_state[f"locations_{row_id}"][i] = new_location
                
                with cols[1]:
                    # Get headcount for this location or default to 0
                    loc_headcount = st.session_state[f"location_headcounts_{row_id}"].get(location, 0)
                    new_headcount = st.number_input(f"Headcount", 
                                                  min_value=0,
                                                  value=int(loc_headcount),
                                                  key=f"loc_hc_{row_id}_{i}")
                    
                    # Update headcount in session state
                    if location:  # Only update if location exists
                        st.session_state[f"location_headcounts_{row_id}"][location] = new_headcount
                    if new_location and new_location != location:
                        # If location name changed, update the headcount for the new name
                        st.session_state[f"location_headcounts_{row_id}"][new_location] = new_headcount
                        # Remove old location name if it's different
                        if location and location in st.session_state[f"location_headcounts_{row_id}"]:
                            del st.session_state[f"location_headcounts_{row_id}"][location]
                
                with cols[2]:
                    if st.button("❌", key=f"remove_loc_{row_id}_{i}"):
                        # Remove the location from the list
                        removed_location = st.session_state[f"locations_{row_id}"].pop(i)
                        # Also remove from headcounts if it exists
                        if removed_location in st.session_state[f"location_headcounts_{row_id}"]:
                            del st.session_state[f"location_headcounts_{row_id}"][removed_location]
                        st.rerun()
            
            # Calculate total headcount from locations
            location_total = sum(st.session_state[f"location_headcounts_{row_id}"].values())
            
            # Section 2: Certification Management
            st.subheader("Certifications Management")
            st.write("Add, edit, or remove certifications before submitting the form")
            
            # Add certification button
            if st.button("➕ Add Certification", key=f"add_cert_{row_id}"):
                st.session_state[f"certifications_{row_id}"].append(("", 0))
                st.rerun()
            
            # Display all certifications with remove buttons
            for i, (cert_name, cert_count) in enumerate(st.session_state[f"certifications_{row_id}"]):
                cols = st.columns([3, 2, 1])
                
                with cols[0]:
                    new_cert = st.text_input(f"Certification {i+1}", 
                                            value=cert_name,
                                            key=f"cert_{row_id}_{i}")
                
                with cols[1]:
                    new_count = st.number_input(f"Certified Employees", 
                                              min_value=0,
                                              value=int(cert_count),
                                              key=f"cert_count_{row_id}_{i}")
                    
                    # Update certification in session state
                    st.session_state[f"certifications_{row_id}"][i] = (new_cert, new_count)
                
                with cols[2]:
                    if st.button("❌", key=f"remove_cert_{row_id}_{i}"):
                        st.session_state[f"certifications_{row_id}"].pop(i)
                        st.rerun()
            
            # Form section - Verify we have form_data before accessing it
            if f"form_data_{row_id}" in st.session_state:
                # Now create the actual form for the main metrics
                st.subheader("Update Hub Metrics")
                st.write("Update the core metrics and submit the form")
                
                # Add an informational box to explain where the other data is
                st.info("""
                - Total Headcount is calculated from Employment Type Metrics (Contract + Permanent Employees)
                - Total Clients is calculated from Client Relationships 
                - Services Offered is calculated from Capabilities
                - Employees on Bench and Gender Distribution can be managed in the People Analytics section
                """)
                
                with st.form(f"edit_hub_metrics_{row_id}"):
                    # Facility information
                    st.subheader("Facility Information")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Total seats is now part of facility information
                        total_seats = st.number_input("Total Seats", 
                                                    min_value=0, 
                                                    value=st.session_state[f"form_data_{row_id}"].get('total_seats', 0))
                        
                        campus_options = ["In-Campus", "Outside-Campus"]
                        campus_default = 0
                        if 'campus_type' in st.session_state[f"form_data_{row_id}"]:
                            if st.session_state[f"form_data_{row_id}"]['campus_type'] in campus_options:
                                campus_default = campus_options.index(st.session_state[f"form_data_{row_id}"]['campus_type'])
                        
                        campus_type = st.selectbox("Campus Type", 
                                                 campus_options,
                                                 index=campus_default)
                        
                        yes_no_options = ["Yes", "No"]
                        sez_default = 1  # Default to "No"
                        if 'sez_status' in st.session_state[f"form_data_{row_id}"]:
                            if st.session_state[f"form_data_{row_id}"]['sez_status'] in yes_no_options:
                                sez_default = yes_no_options.index(st.session_state[f"form_data_{row_id}"]['sez_status'])
                        
                        sez_status = st.selectbox("SEZ Status", 
                                                yes_no_options,
                                                index=sez_default)
                    
                    with col2:
                        # Coverage Hours as days and hours selectors
                        st.write("Coverage Hours")
                        days_options = [5, 6, 7]
                        days_default = 0  # Default to 5 days
                        if 'coverage_days' in st.session_state[f"form_data_{row_id}"]:
                            if st.session_state[f"form_data_{row_id}"]['coverage_days'] in days_options:
                                days_default = days_options.index(st.session_state[f"form_data_{row_id}"]['coverage_days'])
                        
                        coverage_days = st.selectbox("Days per Week", 
                                                  options=days_options,
                                                  index=days_default)
                        
                        hours_options = [8, 12, 16, 24]
                        hours_default = 3  # Default to 24 hours
                        if 'coverage_hours' in st.session_state[f"form_data_{row_id}"]:
                            if st.session_state[f"form_data_{row_id}"]['coverage_hours'] in hours_options:
                                hours_default = hours_options.index(st.session_state[f"form_data_{row_id}"]['coverage_hours'])
                        
                        coverage_hours = st.selectbox("Hours per Day", 
                                                   options=hours_options,
                                                   index=hours_default)
                        
                        # Format coverage hours for display
                        coverage_hours_formatted = f"{coverage_hours}x{coverage_days}"
                        
                        transport_default = 1  # Default to "No"
                        if 'transport_facilities' in st.session_state[f"form_data_{row_id}"]:
                            if st.session_state[f"form_data_{row_id}"]['transport_facilities'] in yes_no_options:
                                transport_default = yes_no_options.index(st.session_state[f"form_data_{row_id}"]['transport_facilities'])
                        
                        transport_facilities = st.selectbox("Transport Facilities", 
                                                         yes_no_options,
                                                         index=transport_default)
                    
                    # Validate total headcount with sum of location headcounts
                    if location_total != total_headcount_calculated and total_headcount_calculated > 0:
                        st.warning(f"Sum of location headcounts ({location_total}) does not match total headcount ({total_headcount_calculated}).")
                    
                    # Add the form submit button
                    submit_pressed = st.form_submit_button("Update Hub Metrics")
                    
                    if submit_pressed:
                        # Update the session state with form values
                        st.session_state[f"form_data_{row_id}"].update({
                            'total_headcount': total_headcount_calculated,  # Using calculated value
                            'total_seats': total_seats,
                            'total_clients': total_clients_calculated,  # Using calculated value
                            'services_offered': total_services_calculated,  # Using calculated value
                            'campus_type': campus_type,
                            'sez_status': sez_status,
                            'coverage_days': coverage_days,
                            'coverage_hours': coverage_hours,
                            'transport_facilities': transport_facilities
                        })
                        
                        # Prepare data from locations and certifications
                        # Convert locations to comma-separated string
                        updated_locations = [loc for loc in st.session_state[f"locations_{row_id}"] if loc.strip()]
                        location_str = ", ".join(updated_locations)
                        
                        # Prepare headcounts dictionary (clean up any empty locations)
                        updated_location_headcounts = {k: v for k, v in st.session_state[f"location_headcounts_{row_id}"].items() if k.strip()}
                        
                        # Prepare certifications dictionary
                        updated_certifications = {cert: count for cert, count in st.session_state[f"certifications_{row_id}"] if cert.strip()}
                        
                        # Convert dictionaries to JSON for storage
                        import json
                        location_headcounts_json = json.dumps(updated_location_headcounts)
                        certifications_json = json.dumps(updated_certifications)
                        
                        # Prepare data for update - now without bench count and gender fields
                        metrics_data = {
                            'id': row_id,
                            'total_headcount': total_headcount_calculated,  # Using calculated value
                            'total_seats': total_seats,
                            'total_clients': total_clients_calculated,  # Using calculated value
                            'services_offered': total_services_calculated,  # Using calculated value
                            'campus_type': campus_type,
                            'sez_status': sez_status,
                            'location': location_str,
                            'location_headcounts': location_headcounts_json,
                            'certifications': certifications_json,
                            'coverage_hours': coverage_hours_formatted,
                            'transport_facilities': transport_facilities,
                            'updated_by': st.session_state.current_hub
                        }
                        
                        # Save to database
                        if update_hub_metrics(metrics_data):
                            st.success("Hub metrics updated successfully!")
                        else:
                            st.error("Failed to update hub metrics. Please try again.")
            else:
                st.error(f"Session state for form data not initialized correctly. Please refresh the page.")
            
            # Add some spacing between hubs if admin view
            if st.session_state.is_admin and st.session_state.current_hub == "ALL":
                st.markdown("---")
    else:
        st.info("No hub metrics available. Please contact an administrator.")

# Define a callback function that will update the selected category
def update_category():
    # Reset the capability selection when category changes
    st.session_state.selected_capability = None


def show_capabilities_view():
    st.header("Hub Capabilities")
    st.write("These metrics are displayed in the Capabilities section of the Hub Overview dashboard.")
    
    # Get capabilities data
    capabilities_df = get_hub_capabilities(st.session_state.current_hub)
    
    if not capabilities_df.empty:
        # Group by hub if admin view
        if st.session_state.is_admin and st.session_state.current_hub == "ALL":
            # Show a dropdown to select the hub to edit
            unique_hubs = capabilities_df['hub_name'].unique()
            selected_hub = st.selectbox("Select Hub to Edit", unique_hubs)
            capabilities_df = capabilities_df[capabilities_df['hub_name'] == selected_hub]
            hub_id = capabilities_df['hub_id'].iloc[0]
        else:
            # For non-admin users, get their hub ID
            conn = sqlite3.connect('gdc_data.db')
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM hubs WHERE hub_name = ?", (st.session_state.current_hub,))
            hub_id = cursor.fetchone()[0]
            conn.close()
        
        # Add new capability section
        st.subheader("Add New Capability")

        # Get the existing capabilities for this hub to determine which category to use
        existing_categories = capabilities_df['capability_category'].unique()
        default_category = existing_categories[0] if len(existing_categories) > 0 else "CX+"
        
        # Creating a form for adding a new capability directly with text input
        with st.form("add_capability_form"):
            # Text input for capability name
            new_capability_name = st.text_input(
                "Enter New Service Name", 
                key="new_capability_name"
            )
            
            # Employee count input
            headcount = st.number_input("Number of Employees", min_value=0, value=0)
            
            # Submit button
            submit_button = st.form_submit_button("Add Capability")
            
            if submit_button and new_capability_name:
                # Add this capability to the hub using the default category from existing capabilities
                conn = sqlite3.connect('gdc_data.db')
                cursor = conn.cursor()
                
                try:
                    # First check if this capability already exists for this hub
                    cursor.execute("""
                        SELECT id FROM hub_capabilities 
                        WHERE hub_id = ? AND capability_name = ?
                    """, (int(hub_id), new_capability_name))
                    
                    existing = cursor.fetchone()
                    
                    if existing:
                        st.error(f"Service '{new_capability_name}' already exists for this hub")
                    else:
                        cursor.execute("""
                            INSERT INTO hub_capabilities 
                            (hub_id, capability_name, capability_category, headcount, updated_at, updated_by, capability_updated_at) 
                            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP)
                        """, (int(hub_id), new_capability_name, default_category, headcount, st.session_state.current_hub))
                        
                        conn.commit()
                        st.success(f"Added '{new_capability_name}' to hub capabilities!")
                        st.rerun()
                except Exception as e:
                    st.error(f"Error adding capability: {e}")
                finally:
                    conn.close()
        
        # Group by capability category
        capability_categories = capabilities_df['capability_category'].unique()
        
        for category in capability_categories:
            st.subheader(f"{category} Capabilities")
            st.write("Edit capability headcounts and select services to remove.")
            
            # Filter dataframe for this category
            category_df = capabilities_df[capabilities_df['capability_category'] == category].copy()
            
            # Add a 'Delete' column for flagging rows to delete
            category_df['Delete'] = False
            
            # Add a unique key for this category's editor
            editor_key = f"editor_{category}_{hub_id}"
            
            # Convert dataframe for display - select only relevant columns
            # Keeping the ID in the dataframe but making it hidden in the UI
            display_df = category_df[['id', 'capability_name', 'headcount', 'Delete']].copy()
            
            # Rename columns for better display
            display_df = display_df.rename(columns={
                'capability_name': 'Service Name',
                'headcount': 'Employees',
                'Delete': 'Remove'
            })
            
            # Configure columns for the editor
            column_config = {
                "id": st.column_config.NumberColumn(
                    "ID",
                    required=True,
                    help="Database ID (cannot be changed)",
                    # Hide this column completely from the UI
                    disabled=True,
                    width="0px"  # Setting width to 0 to hide it
                ),
                "Service Name": st.column_config.TextColumn(
                    "Service Name",
                    disabled=True,  # Make service name non-editable in the table
                    help="Name of the service",
                    width="70%"  # Allocate 70% of width to service name
                ),
                "Employees": st.column_config.NumberColumn(
                    "Employees",
                    min_value=0,
                    step=1,
                    help="Number of employees for this service",
                    width="20%"  # Allocate 20% of width to employee count
                ),
                "Remove": st.column_config.CheckboxColumn(
                    "Remove",
                    help="Select to remove this service",
                    width="10%"  # Allocate 10% of width to remove checkbox
                )
            }
            
            # Create the editable table
            edited_df = st.data_editor(
                display_df,
                column_config=column_config,
                hide_index=True,
                use_container_width=True,
                key=editor_key,
                # Explicitly hide the ID column from the UI
                column_order=["Service Name", "Employees", "Remove"]
            )
            
            # Calculate total employees and show it
            total_employees = edited_df['Employees'].sum()
            st.write(f"**Total employees in {category}: {total_employees}**")
            
            # Add a button to save changes
            if st.button(f"Save {category} Changes", key=f"save_{category}"):
                # Process deletions first
                # We need to ensure the ID column is still in the dataframe for backend operations
                rows_to_delete = edited_df[edited_df['Remove'] == True]
                if not rows_to_delete.empty:
                    delete_count = 0
                    conn = sqlite3.connect('gdc_data.db')
                    cursor = conn.cursor()
                    
                    for _, row in rows_to_delete.iterrows():
                        try:
                            cursor.execute("DELETE FROM hub_capabilities WHERE id = ?", (row['id'],))
                            delete_count += 1
                        except Exception as e:
                            st.error(f"Error removing capability ID {row['id']}: {e}")
                    
                    conn.commit()
                    conn.close()
                    
                    if delete_count > 0:
                        st.success(f"Removed {delete_count} service(s) successfully!")
                
                # Then process updates for remaining rows
                rows_to_update = edited_df[edited_df['Remove'] == False]
                if not rows_to_update.empty:
                    update_count = 0
                    
                    for _, row in rows_to_update.iterrows():
                        # Get original data
                        original_row = category_df[category_df['id'] == row['id']]
                        if not original_row.empty:
                            original_headcount = original_row['headcount'].iloc[0]
                            
                            # Only update if changed
                            if original_headcount != row['Employees']:
                                # Calculate percentage
                                percentage = 0.0
                                if total_employees > 0:
                                    percentage = (row['Employees'] / total_employees) * 100
                                
                                capability_data = {
                                    'id': row['id'],
                                    'percentage': percentage,
                                    'headcount': row['Employees'],
                                    'updated_by': st.session_state.current_hub
                                }
                                
                                if update_hub_capability(capability_data):
                                    update_count += 1
                                else:
                                    st.error(f"Failed to update service ID {row['id']}")
                    
                    if update_count > 0:
                        st.success(f"Updated {update_count} service(s) successfully!")
                
                # Refresh the page to show updated data
                if rows_to_delete.shape[0] > 0 or rows_to_update.shape[0] > 0:
                    st.rerun()
            
            # Add spacing between categories
            st.markdown("---")
    else:
        st.info("No capability data available. Please contact an administrator.")

def get_first_service(service_json):
    """Extract the first service from the JSON string"""
    if pd.isna(service_json):
        return ""
    try:
        services = json.loads(service_json)
        if isinstance(services, list) and services:
            return services[0]  # Return the first service
        return services if services else ""
    except:
        return service_json if service_json else ""


def show_client_relationships_view():
    st.header("Client Relationships")
    st.write("These metrics are displayed in the Client Relationships section of the Hub Overview dashboard.")
    
    # Get client metrics
    clients_df = get_client_metrics(st.session_state.current_hub)
    
    # Get capabilities to populate the services dropdown
    capabilities_df = get_hub_capabilities(st.session_state.current_hub)
    
    # Extract the list of services from capabilities
    services_list = []
    if not capabilities_df.empty:
        services_list = capabilities_df['capability_name'].unique().tolist()
    
    # Fixed capability mapping for hubs
    hub_capability_mapping = {
        "AKQA": ["CX+"],
        "Mirum Digital Pvt Ltd": ["CX+"],
        "GroupM Nexus Global Team": ["MEDIA+"],
        "Hogarth Worldwide": ["CONTENT+"],
        "Hogarth Studios": ["CONTENT+"],
        "Verticurl": ["CX+"],
        "VML-Tech Commerce": ["CX+"],
    }
    
    # Add new client button
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("+ Add New Client"):
            st.session_state.adding_client = True
    
    # Filter clients
    with col2:
        search_term = st.text_input("Search clients by name", key="client_search")
    
    # Filter by search term if provided
    if search_term:
        search_term = search_term.lower()
        filtered_df = clients_df[clients_df['client_name'].str.lower().str.contains(search_term, na=False)]
    else:
        filtered_df = clients_df
    
    # Display client count
    st.write(f"Displaying {len(filtered_df)} of {len(clients_df)} clients")
    
    # Check if we're adding a new client
    if 'adding_client' in st.session_state and st.session_state.adding_client:
        with st.form("add_client_form"):
            st.subheader("Add New Client")
            
            # Get hub ID
            conn = sqlite3.connect('gdc_data.db')
            cursor = conn.cursor()
            
            if st.session_state.is_admin and st.session_state.current_hub == "ALL":
                # For admin users, get all hubs
                hubs_df = pd.read_sql("SELECT id, hub_name FROM hubs", conn)
                hub_options = hubs_df['hub_name'].tolist()
                selected_hub = st.selectbox("Select Hub", hub_options)
                hub_df = hubs_df[hubs_df['hub_name'] == selected_hub]
                hub_id = int(hub_df['id'].iloc[0])
                current_hub_name = selected_hub
                
                # Get services for the selected hub
                hub_capabilities_df = get_hub_capabilities(selected_hub)
                if not hub_capabilities_df.empty:
                    services_list = hub_capabilities_df['capability_name'].unique().tolist()
                else:
                    services_list = []
            else:
                # For non-admin users, get their hub ID
                cursor.execute("SELECT id FROM hubs WHERE hub_name = ?", (st.session_state.current_hub,))
                hub_id = cursor.fetchone()[0]
                current_hub_name = st.session_state.current_hub
            
            conn.close()
            
            # Client details
            client_name = st.text_input("Client Name", key="new_client_name")
            
            # Status and model
            col1, col2 = st.columns(2)
            with col1:
                engagement_status = st.selectbox("Engagement Status", 
                                              ["Active", "Inactive", "Pending", "Completed"], 
                                              key="new_status")
            
            with col2:
                commercial_model = st.selectbox("Commercial Model", 
                                             ["FTE", "Project-based", "Retainer"], 
                                             key="new_model")
            
            # Add relationship duration field
            relationship_duration = st.number_input(
                "Relationship Duration (years)", 
                min_value=0.0, 
                value=0.0, 
                step=0.1,
                help="Enter the duration since when we are associated with this client in years. Can include decimal values.",
                key="new_duration"
            )

            # Employee count field
            employee_count = st.number_input(
                "Number of employees associated", 
                min_value=0, 
                value=0,
                help="Enter the number of employees associated with this client.",
                key="new_employee_count"
            )
            
            # Multi-select for services from capabilities
            if services_list:
                st.write("Select Services")
                selected_services = st.multiselect(
                    "Services", 
                    options=services_list,
                    key="new_services",
                    help="Select the specific services provided to this client. You can select multiple services."
                )
            else:
                st.warning("No services available. Please add services in the Capabilities section first.")
                selected_services = []
            
            # Scope summary
            scope_summary = st.text_area("Scope Summary", key="new_scope", height=100)
            
            # Submit and cancel buttons
            col1, col2 = st.columns(2)
            with col1:
                submit = st.form_submit_button("Add Client")
            with col2:
                if st.form_submit_button("Cancel"):
                    st.session_state.adding_client = False
                    st.rerun()
            
            if submit:
                # Validate input
                if not client_name:
                    st.error("Client name is required.")
                elif not selected_services:
                    st.error("Please select at least one service.")
                else:
                    # Ensure we're properly JSON encoding the selected services
                    services_json = json.dumps(selected_services)
                    
                    # Get the capability category from the mapping
                    capability_category = ""
                    if current_hub_name in hub_capability_mapping:
                        capability_category = hub_capability_mapping[current_hub_name][0]
                    
                    # Prepare client data
                    client_data = {
                        'hub_id': hub_id,
                        'client_name': client_name,
                        'engagement_status': engagement_status,
                        'commercial_model': commercial_model,
                        'capability_category': capability_category,  # Fixed for the hub
                        'capability_name': services_json,
                        'relationship_duration': relationship_duration,
                        'scope_summary': scope_summary,
                        'employee_count': employee_count,  # Add the new field
                        'updated_by': st.session_state.current_hub
                    }

                    # Add to database
                    if add_client_metric(client_data):
                        st.success("Client added successfully!")
                        st.session_state.adding_client = False
                        st.rerun()
                    else:
                        st.error("Failed to add client. Please try again.")
    
    # Display client metrics in an editable table using st.data_editor
    if not filtered_df.empty:
        st.subheader("Current Clients")
        
        # Create a display dataframe with necessary modifications
        display_df = filtered_df.copy()
        
        # Format the services column for display
        def format_services(service_json):
            if pd.isna(service_json):
                return ""
            try:
                services = json.loads(service_json)
                if isinstance(services, list):
                    return ", ".join(services)
                return services
            except:
                return service_json
        
        # Add a column for displayed services (comma-separated string)
        display_df['Services'] = display_df['capability_name'].apply(format_services)
        
        # Add a column for deletion
        display_df['Delete'] = False
        
        # Prepare a column for selecting service again when editing
        # Add a column for the first service (editable via dropdown)
        display_df['First_Service'] = display_df['capability_name'].apply(get_first_service)

        # Update the column configuration to make the first service editable
        column_config = {
            "id": st.column_config.NumberColumn(
                "ID",
                required=True,
                disabled=True,
                width="0px"  # Hide the ID column
            ),
            "client_name": st.column_config.TextColumn(
                "Client Name",
                help="Name of the client"
            ),
            "engagement_status": st.column_config.SelectboxColumn(
                "Status",
                options=["Active", "Inactive", "Pending", "Completed"],
                help="Current engagement status"
            ),
            "commercial_model": st.column_config.SelectboxColumn(
                "Commercial Model",
                options=["FTE", "Project-based", "Retainer"],
                help="Commercial arrangement"
            ),
            "relationship_duration": st.column_config.NumberColumn(
                "Duration (years)",
                min_value=0.0,
                step=0.1,
                format="%.1f",
                help="Relationship duration in years"
            ),
            "employee_count": st.column_config.NumberColumn(
                "Employee Count",
                min_value=0,
                step=1,
                format="%d",
                help="Number of employees associated with this client"
            ),
            "First_Service": st.column_config.SelectboxColumn(
                "Primary Service",
                options=services_list if services_list else [""],
                help="Primary service provided to this client (double-click to edit)"
            ),
            "Services": st.column_config.TextColumn(
                "All Services",
                help="All services provided to this client (view only)",
                disabled=True  # Make this column read-only
            ),
            "scope_summary": st.column_config.TextColumn(
                "Scope Summary",
                help="Brief description of project scope"
            ),
            "Delete": st.column_config.CheckboxColumn(
                "Remove",
                help="Select to remove this client"
            )
        }
        
        # Display the editable dataframe
        edited_df = st.data_editor(
            display_df[['id', 'client_name', 'engagement_status', 'commercial_model', 
                    'relationship_duration', 'employee_count', 'First_Service', 'Services', 'scope_summary', 'Delete']],
            column_config=column_config,
            hide_index=True,
            use_container_width=True,
            key="client_editor"
        )
        
        # Add a section for editing services for a selected client
        with st.expander("Edit Client Services: Multiple Services", expanded=False):
            st.write("Select a client to edit their multiple services:")
            
            # Create a dropdown to select a client to edit services
            client_options = filtered_df['client_name'].tolist()
            client_ids = filtered_df['id'].tolist()
            client_dict = dict(zip(client_options, client_ids))
            
            selected_client = st.selectbox("Select Client", options=client_options, key="select_client_for_services")
            
            if selected_client:
                selected_id = client_dict[selected_client]
                client_row = filtered_df[filtered_df['id'] == selected_id].iloc[0]
                
                # Parse existing services
                current_services = []
                if not pd.isna(client_row['capability_name']):
                    try:
                        current_services = json.loads(client_row['capability_name'])
                        if not isinstance(current_services, list):
                            current_services = [current_services]
                    except:
                        current_services = [client_row['capability_name']] if client_row['capability_name'] else []
                
                # Edit services for this client
                new_services = st.multiselect(
                    f"Services for {selected_client}",
                    options=services_list,
                    default=current_services,
                    key=f"edit_services_{selected_id}"
                )
                
                # Button to update services
                if st.button("Update Services", key=f"update_services_btn_{selected_id}"):
                    # Get the capability category from the mapping
                    capability_category = client_row['capability_category']  # Keep existing category
                    
                    # Encode services as JSON
                    services_json = json.dumps(new_services)
                    
                    # Update just the services for this client
                    conn = sqlite3.connect('gdc_data.db')
                    cursor = conn.cursor()
                    
                    try:
                        cursor.execute("""
                        UPDATE client_metrics SET
                            capability_name = ?,
                            updated_at = CURRENT_TIMESTAMP,
                            updated_by = ?,
                            client_updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """, (services_json, st.session_state.current_hub, selected_id))
                        
                        conn.commit()
                        st.success(f"Services updated for {selected_client}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error updating services: {e}")
                    finally:
                        conn.close()
        
        # Add a button to save changes to the table
        if st.button("Save Client Changes"):
            # Process deletions first
            rows_to_delete = edited_df[edited_df['Delete'] == True]
            if not rows_to_delete.empty:
                delete_count = 0
                conn = sqlite3.connect('gdc_data.db')
                cursor = conn.cursor()
                
                for _, row in rows_to_delete.iterrows():
                    try:
                        cursor.execute("DELETE FROM client_metrics WHERE id = ?", (row['id'],))
                        delete_count += 1
                    except Exception as e:
                        st.error(f"Error removing client ID {row['id']}: {e}")
                
                conn.commit()
                conn.close()
                
                if delete_count > 0:
                    st.success(f"Removed {delete_count} client(s) successfully!")

            # Then process updates for remaining rows (including the primary service)
            rows_to_update = edited_df[edited_df['Delete'] == False]
            update_count = 0

            for _, row in rows_to_update.iterrows():
                # Get original data for comparison
                original_row = filtered_df[filtered_df['id'] == row['id']]
                
                if not original_row.empty:
                    # Check if any changes were made to the row
                    original_client_name = original_row['client_name'].iloc[0]
                    original_status = original_row['engagement_status'].iloc[0]
                    original_model = original_row['commercial_model'].iloc[0]
                    original_duration = original_row['relationship_duration'].iloc[0] if not pd.isna(original_row['relationship_duration'].iloc[0]) else 0.0
                    original_scope = original_row['scope_summary'].iloc[0] if not pd.isna(original_row['scope_summary'].iloc[0]) else ""
                    
                    # Check if the primary service changed
                    primary_service_changed = False
                    service_json_updated = original_row['capability_name'].iloc[0]
                    
                    if row['First_Service'] and row['First_Service'] != get_first_service(original_row['capability_name'].iloc[0]):
                        # Primary service changed - update the JSON
                        current_services = []
                        if not pd.isna(original_row['capability_name'].iloc[0]):
                            try:
                                current_services = json.loads(original_row['capability_name'].iloc[0])
                                if not isinstance(current_services, list):
                                    current_services = [current_services]
                            except:
                                current_services = [original_row['capability_name'].iloc[0]] if original_row['capability_name'].iloc[0] else []
                        
                        # Update or add the first service
                        if current_services:
                            current_services[0] = row['First_Service']
                        else:
                            current_services = [row['First_Service']]
                        
                        # Update the JSON
                        service_json_updated = json.dumps(current_services)
                        primary_service_changed = True
                    
                    # Only update if something changed
                    if (row['client_name'] != original_client_name or
                        row['engagement_status'] != original_status or
                        row['commercial_model'] != original_model or
                        row['relationship_duration'] != original_duration or
                        row['scope_summary'] != original_scope or
                        primary_service_changed):
                        
                        conn = sqlite3.connect('gdc_data.db')
                        cursor = conn.cursor()
                        
                        try:
                            cursor.execute("""
                            UPDATE client_metrics SET
                                client_name = ?,
                                engagement_status = ?,
                                commercial_model = ?,
                                relationship_duration = ?,
                                scope_summary = ?,
                                capability_name = ?,
                                employee_count = ?,
                                updated_at = CURRENT_TIMESTAMP,
                                updated_by = ?,
                                client_updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                            """, (
                                row['client_name'],
                                row['engagement_status'],
                                row['commercial_model'],
                                row['relationship_duration'],
                                row['scope_summary'],
                                service_json_updated,
                                row['employee_count'],  # Add employee count to the query
                                st.session_state.current_hub,
                                row['id']
                            ))
                            
                            conn.commit()
                            update_count += 1
                        except Exception as e:
                            st.error(f"Error updating client ID {row['id']}: {e}")
                        finally:
                            conn.close()
            
            if update_count > 0:
                st.success(f"Updated {update_count} client(s) successfully!")
            
            # Refresh the page if any changes were made
            if rows_to_delete.shape[0] > 0 or update_count > 0:
                st.rerun()
    else:
        st.info("No client relationship data available. Add new clients using the form above.")

def show_people_analytics_view():
    st.header("People Analytics Metrics")
    st.write("These metrics are displayed in the People Analytics dashboard.")
    
    # Get all metric categories (including our new ones)
    people_metrics_df = get_people_metrics(st.session_state.current_hub)
    
    # If Gender category doesn't exist in the dataframe, we'll create UI elements
    # to initialize it with default values
    has_gender_data = 'Gender' in people_metrics_df['metric_category'].unique()
    
    # Same for Staffing category
    has_staffing_data = 'Staffing' in people_metrics_df['metric_category'].unique()
    
    # Add categories for existing categories plus our new ones
    all_categories = list(people_metrics_df['metric_category'].unique())
    
    # Make sure these appear even if no data exists yet
    if 'Gender' not in all_categories:
        all_categories.append('Gender')
    if 'Staffing' not in all_categories:
        all_categories.append('Staffing')
    
    # Filter out "Hiring" category 
    categories = [cat for cat in all_categories if cat != "Hiring"]
    
    # Replace "Turnover" with "Attrition" for display
    display_categories = [cat if cat != "Turnover" else "Attrition" for cat in categories]
    
    if categories:
        # Create tabs for each display category
        tabs = st.tabs(display_categories)
        
        for i, category in enumerate(categories):
            # The display category (what user sees in the UI)
            display_category = display_categories[i]
            
            with tabs[i]:
                # Apply separate handling for our new categories
                if category == "Gender":
                    handle_gender_category(people_metrics_df, st.session_state.current_hub)
                elif category == "Staffing":
                    handle_staffing_category(people_metrics_df, st.session_state.current_hub)
                else:
                    # Original handling for other categories
                    category_df = people_metrics_df[people_metrics_df['metric_category'] == category]
                    
                    # For each category, create an editable table
                    st.subheader(f"{display_category} Metrics")
                    
                    # First, check for metrics in this category
                    if len(category_df) > 0:
                        # Get unique metric names and time periods
                        metric_names = sorted(category_df['metric_name'].unique())
                        
                        # Use different time period formats based on category
                        time_periods = []  # Initialize empty list to prevent UnboundLocalError

                        if category in ["Marital Status", "Tenure"]:
                            # For these categories, use just Year
                            for period in category_df['time_period'].unique():
                                # Extract just the year portion if the period contains a month
                                if period and " " in period:
                                    parts = period.split()
                                    if len(parts) >= 2 and parts[1].isdigit():
                                        time_periods.append(parts[1])  # Just use the year
                                elif period and period.isdigit():
                                    time_periods.append(period)  # Already just a year
                            
                            # Remove duplicates and sort
                            time_periods = sorted(set(time_periods))
                        elif category == "Turnover":
                            # For Turnover/Attrition, use Month Year format
                            if not category_df.empty:
                                time_periods = sorted(category_df['time_period'].unique(), 
                                                    key=lambda x: pd.to_datetime(x, format="%b %Y", errors='coerce'),
                                                    reverse=True)  # Newest first
                        else:
                            # For Employment Type, keep Month Year format and ensure current month exists
                            if not category_df.empty:
                                time_periods = sorted(category_df['time_period'].unique(), 
                                                    key=lambda x: pd.to_datetime(x, format="%b %Y", errors='coerce'))
                            else:
                                time_periods = []
                                
                            # Check if current month-year is missing
                            current_month_year = datetime.now().strftime("%b %Y")
                            is_current_month_missing = current_month_year not in time_periods
                            
                            # Button to add new time period for Employment Type
                            if st.button(f"Add New Month", key=f"add_{category}_period"):
                                # Get current date information
                                new_period = current_month_year
                                
                                # Only add if it doesn't already exist
                                if new_period not in time_periods:
                                    # Create placeholder records in the database
                                    conn = sqlite3.connect('gdc_data.db')
                                    cursor = conn.cursor()
                                    cursor.execute("SELECT id FROM hubs WHERE hub_name = ?", 
                                                (st.session_state.current_hub,))
                                    hub_id = cursor.fetchone()[0]
                                    conn.close()
                                    
                                    # For each metric, create a new record with 0 value
                                    for metric in metric_names:
                                        conn = sqlite3.connect('gdc_data.db')
                                        cursor = conn.cursor()
                                        
                                        # Check if this combination already exists
                                        cursor.execute("""
                                            SELECT id FROM people_metrics 
                                            WHERE hub_id = ? AND metric_name = ? AND time_period = ? AND metric_category = ?
                                        """, (hub_id, metric, new_period, category))
                                        
                                        existing = cursor.fetchone()
                                        
                                        if not existing:
                                            # Insert placeholder record with month-year format
                                            cursor.execute("""
                                                INSERT INTO people_metrics (
                                                    hub_id, metric_name, metric_value, metric_category, 
                                                    time_period, updated_by, updated_at, people_metric_updated_at,
                                                    date_created
                                                ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                                            """, (
                                                hub_id, metric, 0, category, 
                                                new_period, st.session_state.current_hub
                                            ))
                                            
                                            conn.commit()
                                        conn.close()
                                    
                                    # Force a rerun to show the new records
                                    st.rerun()
                            
                            # Auto-create current month if it's missing
                            if is_current_month_missing:
                                st.info(f"The current month ({current_month_year}) is not in the database. Adding it automatically.")
                                
                                # Create placeholder records in the database
                                conn = sqlite3.connect('gdc_data.db')
                                cursor = conn.cursor()
                                cursor.execute("SELECT id FROM hubs WHERE hub_name = ?", 
                                            (st.session_state.current_hub,))
                                hub_id = cursor.fetchone()[0]
                                conn.close()
                                
                                # For each metric, create a new record with 0 value
                                for metric in metric_names:
                                    conn = sqlite3.connect('gdc_data.db')
                                    cursor = conn.cursor()
                                    
                                    # Check if this combination already exists
                                    cursor.execute("""
                                        SELECT id FROM people_metrics 
                                        WHERE hub_id = ? AND metric_name = ? AND time_period = ? AND metric_category = ?
                                    """, (hub_id, metric, current_month_year, category))
                                    
                                    existing = cursor.fetchone()
                                    
                                    if not existing:
                                        # Insert placeholder record with month-year format
                                        cursor.execute("""
                                            INSERT INTO people_metrics (
                                                hub_id, metric_name, metric_value, metric_category, 
                                                time_period, updated_by, updated_at, people_metric_updated_at,
                                                date_created
                                            ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                                        """, (
                                            hub_id, metric, 0, category, 
                                            current_month_year, st.session_state.current_hub
                                        ))
                                        
                                        conn.commit()
                                    conn.close()
                                
                                # Update time_periods to include the newly added month
                                time_periods = sorted(time_periods + [current_month_year], 
                                                    key=lambda x: pd.to_datetime(x, format="%b %Y", errors='coerce'))
                        
                        # Button to add new time period based on category
                        # Special formatting note for Tenure
                        if category == "Tenure":
                            st.info("Note: Tenure metrics consider employee tenure as of January 1st of the current year.")

                        # For year-only categories
                        if category in ["Marital Status", "Tenure"]:
                            # For year-only categories (Marital Status, Tenure)
                            if st.button(f"Add New Year", key=f"add_{category}_period"):
                                # Get current year
                                current_year = str(datetime.now().year)
                                
                                # Only add if it doesn't already exist
                                if current_year not in time_periods:
                                    time_periods = list(time_periods) + [current_year]
                                    
                                    # Create placeholder records in the database
                                    conn = sqlite3.connect('gdc_data.db')
                                    cursor = conn.cursor()
                                    cursor.execute("SELECT id FROM hubs WHERE hub_name = ?", 
                                                (st.session_state.current_hub,))
                                    hub_id = cursor.fetchone()[0]
                                    conn.close()
                                    
                                    # For each metric, create a new record with 0 value
                                    for metric in metric_names:
                                        conn = sqlite3.connect('gdc_data.db')
                                        cursor = conn.cursor()
                                        
                                        # Check if this combination already exists
                                        cursor.execute("""
                                            SELECT id FROM people_metrics 
                                            WHERE hub_id = ? AND metric_name = ? AND time_period = ? AND metric_category = ?
                                        """, (hub_id, metric, current_year, category))
                                        
                                        existing = cursor.fetchone()
                                        
                                        if not existing:
                                            # Insert placeholder record - use current_year as the time_period
                                            cursor.execute("""
                                                INSERT INTO people_metrics (
                                                    hub_id, metric_name, metric_value, metric_category, 
                                                    time_period, updated_by, updated_at, people_metric_updated_at,
                                                    date_created
                                                ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                                            """, (
                                                hub_id, metric, 0, category, 
                                                current_year, st.session_state.current_hub
                                            ))
                                            
                                            conn.commit()
                                        conn.close()
                                    
                                    # Force a rerun to show the new records
                                    st.rerun()
                        elif category == "Turnover":
                            # For Turnover/Attrition, use Month-Year format
                            if st.button(f"Add New Month", key=f"add_{category}_period"):
                                # Get current date information
                                current_month = datetime.now().strftime("%b")
                                current_year = datetime.now().year
                                new_period = f"{current_month} {current_year}"
                                
                                # Only add if it doesn't already exist
                                if new_period not in time_periods:
                                    # Create placeholder records in the database
                                    conn = sqlite3.connect('gdc_data.db')
                                    cursor = conn.cursor()
                                    cursor.execute("SELECT id FROM hubs WHERE hub_name = ?", 
                                                (st.session_state.current_hub,))
                                    hub_id = cursor.fetchone()[0]
                                    conn.close()
                                    
                                    # For each metric, create a new record with 0 value
                                    for metric in metric_names:
                                        conn = sqlite3.connect('gdc_data.db')
                                        cursor = conn.cursor()
                                        
                                        # Check if this combination already exists
                                        cursor.execute("""
                                            SELECT id FROM people_metrics 
                                            WHERE hub_id = ? AND metric_name = ? AND time_period = ? AND metric_category = ?
                                        """, (hub_id, metric, new_period, category))
                                        
                                        existing = cursor.fetchone()
                                        
                                        if not existing:
                                            # Insert placeholder record with month-year format
                                            cursor.execute("""
                                                INSERT INTO people_metrics (
                                                    hub_id, metric_name, metric_value, metric_category, 
                                                    time_period, updated_by, updated_at, people_metric_updated_at,
                                                    date_created
                                                ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                                            """, (
                                                hub_id, metric, 0, category, 
                                                new_period, st.session_state.current_hub
                                            ))
                                            
                                            conn.commit()
                                        conn.close()
                                    
                                    # Force a rerun to show the new records
                                    st.rerun()
                        
                        # Build the table data
                        table_data = []
                        
                        for period in time_periods:
                            if category in ["Marital Status", "Tenure", "Turnover"]:
                                # For Year-only categories, we match any record with this year
                                # This assumes time_period might be "Month Year" or just "Year"
                                row_data = {"Time Period": period}
                                
                                for metric in metric_names:
                                    # Find value for this metric and period
                                    # Match if either time_period is exactly the year, or ends with the year
                                    matching_rows = category_df[(category_df['metric_name'] == metric) & 
                                                              (category_df['time_period'].str.endswith(period) | 
                                                              (category_df['time_period'] == period))]
                                    
                                    if not matching_rows.empty:
                                        # If multiple matches, use the most recent one
                                        matching_rows = matching_rows.sort_values('updated_at', ascending=False)
                                        row = matching_rows.iloc[0]
                                        
                                        row_data[metric] = row['metric_value']
                                        row_data[f"{metric}_id"] = row['id']
                                    else:
                                        row_data[metric] = 0.0
                                        row_data[f"{metric}_id"] = None
                            else:
                                # For month-year categories, exact match
                                row_data = {"Time Period": period}
                                
                                for metric in metric_names:
                                    # Find value for this metric and period
                                    value_row = category_df[(category_df['metric_name'] == metric) & 
                                                          (category_df['time_period'] == period)]
                                    
                                    if not value_row.empty:
                                        row_data[metric] = value_row['metric_value'].iloc[0]
                                        row_data[f"{metric}_id"] = value_row['id'].iloc[0]
                                    else:
                                        row_data[metric] = 0.0
                                        row_data[f"{metric}_id"] = None
                            
                            table_data.append(row_data)
                        
                        # Create a DataFrame from the table data
                        if table_data:
                            edit_df = pd.DataFrame(table_data)
                            
                            # We need to keep track of the IDs but not show them in the editor
                            id_columns = {}
                            for metric in metric_names:
                                id_col = f"{metric}_id"
                                if id_col in edit_df.columns:
                                    id_columns[metric] = edit_df[id_col].tolist()
                                    # Remove ID columns from the display dataframe
                                    edit_df = edit_df.drop(id_col, axis=1)
                            
                            # Configure column metadata for data_editor
                            column_config = {
                                "Time Period": st.column_config.TextColumn(
                                    "Time Period",
                                    disabled=True
                                )
                            }
                            
                            # Configure each metric column
                            for metric in metric_names:
                                if category == 'Turnover':
                                    column_config[metric] = st.column_config.NumberColumn(
                                        metric,
                                        min_value=0.0,
                                        max_value=100.0,
                                        step=0.1,
                                        format="%.1f %%"
                                    )
                                else:
                                    column_config[metric] = st.column_config.NumberColumn(
                                        metric,
                                        min_value=0,
                                        step=1,
                                        format="%d"
                                    )
                            
                            # Display the editable data frame
                            edited_df = st.data_editor(
                                edit_df,
                                column_config=column_config,
                                use_container_width=True,
                                hide_index=True,
                                num_rows="fixed",  # Using "fixed" instead of "dynamic" to prevent row additions in the editor
                                key=f"editor_{category}"
                            )
                            
                            # Button to save changes
                            if st.button("Save Changes", key=f"save_{category}"):
                                save_success = True
                                
                                # Compare original and edited data to find changes
                                for i, row in edited_df.iterrows():
                                    period = row["Time Period"]
                                    
                                    for metric in metric_names:
                                        orig_value = edit_df.loc[i, metric]
                                        new_value = row[metric]
                                        
                                        # Get the ID from our saved map
                                        record_id = id_columns.get(metric, [])[i] if i < len(id_columns.get(metric, [])) else None
                                        
                                        # Only update if value changed
                                        if orig_value != new_value:
                                            if record_id:
                                                # Update existing record
                                                success = update_people_metric({
                                                    'id': record_id,
                                                    'metric_value': new_value,
                                                    'updated_by': st.session_state.current_hub
                                                })
                                                if not success:
                                                    save_success = False
                                            else:
                                                # Add new record
                                                # First get the hub ID
                                                conn = sqlite3.connect('gdc_data.db')
                                                cursor = conn.cursor()
                                                cursor.execute("SELECT id FROM hubs WHERE hub_name = ?", 
                                                            (st.session_state.current_hub,))
                                                hub_id = cursor.fetchone()[0]
                                                conn.close()
                                                
                                                # Insert new record
                                                conn = sqlite3.connect('gdc_data.db')
                                                cursor = conn.cursor()
                                                
                                                try:
                                                    cursor.execute("""
                                                        INSERT INTO people_metrics (
                                                            hub_id, metric_name, metric_value, metric_category, 
                                                            time_period, updated_by, updated_at, people_metric_updated_at,
                                                            date_created
                                                        ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                                                    """, (
                                                        hub_id, metric, new_value, category, 
                                                        period, st.session_state.current_hub
                                                    ))
                                                    
                                                    conn.commit()
                                                except Exception as e:
                                                    st.error(f"Error adding new data point: {e}")
                                                    save_success = False
                                                finally:
                                                    conn.close()
                                
                                if save_success:
                                    st.success(f"All {display_category} data saved successfully!")
                                    st.rerun()
                                else:
                                    st.error("Some data could not be saved. Please try again.")
                        else:
                            st.info(f"No {display_category} data available yet. Add a time period to get started.")
                    else:
                        st.info(f"No {display_category} metrics found. Please add data to get started.")
                        
                        # Button to initialize with first data point
                        if st.button(f"Initialize {display_category} Data", key=f"init_{category}"):
                            st.session_state[f"initializing_{category}"] = True
                            
                        # Simple form to add first data
                        if f"initializing_{category}" in st.session_state and st.session_state[f"initializing_{category}"]:
                            with st.form(f"init_{category}_form"):
                                st.subheader(f"Add Initial {display_category} Data")
                                
                                # Default metrics based on category
                                if category == "Employment Type":
                                    default_metrics = ["Permanent Employees", "Contract Employees"]
                                elif category == "Marital Status":
                                    default_metrics = ["Single", "Married", "Other Marital Status"]
                                elif category == "Tenure":
                                    default_metrics = ["Tenure <1 year", "Tenure 1-2 years", "Tenure 2-3 years", 
                                                     "Tenure 3-5 years", "Tenure 5-7 years", "Tenure 7-10 years", 
                                                     "Tenure 10+ years"]
                                elif category == "Turnover":
                                    default_metrics = ["Overall Turnover Rate", "Voluntary Turnover", "Involuntary Turnover"]
                                else:
                                    default_metrics = []
                                
                                # Time period based on category
                                if category in ["Marital Status", "Tenure"]:
                                    # For these categories, just use the year
                                    time_period = str(datetime.now().year)
                                    st.write(f"Year: {time_period}")
                                elif category == "Turnover":
                                    # For Turnover/Attrition, use Month Year
                                    current_month = datetime.now().strftime("%b")
                                    current_year = datetime.now().year
                                    time_period = f"{current_month} {current_year}"
                                    st.write(f"Month-Year: {time_period}")
                                else:
                                    # For Employment Type, use Month Year
                                    current_month = datetime.now().strftime("%b")
                                    current_year = datetime.now().year
                                    time_period = f"{current_month} {current_year}"
                                    st.write(f"Time Period: {time_period}")
                                
                                # Value inputs for each default metric
                                values = {}
                                for metric in default_metrics:
                                    if category == "Turnover":
                                        values[metric] = st.number_input(
                                            f"{metric} (%)", 
                                            min_value=0.0, 
                                            max_value=100.0,
                                            value=0.0,
                                            step=0.1
                                        )
                                    else:
                                        values[metric] = st.number_input(
                                            f"{metric} (count)", 
                                            min_value=0, 
                                            value=0
                                        )
                                
                                if st.form_submit_button("Save Initial Data"):
                                    # Get hub ID
                                    conn = sqlite3.connect('gdc_data.db')
                                    cursor = conn.cursor()
                                    cursor.execute("SELECT id FROM hubs WHERE hub_name = ?", 
                                                (st.session_state.current_hub,))
                                    hub_id = cursor.fetchone()[0]
                                    conn.close()
                                    
                                    # Insert each metric
                                    success = True
                                    for metric, value in values.items():
                                        conn = sqlite3.connect('gdc_data.db')
                                        cursor = conn.cursor()
                                        
                                        try:
                                            cursor.execute("""
                                                INSERT INTO people_metrics (
                                                    hub_id, metric_name, metric_value, metric_category, 
                                                    time_period, updated_by, updated_at, people_metric_updated_at,
                                                    date_created
                                                ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                                            """, (
                                                hub_id, metric, value, category, 
                                                time_period, st.session_state.current_hub
                                            ))
                                            
                                            conn.commit()
                                        except Exception as e:
                                            st.error(f"Error adding {metric}: {e}")
                                            success = False
                                        finally:
                                            conn.close()
                                    
                                    if success:
                                        st.success(f"Initial {display_category} data added successfully!")
                                        st.session_state[f"initializing_{category}"] = False
                                        st.rerun()
                                    else:
                                        st.error("Some data could not be saved. Please try again.")
    else:
        st.info("No people analytics metrics available. Please contact an administrator.")

# Helper function to handle the Gender category
# Helper function to handle the Gender category
def handle_gender_category(people_metrics_df, current_hub):
    """Handle display and editing of Gender distribution data"""
    st.subheader("Gender Distribution Metrics")
    
    # Filter to gender category data
    gender_df = people_metrics_df[people_metrics_df['metric_category'] == 'Gender']
    
    # Get hub ID for database operations
    conn = sqlite3.connect('gdc_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM hubs WHERE hub_name = ?", (current_hub,))
    result = cursor.fetchone()
    
    if not result:
        st.error(f"Could not find hub ID for {current_hub}")
        conn.close()
        return
    
    hub_id = result[0]
    conn.close()
    
    # Define the gender metrics to track
    gender_metrics = ["Female", "Male", "Other Gender"]
    
    # Get time periods from the data, or use current month/year if none exist
    time_periods = []
    if not gender_df.empty:
        time_periods = sorted(gender_df['time_period'].unique(), 
                            key=lambda x: pd.to_datetime(x, format="%b %Y", errors='coerce'), 
                            reverse=True)
    
    # Button to add new time period
    if st.button("Add New Time Period", key="add_gender_period"):
        # Get current date information
        current_month = datetime.now().strftime("%b")
        current_year = datetime.now().year
        new_period = f"{current_month} {current_year}"
        
        # Only add if it doesn't already exist
        if not time_periods or new_period not in time_periods:
            # Initialize with zero values
            conn = sqlite3.connect('gdc_data.db')
            cursor = conn.cursor()
            
            for metric in gender_metrics:
                cursor.execute("""
                    INSERT INTO people_metrics (
                        hub_id, metric_name, metric_value, metric_category, 
                        time_period, updated_by, updated_at, people_metric_updated_at,
                        date_created
                    ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, (
                    hub_id, metric, 0, "Gender", 
                    new_period, current_hub
                ))
            
            conn.commit()
            conn.close()
            st.success(f"Added new time period: {new_period}")
            st.rerun()
    
    # If we have data or just added it, show the editable table
    if not gender_df.empty or time_periods:
        # If we have no time periods but have gender data, something's wrong - refresh
        if not time_periods and not gender_df.empty:
            st.rerun()
            
        # If we have no time periods at all, add the current one
        if not time_periods:
            current_month = datetime.now().strftime("%b")
            current_year = datetime.now().year
            time_periods = [f"{current_month} {current_year}"]
        
        # Build table data for the editor
        table_data = []
        
        for period in time_periods:
            row_data = {"Time Period": period}
            
            for metric in gender_metrics:
                # Find the value for this metric and period
                value_row = gender_df[(gender_df['metric_name'] == metric) & 
                                    (gender_df['time_period'] == period)]
                
                if not value_row.empty:
                    row_data[metric] = value_row['metric_value'].iloc[0]
                    row_data[f"{metric}_id"] = value_row['id'].iloc[0]
                else:
                    row_data[metric] = 0
                    row_data[f"{metric}_id"] = None
            
            # Calculate total for validation
            female_count = row_data.get("Female", 0)
            male_count = row_data.get("Male", 0)
            other_count = row_data.get("Other Gender", 0)
            row_data["Total"] = female_count + male_count + other_count
            
            table_data.append(row_data)
        
        # Create dataframe for the editor
        if table_data:
            edit_df = pd.DataFrame(table_data)
            
            # Track IDs for updates
            id_columns = {}
            for metric in gender_metrics:
                id_col = f"{metric}_id"
                if id_col in edit_df.columns:
                    id_columns[metric] = edit_df[id_col].tolist()
                    # Remove ID columns from display
                    edit_df = edit_df.drop(id_col, axis=1)
            
            # Configure columns for the editor
            column_config = {
                "Time Period": st.column_config.TextColumn(
                    "Time Period",
                    disabled=True
                ),
                "Total": st.column_config.NumberColumn(
                    "Total",
                    disabled=True,
                    help="Total of all gender counts"
                )
            }
            
            # Add gender metric columns
            for metric in gender_metrics:
                column_config[metric] = st.column_config.NumberColumn(
                    metric,
                    min_value=0,
                    step=1,
                    format="%d",
                    help=f"Count of {metric.lower()} employees"
                )
            
            # Display the editable table
            st.write("Edit the gender distribution by time period")
            edited_df = st.data_editor(
                edit_df,
                column_config=column_config,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="gender_editor"
            )
            
            # Save button
            if st.button("Save Gender Data", key="save_gender"):
                save_success = True
                
                # Update database with edited values
                for i, row in edited_df.iterrows():
                    period = row["Time Period"]
                    
                    for metric in gender_metrics:
                        # Get the new value from edited dataframe
                        new_value = int(row[metric])  # Ensure integer
                        
                        # Get record ID if it exists
                        metric_id = id_columns.get(metric, [])[i] if i < len(id_columns.get(metric, [])) else None
                        
                        if metric_id:
                            # Update existing record
                            success = update_people_metric({
                                'id': metric_id,
                                'metric_value': new_value,
                                'updated_by': current_hub
                            })
                            if not success:
                                save_success = False
                        else:
                            # Insert new record if needed
                            conn = sqlite3.connect('gdc_data.db')
                            cursor = conn.cursor()
                            
                            try:
                                cursor.execute("""
                                    INSERT INTO people_metrics (
                                        hub_id, metric_name, metric_value, metric_category, 
                                        time_period, updated_by, updated_at, people_metric_updated_at,
                                        date_created
                                    ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                                """, (
                                    hub_id, metric, new_value, "Gender", 
                                    period, current_hub
                                ))
                                
                                conn.commit()
                            except Exception as e:
                                st.error(f"Error adding gender data: {e}")
                                save_success = False
                            finally:
                                conn.close()
                
                if save_success:
                    st.success("Gender distribution data saved successfully!")
                    
                    # Also update the percentage values in hub_metrics
                    if edited_df.shape[0] > 0:
                        # Use the most recent time period
                        latest_row = edited_df.iloc[0]
                        total = latest_row["Total"]
                        
                        if total > 0:
                            female_pct = (latest_row["Female"] / total) * 100
                            male_pct = (latest_row["Male"] / total) * 100
                            other_pct = (latest_row["Other Gender"] / total) * 100
                            
                            conn = sqlite3.connect('gdc_data.db')
                            cursor = conn.cursor()
                            
                            # Update the hub_metrics table with these percentages
                            cursor.execute("""
                                UPDATE hub_metrics
                                SET female_percent = ?,
                                    male_percent = ?,
                                    other_gender_percent = ?,
                                    updated_at = CURRENT_TIMESTAMP,
                                    updated_by = ?
                                WHERE hub_id = ?
                            """, (female_pct, male_pct, other_pct, current_hub, hub_id))
                            
                            conn.commit()
                            conn.close()
                    
                    st.rerun()
                else:
                    st.error("Some gender data could not be saved. Please try again.")
    else:
        # No data exists yet, show initialization form
        st.info("No gender distribution data found. Please add data to get started.")
        
        # Button to initialize first data point
        if st.button("Initialize Gender Data", key="init_gender"):
            # Get current date
            current_month = datetime.now().strftime("%b")
            current_year = datetime.now().year
            time_period = f"{current_month} {current_year}"
            
            # Simple form to collect initial data
            with st.form("init_gender_form"):
                st.subheader("Add Initial Gender Distribution Data")
                
                female_count = st.number_input("Female Count", min_value=0, value=0)
                male_count = st.number_input("Male Count", min_value=0, value=0)
                other_count = st.number_input("Other Gender Count", min_value=0, value=0)
                
                total_count = female_count + male_count + other_count
                st.write(f"Total Count: {total_count}")
                
                if st.form_submit_button("Save Initial Data"):
                    # Insert the initial data records
                    conn = sqlite3.connect('gdc_data.db')
                    cursor = conn.cursor()
                    
                    try:
                        # Insert gender metrics
                        gender_data = [
                            ("Female", female_count),
                            ("Male", male_count),
                            ("Other Gender", other_count)
                        ]
                        
                        for metric_name, value in gender_data:
                            cursor.execute("""
                                INSERT INTO people_metrics (
                                    hub_id, metric_name, metric_value, metric_category, 
                                    time_period, updated_by, updated_at, people_metric_updated_at,
                                    date_created
                                ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                            """, (
                                hub_id, metric_name, value, "Gender", 
                                time_period, current_hub
                            ))
                        
                        # Also update hub_metrics with the percentages
                        if total_count > 0:
                            female_pct = (female_count / total_count) * 100
                            male_pct = (male_count / total_count) * 100
                            other_pct = (other_count / total_count) * 100
                            
                            cursor.execute("""
                                UPDATE hub_metrics
                                SET female_percent = ?,
                                    male_percent = ?,
                                    other_gender_percent = ?,
                                    updated_at = CURRENT_TIMESTAMP,
                                    updated_by = ?
                                WHERE hub_id = ?
                            """, (female_pct, male_pct, other_pct, current_hub, hub_id))
                        
                        conn.commit()
                        st.success("Initial gender data added successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error adding gender data: {e}")
                    finally:
                        conn.close()

# Helper function to handle the Staffing category
def handle_staffing_category(people_metrics_df, current_hub):
    """Handle display and editing of Staffing metrics including Bench Count"""
    st.subheader("Staffing Metrics")
    
    # Filter to staffing category data
    staffing_df = people_metrics_df[people_metrics_df['metric_category'] == 'Staffing']
    
    # Get hub ID for database operations
    conn = sqlite3.connect('gdc_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM hubs WHERE hub_name = ?", (current_hub,))
    result = cursor.fetchone()
    
    if not result:
        st.error(f"Could not find hub ID for {current_hub}")
        conn.close()
        return
    
    hub_id = result[0]
    conn.close()
    
    # Define the staffing metrics to track
    staffing_metrics = ["Bench Count"]
    
    # Get time periods from the data, or use current month/year if none exist
    time_periods = []
    if not staffing_df.empty:
        time_periods = sorted(staffing_df['time_period'].unique(), 
                            key=lambda x: pd.to_datetime(x, format="%b %Y", errors='coerce'), 
                            reverse=True)
    
    # Button to add new time period
    if st.button("Add New Time Period", key="add_staffing_period"):
        # Get current date information
        current_month = datetime.now().strftime("%b")
        current_year = datetime.now().year
        new_period = f"{current_month} {current_year}"
        
        # Only add if it doesn't already exist
        if not time_periods or new_period not in time_periods:
            # Initialize with zero values
            conn = sqlite3.connect('gdc_data.db')
            cursor = conn.cursor()
            
            for metric in staffing_metrics:
                cursor.execute("""
                    INSERT INTO people_metrics (
                        hub_id, metric_name, metric_value, metric_category, 
                        time_period, updated_by, updated_at, people_metric_updated_at,
                        date_created
                    ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, (
                    hub_id, metric, 0, "Staffing", 
                    new_period, current_hub
                ))
            
            conn.commit()
            conn.close()
            st.success(f"Added new time period: {new_period}")
            st.rerun()
    
    # If we have data or just added it, show the editable table
    if not staffing_df.empty or time_periods:
        # If we have no time periods but have staffing data, something's wrong - refresh
        if not time_periods and not staffing_df.empty:
            st.rerun()
            
        # If we have no time periods at all, add the current one
        if not time_periods:
            current_month = datetime.now().strftime("%b")
            current_year = datetime.now().year
            time_periods = [f"{current_month} {current_year}"]
        
        # Build table data for the editor
        table_data = []
        
        for period in time_periods:
            row_data = {"Time Period": period}
            
            for metric in staffing_metrics:
                # Find the value for this metric and period
                value_row = staffing_df[(staffing_df['metric_name'] == metric) & 
                                      (staffing_df['time_period'] == period)]
                
                if not value_row.empty:
                    row_data[metric] = value_row['metric_value'].iloc[0]
                    row_data[f"{metric}_id"] = value_row['id'].iloc[0]
                else:
                    row_data[metric] = 0
                    row_data[f"{metric}_id"] = None
            
            table_data.append(row_data)
        
        # Create dataframe for the editor
        if table_data:
            edit_df = pd.DataFrame(table_data)
            
            # Track IDs for updates
            id_columns = {}
            for metric in staffing_metrics:
                id_col = f"{metric}_id"
                if id_col in edit_df.columns:
                    id_columns[metric] = edit_df[id_col].tolist()
                    # Remove ID columns from display
                    edit_df = edit_df.drop(id_col, axis=1)
            
            # Configure columns for the editor
            column_config = {
                "Time Period": st.column_config.TextColumn(
                    "Time Period",
                    disabled=True
                )
            }
            
            # Add staffing metric columns
            for metric in staffing_metrics:
                column_config[metric] = st.column_config.NumberColumn(
                    metric,
                    min_value=0,
                    step=1,
                    format="%d",
                    help=f"Number of employees on bench"
                )
            
            # Display the editable table
            st.write("Edit the staffing metrics by time period")
            edited_df = st.data_editor(
                edit_df,
                column_config=column_config,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="staffing_editor"
            )
            
            # Save button
            if st.button("Save Staffing Data", key="save_staffing"):
                save_success = True
                
                # Update database with edited values
                for i, row in edited_df.iterrows():
                    period = row["Time Period"]
                    
                    for metric in staffing_metrics:
                        # Get the new value from edited dataframe
                        new_value = int(row[metric])  # Ensure integer
                        
                        # Get record ID if it exists
                        metric_id = id_columns.get(metric, [])[i] if i < len(id_columns.get(metric, [])) else None
                        
                        if metric_id:
                            # Update existing record
                            success = update_people_metric({
                                'id': metric_id,
                                'metric_value': new_value,
                                'updated_by': current_hub
                            })
                            if not success:
                                save_success = False
                        else:
                            # Insert new record if needed
                            conn = sqlite3.connect('gdc_data.db')
                            cursor = conn.cursor()
                            
                            try:
                                cursor.execute("""
                                    INSERT INTO people_metrics (
                                        hub_id, metric_name, metric_value, metric_category, 
                                        time_period, updated_by, updated_at, people_metric_updated_at,
                                        date_created
                                    ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                                """, (
                                    hub_id, metric, new_value, "Staffing", 
                                    period, current_hub
                                ))
                                
                                conn.commit()
                            except Exception as e:
                                st.error(f"Error adding staffing data: {e}")
                                save_success = False
                            finally:
                                conn.close()
                
                if save_success:
                    st.success("Staffing data saved successfully!")
                    
                    # Also update the bench_count in hub_metrics
                    if edited_df.shape[0] > 0:
                        # Use the most recent time period
                        latest_row = edited_df.iloc[0]
                        bench_count = latest_row["Bench Count"]
                        
                        conn = sqlite3.connect('gdc_data.db')
                        cursor = conn.cursor()
                        
                        # Update the hub_metrics table with the bench count
                        cursor.execute("""
                            UPDATE hub_metrics
                            SET bench_count = ?,
                                updated_at = CURRENT_TIMESTAMP,
                                updated_by = ?
                            WHERE hub_id = ?
                        """, (bench_count, current_hub, hub_id))
                        
                        conn.commit()
                        conn.close()
                    
                    st.rerun()
                else:
                    st.error("Some staffing data could not be saved. Please try again.")
    else:
        # No data exists yet, show initialization form
        st.info("No staffing data found. Please add data to get started.")
        
        # Button to initialize first data point
        if st.button("Initialize Staffing Data", key="init_staffing"):
            # Get current date
            current_month = datetime.now().strftime("%b")
            current_year = datetime.now().year
            time_period = f"{current_month} {current_year}"
            
            # Simple form to collect initial data
            with st.form("init_staffing_form"):
                st.subheader("Add Initial Staffing Data")
                
                bench_count = st.number_input("Employees on Bench", min_value=0, value=0)
                
                if st.form_submit_button("Save Initial Data"):
                    # Insert the initial data records
                    conn = sqlite3.connect('gdc_data.db')
                    cursor = conn.cursor()
                    
                    try:
                        # Insert bench count metric
                        cursor.execute("""
                            INSERT INTO people_metrics (
                                hub_id, metric_name, metric_value, metric_category, 
                                time_period, updated_by, updated_at, people_metric_updated_at,
                                date_created
                            ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """, (
                            hub_id, "Bench Count", bench_count, "Staffing", 
                            time_period, current_hub
                        ))
                        
                        # Also update hub_metrics with the bench count
                        cursor.execute("""
                            UPDATE hub_metrics
                            SET bench_count = ?,
                                updated_at = CURRENT_TIMESTAMP,
                                updated_by = ?
                            WHERE hub_id = ?
                        """, (bench_count, current_hub, hub_id))
                        
                        conn.commit()
                        st.success("Initial staffing data added successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error adding staffing data: {e}")
                    finally:
                        conn.close()
# Admin section for data import/export
def show_admin_tools():
    st.header("Administrator Tools")
    
    # Create tabs for different admin functions
    admin_tabs = st.tabs(["Data Import/Export", "User Management", "Data Health Monitoring"])
    
    with admin_tabs[0]:
        st.subheader("Export Data")
        
        if st.button("Export All Data to Excel", key="export_excel_button"):  # Added unique key
            try:
                conn = sqlite3.connect('gdc_data.db')
                
                # Export all tables
                tables = ['hubs', 'hub_metrics',
                         'hub_capabilities', 'client_metrics', 'people_metrics']
                
                # Create Excel writer
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                excel_file = f"gdc_dashboard_data_{timestamp}.xlsx"
                
                with pd.ExcelWriter(excel_file) as writer:
                    for table in tables:
                        df = pd.read_sql(f"SELECT * FROM {table}", conn)
                        df.to_excel(writer, sheet_name=table, index=False)
                
                conn.close()
                
                st.success(f"Data exported successfully to {excel_file}")
                
                # Provide download link
                with open(excel_file, 'rb') as f:
                    st.download_button(
                        label="Download Excel File",
                        data=f,
                        file_name=excel_file,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_excel_button"  # Added unique key
                    )
            except Exception as e:
                st.error(f"Export failed: {e}")
        
        st.subheader("Database Backup")
        if st.button("Backup Database", key="backup_db_button"):  # Added unique key
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"gdc_data_backup_{timestamp}.db"
            
            try:
                import shutil
                shutil.copy2('gdc_data.db', backup_file)
                st.success(f"Database backed up successfully to {backup_file}")
                
                # Provide download link for the backup file
                with open(backup_file, 'rb') as f:
                    st.download_button(
                        label="Download Database Backup",
                        data=f,
                        file_name=backup_file,
                        mime="application/x-sqlite3",
                        key="download_backup_button"  # Added unique key
                    )
            except Exception as e:
                st.error(f"Backup failed: {e}")
    
    with admin_tabs[1]:
        st.subheader("User Management")
        
        # Show current users
        conn = sqlite3.connect('gdc_data.db')
        users_df = pd.read_sql("SELECT id, username, hub_name, is_admin FROM users", conn)
        conn.close()
        
        st.dataframe(users_df, use_container_width=True)
        
        # Add user form
        with st.form("add_user_form"):
            st.subheader("Add New User")
            
            # Get hubs for dropdown
            conn = sqlite3.connect('gdc_data.db')
            hubs_df = pd.read_sql("SELECT hub_name FROM hubs", conn)
            conn.close()
            
            hub_options = list(hubs_df['hub_name'])
            hub_options.append("ALL")  # Add ALL for admin users
            
            # Form fields
            col1, col2 = st.columns(2)
            
            with col1:
                username = st.text_input("Username")
                password = st.text_input("Password", type="password")
                confirm_password = st.text_input("Confirm Password", type="password")
            
            with col2:
                hub_name = st.selectbox("Hub Access", options=hub_options)
                is_admin = st.checkbox("Administrator Access")
            
            # Submit button
            if st.form_submit_button("Add User"):
                if not username or not password:
                    st.error("Username and password are required")
                elif password != confirm_password:
                    st.error("Passwords do not match")
                else:
                    # Save new user to database
                    password_hash = hashlib.sha256(password.encode()).hexdigest()
                    
                    conn = sqlite3.connect('gdc_data.db')
                    cursor = conn.cursor()
                    
                    try:
                        cursor.execute("""
                        INSERT INTO users (username, password_hash, hub_name, is_admin)
                        VALUES (?, ?, ?, ?)
                        """, (username, password_hash, hub_name, 1 if is_admin else 0))
                        
                        conn.commit()
                        st.success(f"User '{username}' added successfully!")
                        st.rerun()
                    except sqlite3.IntegrityError:
                        st.error(f"Username '{username}' already exists. Please choose another username.")
                    except Exception as e:
                        st.error(f"Error adding user: {e}")
                    finally:
                        conn.close()
    
    with admin_tabs[2]:
        st.subheader("Data Health Monitoring")
        
        # Get overall data health metrics
        conn = sqlite3.connect('gdc_data.db')
        
        # Get all hubs
        hubs_df = pd.read_sql("SELECT id, hub_name FROM hubs", conn)
        
        # Create a data health summary
        health_data = []
        
        for _, hub_row in hubs_df.iterrows():
            hub_id = hub_row['id']
            hub_name = hub_row['hub_name']
            
            # Get metrics data
            metrics_df = pd.read_sql(f"SELECT * FROM hub_metrics WHERE hub_id = {hub_id}", conn)
            
            if not metrics_df.empty:
                # Check core metrics update time
                metrics_updated = metrics_df['updated_at'].iloc[0] if not pd.isna(metrics_df['updated_at'].iloc[0]) else None
                metrics_outdated = is_outdated(metrics_updated)
                
                # Check capabilities
                capabilities_df = pd.read_sql(f"SELECT * FROM hub_capabilities WHERE hub_id = {hub_id}", conn)
                capabilities_count = len(capabilities_df)
                capabilities_updated = capabilities_df['updated_at'].max() if not capabilities_df.empty else None
                capabilities_outdated = is_outdated(capabilities_updated)
                
                # Check clients
                clients_df = pd.read_sql(f"SELECT * FROM client_metrics WHERE hub_id = {hub_id}", conn)
                clients_count = len(clients_df)
                clients_updated = clients_df['updated_at'].max() if not clients_df.empty else None
                clients_outdated = is_outdated(clients_updated)
                
                # Check people metrics
                people_df = pd.read_sql(f"SELECT * FROM people_metrics WHERE hub_id = {hub_id}", conn)
                people_metrics_count = len(people_df)
                people_updated = people_df['updated_at'].max() if not people_df.empty else None
                people_outdated = is_outdated(people_updated)
                
                # Calculate health score
                total_checks = 4  # Core metrics, capabilities, clients, people metrics
                outdated_count = sum([
                    1 if metrics_outdated else 0,
                    1 if capabilities_outdated else 0,
                    1 if clients_outdated else 0,
                    1 if people_outdated else 0
                ])
                
                health_score = 100 * (total_checks - outdated_count) / total_checks
                
                # Add to health data
                health_data.append({
                    'Hub': hub_name,
                    'Health Score': f"{health_score:.0f}%",
                    'Core Metrics': "Outdated" if metrics_outdated else "Current",
                    'Capabilities': "Outdated" if capabilities_outdated else "Current",
                    'Clients': "Outdated" if clients_outdated else "Current",
                    'People Metrics': "Outdated" if people_outdated else "Current",
                    'Total Score': health_score  # For sorting
                })
        
        conn.close()
        
        if health_data:
            # Convert to DataFrame and sort by health score
            health_df = pd.DataFrame(health_data)
            health_df = health_df.sort_values('Total Score', ascending=False)
            health_df = health_df.drop('Total Score', axis=1)  # Remove sorting column
            
            # Display health dashboard
            st.dataframe(health_df, use_container_width=True)
            
            # Create a summary chart
            st.subheader("Data Health by Hub")
            
            # Extract data for chart
            chart_data = pd.DataFrame({
                'Hub': health_df['Hub'],
                'Health Score': health_df['Health Score'].str.rstrip('%').astype(float)
            })
            
            # Display bar chart
            st.bar_chart(chart_data.set_index('Hub'))
        else:
            st.info("No data health information available.")

def main():
    # Ensure database is set up
    setup_database()
    
    # Periodically prune SQLite WAL files (once per app startup)
    try:
        with get_db_connection() as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception as e:
        st.error(f"Error cleaning up WAL files: {e}")
    
    # Display appropriate interface based on login status
    if not st.session_state.logged_in:
        show_login_screen()
    else:
        # Set dashboard as default view when logged in
        if 'current_view' not in st.session_state or st.session_state.current_view == "hub_metrics":
            st.session_state.current_view = "dashboard"
        
        # Main interface for logged-in users
        show_main_interface()

if __name__ == "__main__":
    main()