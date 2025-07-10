import streamlit as st
import pandas as pd
from datetime import timedelta, date
import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv
import os
from psycopg2 import connect
import requests
import json

def get_db_connection():
    load_dotenv()
    PG_DATABASE_URL = os.environ.get("PG_DATABASE_URL")
    if not PG_DATABASE_URL:
        raise ValueError("Environment variable 'PG_DATABASE_URL' is required")

    parsed = urlparse(PG_DATABASE_URL)
    host = parsed.hostname
    port = int(parsed.port) if parsed.port else 5432
    user = parsed.username
    password = parsed.password

    # Extract database name from URL path (e.g., /my_db -> my_db)
    db_name = parsed.path[1:] if parsed.path else 'default_db'

    new_conn = connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=db_name
    )
    return new_conn


def get_schema_name():
    """Get the schema name based on database name"""
    load_dotenv()
    PG_DATABASE_URL = os.environ.get("PG_DATABASE_URL")
    parsed = urlparse(PG_DATABASE_URL)
    db_name = parsed.path[1:] if parsed.path else 'default_db'
    return f"{db_name}_schema"


def get_auth_config():
    """Get authentication configuration from environment variables"""
    load_dotenv()
    
    # Check which auth method to use
    auth_method = os.environ.get("AUTH_METHOD", "HTTP").upper()
    
    if auth_method == "HTTP":
        auth_url = os.environ.get("AUTH_URL")
        if not auth_url:
            raise ValueError("Environment variable 'AUTH_URL' is required for HTTP auth")
        return {"method": "HTTP", "url": auth_url}
    
    elif auth_method == "LDAP":
        # Original LDAP configuration
        LDAP_HOST_URL = os.environ.get("LDAP_HOST_URL")
        if not LDAP_HOST_URL:
            raise ValueError("Environment variable 'LDAP_HOST_URL' is required for LDAP auth")
        LDAP_BASE_DN = os.environ.get("LDAP_BASE_DN")
        if not LDAP_BASE_DN:
            raise ValueError("Environment variable 'LDAP_BASE_DN' is required for LDAP auth")
        LDAP_MANAGER_DN = os.environ.get("LDAP_MANAGER_DN")
        if not LDAP_MANAGER_DN:
            raise ValueError("Environment variable 'LDAP_MANAGER_DN' is required for LDAP auth")
        LDAP_MANAGER_PASSWORD = os.environ.get("LDAP_MANAGER_PASSWORD")
        if not LDAP_MANAGER_PASSWORD:
            raise ValueError("Environment variable 'LDAP_MANAGER_PASSWORD' is required for LDAP auth")
        
        return {
            "method": "LDAP",
            "server": LDAP_HOST_URL,
            "base_dn": LDAP_BASE_DN,
            "manager_dn": LDAP_MANAGER_DN,
            "manager_password": LDAP_MANAGER_PASSWORD
        }
    
    else:
        raise ValueError(f"Invalid AUTH_METHOD: {auth_method}. Must be 'HTTP' or 'LDAP'")


def http_authenticate(username, password):
    """
    Authenticate user using HTTP API
    """
    try:
        auth_config = get_auth_config()
        
        if auth_config["method"] != "HTTP":
            return False
            
        # Prepare the request payload
        payload = {
            "username": username,
            "password": password
        }
        
        # Make the authentication request
        response = requests.post(
            auth_config["url"],
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if "access_token" in data and "token_type" in data:
                # Store token in session state for potential future use
                st.session_state['access_token'] = data['access_token']
                st.session_state['token_type'] = data['token_type']
                return True
            else:
                st.error("Invalid response format from authentication server")
                return False
        else:
            # Handle error response
            try:
                error_data = response.json()
                if "details" in error_data:
                    st.error(f"Authentication failed: {error_data['details']}")
                else:
                    st.error(f"Authentication failed: {error_data}")
            except:
                st.error(f"Authentication failed: HTTP {response.status_code}")
            return False
            
    except requests.exceptions.Timeout:
        st.error("Authentication request timed out")
        return False
    except requests.exceptions.ConnectionError:
        st.error("Cannot connect to authentication server")
        return False
    except Exception as e:
        st.error(f"Authentication Error: {str(e)}")
        return False


def ldap_authenticate(username_or_email, password):
    """
    Authenticate user with LDAP using either username or email
    (Original LDAP code preserved for backward compatibility)
    """
    try:
        from ldap3 import Server, Connection, ALL, SIMPLE, SUBTREE
        
        auth_config = get_auth_config()
        
        if auth_config["method"] != "LDAP":
            return False
        
        # Create server connection simply, like in the working example
        server = Server(auth_config['server'])
        
        # First bind with manager credentials to search for user
        manager_conn = Connection(
            server, 
            user=auth_config['manager_dn'], 
            password=auth_config['manager_password'], 
            authentication=SIMPLE
        )
        
        if not manager_conn.bind():
            st.error("Failed to bind with manager credentials")
            return False
        
        # Determine if input is email or username
        if '@' in username_or_email:
            # It's an email - search by mail or userPrincipalName
            search_filter = f"(|(mail={username_or_email})(userPrincipalName={username_or_email}))"
        else:
            # It's a username - search by sAMAccountName
            search_filter = f"(sAMAccountName={username_or_email})"
        
        # Search for user DN
        manager_conn.search(
            search_base=auth_config['base_dn'],
            search_filter=search_filter,
            search_scope=SUBTREE,
            attributes=['distinguishedName', 'sAMAccountName', 'mail', 'userPrincipalName']
        )
        
        if not manager_conn.entries:
            st.error("User not found in LDAP")
            manager_conn.unbind()
            return False
        
        user_dn = manager_conn.entries[0].distinguishedName.value
        st.write(f"Found user DN: {user_dn}")  # Debug info
        manager_conn.unbind()
        
        # Now authenticate with user credentials
        user_conn = Connection(
            server, 
            user=user_dn, 
            password=password, 
            authentication=SIMPLE
        )
        
        if user_conn.bind():
            user_conn.unbind()
            return True
        else:
            st.error("Authentication failed - invalid password")
            return False
            
    except Exception as e:
        st.error(f"LDAP Authentication Error: {str(e)}")
        return False


def authenticate_user(username, password):
    """
    Main authentication function that routes to appropriate method
    """
    try:
        auth_config = get_auth_config()
        
        if auth_config["method"] == "HTTP":
            return http_authenticate(username, password)
        elif auth_config["method"] == "LDAP":
            return ldap_authenticate(username, password)
        else:
            st.error("Invalid authentication method configured")
            return False
            
    except Exception as e:
        st.error(f"Authentication configuration error: {str(e)}")
        return False


def get_user_info(username_or_email):
    """
    Get user information - for HTTP auth, we'll create basic info from username
    For LDAP, use the original implementation
    """
    try:
        auth_config = get_auth_config()
        
        if auth_config["method"] == "HTTP":
            # For HTTP auth, create basic user info from username
            # You may want to modify this based on your needs
            if '@' in username_or_email:
                name = username_or_email.split('@')[0]
                email = username_or_email
            else:
                name = username_or_email
                email = f"{username_or_email}@company.com"  # Default domain
            
            return {
                'name': name.title(),
                'email': email,
                'team': 'IT',  # Default team
                'username': username_or_email
            }
        
        elif auth_config["method"] == "LDAP":
            # Original LDAP user info retrieval
            from ldap3 import Server, Connection, ALL, SIMPLE, SUBTREE
            
            # Use the same server connection logic as authentication
            server_urls = [
                auth_config['server'],
                auth_config['server'].replace('ldap://',''),
                f"{auth_config['server']}:389"
            ]
            
            server = None
            for url in server_urls:
                try:
                    server = Server(url, port=389, use_ssl=False, get_info=ALL)
                    test_conn = Connection(server)
                    if test_conn.bind():
                        test_conn.unbind()
                        break
                except Exception:
                    continue
            
            if not server:
                return None
            
            # Bind with manager credentials
            conn = Connection(
                server, 
                user=auth_config['manager_dn'], 
                password=auth_config['manager_password'], 
                authentication=SIMPLE
            )
            
            if not conn.bind():
                return None
            
            # Determine if input is email or username
            if '@' in username_or_email:
                search_filter = f"(|(sAMAccountName={username_or_email})(userPrincipalName={username_or_email}))"
            else:
                search_filter = f"(sAMAccountName={username_or_email})"
                
            # Search for user information
            conn.search(
                search_base=auth_config['base_dn'],
                search_filter=search_filter,
                search_scope=SUBTREE,
                attributes=['displayName', 'mail', 'department', 'sAMAccountName', 'userPrincipalName']
            )
            
            if conn.entries:
                entry = conn.entries[0]
                user_info = {
                    'name': entry.displayName.value if entry.displayName else username_or_email,
                    'email': entry.mail.value if entry.mail else (entry.userPrincipalName.value if entry.userPrincipalName else username_or_email),
                    'team': entry.department.value if entry.department else 'IT',
                    'username': entry.sAMAccountName.value if entry.sAMAccountName else username_or_email
                }
                conn.unbind()
                return user_info
            else:
                conn.unbind()
                return None
        
        return None
            
    except Exception as e:
        st.error(f"User Info Error: {str(e)}")
        return None


def ensure_schema_exists():
    """Create schema if it doesn't exist"""
    schema_name = get_schema_name()
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Create schema if it does not already exist
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        conn.commit()
        return schema_name
    except Exception as e:
        st.error(f"Schema creation failed: {e}")
        raise e
    finally:
        cur.close()
        conn.close()


def create_system_tables():
    """Create system tables in the proper schema"""
    schema_name = ensure_schema_exists()
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Create tables in specified schema
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.iamin_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                username VARCHAR(255),
                car_park VARCHAR(100),
                team VARCHAR(100)
            );
        """)

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.iamin_availability (
                user_id INT REFERENCES {schema_name}.iamin_users(id),
                date DATE NOT NULL,
                availability_code VARCHAR(50) NOT NULL,
                PRIMARY KEY (user_id, date)
            );
        """)

        conn.commit()
        
    except Exception as e:
        st.error(f"Table creation failed: {e}")
        raise e
    finally:
        cur.close()
        conn.close()


def get_users():
    schema_name = get_schema_name()
    conn = get_db_connection()
    try:
        df = pd.read_sql(f"SELECT * FROM {schema_name}.iamin_users", conn)
        return df
    finally:
        conn.close()


def add_user(email, name=None, username=None, team='Engineering'):
    schema_name = get_schema_name()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Use provided name or extract from email
        if not name:
            name = email.split('@')[0]
        
        cur.execute(f"""
            INSERT INTO {schema_name}.iamin_users (email, name, username, team) 
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (email) DO UPDATE SET 
                name = EXCLUDED.name,
                username = EXCLUDED.username,
                team = EXCLUDED.team;
        """, (email, name, username, team))
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        conn.close()


def get_user_by_email(email):
    schema_name = get_schema_name()
    conn = get_db_connection()
    try:
        df = pd.read_sql(f"SELECT * FROM {schema_name}.iamin_users WHERE email = %s", conn, params=(email,))
        return df
    finally:
        conn.close()


def get_or_create_user(user_info):
    """Get user from database or create if doesn't exist"""
    schema_name = get_schema_name()
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # First try to get user by email
        cur.execute(f"SELECT * FROM {schema_name}.iamin_users WHERE email = %s", (user_info['email'],))
        user = cur.fetchone()
        
        if user:
            # Update existing user info
            cur.execute(f"""
                UPDATE {schema_name}.iamin_users 
                SET name = %s, username = %s, team = %s 
                WHERE email = %s
                RETURNING id
            """, (user_info['name'], user_info.get('username'), user_info['team'], user_info['email']))
            user_id = cur.fetchone()[0]
        else:
            # Create new user
            cur.execute(f"""
                INSERT INTO {schema_name}.iamin_users (email, name, username, team) 
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (user_info['email'], user_info['name'], user_info.get('username'), user_info['team']))
            user_id = cur.fetchone()[0]
        
        conn.commit()
        return user_id
    finally:
        cur.close()
        conn.close()


def update_car_park(user_id, car_park):
    schema_name = get_schema_name()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"UPDATE {schema_name}.iamin_users SET car_park = %s WHERE id = %s", (car_park, user_id))
        conn.commit()
    finally:
        cur.close()
        conn.close()


def save_availability(user_id, dates, codes):
    schema_name = get_schema_name()
    corrected_codes = ["In Office" if code == "" else code for code in codes]

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Use raw date objects (not strings)
        cur.execute(
            f"DELETE FROM {schema_name}.iamin_availability WHERE user_id = %s AND date = ANY(%s::date[])",
            (user_id, dates)
        )

        for date_val, code in zip(dates, corrected_codes):
            cur.execute(f"""
                INSERT INTO {schema_name}.iamin_availability (user_id, date, availability_code)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id, date) DO UPDATE SET availability_code = EXCLUDED.availability_code
            """, (user_id, date_val, code))

        conn.commit()
    finally:
        cur.close()
        conn.close()



def get_weekly_availability(start_date):
    schema_name = get_schema_name()
    conn = get_db_connection()
    try:
        query = f"""
            SELECT u.name, u.car_park, a.date, a.availability_code
            FROM {schema_name}.iamin_users u
            JOIN {schema_name}.iamin_availability a ON u.id = a.user_id
            WHERE a.date BETWEEN %s AND %s
            ORDER BY u.name, a.date
        """
        end_date = start_date + timedelta(days=4)
        df = pd.read_sql(query, conn, params=(start_date, end_date))
        
        # Format the dataframe for download
        days = ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY']
        weekly_data = {}
        
        for _, row in df.iterrows():
            date_str = pd.to_datetime(row['date']).strftime('%m/%d/%Y')
            day_name = pd.to_datetime(row['date']).day_name().upper()
            
            if row['name'] not in weekly_data:
                weekly_data[row['name']] = {
                    'NAME': row['name'], 
                    'CAR': row['car_park'] or 'N/A',
                    **{day: 'BLANK' for day in days}
                }
            
            if day_name in days:
                weekly_data[row['name']][day_name] = row['availability_code']
        
        # Create final dataframe
        if weekly_data:
            result_df = pd.DataFrame(list(weekly_data.values()))
            result_df = result_df[['NAME', 'CAR', 'MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY']]
        else:
            result_df = pd.DataFrame(columns=['NAME', 'CAR', 'MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY'])
        
        return result_df
    finally:
        conn.close()


def main():
    st.set_page_config(page_title="IAMIN: Employee Availability App", layout="wide")
    
    # Initialize database tables
    try:
        create_system_tables()
    except Exception as e:
        st.error(f"Database initialization failed: {e}")
        return
    
    # Login Page
    if 'logged_in' not in st.session_state:
        st.title("IAMIN Login")
        
        # Show auth method info
        try:
            auth_config = get_auth_config()
            if auth_config["method"] == "HTTP":
                st.info("Using HTTP Authentication - Enter your username and password")
            else:
                st.info("Using LDAP Authentication - You can login with either your username or email address")
        except Exception as e:
            st.error(f"Authentication configuration error: {e}")
            return
        
        username = st.text_input("Username", key="login_username")
        password = st.text_input("Password", type="password", key="login_password")
        
        if st.button("Login"):
            if username and password:
                if authenticate_user(username, password):
                    user_info = get_user_info(username)
                    if user_info:
                        # Get or create user in database
                        user_id = get_or_create_user(user_info)
                        
                        st.session_state['logged_in'] = True
                        st.session_state['user_id'] = user_id
                        st.session_state['username'] = username
                        st.session_state['user_info'] = user_info
                        st.session_state['is_admin'] = 'admin' in username.lower()
                        st.success("Login successful!")
                        st.rerun()
                    else:
                        st.error("Could not retrieve user information")
                else:
                    st.error("Invalid credentials")
            else:
                st.error("Please enter both username and password")
        return
    
    # Main Application
    st.title("IAMIN: Employee Availability App")
    st.sidebar.title(f"Welcome, {st.session_state['user_info']['name']}")
    st.sidebar.write(f"Email: {st.session_state['user_info']['email']}")
    st.sidebar.write(f"Team: {st.session_state['user_info']['team']}")
    
    # Logout button
    if st.sidebar.button("Logout"):
        for key in ['logged_in', 'user_id', 'username', 'user_info', 'is_admin', 'access_token', 'token_type']:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()
    
    # Navigation
    if st.session_state['is_admin']:
        tab1, tab2, tab3 = st.tabs(["My Availability", "Admin Panel", "Reports"])
        
        with tab2:
            st.header("Admin Panel")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Add New User")
                email = st.text_input("Enter Email Address")
                name = st.text_input("Enter Name (optional)")
                username = st.text_input("Enter Username (optional)")
                team = st.selectbox("Select Team", ["Engineering", "IT", "HR", "Finance", "Sales", "Marketing"])
                
                if st.button("Add User"):
                    if email:
                        if add_user(email, name, username, team):
                            st.success("User added successfully!")
                        else:
                            st.error("Failed to add user")
                    else:
                        st.error("Please enter an email address")
                        
            with col2:
                st.subheader("Manage Car Parks")
                try:
                    user_df = get_users()
                    if not user_df.empty:
                        edited_df = st.data_editor(user_df, key="car_park_editor", use_container_width=True)
                        
                        if st.button("Save Car Park Changes"):
                            for index, row in edited_df.iterrows():
                                if index < len(user_df):
                                    original_car_park = user_df.iloc[index]['car_park']
                                    new_car_park = row['car_park']
                                    if original_car_park != new_car_park:
                                        update_car_park(row['id'], new_car_park)
                            st.success("Car park assignments updated!")
                    else:
                        st.info("No users found in the system")
                except Exception as e:
                    st.error(f"Error loading users: {e}")
        
        with tab3:
            st.header("Weekly Reports")
            today = date.today()
            start_date = st.date_input("Select Week Start", value=today)
            
            if st.button("Generate Weekly Report"):
                try:
                    df = get_weekly_availability(start_date)
                    if not df.empty:
                        st.dataframe(df, use_container_width=True)
                        
                        # Download functionality
                        csv = df.to_csv(index=False)
                        st.download_button(
                            label="Download CSV",
                            data=csv,
                            file_name=f"weekly_report_{start_date.strftime('%Y%m%d')}.csv",
                            mime="text/csv"
                        )
                    else:
                        st.info("No availability data found for the selected week")
                except Exception as e:
                    st.error(f"Error generating report: {e}")
    else:
        tab1, tab2 = st.tabs(["My Availability", "View Reports"])
        
        with tab2:
            st.header("Weekly Reports")
            today = date.today()
            start_date = st.date_input("Select Week Start", value=today)
            
            if st.button("View Weekly Report"):
                try:
                    df = get_weekly_availability(start_date)
                    if not df.empty:
                        st.dataframe(df, use_container_width=True)
                    else:
                        st.info("No availability data found for the selected week")
                except Exception as e:
                    st.error(f"Error loading report: {e}")
    
    # Common availability tab
    with tab1:
        st.header("My Weekly Availability")
        
        today = date.today()
        # Find the Monday of the current week
        monday = today - timedelta(days=today.weekday())
        start_date = st.date_input("Select Week Start (Monday)", value=monday)
        
        # Display Calendar
        days = [start_date + timedelta(days=i) for i in range(5)]
        availability_options = ["In Office", "WFH", "AL", "OL", "O/S", "REM", "CONF"]
        
        st.subheader("Select your availability for each day:")
        cols = st.columns(5)
        codes = []
        
        for day, col in zip(days, cols):
            with col:
                st.write(f"**{day.strftime('%A')}**")
                st.write(day.strftime('%m/%d/%Y'))
                code = st.selectbox(
                    "Status",
                    availability_options,
                    key=f"availability_{day}",
                    label_visibility="collapsed"
                )
                codes.append(code)
        
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("Submit Availability", type="primary"):
                try:
                    save_availability(st.session_state['user_id'], days, codes)
                    st.success("Availability saved successfully!")
                except Exception as e:
                    st.error(f"Error saving availability: {e}")


if __name__ == "__main__":
    main()
