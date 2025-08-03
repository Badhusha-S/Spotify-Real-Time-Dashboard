import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import os
import time
from sqlalchemy import create_engine
import psycopg2 # New import for direct PostgreSQL interaction
from psycopg2 import sql # For safely building SQL queries
from spotipy.exceptions import SpotifyException

# --- 1. Store your Spotify App Credentials ---
CLIENT_ID = '***********'  # Replace with your actual Client ID
CLIENT_SECRET = '**********8' # Replace with your actual Client Secret

# --- 2. PostgreSQL Database Configuration ---
DB_USER = '****' # Default PostgreSQL user
DB_PASSWORD = '*****' # Use the password you set for spotify_user
DB_HOST = '*****' # Or your PostgreSQL server IP/hostname
DB_PORT = '*****' # Default PostgreSQL port
DB_NAME = '**********' # The database you created

# SQLAlchemy connection string format for PostgreSQL (still useful for engine)
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- 3. Authenticate with Spotify API ---
try:
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=CLIENT_ID,
                                                               client_secret=CLIENT_SECRET))
    print("Successfully connected to Spotify API!")
except Exception as e:
    print(f"Error connecting to Spotify API. Check your CLIENT_ID and CLIENT_SECRET. Error: {e}")
    exit()

# --- 4. Establish Database Connection Engine and Direct Connection ---
# We'll use SQLAlchemy engine for pandas if needed, but psycopg2 for UPSERT
conn = None # Initialize conn
try:
    # Direct psycopg2 connection for UPSERT
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    conn.autocommit = True # Set to True for individual statements to commit immediately
    cursor = conn.cursor()
    print(f"Successfully connected to PostgreSQL database: {DB_NAME}")

    # SQLAlchemy engine (still useful if we ever switch back to df.to_sql or other SQLAlchemy features)
    engine = create_engine(DATABASE_URL)
    # Test with engine also
    with engine.connect() as connection:
        pass # Connection successful
except Exception as e:
    print(f"Error connecting to PostgreSQL database. Check your DB credentials and connection. Error: {e}")
    if conn:
        conn.close()
    exit()

# --- 5. Define the Artists You Want to Get Data For ---
artists_to_fetch = [
    "The Weeknd","Drake","Ed Sheeran","Shawn Mendes","Billie Eilish",
    "Sai Abhyankkar", # Corrected name from previous output
    "Anirudh Ravichander", # Added based on your output
    "Hanumankind", "A.R.Rahman","Arijith Singh","Shreya Ghoshal","Taylor Swift"
]

# --- 6. Prepare Lists to Collect Data for Direct SQL Insertion ---
# We'll collect data as lists of dictionaries directly for psycopg2
all_artists_details_data = []
all_top_tracks_data = []
all_albums_data = []

# --- 7. Loop Through Each Artist and Fetch Data ---
for artist_name_input in artists_to_fetch:
    print(f"\n--- Processing Artist: {artist_name_input} ---")
    artist_id = None
    actual_artist_name = None

    # Step 7a: Search for the Artist on Spotify
    try:
        search_results = sp.search(q='artist:' + artist_name_input, type='artist', limit=1)
        time.sleep(0.5)

        if search_results and search_results['artists']['items']:
            artist_id = search_results['artists']['items'][0]['id']
            actual_artist_name = search_results['artists']['items'][0]['name']
            print(f"Found Spotify ID for '{actual_artist_name}': {artist_id}")
        else:
            print(f"Artist '{artist_name_input}' not found on Spotify. Skipping this artist.")
            continue
    except Exception as e:
        print(f"Error searching for artist '{artist_name_input}': {e}. Skipping this artist.")
        continue

    # Step 7b: If Artist ID is found, proceed to get more details
    if artist_id:
        # Get Detailed Artist Data (Followers, Popularity, Genres)
        print(f"Fetching detailed artist profile for {actual_artist_name}...")
        artist_details = sp.artist(artist_id)
        time.sleep(0.5)

        # Prepare data for spotify_artist_profiles table
        all_artists_details_data.append({
            'artist_spotify_id': artist_id,
            'artist_name': actual_artist_name,
            'followers_total': artist_details['followers']['total'],
            'popularity_score': artist_details['popularity'],
            'genres': ', '.join(artist_details['genres']),
            'spotify_url': artist_details['external_urls']['spotify']
        })

        print(f"  Followers: {artist_details['followers']['total']:,}")
        print(f"  Popularity (0-100): {artist_details['popularity']}")
        print(f"  Genres: {', '.join(artist_details['genres'])}")
        print(f"  Spotify URL: {artist_details['external_urls']['spotify']}")

        # Get Artist's Top Tracks
        print(f"Fetching top tracks for {actual_artist_name}...")
        top_tracks_response = sp.artist_top_tracks(artist_id)
        time.sleep(0.5)
        for track in top_tracks_response['tracks']:
            all_top_tracks_data.append({
                'artist_spotify_id': artist_id,
                'artist_name': actual_artist_name,
                'track_id': track['id'],
                'track_name': track['name'],
                'album_id': track['album']['id'],
                'album_name': track['album']['name'],
                'track_popularity': track['popularity'],
                'release_date': track['album']['release_date'],
                'track_url': track['external_urls']['spotify']
            })

        # Get Artist's Albums
        print(f"Fetching albums for {actual_artist_name}...")
        albums_response = sp.artist_albums(artist_id, album_type='album', limit=50)
        time.sleep(0.5)
        for album in albums_response['items']:
            all_albums_data.append({
                'artist_spotify_id': artist_id,
                'artist_name': actual_artist_name,
                'album_id': album['id'],
                'album_name': album['name'],
                'album_type': album['album_type'],
                'release_date': album['release_date'],
                'total_tracks': album['total_tracks'],
                'album_url': album['external_urls']['spotify']
            })

        # Get Related Artists - Handled with error catching and not collected for DB
        print(f"Attempting to fetch related artists for {actual_artist_name} (Note: This endpoint was recently deprecated by Spotify and may fail)...")
        try:
            related_artists_response = sp.artist_related_artists(artist_id)
            time.sleep(0.5)
            # You can process this data if the endpoint works, but it's not being stored in DB
            print(f"  Successfully fetched related artists for {actual_artist_name} (may be empty if no data available).")
        except SpotifyException as se:
            print(f"  Warning: Could not fetch related artists for '{actual_artist_name}' due to Spotify API error: {se.http_status} - {se.msg}. This endpoint is likely deprecated.")
        except Exception as e:
            print(f"  An unexpected error occurred while fetching related artists for '{actual_artist_name}': {e}.")
    else:
        print(f"Skipping data fetch for '{artist_name_input}' as no Spotify ID was found.")

# --- 8. Write All Collected Data to PostgreSQL with UPSERT ---
print("\n--- Writing all collected data to PostgreSQL with UPSERT logic ---")

# Function to execute UPSERT
def upsert_data(cursor, table_name, data_list, unique_cols, update_cols):
    if not data_list:
        print(f"  No data to write for {table_name}.")
        return

    # Convert list of dicts to DataFrame to handle various data types correctly
    df_temp = pd.DataFrame(data_list)
    cols = df_temp.columns.tolist() # Get all column names

    # Build the INSERT part of the UPSERT query
    # Using sql.Identifier for table/column names and sql.Placeholder for values is best practice
    # to prevent SQL injection and handle complex names.
    insert_cols_sql = sql.SQL(', ').join(map(sql.Identifier, cols))
    value_placeholders = sql.SQL(', ').join(sql.Placeholder(col) for col in cols)

    # Build the ON CONFLICT part
    unique_constraint_cols_sql = sql.SQL(', ').join(map(sql.Identifier, unique_cols))

    # Build the DO UPDATE SET part
    update_set_sql = sql.SQL(', ').join(
        sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(col)) for col in update_cols
    )

    # Construct the full UPSERT query
    upsert_query = sql.SQL(
        "INSERT INTO {table} ({insert_cols}) VALUES ({values_ph}) "
        "ON CONFLICT ({unique_cols}) DO UPDATE SET {update_set};"
    ).format(
        table=sql.Identifier(table_name),
        insert_cols=insert_cols_sql,
        values_ph=value_placeholders,
        unique_cols=unique_constraint_cols_sql,
        update_set=update_set_sql
    )

    print(f"  Writing to {table_name}...")
    for index, row in df_temp.iterrows():
        try:
            # Execute the UPSERT query for each row
            cursor.execute(upsert_query, row.to_dict())
        except Exception as e:
            print(f"    Error upserting row to {table_name}: {e}")
            # Consider adding more robust error logging here

    print(f"  Finished writing to {table_name}.")

# --- Call upsert_data for each table ---

# spotify_artist_profiles: Unique by 'artist_spotify_id'
# Columns to update if conflict: followers_total, popularity_score, genres, spotify_url
upsert_data(cursor, 'spotify_artist_profiles', all_artists_details_data,
            ['artist_spotify_id'],
            ['followers_total', 'popularity_score', 'genres', 'spotify_url'])

# spotify_top_tracks: Unique by 'track_id'
# Update if conflict: track_popularity, album_name, release_date (as these might change)
upsert_data(cursor, 'spotify_top_tracks', all_top_tracks_data,
            ['track_id'],
            ['track_name', 'album_name', 'track_popularity', 'release_date', 'track_url', 'artist_name']) # Update all non-key fields

# spotify_albums: Unique by 'album_id'
# Update if conflict: album_name, release_date, total_tracks
upsert_data(cursor, 'spotify_albums', all_albums_data,
            ['album_id'],
            ['album_name', 'album_type', 'release_date', 'total_tracks', 'album_url', 'artist_name'])


print("\n--- All requested artist data processing complete ---")

# --- Close the database connection ---
if conn:
    cursor.close()
    conn.close()
    print("Database connection closed.")
