from flask import Flask, jsonify, request
import sqlite3
import json

app = Flask(__name__)


def normalize_data(json_data):
    num_entries = len(json_data["id"])
    normalized_data = []
    for i in range(num_entries):
        entry = {}
        for key, values in json_data.items():
            entry[key] = values[str(i)]
        entry['rating'] = 0  # Add 'rating' column with value 0
        normalized_data.append(entry)
    return normalized_data

def create_table(cursor, keys):
    cursor.execute("DROP TABLE IF EXISTS entries")
    cursor.execute('''CREATE TABLE entries
                      (id TEXT PRIMARY KEY, {} TEXT, rating INTEGER)'''.format(" TEXT, ".join(keys)))


def insert_data(cursor, data):
    placeholders = ', '.join(['?' for _ in range(len(data[0]) + 1)])  # Include one additional placeholder for the 'rating' column
    keys = list(data[0].keys())
    cursor.executemany(
        f"INSERT INTO entries ({', '.join(keys + ['rating'])}) VALUES ({placeholders})",
        [tuple(entry[key] for key in keys) + (entry['rating'],) for entry in data]
    )


def push_data():
    # Load JSON data from file
    with open('data.json') as f:
        json_data = json.load(f)
    
    # Normalize data
    normalized_data = normalize_data(json_data)
    
    # Connect to SQLite database
    conn = sqlite3.connect('music_data.db')
    cursor = conn.cursor()
    
    # Get all keys excluding 'id'
    keys = list(json_data.keys())
    keys.remove('id')
    
    # Create table if not exists
    create_table(cursor, keys)
    
    # Insert data into the table
    insert_data(cursor, normalized_data)
    
    # Commit changes and close connection
    conn.commit()
    conn.close()


app = Flask(__name__)

# API endpoint for retrieving all items
@app.route('/api/musics', methods=['GET'])
def get_all_items():
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', 10))
    start_index = (page - 1) * page_size
    
    conn = sqlite3.connect('music_data.db')
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM entries LIMIT ? OFFSET ?", (page_size, start_index))
    items = cursor.fetchall()
    
    conn.close()
    
    return jsonify(items)

@app.route('/api/song', methods=['GET'])
def get_song_attributes():
    title = request.args.get('title')
    
    conn = sqlite3.connect('music_data.db')
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM entries WHERE title = ?", (title,))
    song = cursor.fetchone()
    
    conn.close()
    
    if song:
        return jsonify(song)
    else:
        return jsonify({'error': 'Song not found'}), 404


# API endpoint for updating star rating of a song by title
@app.route('/api/song/rating', methods=['POST'])
def update_song_rating():
    title = request.json.get('title')
    rating = request.json.get('rating')
    
    conn = sqlite3.connect('music_data.db')
    cursor = conn.cursor()

    cursor.execute("UPDATE entries SET rating = ? WHERE title = ?", (rating, title))
    conn.commit()
    
    conn.close()
    
    return jsonify({'message': 'Rating updated successfully'})


if __name__ == "__main__":
    push_data()
    app.run(debug=True)
