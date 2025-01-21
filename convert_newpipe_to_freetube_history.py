import sqlite3
import os, json
import datetime

try:
    print("modified from the original version on https://github.com/iamtalhaasghar/newpipe-to-freetube-history-sync version de 09/03/2023")
    filename = next(iter([f for f in os.listdir('.') if f.startswith('NewPipeData-')]), None)
    if filename is not None:
        print(filename)
        os.system(f'unzip {filename}')
    # Get the current directory
    current_dir = os.getcwd()
    # Set the path to the newpipe.db file in the current directory
    newpipe_db_path = os.path.join(current_dir, "newpipe.db")
    conn = sqlite3.connect(newpipe_db_path)        
except Exception as e:
    print(e) 
    exit()

conn.row_factory = sqlite3.Row   # keep row access by column name
cur = conn.cursor()
cur.execute("SELECT * FROM streams")
rows = cur.fetchall()

print('rows count : ' + str(len(rows)))

if(len(rows) <= 0):
    print('No Data available')

newpipe_history = []
bad_rows = []  # List to store info about bad rows

for row in rows:
    try:
        # Attempt to fetch stream_history and stream_state for this row
        cur.execute(f"SELECT * FROM stream_history where stream_id={row['uid']}")
        history = cur.fetchone()
        cur.execute(f"SELECT * FROM stream_state where stream_id={row['uid']}")
        state = cur.fetchone()

        data = {
            "videoId": row['url'].split('?v=')[-1],
            "title": row['title'],
            "author": row['uploader'],
            "authorId": row['uploader_url'].split('channel/')[-1],
            "published": row['upload_date'],
            "description": "",
            "viewCount": row['view_count'],
            "lengthSeconds": row['duration'],
            "watchProgress": state['progress_time'] // 1000 if state else 0,
            "timeWatched": history['access_date'],
            "isLive": False,
            "paid": False,
            "type": "video"
        }
        newpipe_history.append(data)
    except Exception as e:
        # If something goes wrong, store row info and error for debugging
        bad_rows.append({
            "uid": row["uid"] if "uid" in row.keys() else None,
            "url": row["url"] if "url" in row.keys() else None,
            "error": str(e)
        })
        continue

print('Good rows:', len(newpipe_history))
print('Bad rows :', len(bad_rows))

# Write bad rows to a file
with open('bad_rows.json', 'w', encoding='utf-8') as bad_file:
    json.dump(bad_rows, bad_file, indent=4)

# Write valid history data
date_creation = datetime.datetime.now().strftime('%Y-%m-%d')
final_name = f'freetube-history-{date_creation}.db'
with open(final_name, 'w', encoding='utf-8') as f:
    for i in newpipe_history:
        f.write(json.dumps(i) + '\n')

# Close the connection
conn.close()

# Remove the extracted database and settings if they exist
if os.path.exists('newpipe.db'):
    os.remove('newpipe.db')
if os.path.exists('newpipe.settings'):
    os.remove('newpipe.settings')
if os.path.exists('preferences.json'):
    os.remove('preferences.json')
