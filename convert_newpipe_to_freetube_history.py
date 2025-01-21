import sqlite3
import os
import json
import datetime

def process_newpipe():
    """
    1) Unzip the NewPipeData file if found
    2) Read from newpipe.db
    3) Write FreeTube-compatible file (freetube-history-YYYY-MM-DD.db)
    4) Create bad_rows.json for rows that raised exceptions
    """

    try:
        print("modified from the original version on https://github.com/iamtalhaasghar/newpipe-to-freetube-history-sync version de 09/03/2023")
        filename = next(iter([f for f in os.listdir('.') if f.startswith('NewPipeData-')]), None)
        if filename is not None:
            print("Found:", filename)
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

    print('Total rows found in "streams":', len(rows))

    if len(rows) <= 0:
        print('No Data available')

    newpipe_history = []
    bad_rows = []  # List to store info about bad rows

    for row in rows:
        try:
            # Attempt to fetch stream_history and stream_state for this row
            cur.execute(f"SELECT * FROM stream_history WHERE stream_id={row['uid']}")
            history = cur.fetchone()
            cur.execute(f"SELECT * FROM stream_state WHERE stream_id={row['uid']}")
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
                "timeWatched": history['access_date'] if history else 0,
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

    # Write bad rows to a file for later reprocessing
    with open('bad_rows.json', 'w', encoding='utf-8') as bad_file:
        json.dump(bad_rows, bad_file, indent=4)

    # Write valid history data
    date_creation = datetime.datetime.now().strftime('%Y-%m-%d')
    final_name = f'freetube-history-{date_creation}.db'
    with open(final_name, 'w', encoding='utf-8') as f:
        for i in newpipe_history:
            f.write(json.dumps(i) + '\n')

    # Close the connection (but do NOT remove newpipe.db yet—we need it for reprocessing!)
    conn.close()

def reprocess_bad_rows():
    """
    Reprocess rows from bad_rows.json, attempting the same data retrieval 
    from newpipe.db. Outputs a new FreeTube-compatible file 
    named freetube-history-YYYY-MM-DD-bad-requested.db and 
    still_bad_rows.json if any fail again.
    """
    current_dir = os.getcwd()
    bad_rows_path = os.path.join(current_dir, "bad_rows.json")
    newpipe_db_path = os.path.join(current_dir, "newpipe.db")

    # Check for required files
    if not os.path.exists(bad_rows_path):
        print(f"bad_rows.json not found. Nothing to reprocess.")
        return
    if not os.path.exists(newpipe_db_path):
        print(f"newpipe.db not found. Cannot reprocess bad rows.")
        return

    # Read bad_rows.json
    with open(bad_rows_path, "r", encoding="utf-8") as f:
        bad_rows = json.load(f)

    if not isinstance(bad_rows, list) or len(bad_rows) == 0:
        print("No bad rows to process or the file is empty.")
        return

    try:
        conn = sqlite3.connect(newpipe_db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
    except Exception as e:
        print("Failed to connect to newpipe.db:", str(e))
        return

    reprocessed_history = []
    still_bad_rows = []

    for item in bad_rows:
        uid = item.get("uid")
        url = item.get("url")

        # If missing essential info, skip immediately
        if uid is None or url is None:
            still_bad_rows.append({
                "uid": uid,
                "url": url,
                "error": "Missing uid or url in bad_rows.json"
            })
            continue

        try:
            # Re-fetch from 'streams' table
            cur.execute("SELECT * FROM streams WHERE uid=?", (uid,))
            row = cur.fetchone()

            if not row:
                still_bad_rows.append({
                    "uid": uid,
                    "url": url,
                    "error": "No matching row in 'streams' table"
                })
                continue

            # Attempt the same queries
            cur.execute("SELECT * FROM stream_history WHERE stream_id=?", (uid,))
            history = cur.fetchone()
            cur.execute("SELECT * FROM stream_state WHERE stream_id=?", (uid,))
            state = cur.fetchone()

            data = {
                "videoId": row['url'].split('?v=')[-1] if row['url'] else '',
                "title": row['title'] if row['title'] else '',
                "author": row['uploader'] if row['uploader'] else '',
                "authorId": row['uploader_url'].split('channel/')[-1] if row['uploader_url'] else '',
                "published": row['upload_date'] if row['upload_date'] else '',
                "description": "",
                "viewCount": row['view_count'] if row['view_count'] else 0,
                "lengthSeconds": row['duration'] if row['duration'] else 0,
                "watchProgress": (state['progress_time'] // 1000) if (state and 'progress_time' in state.keys()) else 0,
                "timeWatched": history['access_date'] if history else 0,
                "isLive": False,
                "paid": False,
                "type": "video"
            }
            reprocessed_history.append(data)

        except Exception as e:
            still_bad_rows.append({
                "uid": uid,
                "url": url,
                "error": str(e)
            })

    # Close DB connection
    conn.close()

    print("Reprocessed successfully:", len(reprocessed_history))
    print("Still bad rows:", len(still_bad_rows))

    # Write reprocessed results
    if reprocessed_history:
        date_creation = datetime.datetime.now().strftime('%Y-%m-%d')
        out_name = f"freetube-history-{date_creation}-bad-requested.db"
        with open(out_name, "w", encoding="utf-8") as out_file:
            for entry in reprocessed_history:
                out_file.write(json.dumps(entry) + "\n")

    if still_bad_rows:
        with open("still_bad_rows.json", "w", encoding="utf-8") as still_bad_file:
            json.dump(still_bad_rows, still_bad_file, indent=4, ensure_ascii=False)

def main():
    # STEP 1) Process the main DB and generate bad_rows.json
    process_newpipe()

    # STEP 2) Attempt to reprocess those bad_rows immediately
    reprocess_bad_rows()

    # STEP 3) Cleanup
    # Only remove these after both steps are done
    for file_to_remove in ["newpipe.db", "newpipe.settings", "preferences.json", "bad_rows.json"]:
        if os.path.exists(file_to_remove):
            os.remove(file_to_remove)
            print(f"Removed {file_to_remove}.")

if __name__ == "__main__":
    main()

