import sqlite3
import os, json
import datetime

def reprocess_bad_rows():
    """
    Reprocess rows from bad_rows.json, trying to get the same data 
    as the main script. Outputs a new FreeTube-compatible file 
    named freetube-history-YYYY-MM-DD-bad-requested.db and 
    a still_bad_rows.json if any fail again.
    """
    # Constants / file paths
    current_dir = os.getcwd()
    bad_rows_path = os.path.join(current_dir, "bad_rows.json")
    newpipe_db_path = os.path.join(current_dir, "newpipe.db")

    # Check for required files
    if not os.path.exists(bad_rows_path):
        print(f"Error: {bad_rows_path} not found.")
        return
    if not os.path.exists(newpipe_db_path):
        print(f"Error: {newpipe_db_path} not found.")
        return

    # Read the bad_rows.json
    with open(bad_rows_path, "r", encoding="utf-8") as f:
        bad_rows = json.load(f)

    if not isinstance(bad_rows, list) or len(bad_rows) == 0:
        print("No bad rows to process or file is invalid.")
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
        # Each `item` typically looks like {"uid":..., "url":..., "error":...}
        # We'll try to re-run the same queries as we did in the main script.
        uid = item.get("uid")
        url = item.get("url")

        if uid is None or url is None:
            # If either uid or url is missing, we cannot fully reprocess
            still_bad_rows.append({
                "uid": uid,
                "url": url,
                "error": "Missing uid or url in bad_rows.json"
            })
            continue

        try:
            # Fetch the "streams" data again (or at least the relevant columns)
            # We assume the "streams" table still exists in newpipe.db
            # and that the row with this uid is still there.
            cur.execute("SELECT * FROM streams WHERE uid=?", (uid,))
            row = cur.fetchone()

            # If the row doesn't exist anymore, we cannot reprocess it
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

            # Construct the same dictionary for FreeTube
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
            # If it fails again, add it to still_bad_rows
            still_bad_rows.append({
                "uid": uid,
                "url": url,
                "error": str(e)
            })

    # Close DB connection
    conn.close()

    # Print stats
    print("Reprocessed successfully:", len(reprocessed_history))
    print("Still bad rows:", len(still_bad_rows))

    # Write reprocessed results to a new FreeTube file
    date_creation = datetime.datetime.now().strftime('%Y-%m-%d')
    out_name = f"freetube-history-{date_creation}-bad-requested.db"

    with open(out_name, "w", encoding="utf-8") as out_file:
        for entry in reprocessed_history:
            out_file.write(json.dumps(entry) + "\n")

    if still_bad_rows:
        # Save still bad rows to a new file
        with open("still_bad_rows.json", "w", encoding="utf-8") as still_bad_file:
            json.dump(still_bad_rows, still_bad_file, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    reprocess_bad_rows()

