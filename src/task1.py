import pandas as pd
import json
import sqlite3
from datetime import datetime

DB = "main.db"
conn = sqlite3.connect(DB)

def normalize_date(d):
    if pd.isna(d): 
        return None
    for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y"]:
        try:
            return datetime.strptime(d, fmt).strftime("%Y-%m-%d")
        except:
            pass
    return None

def load_jobs():
    df = pd.read_csv("../dataset/jobs.csv")
    df["posted_date"] = df["posted_date"].apply(normalize_date)
    df.to_sql("jobs", conn, if_exists="append", index=False)

def load_candidates():
    with open("../dataset/candidates.json") as f:
        data = json.load(f)

    df = pd.json_normalize(data)

    # Convert list-type columns to JSON strings
    for col in df.columns:
        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)

    df.to_sql("candidates", conn, if_exists="append", index=False)


def load_education():
    df = pd.read_csv("../dataset/education.csv")
    df.to_sql("education", conn, if_exists="append", index=False)

def load_applications():
    df = pd.read_csv("../dataset/applications.csv")
    df["apply_date"] = df["apply_date"].apply(normalize_date)
    df.to_sql("applications", conn, if_exists="append", index=False)

def load_workflow():
    rows = []
    with open("../dataset/workflow_events.jsonl") as f:
        for line in f:
            rows.append(json.loads(line))
    df = pd.DataFrame(rows)
    df["event_timestamp"] = df["event_timestamp"].apply(normalize_date)
    df.to_sql("workflow_events", conn, if_exists="append", index=False)

if __name__ == "__main__":
    # idempotency: clear tables
    for tbl in ["jobs","candidates","education","applications","workflow_events"]:
        conn.execute(f"DROP TABLE IF EXISTS {tbl}")
    load_jobs()
    load_candidates()
    load_education()
    load_applications()
    load_workflow()

for tbl in ["jobs","candidates","education","applications","workflow_events"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(tbl, "->", count, "rows")

#SQL ANALYSIS
#How many jobs are currently open?
print("How many jobs are currently open?")
jobcount = conn.execute("SELECT COUNT(*) FROM jobs WHERE status = 'Open'").fetchall()
for r in jobcount:
    print(r)

#Top 5 departments by number of applications
print("Top 5 departments by number of applications")
numapps = conn.execute("""SELECT j.department, COUNT(a.application_id) AS total_apps 
                     FROM applications a JOIN jobs j ON j.job_id = a.job_id 
                     GROUP BY j.department 
                    ORDER BY total_apps DESC LIMIT 5""").fetchall()
for r in numapps:
    print(r)

#Candidates who applied to more than 3 jobs
print("Candidates who applied to more than 3 jobs")
candidates = conn.execute("""
    SELECT c.candidate_id,
        c.first_name,
        c.last_name,
        COUNT(a.job_id) AS apps
    FROM applications a
    JOIN candidates c ON c.candidate_id = a.candidate_id
    GROUP BY c.candidate_id
    HAVING COUNT(a.job_id) > 3;
    """).fetchall()
for r in candidates:
    print(r)

    