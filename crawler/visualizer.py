import matplotlib.pyplot as plt
import csv
from datetime import datetime

def plot_from_csv(path="stats_timeseries.csv", out_png="stats.png"):
    ts, processed, matches, errors = [], [], [], []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts.append(datetime.fromtimestamp(int(row['ts'])))
            processed.append(int(row['processed']))
            matches.append(int(row['matches']))
            errors.append(int(row['errors']))

    plt.figure(figsize=(12,6))
    plt.plot(ts, processed, label="Processed URLs")
    plt.plot(ts, matches, label="Matches")
    plt.plot(ts, errors, label="Errors")
    plt.xlabel("Time")
    plt.ylabel("Count")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_png)
    print("Saved", out_png)
