import re
import json
import csv

# Путь к лог-файлу
log_file_path = "logs.txt"

# Паттерн для парсинга
pattern = re.compile(
    r"# <p>(?P<timestamp>[\d\-T:\.Z]+) <-> \| \[(?P<vid>.*?)\] (?P<source>\w+): (?P<message>.+)"
)

# Сюда будем складывать разобранные данные
parsed_logs = []

with open(log_file_path, "r", encoding="utf-8") as file:
    for line in file:
        match = pattern.match(line.strip())
        if match:
            parsed_logs.append(match.groupdict())

# Сохраняем как JSON
with open("logs.json", "w", encoding="utf-8") as json_file:
    json.dump(parsed_logs, json_file, indent=4)

# Сохраняем как CSV
with open("logs.csv", "w", newline='', encoding="utf-8") as csv_file:
    fieldnames = ["timestamp", "vid", "source", "message"]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    for row in parsed_logs:
        writer.writerow(row)

print("Готово! Сохранено в logs.json и logs.csv")
