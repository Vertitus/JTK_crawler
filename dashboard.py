import json
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
import os

st.set_page_config(page_title="JTK Crawler Dashboard", layout="wide")

# Автообновление каждые 10 секунд
st_autorefresh(interval=10 * 1000, key="data_refresh")

# Заголовок
st.title("📊 JTK Crawler Dashboard")

metrics_file = "metrics.json"

if not os.path.exists(metrics_file):
    st.warning(f"Файл метрик '{metrics_file}' не найден.")
    st.stop()

try:
    with open(metrics_file, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
except json.JSONDecodeError:
    st.error("Файл метрик повреждён или невалиден.")
    st.stop()

# Преобразование в DataFrame
data = []
for url, info in raw_data.items():
    data.append({
        "URL": url,
        "Matches": ", ".join([f"{m['match']} ({m['type']})" for m in info["matches"]]),
        "Match Count": len(info["matches"]),
        "Timestamp": info.get("timestamp", None),
        "Depth": info.get("depth", None),
        "Status": info.get("status", None),
    })

df = pd.DataFrame(data)

if df.empty:
    st.info("Данные ещё не собраны.")
    st.stop()

# Таблица с возможностью сортировки
st.subheader("📄 Совпадения")
st.dataframe(df.sort_values("Match Count", ascending=False), use_container_width=True)

# График: количество совпадений по глубине
if "Depth" in df and df["Depth"].notna().any():
    depth_chart = px.histogram(df, x="Depth", y="Match Count", nbins=20, title="Количество совпадений по глубине")
    st.plotly_chart(depth_chart, use_container_width=True)

# График: статус страниц
if "Status" in df and df["Status"].notna().any():
    status_chart = px.pie(df, names="Status", title="Статусы страниц")
    st.plotly_chart(status_chart, use_container_width=True)

# История по времени (если есть timestamp)
if "Timestamp" in df and df["Timestamp"].notna().any():
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    timeline = df.dropna(subset=["Timestamp"])
    timeline = timeline.sort_values("Timestamp")

    if not timeline.empty:
        line_chart = px.line(
            timeline,
            x="Timestamp",
            y="Match Count",
            title="История количества совпадений по времени",
            markers=True
        )
        st.plotly_chart(line_chart, use_container_width=True)
