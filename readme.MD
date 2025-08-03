# 🎧 Spotify Data Analysis Dashboard

A comprehensive Power BI dashboard for analyzing music data pulled from the **Spotify Web API**. This project demonstrates a full-stack data analytics workflow — from real-time data acquisition and transformation to modeling and visualization.

---

## 🚀 Live Dashboard Link  
![alt text](<Screenshot 2025-08-03 192145.png>)

<video controls src="Spotify.mp4" title="Title"></video>
---

## ✨ Features

- 🔄 **Real-Time Data**: Powered by live data pulled directly from the Spotify API via scheduled Python scripts.
- 📊 **Artist & Track Analysis**: Analyze key metrics like followers, top tracks, album releases, and popularity.
- 🟢 **Popularity Tier Breakdown**: Categorizes tracks into popularity bands (e.g., 90–100, 80–89, etc.).
- 🎛️ **Interactive Filters**: Slicers for artist selection, release date, or track popularity.
- 🖤 **Branded Aesthetic**: Clean, dark-themed UI inspired by the Spotify brand palette.
- ⚙️ **Automated Refresh**: Data extraction is automated using **Windows Task Scheduler** for regular updates.

---

## ⚙️ Tech Stack

| Tool        | Purpose                               |
|-------------|----------------------------------------|
| **Python**  | API integration & ETL scripting        |
| **Spotify Web API** | Primary data source             |
| **PostgreSQL** | Data storage & SQL views (optional) |
| **Power BI** | Data modeling & dashboard design      |
| **DAX**     | Custom measures & time-based analysis  |
| **Windows Task Scheduler** | Automate Python scripts |

---

## 📈 Key Metrics & Visuals

- **KPI Cards**: Total Tracks, Artists, and Followers  
- **Area Chart**: Albums Released by Year  
- **Bar Chart**: Top 10 Tracks by Popularity  
- **Donut Chart**: Track Popularity Tier Breakdown  
- **Table**: Artist Key Metrics (Followers, Albums, Tracks, Popularity Stars)

---

## 🛠️ How It Works

### 1. Data Acquisition  
A Python script authenticates with the Spotify Web API and pulls artist, track, and album data.

### 2. ETL Pipeline  
Raw JSON data is transformed using `pandas` into structured tables (CSV or PostgreSQL). Logic includes formatting dates, flattening nested JSON, and selecting key fields.

### 3. SQL Views (Optional)  
Reusable views are created in PostgreSQL to optimize performance and simplify Power BI queries.

### 4. Power BI Data Model  
Data is loaded into Power BI and modeled via relationships across artists, albums, and tracks.

### 5. DAX Measures  
Advanced calculations including:
- Total Followers
- Albums Over Time
- Popularity Tier Banding
- Track Ranking

### 6. Dashboard Design  
The visuals are styled using Spotify’s color scheme, with dynamic filters, slicers, and intuitive layout.

---



## 🚀 Getting Started



### 🔑 Set up API Access

1. **Create a Spotify Developer Account**  
   - Visit [Spotify Developer Portal](https://developer.spotify.com)
   - Log in and create an app to get your **Client ID** and **Client Secret**

2. **Update `config.py` with your credentials**  
   Create a new file named `config.py` in your project directory and add:

```python
# config.py
CLIENT_ID = 'your_client_id_here'
CLIENT_SECRET = 'your_client_secret_here'
```
3. **Run the Python Script**
Run the ETL script to extract and load Spotify data:
[Python Script
](<python and upsert code for getting spotify data.py>)

👉 Optional: Use Windows Task Scheduler to schedule the script and automate daily or weekly data refreshes.

4. **Open the Power BI File**

   Open [Spotify dashboard
](<Spotify Dashboard.pbix>)in Power BI Desktop

   Click Refresh to load the latest data from your database

**🤝 Credits**

   Data Source: Spotify Web API

   Developed by: Muhamed Badhusha

**📄 License**

This project is licensed under the MIT License – see the LICENSE file for details
