# YouTube Data Harvesting & Warehousing

A Streamlit-powered ETL pipeline that:

1. **Extracts** channel, video, and comment data from the YouTube Data API
2. **Loads** raw JSON into MongoDB
3. **Transforms & Migrates** the data into a MySQL data warehouse
4. **Serves** an interactive dashboard to ingest new channels and run SQL analytics

---

## 🚀 Features

* Input up to 10+ YouTube channel IDs to fetch:

  * Channel metadata (name, subscribers, video count, thumbnails, etc.)
  * All videos (title, description, duration, view/like/comment counts)
  * Top 10 comments per video
* Store raw API responses in MongoDB for replayability
* Bulk-migrate to MySQL via SQLAlchemy + Pandas
* Built-in queries (via sidebar) for:

  1. Video ⇄ Channel listings
  2. Top 10 most viewed videos
  3. Comment & like statistics
  4. Year-based filters (e.g. videos published in 2022)
  5. Average durations, totals, and more…

---

## 🛠️ Tech Stack

* **Python**
* **Streamlit** for UI
* **Google API Python Client** for YouTube Data API
* **MongoDB** for raw JSON storage
* **MySQL** (via SQLAlchemy + PyMySQL) for warehoused data
* **Pandas** for DataFrame → SQL bulk loads
* **Pillow** for image display in sidebar

---

## 🔧 Installation

1. **Clone this repo**

   ```bash
   git clone https://github.com/your-username/your-repo.git
   cd your-repo
   ```

2. **Create & activate** a virtual environment

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate   # macOS / Linux
   .\.venv\Scripts\Activate.ps1 # Windows PowerShell
   ```

3. **Install dependencies**

   ```bash
   pip install sqlalchemy pymongo google-api-python-client pandas streamlit pillow PyMySQL
   ```

4. **Set up environment variables**
   Create a `.env` (or export in your shell) with:

   ```dotenv
   YT_API_KEY=<YOUR_YOUTUBE_API_KEY>
   MONGO_URI=mongodb://localhost:27017/
   MYSQL_URI=mysql+pymysql://<user>:<password>@localhost:3306/youtubedata
   ```

5. **Configure databases**

   * **MongoDB**: ensure `mongod` is running on `localhost:27017`.
   * **MySQL**: create `youtubedata` database and grant your user privileges.

---

## ⚙️ Configuration

Edit the top of `capstoneproject1.py` (or use `.env`) to point at your:

* **YouTube API key**
* **MongoDB** collection name
* **MySQL** connection string

```python
# Example
API_key = os.getenv("YT_API_KEY")
mongo_client = MongoClient(os.getenv("MONGO_URI"))
sql_engine   = create_engine(os.getenv("MYSQL_URI"))
```

---

## ▶️ Usage

From your project root, run:


* **Sidebar**: enter a channel ID and click **Collect and store data**
* **Migrate to SQL**: once data exists in Mongo, select channel name → click **Migrate to SQL**
* **View Tables**: choose Channels, Videos, or Comments to inspect raw Mongo data
* **SQL Questions**: pick one of the 10 pre-built analytics queries and click **Get the table**

---

## 🗂️ Project Structure

```
.
├── capstoneproject1.py   # main Streamlit + ETL script
├── README.md             # this file
└── requirements.txt      # (optional) pinned dependencies
```

---

## 🤝 Contributing

1. Fork the repo
2. Create a branch: `git checkout -b feature/my-new-feature`
3. Commit: `git commit -m "Add some feature"`
4. Push: `git push origin feature/my-new-feature`
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License. See [LICENSE](./LICENSE) for details.
