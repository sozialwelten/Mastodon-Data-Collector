#!/usr/bin/env python3
"""
Kaggle Export-Skript für Mastodon-Daten
Exportiert bereinigte CSV-Dateien aus der SQLite-Datenbank für Kaggle Upload
"""

import sqlite3
import pandas as pd
import os
from pathlib import Path
import argparse


class KaggleExporter:
    def __init__(self, db_path: str, export_dir: str = "kaggle_export"):
        """
        Initialisiert den Exporter

        Args:
            db_path: Pfad zur SQLite-Datenbank
            export_dir: Zielverzeichnis für CSV-Dateien
        """
        self.db_path = db_path
        self.export_dir = Path(export_dir)
        self.conn = None

        # Erstelle Export-Verzeichnis
        self.export_dir.mkdir(exist_ok=True)

        # Statistiken
        self.stats = {}

    def connect(self):
        """Verbindet mit der Datenbank"""
        print(f"📂 Öffne Datenbank: {self.db_path}")
        self.conn = sqlite3.connect(self.db_path)
        print("✓ Verbindung hergestellt")

    def export_table(self, table_name: str, output_filename: str, query: str = None):
        """
        Exportiert eine Tabelle/Query als CSV

        Args:
            table_name: Name der Tabelle (für Logging)
            output_filename: Dateiname der CSV
            query: Optional SQL-Query, sonst SELECT * FROM table_name
        """
        if query is None:
            query = f"SELECT * FROM {table_name}"

        print(f"\n📊 Exportiere {table_name}...")

        try:
            # Lade Daten
            df = pd.read_sql_query(query, self.conn)

            # Speichere als CSV
            output_path = self.export_dir / output_filename
            df.to_csv(output_path, index=False, encoding='utf-8')

            # Statistiken
            file_size = output_path.stat().st_size / (1024 * 1024)  # MB
            self.stats[table_name] = {
                'rows': len(df),
                'columns': len(df.columns),
                'size_mb': round(file_size, 2),
                'filename': output_filename
            }

            print(f"  ✓ {output_filename}")
            print(f"    Zeilen: {len(df):,}")
            print(f"    Spalten: {len(df.columns)}")
            print(f"    Größe: {file_size:.2f} MB")

        except Exception as e:
            print(f"  ❌ Fehler: {e}")

    def export_all(self):
        """Exportiert alle relevanten Tabellen und Views"""
        print("=" * 60)
        print("📦 KAGGLE EXPORT - MASTODON DATASET")
        print("=" * 60)

        # 1. Haupttabelle: Posts
        self.export_table(
            'posts',
            'mastodon_posts.csv'
        )

        # 2. Hashtag Posts
        self.export_table(
            'hashtag_posts',
            'mastodon_hashtag_posts.csv'
        )

        # 3. Trending Tags
        self.export_table(
            'trending_tags',
            'mastodon_trending_tags.csv'
        )

        # 4. Instance Stats
        self.export_table(
            'instance_stats',
            'mastodon_instance_stats.csv'
        )

        # 5. Daily Stats (View)
        self.export_table(
            'daily_stats',
            'mastodon_daily_stats.csv'
        )

        # 6. Hashtag Performance (View)
        self.export_table(
            'hashtag_performance',
            'mastodon_hashtag_performance.csv'
        )

        # 7. BONUS: Hourly Activity (View)
        self.export_table(
            'hourly_activity',
            'mastodon_hourly_activity.csv'
        )

        # 8. BONUS: Language Stats (View)
        self.export_table(
            'language_stats',
            'mastodon_language_stats.csv'
        )

    def create_readme(self):
        """Erstellt eine README.md für das Dataset"""
        print("\n📝 Erstelle README.md...")

        readme_content = f"""# Mastodon/Fediverse Social Media Dataset

## 📊 Overview
This dataset contains **80 days** of data from Mastodon/Fediverse, a decentralized social network, collected between **November 28, 2024** and **February 15, 2025**. 

It includes **22,930 posts** with detailed engagement metrics, hashtag analysis, and trending topics from the federated social network.

## 🎯 Dataset Highlights
- ✅ **22,930 posts** from public and federated timelines
- ✅ **15,383 hashtag-specific posts** tracking 24 different hashtags
- ✅ **80 days** of continuous data collection
- ✅ Includes **39C3 (Chaos Communication Congress)** event data for event-impact analysis
- ✅ **Multi-language content**: English (70%), German (24%), Japanese, French, Spanish
- ✅ **Real-world engagement metrics**: likes, reblogs, replies
- ✅ **Temporal patterns**: hourly and daily activity analysis

## 📁 Files

### Core Data Files

#### 1. `mastodon_posts.csv` (Main Dataset)
**{self.stats.get('posts', {}).get('rows', 'N/A'):,} rows** × **{self.stats.get('posts', {}).get('columns', 'N/A')} columns** | **{self.stats.get('posts', {}).get('size_mb', 'N/A')} MB**

The main dataset containing all collected posts with comprehensive metrics.

**Key Columns:**
- `post_id`: Unique post identifier
- `created_at`: Timestamp of post creation
- `collection_date`: Date when data was collected
- `language`: Post language (en, de, ja, fr, es, etc.)
- `visibility`: Post visibility (public, unlisted, private)
- `replies_count`, `reblogs_count`, `favourites_count`: Engagement metrics
- `engagement_total`: Calculated total engagement score
- `has_media`, `media_count`: Media attachment information
- `has_poll`, `has_cw`: Content flags
- `character_count`, `hashtag_count`, `mention_count`: Content metrics
- `is_reply`, `is_reblog`: Post type flags
- `hour_of_day`, `day_of_week`: Temporal information
- `account_id`, `account_username`: Author information
- `account_followers`, `account_following`, `account_statuses_count`: Author metrics
- `source`: Data source (public_timeline, local_timeline)

#### 2. `mastodon_hashtag_posts.csv`
**{self.stats.get('hashtag_posts', {}).get('rows', 'N/A'):,} rows** × **{self.stats.get('hashtag_posts', {}).get('columns', 'N/A')} columns** | **{self.stats.get('hashtag_posts', {}).get('size_mb', 'N/A')} MB**

Posts tracked by specific hashtags for detailed hashtag performance analysis.

**Tracked Hashtags:**
- **Tech/Programming:** python, datascience, tech
- **Data Analytics:** powerbi, looker, tableau, analytics, dataviz, dataanalysis
- **Job Market:** fedihire, getfedihired, fedijobs, fedihired, fedihire_de, jobsuche, jobsearch, jobalert, hiring
- **Events:** 39c3, ccc, congress, chaoswest

**Key Columns:**
- `collected_hashtag`: The hashtag used for collection
- `all_hashtags`: All hashtags in the post (pipe-separated)
- `engagement_score`: Weighted engagement metric

#### 3. `mastodon_trending_tags.csv`
**{self.stats.get('trending_tags', {}).get('rows', 'N/A'):,} rows** × **{self.stats.get('trending_tags', {}).get('columns', 'N/A')} columns** | **{self.stats.get('trending_tags', {}).get('size_mb', 'N/A')} MB**

Daily trending hashtags over the collection period.

**Key Columns:**
- `collection_date`: Date of trending data
- `tag_name`: Trending hashtag name
- `total_uses`: Total usage count
- `day_1_uses`, `day_2_uses`: Usage by day for trend analysis

#### 4. `mastodon_instance_stats.csv`
**{self.stats.get('instance_stats', {}).get('rows', 'N/A'):,} rows** × **{self.stats.get('instance_stats', {}).get('columns', 'N/A')} columns** | **{self.stats.get('instance_stats', {}).get('size_mb', 'N/A')} MB**

Daily statistics about the Mastodon instance.

**Key Columns:**
- `user_count`: Total registered users
- `status_count`: Total posts on instance
- `domain_count`: Number of federated domains

### Aggregated Analysis Files

#### 5. `mastodon_daily_stats.csv`
**{self.stats.get('daily_stats', {}).get('rows', 'N/A'):,} rows** × **{self.stats.get('daily_stats', {}).get('columns', 'N/A')} columns** | **{self.stats.get('daily_stats', {}).get('size_mb', 'N/A')} MB**

Pre-calculated daily aggregated metrics for time-series analysis.

**Key Columns:**
- `total_posts`: Posts collected per day
- `unique_accounts`: Unique posters per day
- `avg_engagement`: Average engagement per post
- `total_replies`, `total_reblogs`, `total_favourites`: Daily engagement sums
- `posts_with_media`: Media post count
- `avg_characters`: Average post length

#### 6. `mastodon_hashtag_performance.csv`
**{self.stats.get('hashtag_performance', {}).get('rows', 'N/A'):,} rows** × **{self.stats.get('hashtag_performance', {}).get('columns', 'N/A')} columns** | **{self.stats.get('hashtag_performance', {}).get('size_mb', 'N/A')} MB**

Aggregated performance metrics for each tracked hashtag.

**Key Columns:**
- `post_count`: Total posts per hashtag
- `avg_engagement`: Average engagement score
- `total_engagement`: Sum of all engagement
- `unique_users`: Number of unique posters
- `days_active`: Days the hashtag was used

#### 7. `mastodon_hourly_activity.csv`
**{self.stats.get('hourly_activity', {}).get('rows', 'N/A'):,} rows** × **{self.stats.get('hourly_activity', {}).get('columns', 'N/A')} columns** | **{self.stats.get('hourly_activity', {}).get('size_mb', 'N/A')} MB**

Activity patterns by hour of day (0-23).

**Perfect for:** Heatmaps, time-of-day analysis, optimal posting times

#### 8. `mastodon_language_stats.csv`
**{self.stats.get('language_stats', {}).get('rows', 'N/A'):,} rows** × **{self.stats.get('language_stats', {}).get('columns', 'N/A')} columns** | **{self.stats.get('language_stats', {}).get('size_mb', 'N/A')} MB**

Language distribution with engagement comparison.

## 🎓 Use Cases

### Social Media Analytics
- Engagement factor analysis (what drives likes/reblogs?)
- Content type performance (text vs. media vs. polls)
- Account size impact on reach

### Time-Series Analysis
- Daily/hourly activity patterns
- Weekly cycles (weekday vs. weekend behavior)
- Seasonal trends over 80 days

### Event Impact Studies
- **39C3 Case Study**: Compare activity before/during/after the Chaos Communication Congress (Dec 27-30, 2024)
- Event-driven engagement spikes
- Hashtag lifecycle analysis

### Hashtag Analysis
- Top performing hashtags
- Hashtag co-occurrence networks
- Community-specific hashtags (tech, jobs, events)

### Job Market Research
- Federated job platform effectiveness (fedihire, getfedihired)
- Optimal posting times for job announcements
- Comparison: German vs. English job posts

### Multi-Language Content
- Language distribution in federated networks
- Engagement differences across languages
- Cross-cultural communication patterns

### Platform Research
- Federated vs. local content comparison
- Decentralized social network dynamics
- Alternative to traditional social media platforms

## 📈 Sample Analyses

### Most Active Day
Check `mastodon_daily_stats.csv` for the day with highest `total_posts`

### Top Hashtag
`#python` leads with **6,280 posts**, followed by `#39c3` (**3,041 posts** during event)

### Language Split
- English: **70%** (16,057 posts)
- German: **24%** (5,502 posts)
- Others: **6%** (Japanese, French, Spanish, etc.)

### Peak Posting Hour
Analyze `mastodon_hourly_activity.csv` to find when users are most active

## 🛠️ Data Collection

**Method:** Custom Python collector using Mastodon API  
**Frequency:** Daily automated collection via cronjob  
**Period:** November 28, 2024 - February 15, 2025 (80 days)  
**Privacy:** Public data only (GDPR compliant)  
**Sources:** Public timeline, local timeline, hashtag timelines  

## 📊 Recommended Tools

- **Power BI** / **Tableau** / **Looker Studio**: For dashboards
- **Python** (pandas, matplotlib, seaborn): For analysis
- **R** (ggplot2, dplyr): For statistical analysis
- **SQL**: All tables can be loaded into databases for queries

## 💡 Getting Started

### Quick Analysis in Python
```python
import pandas as pd

# Load main dataset
posts = pd.read_csv('mastodon_posts.csv', parse_dates=['created_at', 'collection_date'])

# Basic stats
print(f"Total posts: {{len(posts):,}}")
print(f"Date range: {{posts['collection_date'].min()}} to {{posts['collection_date'].max()}}")
print(f"Average engagement: {{posts['engagement_total'].mean():.1f}}")

# Top languages
print(posts['language'].value_counts().head())
```

### Event Analysis (39C3)
```python
# Filter for event period
event_posts = posts[
    (posts['collection_date'] >= '2024-12-27') & 
    (posts['collection_date'] <= '2024-12-30')
]

# Compare with baseline
baseline = posts[posts['collection_date'] < '2024-12-27']

print(f"Baseline avg engagement: {{baseline['engagement_total'].mean():.1f}}")
print(f"Event avg engagement: {{event_posts['engagement_total'].mean():.1f}}")
```

### Hashtag Performance
```python
hashtags = pd.read_csv('mastodon_hashtag_performance.csv')
print(hashtags.sort_values('total_engagement', ascending=False).head(10))
```

## 📜 License
**CC BY-SA 4.0** - You are free to share and adapt this dataset with attribution.

## 🙏 Acknowledgments
Data collected from the Fediverse/Mastodon network. Special thanks to the open-source community and all Mastodon instance administrators.

## 📧 Contact
For questions about this dataset, please open a discussion on Kaggle.

---

**Dataset Version:** 1.0  
**Last Updated:** {pd.Timestamp.now().strftime('%Y-%m-%d')}  
**Total Size:** {sum(s.get('size_mb', 0) for s in self.stats.values()):.2f} MB
"""

        readme_path = self.export_dir / "README.md"
        with open(readme_path, 'w', encoding='utf-8') as f:
            f.write(readme_content)

        print(f"  ✓ README.md erstellt")

    def create_data_dictionary(self):
        """Erstellt ein Data Dictionary (Spalten-Beschreibungen)"""
        print("\n📖 Erstelle data_dictionary.csv...")

        # Hole Spalteninformationen aus der Datenbank
        cursor = self.conn.cursor()

        data_dict = []

        # Posts Tabelle
        cursor.execute("PRAGMA table_info(posts)")
        for col in cursor.fetchall():
            data_dict.append({
                'table': 'posts',
                'column': col[1],
                'type': col[2],
                'description': self._get_column_description('posts', col[1])
            })

        # Hashtag Posts
        cursor.execute("PRAGMA table_info(hashtag_posts)")
        for col in cursor.fetchall():
            data_dict.append({
                'table': 'hashtag_posts',
                'column': col[1],
                'type': col[2],
                'description': self._get_column_description('hashtag_posts', col[1])
            })

        # Als CSV speichern
        df = pd.DataFrame(data_dict)
        output_path = self.export_dir / "data_dictionary.csv"
        df.to_csv(output_path, index=False, encoding='utf-8')

        print(f"  ✓ data_dictionary.csv erstellt ({len(df)} Spalten)")

    def _get_column_description(self, table: str, column: str) -> str:
        """Gibt Beschreibung für eine Spalte zurück"""
        descriptions = {
            'posts': {
                'post_id': 'Unique identifier for the post',
                'collection_date': 'Date when this post was collected',
                'created_at': 'Timestamp when post was created on Mastodon',
                'language': 'Language code (ISO 639-1) of the post content',
                'visibility': 'Post visibility setting (public, unlisted, private, direct)',
                'replies_count': 'Number of replies to this post',
                'reblogs_count': 'Number of times this post was reblogged/shared',
                'favourites_count': 'Number of times this post was favorited/liked',
                'engagement_total': 'Total engagement score (sum of replies + reblogs + favorites)',
                'has_media': 'Boolean: Does this post contain media attachments?',
                'media_count': 'Number of media attachments (images, videos, audio)',
                'has_poll': 'Boolean: Does this post contain a poll?',
                'has_cw': 'Boolean: Does this post have a content warning?',
                'character_count': 'Number of characters in post content',
                'hashtag_count': 'Number of hashtags in the post',
                'mention_count': 'Number of user mentions (@username) in the post',
                'url_count': 'Number of URLs in the post',
                'is_reply': 'Boolean: Is this post a reply to another post?',
                'is_reblog': 'Boolean: Is this post a reblog/share of another post?',
                'hour_of_day': 'Hour of day when posted (0-23)',
                'day_of_week': 'Day of week when posted (Monday, Tuesday, etc.)',
                'account_id': 'Unique identifier of the posting account',
                'account_username': 'Username of the posting account',
                'account_followers': 'Number of followers the account has',
                'account_following': 'Number of accounts this account follows',
                'account_statuses_count': 'Total number of posts from this account',
                'source': 'Data source (public_timeline or local_timeline)',
            },
            'hashtag_posts': {
                'post_id': 'Unique identifier for the post',
                'collection_date': 'Date when this post was collected',
                'collected_hashtag': 'The specific hashtag this post was collected for',
                'created_at': 'Timestamp when post was created',
                'language': 'Language code of the post',
                'engagement_score': 'Weighted engagement score (replies×3 + reblogs×2 + favorites)',
                'replies_count': 'Number of replies',
                'reblogs_count': 'Number of reblogs',
                'favourites_count': 'Number of favorites',
                'all_hashtags': 'All hashtags in the post (pipe-separated)',
                'account_id': 'Account identifier',
            }
        }

        return descriptions.get(table, {}).get(column, '')

    def print_summary(self):
        """Gibt Zusammenfassung des Exports aus"""
        print("\n" + "=" * 60)
        print("✅ EXPORT ABGESCHLOSSEN")
        print("=" * 60)

        print(f"\n📁 Export-Verzeichnis: {self.export_dir.absolute()}")
        print(f"\n📊 Exportierte Dateien:")

        total_size = 0
        for name, stats in self.stats.items():
            print(f"\n  {stats['filename']}")
            print(f"    Zeilen: {stats['rows']:,}")
            print(f"    Spalten: {stats['columns']}")
            print(f"    Größe: {stats['size_mb']} MB")
            total_size += stats['size_mb']

        print(f"\n📦 Gesamtgröße: {total_size:.2f} MB")

        print("\n" + "=" * 60)
        print("🚀 NÄCHSTE SCHRITTE")
        print("=" * 60)
        print("\n1. Gehe zu: https://www.kaggle.com/datasets")
        print("2. Klicke 'New Dataset'")
        print("3. Lade alle CSV-Dateien aus dem Export-Verzeichnis hoch")
        print("4. Kopiere den Inhalt von README.md in die Beschreibung")
        print("5. Füge Tags hinzu: social-media, time-series, mastodon, fediverse")
        print("6. Veröffentliche das Dataset!")
        print("\n💡 Tipp: Du kannst die README.md auch als Markdown auf Kaggle einfügen")
        print("=" * 60)

    def close(self):
        """Schließt Datenbankverbindung"""
        if self.conn:
            self.conn.close()

    def run(self):
        """Führt kompletten Export aus"""
        self.connect()
        self.export_all()
        self.create_readme()
        self.create_data_dictionary()
        self.print_summary()
        self.close()


def main():
    parser = argparse.ArgumentParser(
        description='Exportiert Mastodon-Daten für Kaggle Upload'
    )
    parser.add_argument(
        '--db',
        default='mastodon_analysis.db',
        help='Pfad zur SQLite-Datenbank (Standard: mastodon_analysis.db)'
    )
    parser.add_argument(
        '--output',
        default='kaggle_export',
        help='Export-Verzeichnis (Standard: kaggle_export)'
    )

    args = parser.parse_args()

    # Prüfe ob Datenbank existiert
    if not os.path.exists(args.db):
        print(f"❌ Fehler: Datenbank '{args.db}' nicht gefunden!")
        return

    # Starte Export
    exporter = KaggleExporter(args.db, args.output)
    exporter.run()


if __name__ == "__main__":
    main()