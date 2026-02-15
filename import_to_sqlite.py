#!/usr/bin/env python3
"""
SQLite Import-Skript für Mastodon Data Collector
Importiert alle CSV/JSON-Dateien in eine strukturierte SQLite-Datenbank
"""

import sqlite3
import csv
import json
import os
from datetime import datetime
from pathlib import Path
import argparse


class MastodonDataImporter:
    def __init__(self, data_dir: str, db_path: str = "mastodon_analysis.db"):
        """
        Initialisiert den Importer

        Args:
            data_dir: Verzeichnis mit den gesammelten Daten
            db_path: Pfad zur SQLite-Datenbank
        """
        self.data_dir = Path(data_dir)
        self.db_path = db_path
        self.conn = None
        self.cursor = None

        # Statistiken
        self.stats = {
            'posts_imported': 0,
            'posts_duplicates': 0,
            'hashtag_posts_imported': 0,
            'trending_tags_imported': 0,
            'instance_stats_imported': 0,
            'files_processed': 0,
            'files_skipped': 0
        }

    def connect(self):
        """Verbindet mit der Datenbank"""
        print(f"📂 Erstelle/Öffne Datenbank: {self.db_path}")
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        print("✓ Verbindung hergestellt")

    def create_tables(self):
        """Erstellt die Datenbank-Tabellen"""
        print("\n🏗️  Erstelle Tabellen...")

        # Haupttabelle: Posts (aus posts_analysis und local_posts)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                post_id TEXT PRIMARY KEY,
                collection_date DATE,
                created_at TIMESTAMP,
                language TEXT,
                visibility TEXT,
                replies_count INTEGER DEFAULT 0,
                reblogs_count INTEGER DEFAULT 0,
                favourites_count INTEGER DEFAULT 0,
                engagement_total INTEGER DEFAULT 0,
                has_media BOOLEAN,
                media_count INTEGER DEFAULT 0,
                has_poll BOOLEAN,
                has_cw BOOLEAN,
                character_count INTEGER DEFAULT 0,
                hashtag_count INTEGER DEFAULT 0,
                mention_count INTEGER DEFAULT 0,
                url_count INTEGER DEFAULT 0,
                is_reply BOOLEAN,
                is_reblog BOOLEAN,
                hour_of_day INTEGER,
                day_of_week TEXT,
                account_id TEXT,
                account_username TEXT,
                account_followers INTEGER DEFAULT 0,
                account_following INTEGER DEFAULT 0,
                account_statuses_count INTEGER DEFAULT 0,
                source TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Tabelle: Hashtag Posts
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS hashtag_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id TEXT,
                collection_date DATE,
                collected_hashtag TEXT,
                created_at TIMESTAMP,
                language TEXT,
                engagement_score INTEGER DEFAULT 0,
                replies_count INTEGER DEFAULT 0,
                reblogs_count INTEGER DEFAULT 0,
                favourites_count INTEGER DEFAULT 0,
                all_hashtags TEXT,
                account_id TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(post_id, collected_hashtag)
            )
        """)

        # Tabelle: Trending Tags
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS trending_tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_date DATE,
                tag_name TEXT,
                url TEXT,
                total_uses INTEGER DEFAULT 0,
                day_1_uses INTEGER DEFAULT 0,
                day_2_uses INTEGER DEFAULT 0,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(collection_date, tag_name)
            )
        """)

        # Tabelle: Instanz-Statistiken
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS instance_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                collection_date DATE,
                user_count INTEGER DEFAULT 0,
                status_count INTEGER DEFAULT 0,
                domain_count INTEGER DEFAULT 0,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(collection_date)
            )
        """)

        # Indizes für bessere Performance
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_language ON posts(language)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_source ON posts(source)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_collection_date ON posts(collection_date)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_hashtag_posts_tag ON hashtag_posts(collected_hashtag)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_hashtag_posts_date ON hashtag_posts(collection_date)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_trending_date ON trending_tags(collection_date)")

        self.conn.commit()
        print("✓ Tabellen erstellt")

    def extract_date_from_filename(self, filename: str) -> str:
        """Extrahiert Datum aus Dateinamen (z.B. posts_20251128_183042.csv -> 2025-11-28)"""
        try:
            # Format: *_YYYYMMDD_HHMMSS.*
            parts = filename.split('_')
            for part in parts:
                if len(part) == 8 and part.isdigit():
                    year = part[:4]
                    month = part[4:6]
                    day = part[6:8]
                    return f"{year}-{month}-{day}"
        except Exception as e:
            print(f"⚠️  Fehler beim Extrahieren des Datums aus {filename}: {e}")
        return None

    def import_posts_analysis(self):
        """Importiert posts_analysis CSV-Dateien"""
        print("\n📊 Importiere posts_analysis Dateien...")

        files = list(self.data_dir.glob("posts_analysis_*.csv"))
        print(f"Gefunden: {len(files)} Dateien")

        for file_path in files:
            collection_date = self.extract_date_from_filename(file_path.name)
            if not collection_date:
                print(f"⚠️  Überspringe {file_path.name} (kein Datum erkennbar)")
                self.stats['files_skipped'] += 1
                continue

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)

                    for row in reader:
                        try:
                            self.cursor.execute("""
                                INSERT OR IGNORE INTO posts (
                                    post_id, collection_date, created_at, language, visibility,
                                    replies_count, reblogs_count, favourites_count,
                                    has_media, media_count, has_poll, has_cw,
                                    character_count, hashtag_count, mention_count, url_count,
                                    is_reply, is_reblog, hour_of_day, day_of_week,
                                    account_id, account_followers, account_following,
                                    account_statuses_count, source
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                row.get('post_id'),
                                collection_date,
                                row.get('created_at'),
                                row.get('language'),
                                row.get('visibility'),
                                int(row.get('replies_count', 0) or 0),
                                int(row.get('reblogs_count', 0) or 0),
                                int(row.get('favourites_count', 0) or 0),
                                row.get('has_media', 'False') == 'True',
                                int(row.get('media_count', 0) or 0),
                                row.get('has_poll', 'False') == 'True',
                                row.get('has_cw', 'False') == 'True',
                                int(row.get('character_count', 0) or 0),
                                int(row.get('hashtag_count', 0) or 0),
                                int(row.get('mention_count', 0) or 0),
                                int(row.get('url_count', 0) or 0),
                                row.get('is_reply', 'False') == 'True',
                                row.get('is_reblog', 'False') == 'True',
                                int(row.get('hour_of_day', 0) or 0),
                                row.get('day_of_week'),
                                row.get('account_id'),
                                int(row.get('account_followers', 0) or 0),
                                int(row.get('account_following', 0) or 0),
                                int(row.get('account_statuses_count', 0) or 0),
                                'public_timeline'
                            ))

                            if self.cursor.rowcount > 0:
                                self.stats['posts_imported'] += 1
                            else:
                                self.stats['posts_duplicates'] += 1

                        except Exception as e:
                            print(f"⚠️  Fehler bei Post: {e}")
                            continue

                self.stats['files_processed'] += 1
                print(f"  ✓ {file_path.name}")

            except Exception as e:
                print(f"⚠️  Fehler bei Datei {file_path.name}: {e}")
                self.stats['files_skipped'] += 1

        self.conn.commit()
        print(
            f"✓ Posts importiert: {self.stats['posts_imported']} (Duplikate übersprungen: {self.stats['posts_duplicates']})")

    def import_local_posts(self):
        """Importiert local_posts CSV-Dateien"""
        print("\n🏠 Importiere local_posts Dateien...")

        files = list(self.data_dir.glob("local_posts_*.csv"))
        print(f"Gefunden: {len(files)} Dateien")

        for file_path in files:
            collection_date = self.extract_date_from_filename(file_path.name)
            if not collection_date:
                print(f"⚠️  Überspringe {file_path.name}")
                self.stats['files_skipped'] += 1
                continue

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)

                    for row in reader:
                        try:
                            # Berechne engagement_total
                            engagement = (int(row.get('replies', 0) or 0) +
                                          int(row.get('reblogs', 0) or 0) +
                                          int(row.get('favourites', 0) or 0))

                            self.cursor.execute("""
                                INSERT OR IGNORE INTO posts (
                                    post_id, collection_date, created_at, account_username,
                                    language, engagement_total, replies_count, reblogs_count,
                                    favourites_count, character_count, hashtag_count,
                                    has_media, visibility, source
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                row.get('post_id'),
                                collection_date,
                                row.get('created_at'),
                                row.get('account_username'),
                                row.get('language'),
                                engagement,
                                int(row.get('replies', 0) or 0),
                                int(row.get('reblogs', 0) or 0),
                                int(row.get('favourites', 0) or 0),
                                int(row.get('char_count', 0) or 0),
                                int(row.get('hashtags', 0) or 0),
                                row.get('has_media', 'False') == 'True',
                                row.get('visibility'),
                                'local_timeline'
                            ))

                            if self.cursor.rowcount > 0:
                                self.stats['posts_imported'] += 1
                            else:
                                self.stats['posts_duplicates'] += 1

                        except Exception as e:
                            print(f"⚠️  Fehler bei Post: {e}")
                            continue

                self.stats['files_processed'] += 1
                print(f"  ✓ {file_path.name}")

            except Exception as e:
                print(f"⚠️  Fehler bei Datei {file_path.name}: {e}")
                self.stats['files_skipped'] += 1

        self.conn.commit()
        print(f"✓ Lokale Posts importiert")

    def import_hashtag_analysis(self):
        """Importiert hashtag_analysis CSV-Dateien"""
        print("\n#️⃣ Importiere hashtag_analysis Dateien...")

        files = list(self.data_dir.glob("hashtag_analysis_*.csv"))
        print(f"Gefunden: {len(files)} Dateien")

        for file_path in files:
            collection_date = self.extract_date_from_filename(file_path.name)
            if not collection_date:
                print(f"⚠️  Überspringe {file_path.name}")
                self.stats['files_skipped'] += 1
                continue

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)

                    for row in reader:
                        try:
                            self.cursor.execute("""
                                INSERT OR IGNORE INTO hashtag_posts (
                                    post_id, collection_date, collected_hashtag, created_at,
                                    language, engagement_score, replies_count, reblogs_count,
                                    favourites_count, all_hashtags, account_id
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                row.get('post_id'),
                                collection_date,
                                row.get('collected_hashtag'),
                                row.get('created_at'),
                                row.get('language'),
                                int(row.get('engagement_score', 0) or 0),
                                int(row.get('replies_count', 0) or 0),
                                int(row.get('reblogs_count', 0) or 0),
                                int(row.get('favourites_count', 0) or 0),
                                row.get('all_hashtags'),
                                row.get('account_id')
                            ))

                            if self.cursor.rowcount > 0:
                                self.stats['hashtag_posts_imported'] += 1

                        except Exception as e:
                            print(f"⚠️  Fehler bei Hashtag-Post: {e}")
                            continue

                self.stats['files_processed'] += 1
                print(f"  ✓ {file_path.name}")

            except Exception as e:
                print(f"⚠️  Fehler bei Datei {file_path.name}: {e}")
                self.stats['files_skipped'] += 1

        self.conn.commit()
        print(f"✓ Hashtag-Posts importiert: {self.stats['hashtag_posts_imported']}")

    def import_trending_tags(self):
        """Importiert trending_tags CSV-Dateien"""
        print("\n🔥 Importiere trending_tags Dateien...")

        files = list(self.data_dir.glob("trending_tags_*.csv"))
        print(f"Gefunden: {len(files)} Dateien")

        for file_path in files:
            collection_date = self.extract_date_from_filename(file_path.name)
            if not collection_date:
                print(f"⚠️  Überspringe {file_path.name}")
                self.stats['files_skipped'] += 1
                continue

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)

                    for row in reader:
                        try:
                            self.cursor.execute("""
                                INSERT OR IGNORE INTO trending_tags (
                                    collection_date, tag_name, url, total_uses,
                                    day_1_uses, day_2_uses
                                ) VALUES (?, ?, ?, ?, ?, ?)
                            """, (
                                collection_date,
                                row.get('tag_name'),
                                row.get('url'),
                                int(row.get('total_uses', 0) or 0),
                                int(row.get('day_1_uses', 0) or 0),
                                int(row.get('day_2_uses', 0) or 0)
                            ))

                            if self.cursor.rowcount > 0:
                                self.stats['trending_tags_imported'] += 1

                        except Exception as e:
                            print(f"⚠️  Fehler bei Trending-Tag: {e}")
                            continue

                self.stats['files_processed'] += 1
                print(f"  ✓ {file_path.name}")

            except Exception as e:
                print(f"⚠️  Fehler bei Datei {file_path.name}: {e}")
                self.stats['files_skipped'] += 1

        self.conn.commit()
        print(f"✓ Trending-Tags importiert: {self.stats['trending_tags_imported']}")

    def import_instance_stats(self):
        """Importiert instance_stats CSV-Dateien"""
        print("\n📈 Importiere instance_stats Dateien...")

        files = list(self.data_dir.glob("instance_stats_*.csv"))
        print(f"Gefunden: {len(files)} Dateien")

        for file_path in files:
            collection_date = self.extract_date_from_filename(file_path.name)
            if not collection_date:
                print(f"⚠️  Überspringe {file_path.name}")
                self.stats['files_skipped'] += 1
                continue

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)

                    for row in reader:
                        try:
                            self.cursor.execute("""
                                INSERT OR IGNORE INTO instance_stats (
                                    timestamp, collection_date, user_count,
                                    status_count, domain_count
                                ) VALUES (?, ?, ?, ?, ?)
                            """, (
                                row.get('timestamp'),
                                collection_date,
                                int(row.get('user_count', 0) or 0),
                                int(row.get('status_count', 0) or 0),
                                int(row.get('domain_count', 0) or 0)
                            ))

                            if self.cursor.rowcount > 0:
                                self.stats['instance_stats_imported'] += 1

                        except Exception as e:
                            print(f"⚠️  Fehler bei Instance-Stats: {e}")
                            continue

                self.stats['files_processed'] += 1
                print(f"  ✓ {file_path.name}")

            except Exception as e:
                print(f"⚠️  Fehler bei Datei {file_path.name}: {e}")
                self.stats['files_skipped'] += 1

        self.conn.commit()
        print(f"✓ Instanz-Statistiken importiert: {self.stats['instance_stats_imported']}")

    def create_views(self):
        """Erstellt hilfreiche Views für die Analyse"""
        print("\n🔍 Erstelle Analyse-Views...")

        # View: Tägliche Statistiken
        self.cursor.execute("""
            CREATE VIEW IF NOT EXISTS daily_stats AS
            SELECT 
                collection_date,
                COUNT(*) as total_posts,
                COUNT(DISTINCT account_id) as unique_accounts,
                AVG(engagement_total) as avg_engagement,
                SUM(replies_count) as total_replies,
                SUM(reblogs_count) as total_reblogs,
                SUM(favourites_count) as total_favourites,
                SUM(CASE WHEN has_media THEN 1 ELSE 0 END) as posts_with_media,
                AVG(character_count) as avg_characters
            FROM posts
            GROUP BY collection_date
            ORDER BY collection_date
        """)

        # View: Hashtag Performance
        self.cursor.execute("""
            CREATE VIEW IF NOT EXISTS hashtag_performance AS
            SELECT 
                collected_hashtag,
                COUNT(*) as post_count,
                AVG(engagement_score) as avg_engagement,
                SUM(engagement_score) as total_engagement,
                COUNT(DISTINCT account_id) as unique_users,
                COUNT(DISTINCT collection_date) as days_active
            FROM hashtag_posts
            GROUP BY collected_hashtag
            ORDER BY total_engagement DESC
        """)

        # View: Hourly Activity
        self.cursor.execute("""
            CREATE VIEW IF NOT EXISTS hourly_activity AS
            SELECT 
                hour_of_day,
                COUNT(*) as post_count,
                AVG(engagement_total) as avg_engagement,
                COUNT(DISTINCT account_id) as unique_accounts
            FROM posts
            WHERE hour_of_day IS NOT NULL
            GROUP BY hour_of_day
            ORDER BY hour_of_day
        """)

        # View: Language Distribution
        self.cursor.execute("""
            CREATE VIEW IF NOT EXISTS language_stats AS
            SELECT 
                language,
                COUNT(*) as post_count,
                AVG(engagement_total) as avg_engagement,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM posts), 2) as percentage
            FROM posts
            WHERE language IS NOT NULL AND language != ''
            GROUP BY language
            ORDER BY post_count DESC
        """)

        self.conn.commit()
        print("✓ Views erstellt")

    def print_summary(self):
        """Gibt eine Zusammenfassung der Datenbank aus"""
        print("\n" + "=" * 60)
        print("📊 IMPORT ABGESCHLOSSEN - ZUSAMMENFASSUNG")
        print("=" * 60)

        print(f"\n📁 Dateien:")
        print(f"  Verarbeitet: {self.stats['files_processed']}")
        print(f"  Übersprungen: {self.stats['files_skipped']}")

        print(f"\n📊 Datenbank-Inhalt:")

        # Posts
        self.cursor.execute("SELECT COUNT(*) FROM posts")
        total_posts = self.cursor.fetchone()[0]
        print(f"  Posts: {total_posts:,}")
        print(f"    Neu importiert: {self.stats['posts_imported']:,}")
        print(f"    Duplikate übersprungen: {self.stats['posts_duplicates']:,}")

        # Hashtag Posts
        self.cursor.execute("SELECT COUNT(*) FROM hashtag_posts")
        total_hashtag_posts = self.cursor.fetchone()[0]
        print(f"  Hashtag-Posts: {total_hashtag_posts:,}")

        # Trending Tags
        self.cursor.execute("SELECT COUNT(*) FROM trending_tags")
        total_trending = self.cursor.fetchone()[0]
        print(f"  Trending-Tags: {total_trending:,}")

        # Instance Stats
        self.cursor.execute("SELECT COUNT(*) FROM instance_stats")
        total_instance = self.cursor.fetchone()[0]
        print(f"  Instanz-Statistiken: {total_instance:,}")

        # Zeitraum
        self.cursor.execute("SELECT MIN(collection_date), MAX(collection_date) FROM posts")
        date_range = self.cursor.fetchone()
        if date_range[0]:
            print(f"\n📅 Zeitraum:")
            print(f"  Von: {date_range[0]}")
            print(f"  Bis: {date_range[1]}")

            # Tage berechnen
            from datetime import datetime
            start = datetime.strptime(date_range[0], '%Y-%m-%d')
            end = datetime.strptime(date_range[1], '%Y-%m-%d')
            days = (end - start).days + 1
            print(f"  Tage: {days}")

        # Top Hashtags
        print(f"\n#️⃣ Top 10 Hashtags:")
        self.cursor.execute("""
            SELECT collected_hashtag, COUNT(*) as count 
            FROM hashtag_posts 
            GROUP BY collected_hashtag 
            ORDER BY count DESC 
            LIMIT 10
        """)
        for i, (tag, count) in enumerate(self.cursor.fetchall(), 1):
            print(f"  {i:2d}. #{tag}: {count:,} Posts")

        # Sprachen
        print(f"\n🌍 Top 5 Sprachen:")
        self.cursor.execute("""
            SELECT language, COUNT(*) as count 
            FROM posts 
            WHERE language IS NOT NULL AND language != ''
            GROUP BY language 
            ORDER BY count DESC 
            LIMIT 5
        """)
        for i, (lang, count) in enumerate(self.cursor.fetchall(), 1):
            print(f"  {i}. {lang}: {count:,} Posts")

        print("\n" + "=" * 60)
        print(f"✅ Datenbank gespeichert: {self.db_path}")
        print("=" * 60)

        print("\n💡 NÄCHSTE SCHRITTE:")
        print("1. Öffne Power BI / Looker Studio")
        print("2. Verbinde mit SQLite-Datenbank:")
        print(f"   {os.path.abspath(self.db_path)}")
        print("3. Nutze die Views für schnelle Analysen:")
        print("   - daily_stats")
        print("   - hashtag_performance")
        print("   - hourly_activity")
        print("   - language_stats")
        print("4. Schreibe SQL-Abfragen für deine Dashboards!")
        print("=" * 60)

    def close(self):
        """Schließt die Datenbankverbindung"""
        if self.conn:
            self.conn.close()
            print("\n✓ Datenbankverbindung geschlossen")

    def run(self):
        """Führt den kompletten Import aus"""
        print("=" * 60)
        print("🐘 MASTODON DATA IMPORTER - SQLite")
        print("=" * 60)

        self.connect()
        self.create_tables()

        self.import_posts_analysis()
        self.import_local_posts()
        self.import_hashtag_analysis()
        self.import_trending_tags()
        self.import_instance_stats()

        self.create_views()
        self.print_summary()

        self.close()


def main():
    parser = argparse.ArgumentParser(
        description='Importiert Mastodon-Daten in SQLite-Datenbank'
    )
    parser.add_argument(
        '--data-dir',
        default='mastodon_data',
        help='Verzeichnis mit den gesammelten Daten (Standard: mastodon_data)'
    )
    parser.add_argument(
        '--db',
        default='mastodon_analysis.db',
        help='Pfad zur SQLite-Datenbank (Standard: mastodon_analysis.db)'
    )

    args = parser.parse_args()

    # Prüfe ob Datenverzeichnis existiert
    if not os.path.exists(args.data_dir):
        print(f"❌ Fehler: Verzeichnis '{args.data_dir}' nicht gefunden!")
        print(f"Bitte führe das Skript aus dem Verzeichnis aus, wo 'mastodon_data/' liegt,")
        print(f"oder gib den korrekten Pfad mit --data-dir an.")
        return

    # Starte Import
    importer = MastodonDataImporter(args.data_dir, args.db)
    importer.run()


if __name__ == "__main__":
    main()