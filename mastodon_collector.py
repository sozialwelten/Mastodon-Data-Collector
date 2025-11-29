#!/usr/bin/env python3
"""
Mastodon Data Collector für IHK Data Analyst Kurs
Sammelt umfangreiche Daten via Mastodon API für spätere Analyse
"""

import requests
import json
import csv
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time
import argparse


class MastodonDataCollector:
    def __init__(self, instance_url: str, access_token: Optional[str] = None):
        """
        Initialisiert den Collector

        Args:
            instance_url: URL deiner Mastodon-Instanz (z.B. "https://mastodon.social")
            access_token: Optional - für erweiterte API-Zugriffe
        """
        self.instance_url = instance_url.rstrip('/')
        self.access_token = access_token
        self.headers = {}
        if access_token:
            self.headers['Authorization'] = f'Bearer {access_token}'

        # Erstelle Datenverzeichnis
        self.data_dir = "mastodon_data"
        os.makedirs(self.data_dir, exist_ok=True)

        # Timestamp für diese Sammlung
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Führt API-Request aus mit Error Handling"""
        url = f"{self.instance_url}/api/v1/{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Fehler bei {endpoint}: {e}")
            return None

    def _get_paginated_data(self, endpoint: str, params: Dict = None, max_pages: int = 10) -> List[Dict]:
        """Holt paginierte Daten von der API"""
        all_data = []
        url = f"{self.instance_url}/api/v1/{endpoint}"
        current_params = params.copy() if params else {}

        for page in range(max_pages):
            try:
                response = requests.get(url, headers=self.headers, params=current_params, timeout=30)
                response.raise_for_status()
                data = response.json()

                if not data:
                    break

                all_data.extend(data)

                # Prüfe auf Link-Header für nächste Seite
                if 'Link' in response.headers:
                    links = response.headers['Link']
                    if 'rel="next"' in links:
                        # Extrahiere max_id für nächste Seite
                        for link in links.split(','):
                            if 'rel="next"' in link:
                                next_url = link[link.find('<') + 1:link.find('>')]
                                # Extrahiere max_id aus URL
                                if 'max_id=' in next_url:
                                    max_id = next_url.split('max_id=')[1].split('&')[0]
                                    current_params['max_id'] = max_id
                                break
                    else:
                        break
                else:
                    break

                time.sleep(1)  # Rate limiting

            except requests.exceptions.RequestException as e:
                print(f"Fehler bei Pagination Seite {page}: {e}")
                break

        return all_data

    def collect_instance_info(self):
        """Sammelt Instanz-Informationen"""
        print("📊 Sammle Instanz-Informationen...")
        instance_info = self._make_request("instance")

        if instance_info:
            filename = f"{self.data_dir}/instance_info_{self.timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(instance_info, f, indent=2, ensure_ascii=False)
            print(f"✓ Gespeichert: {filename}")

            # CSV für einfache Analyse
            csv_filename = f"{self.data_dir}/instance_stats_{self.timestamp}.csv"
            stats = instance_info.get('stats', {})
            with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['timestamp', 'user_count', 'status_count', 'domain_count'])
                writer.writerow([
                    self.timestamp,
                    stats.get('user_count', 0),
                    stats.get('status_count', 0),
                    stats.get('domain_count', 0)
                ])
            print(f"✓ Gespeichert: {csv_filename}")

    def collect_public_timeline(self, limit: int = 200):
        """Sammelt öffentliche Timeline-Posts"""
        print(f"📝 Sammle öffentliche Timeline (bis zu {limit} Posts)...")

        posts = self._get_paginated_data(
            "timelines/public",
            params={'limit': 40},  # Max per Request (API-Limit)
            max_pages=(limit // 40) + 2  # +2 für Sicherheit, da manche Requests weniger zurückgeben
        )

        if posts:
            # Vollständige JSON-Daten
            filename = f"{self.data_dir}/public_timeline_{self.timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(posts, f, indent=2, ensure_ascii=False)
            print(f"✓ Gespeichert: {filename} ({len(posts)} Posts)")

            # Strukturierte CSV für Analyse
            csv_filename = f"{self.data_dir}/posts_analysis_{self.timestamp}.csv"
            with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    'post_id', 'created_at', 'language', 'visibility',
                    'replies_count', 'reblogs_count', 'favourites_count',
                    'has_media', 'media_count', 'has_poll', 'has_cw',
                    'character_count', 'hashtag_count', 'mention_count',
                    'url_count', 'is_reply', 'is_reblog', 'hour_of_day',
                    'day_of_week', 'account_id', 'account_followers',
                    'account_following', 'account_statuses_count'
                ])
                writer.writeheader()

                for post in posts:
                    created = datetime.strptime(post['created_at'], '%Y-%m-%dT%H:%M:%S.%fZ')

                    writer.writerow({
                        'post_id': post['id'],
                        'created_at': post['created_at'],
                        'language': post.get('language', ''),
                        'visibility': post.get('visibility', ''),
                        'replies_count': post.get('replies_count', 0),
                        'reblogs_count': post.get('reblogs_count', 0),
                        'favourites_count': post.get('favourites_count', 0),
                        'has_media': len(post.get('media_attachments', [])) > 0,
                        'media_count': len(post.get('media_attachments', [])),
                        'has_poll': post.get('poll') is not None,
                        'has_cw': bool(post.get('spoiler_text', '')),
                        'character_count': len(post.get('content', '')),
                        'hashtag_count': len(post.get('tags', [])),
                        'mention_count': len(post.get('mentions', [])),
                        'url_count': len(post.get('card', {}).get('url', [])) if post.get('card') else 0,
                        'is_reply': post.get('in_reply_to_id') is not None,
                        'is_reblog': post.get('reblog') is not None,
                        'hour_of_day': created.hour,
                        'day_of_week': created.strftime('%A'),
                        'account_id': post['account']['id'],
                        'account_followers': post['account'].get('followers_count', 0),
                        'account_following': post['account'].get('following_count', 0),
                        'account_statuses_count': post['account'].get('statuses_count', 0)
                    })

            print(f"✓ Gespeichert: {csv_filename}")

    def collect_trending_tags(self):
        """Sammelt trending Hashtags"""
        print("🔥 Sammle Trending Hashtags...")

        trends = self._make_request("trends/tags", params={'limit': 40})

        if trends:
            filename = f"{self.data_dir}/trending_tags_{self.timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(trends, f, indent=2, ensure_ascii=False)
            print(f"✓ Gespeichert: {filename}")

            # CSV für Analyse
            csv_filename = f"{self.data_dir}/trending_tags_{self.timestamp}.csv"
            with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    'tag_name', 'url', 'total_uses', 'day_1_uses', 'day_2_uses'
                ])
                writer.writeheader()

                for tag in trends:
                    history = tag.get('history', [])
                    writer.writerow({
                        'tag_name': tag['name'],
                        'url': tag.get('url', ''),
                        'total_uses': sum(int(h.get('uses', 0)) for h in history),
                        'day_1_uses': int(history[0].get('uses', 0)) if len(history) > 0 else 0,
                        'day_2_uses': int(history[1].get('uses', 0)) if len(history) > 1 else 0
                    })

            print(f"✓ Gespeichert: {csv_filename}")

    def collect_hashtag_timeline(self, hashtags: List[str], posts_per_tag: int = 100):
        """Sammelt Posts zu bestimmten Hashtags"""
        print(f"#️⃣ Sammle Posts zu Hashtags: {', '.join(hashtags)}")

        all_hashtag_data = []

        for hashtag in hashtags:
            print(f"  Sammle #{hashtag}...")
            posts = self._get_paginated_data(
                f"timelines/tag/{hashtag}",
                params={'limit': 40},
                max_pages=(posts_per_tag // 40) + 2  # +2 für Sicherheit
            )

            for post in posts:
                post['_collected_for_hashtag'] = hashtag
                all_hashtag_data.append(post)

            print(f"  ✓ {len(posts)} Posts gefunden")
            time.sleep(2)  # Rate limiting

        if all_hashtag_data:
            # JSON speichern
            filename = f"{self.data_dir}/hashtag_posts_{self.timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(all_hashtag_data, f, indent=2, ensure_ascii=False)
            print(f"✓ Gespeichert: {filename} ({len(all_hashtag_data)} Posts)")

            # CSV für Analyse
            csv_filename = f"{self.data_dir}/hashtag_analysis_{self.timestamp}.csv"
            with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    'collected_hashtag', 'post_id', 'created_at', 'language',
                    'engagement_score', 'replies_count', 'reblogs_count',
                    'favourites_count', 'all_hashtags', 'account_id'
                ])
                writer.writeheader()

                for post in all_hashtag_data:
                    engagement = (post.get('replies_count', 0) +
                                  post.get('reblogs_count', 0) * 2 +
                                  post.get('favourites_count', 0))

                    writer.writerow({
                        'collected_hashtag': post.get('_collected_for_hashtag', ''),
                        'post_id': post['id'],
                        'created_at': post['created_at'],
                        'language': post.get('language', ''),
                        'engagement_score': engagement,
                        'replies_count': post.get('replies_count', 0),
                        'reblogs_count': post.get('reblogs_count', 0),
                        'favourites_count': post.get('favourites_count', 0),
                        'all_hashtags': '|'.join([tag['name'] for tag in post.get('tags', [])]),
                        'account_id': post['account']['id']
                    })

            print(f"✓ Gespeichert: {csv_filename}")

    def collect_local_timeline(self, limit: int = 200):
        """Sammelt lokale Timeline (nur deine Instanz)"""
        print(f"🏠 Sammle lokale Timeline (bis zu {limit} Posts)...")

        posts = self._get_paginated_data(
            "timelines/public",
            params={'limit': 40, 'local': 'true'},
            max_pages=(limit // 40) + 2  # +2 für Sicherheit
        )

        if posts:
            filename = f"{self.data_dir}/local_timeline_{self.timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(posts, f, indent=2, ensure_ascii=False)
            print(f"✓ Gespeichert: {filename} ({len(posts)} Posts)")

            # Detaillierte CSV
            csv_filename = f"{self.data_dir}/local_posts_{self.timestamp}.csv"
            with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    'post_id', 'created_at', 'account_username', 'language',
                    'engagement_total', 'replies', 'reblogs', 'favourites',
                    'char_count', 'hashtags', 'has_media', 'visibility'
                ])
                writer.writeheader()

                for post in posts:
                    writer.writerow({
                        'post_id': post['id'],
                        'created_at': post['created_at'],
                        'account_username': post['account']['username'],
                        'language': post.get('language', ''),
                        'engagement_total': (post.get('replies_count', 0) +
                                             post.get('reblogs_count', 0) +
                                             post.get('favourites_count', 0)),
                        'replies': post.get('replies_count', 0),
                        'reblogs': post.get('reblogs_count', 0),
                        'favourites': post.get('favourites_count', 0),
                        'char_count': len(post.get('content', '')),
                        'hashtags': len(post.get('tags', [])),
                        'has_media': len(post.get('media_attachments', [])) > 0,
                        'visibility': post.get('visibility', '')
                    })

            print(f"✓ Gespeichert: {csv_filename}")

    def generate_summary_report(self):
        """Erstellt einen Zusammenfassungs-Report"""
        print("📋 Erstelle Zusammenfassung...")

        report = {
            'collection_timestamp': self.timestamp,
            'collection_date': datetime.now().isoformat(),
            'instance_url': self.instance_url,
            'data_directory': self.data_dir,
            'files_created': os.listdir(self.data_dir)
        }

        filename = f"{self.data_dir}/collection_summary_{self.timestamp}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        print(f"✓ Zusammenfassung: {filename}")
        print(f"\n✅ Datensammlung abgeschlossen!")
        print(f"📁 Alle Dateien in: {self.data_dir}/")
        print(f"📊 {len(report['files_created'])} Dateien erstellt")


def main():
    parser = argparse.ArgumentParser(
        description='Mastodon Data Collector für IHK Data Analyst Kurs',
        epilog="""
TOKEN ERSTELLEN (falls benötigt):
1. Gehe zu: https://deine-instanz.de/settings/applications/new
2. Name: "IHK Data Collector"
3. Scopes: Wähle nur "read"
4. Submit
5. Kopiere den "Access token" und verwende ihn mit --token
        """
    )
    parser.add_argument(
        '--instance',
        required=True,
        help='URL deiner Mastodon-Instanz (z.B. https://mastodon.social)'
    )
    parser.add_argument(
        '--token',
        help='Access Token für lokale Timeline (optional, siehe --help)'
    )
    parser.add_argument(
        '--hashtags',
        nargs='+',
        default=[
            'python', 'datascience', 'tech',
            'powerbi', 'looker', 'tableau', 'analytics', 'dataviz', 'dataanalysis',
            'fedihire', 'getfedihired', 'fedijobs', 'fedihired', 'fedihire_de',
            'jobsuche', 'jobsearch', 'jobalert', 'hiring',
            '39C3', '39c3', 'ccc', 'congress', 'chaoswest'
        ],
        help='Hashtags zum Sammeln (Standard: Data Analytics + Job-Hashtags + CCC)'
    )
    parser.add_argument(
        '--posts-per-source',
        type=int,
        default=200,
        help='Anzahl Posts pro Quelle (Standard: 200)'
    )

    args = parser.parse_args()

    print("=" * 60)
    print("🐘 MASTODON DATA COLLECTOR")
    print("=" * 60)
    print(f"Instanz: {args.instance}")
    print(f"Hashtags: {', '.join(args.hashtags)}")
    print(f"Posts pro Quelle: {args.posts_per_source}")
    print("=" * 60)
    print()

    collector = MastodonDataCollector(args.instance, args.token)

    # Sammle verschiedene Datentypen
    collector.collect_instance_info()
    collector.collect_trending_tags()
    collector.collect_public_timeline(limit=args.posts_per_source)
    collector.collect_local_timeline(limit=args.posts_per_source)
    collector.collect_hashtag_timeline(args.hashtags, posts_per_tag=100)

    collector.generate_summary_report()

    print("\n" + "=" * 60)
    print("💡 NÄCHSTE SCHRITTE:")
    print("=" * 60)
    print("1. Importiere die CSV-Dateien in Power BI / Looker Studio")
    print("2. Nutze die JSON-Dateien für PowerQuery Transformationen")
    print("3. Erstelle Zeitreihen-Analysen aus den Timestamps")
    print("4. Vergleiche lokale vs. föderierte Daten")
    print("5. Analysiere Engagement-Metriken und Hashtag-Performance")
    print("=" * 60)


if __name__ == "__main__":
    main()