# Mastodon Data Collector

Ein umfassendes Python-Tool zur Sammlung und Analyse von Mastodon/Fediverse-Daten via API. Entwickelt für Data Analytics Projekte.

## Features

- 📊 **Instanz-Statistiken**: User, Posts, Föderationsdaten
- 📝 **Timeline-Daten**: Öffentliche und lokale Posts mit detaillierten Metriken
- #️⃣ **Hashtag-Analyse**: Sammelt Posts zu beliebigen Hashtags
- 🔥 **Trending Tags**: Aktuelle Trends mit historischen Daten
- 📈 **Export-Formate**: CSV (für sofortige Analyse) und JSON (für Transformationen)
- ⏰ **Cronjob-ready**: Automatische tägliche Datensammlung

## Installation

```bash
# Virtual Environment erstellen
python3 -m venv mastodon-env
source mastodon-env/bin/activate

# Abhängigkeiten installieren
pip install requests
```

## Verwendung

```bash
# Einfache Ausführung
python mastodon_collector.py --instance https://mastodon.social

# Mit eigenen Hashtags
python mastodon_collector.py --instance https://mastodon.social \
  --hashtags python opensource linux

# Mehr Posts pro Quelle
python mastodon_collector.py --instance https://mastodon.social \
  --posts-per-source 500

# Mit Access Token (für lokale Timeline bei geschützten Instanzen)
python mastodon_collector.py --instance https://mastodon.social \
  --token YOUR_ACCESS_TOKEN
```

## Access Token erstellen (optional)

1. Gehe zu: `https://deine-instanz.de/settings/applications/new`
2. App-Name: "Data Collector"
3. Berechtigungen: Nur **"read"** anhaken
4. Token kopieren und mit `--token` verwenden

## Cronjob einrichten

```bash
# Crontab öffnen
crontab -e

# Täglich um 3:00 Uhr
0 3 * * * /pfad/zum/mastodon-env/bin/python /pfad/zum/mastodon_collector.py --instance https://mastodon.social >> /pfad/zu/cronjob.log 2>&1
```

## Gesammelte Daten

Das Tool erstellt automatisch ein `mastodon_data/` Verzeichnis mit:

### CSV-Dateien (für Power BI, Looker Studio, etc.)
- `posts_analysis_*.csv` - Detaillierte Post-Metriken
- `hashtag_analysis_*.csv` - Hashtag-Performance
- `local_posts_*.csv` - Lokale Instanz-Posts
- `trending_tags_*.csv` - Trending Tags
- `instance_stats_*.csv` - Instanz-Statistiken

### JSON-Dateien (für PowerQuery, Transformationen)
- `public_timeline_*.json` - Vollständige öffentliche Timeline
- `hashtag_posts_*.json` - Alle Hashtag-Posts
- `local_timeline_*.json` - Lokale Timeline
- `instance_info_*.json` - Instanz-Informationen
- `collection_summary_*.json` - Sammlung-Zusammenfassung

## Analysemöglichkeiten

- **Zeitreihen**: Posting-Aktivität nach Stunden/Wochentagen
- **Engagement**: Faktoren für Likes/Reblogs/Replies
- **Hashtag-Vergleich**: Performance verschiedener Tags
- **Sprach-Distribution**: Mehrsprachigkeit im Fediverse
- **Lokal vs. Föderal**: Unterschiede in Reichweite und Interaktion
- **Content-Typen**: Text vs. Media vs. Polls

## Technische Details

- **API-Limit**: 40 Posts pro Request (Mastodon-Standard)
- **Pagination**: Automatisch über mehrere Seiten
- **Rate Limiting**: 1 Sekunde Pause zwischen Requests
- **Standardmäßig gesammelt**: ~2.000-3.000 Posts pro Durchlauf

## Lizenz

GPL-3.0

## Autor

Michael Karbacher

## Beitragen

Issues und Pull Requests sind willkommen!

## Haftungsausschluss

Dieses Tool respektiert die Mastodon API-Limits und sammelt nur öffentlich verfügbare Daten. Bitte beachte die Datenschutzrichtlinien deiner Instanz und verwende die Daten verantwortungsvoll.