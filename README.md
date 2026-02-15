# Mastodon Data Collector

Ein umfassendes Python-Tool zur Sammlung und Analyse von Mastodon/Fediverse-Daten via API. Entwickelt für Data Analytics Projekte.

## Features

- 📊 **Instanz-Statistiken**: User, Posts, Föderationsdaten
- 📝 **Timeline-Daten**: Öffentliche und lokale Posts mit detaillierten Metriken
- #️⃣ **Hashtag-Analyse**: Sammelt Posts zu beliebigen Hashtags
- 🔥 **Trending Tags**: Aktuelle Trends mit historischen Daten
- 📈 **Export-Formate**: CSV (für sofortige Analyse) und JSON (für Transformationen)
- 💾 **SQLite-Import**: Strukturierte Datenbank für SQL-Analysen
- 📦 **Kaggle-Export**: Bereite Daten für Kaggle-Upload auf
- ⏰ **Cronjob-ready**: Automatische tägliche Datensammlung

## Installation

```bash
# Virtual Environment erstellen
python3 -m venv mastodon-env
source mastodon-env/bin/activate

# Abhängigkeiten installieren
pip install requests pandas
```

## Workflow

### 1. Daten sammeln

```bash
# Einfache Ausführung
python mastodon_collector.py --instance https://mastodon.social

# Mit eigenen Hashtags
python mastodon_collector.py --instance https://mastodon.social \
  --hashtags python datascience tech

# Mit Access Token (optional)
python mastodon_collector.py --instance https://mastodon.social \
  --token YOUR_ACCESS_TOKEN
```

### 2. In SQLite-Datenbank importieren

```bash
# Importiert alle CSV/JSON-Dateien in strukturierte Datenbank
python import_to_sqlite.py --data-dir mastodon_data

# Ergebnis: mastodon_analysis.db
# - 4 Tabellen (posts, hashtag_posts, trending_tags, instance_stats)
# - 4 Views für schnelle Analysen
# - Automatische Duplikat-Erkennung
```

### 3. Für Kaggle exportieren

```bash
# Erstellt bereinigte CSV-Dateien für Kaggle-Upload
python export_to_kaggle.py --db mastodon_analysis.db

# Ergebnis: kaggle_export/ Verzeichnis
# - 8 CSV-Dateien (Hauptdaten + aggregierte Views)
# - README.md (Kaggle-Beschreibung)
# - data_dictionary.csv (Spalten-Dokumentation)
```

## Cronjob einrichten

```bash
# Crontab öffnen
crontab -e

# Täglich um 3:00 Uhr Daten sammeln
0 3 * * * /pfad/zum/mastodon-env/bin/python /pfad/zum/mastodon_collector.py --instance https://mastodon.social >> /pfad/zu/cronjob.log 2>&1
```

## Gesammelte Daten

### Rohdaten (mastodon_data/)
- **CSV-Dateien**: posts_analysis, hashtag_analysis, local_posts, trending_tags, instance_stats
- **JSON-Dateien**: public_timeline, hashtag_posts, local_timeline, instance_info

### SQLite-Datenbank (mastodon_analysis.db)
- **Tabellen**: posts, hashtag_posts, trending_tags, instance_stats
- **Views**: daily_stats, hashtag_performance, hourly_activity, language_stats

### Kaggle-Export (kaggle_export/)
- 8 bereinigte CSV-Dateien mit vollständiger Dokumentation

## Analysemöglichkeiten

- **Zeitreihen**: Posting-Aktivität nach Stunden/Wochentagen
- **Engagement**: Faktoren für Likes/Reblogs/Replies
- **Hashtag-Vergleich**: Performance verschiedener Tags
- **Event-Analysen**: Before/During/After Vergleiche
- **Sprach-Distribution**: Mehrsprachigkeit im Fediverse
- **Lokal vs. Föderal**: Unterschiede in Reichweite und Interaktion
- **Job-Plattform-Forschung**: fedihire/hiring Hashtag-Analysen

## Tools & Integration

- **Power BI**: SQLite-Datenbank direkt verbinden
- **Tableau / Looker Studio**: CSV-Export nutzen
- **Python/R**: Pandas/dplyr für Analysen
- **SQL**: Strukturierte Abfragen auf SQLite-DB
- **Kaggle**: Dataset-Veröffentlichung für Community

## Access Token erstellen (optional)

1. Gehe zu: `https://deine-instanz.de/settings/applications/new`
2. App-Name: "Data Collector"
3. Berechtigungen: Nur **"read"** anhaken
4. Token kopieren und mit `--token` verwenden

## Technische Details

- **API-Limit**: 40 Posts pro Request (Mastodon-Standard)
- **Pagination**: Automatisch über mehrere Seiten
- **Rate Limiting**: 1 Sekunde Pause zwischen Requests
- **Standardmäßig gesammelt**: ~2.000-3.000 Posts pro Durchlauf
- **Duplikat-Handling**: Automatisch beim SQLite-Import

## Lizenz

GPL-3.0

## Autor

Michael Karbacher

## Beitragen

Issues und Pull Requests sind willkommen!

## Haftungsausschluss

Dieses Tool respektiert die Mastodon API-Limits und sammelt nur öffentlich verfügbare Daten. Bitte beachte die Datenschutzrichtlinien deiner Instanz und verwende die Daten verantwortungsvoll.