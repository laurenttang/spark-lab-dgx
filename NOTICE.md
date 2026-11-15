# Data sources and licenses

The code in this repository is a re-implementation, on Spark 4.1, of the exercises from a
2020 Apache Spark course (Spark 2.4 / Hadoop / Hive / Kafka). The original course data is
**not** included. Each dataset is either committed (when its license allows redistribution),
downloaded by `tools/prepare_data.py`, or generated synthetically.

## Committed to this repository

| File | Source | License / terms |
|---|---|---|
| `data/spark101/wordcount/data/war_and_peace.txt` | Leo Tolstoy, *War and Peace*, [Project Gutenberg #2600](https://www.gutenberg.org/ebooks/2600) | Public domain in the USA; Project Gutenberg License header kept in the file |
| `data/spark101/avg_temperature/data/200701hourly_small.txt` | NOAA Quality Controlled Local Climatological Data, January 2007 (sample) | U.S. Government work, public domain |
| `data/spark_sql_101/crime/data/london_crime_by_lsoa.csv.gz` | Metropolitan Police Service recorded crime by LSOA 2008–2016, [London Datastore](https://data.london.gov.uk/) | Contains public sector information licensed under the [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) |
| `data/hive101/join/data/{sales,things}.txt` | Sample data from Tom White, *Hadoop: The Definitive Guide* ([tomwhite/hadoop-book](https://github.com/tomwhite/hadoop-book)) | Apache License 2.0 |

## Downloaded by `tools/prepare_data.py` (not redistributed here)

| Dataset | Source | License / terms |
|---|---|---|
| MovieLens 100K, MovieLens Latest Small | [GroupLens Research](https://grouplens.org/datasets/movielens/) | GroupLens usage license (research/education; no redistribution) |
| Sentiment140 | [Go, Bhayani & Huang, Stanford](http://help.sentiment140.com/) | Academic use |
| SMS Spam Collection | [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/228/sms+spam+collection), Almeida & Hidalgo | CC BY 4.0 |
| Titanic passenger data | Kaggle "Titanic" training set, via [datasciencedojo/datasets](https://github.com/datasciencedojo/datasets) | Public educational dataset |
| DB-IP City Lite (MMDB) | [IP Geolocation by DB-IP](https://db-ip.com) | CC BY 4.0 |
| Taiwan county boundaries (twCounty2010) | Taiwan government open data, via [g0v/twgeojson](https://github.com/g0v/twgeojson) | See the g0v/twgeojson repository |

The course used MaxMind GeoLite2, whose license does not allow redistribution; DB-IP City Lite
uses the same MMDB format, so the code is unchanged apart from the file path.

## Generated synthetically by `tools/prepare_data.py`

These stand in for course data that cannot be published. Generation is seeded, so every clone
produces identical files.

| Dataset | How it is generated |
|---|---|
| E-commerce web logs (`spark_mllib_101/web_logs_analysis/data/`, ~1.28 M rows, 1,000 devices, May 2019) | Same 15-column schema as the course data. Distributions (hour-of-day and weekday traffic, product-category blocks and device / connection type by age group, age × gender device counts, city shares) follow **aggregate statistics** of the course data; no original rows are used. Device locations are sampled inside Taiwan county polygons, with home / work / trip places, shoppers in Taipei's East District, and airport visits, so the final project's quadkey lookup and KMeans have structure to find. |
| Apache access log (`spark101/access_log_analysis/data/access_log.txt`, 76 k lines) | Random public IPv4 visitors with a long-tail frequency, blog-style paths, status codes and user agents |
| Hive `page_views` logs and `stocks.txt` | Random page views (users 1–100) and made-up yearly stock prices for six tickers |

Because the e-commerce logs are synthetic, results such as model accuracy and the dashboard counts
differ from the original course's numbers.
