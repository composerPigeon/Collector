# Collector

# Links to other monitoring systems
- [Zabbix](https://www.zabbix.com)
- [Prometheus](https://prometheus.io)
- [loggly](https://www.loggly.com)
- [ELK stack](https://www.elastic.co/guide/index.html)
- [Data script presentation about Prometheus and so](https://www.datascript.cz/morning-talks/prometheus-mimir-loki-tempo-parca/?utm_source=46664-WebaMorningTalks&utm_medium=email&utm_term=13922069436&utm_content=Prometheus&utm_campaign=Morning%20Talks%202023%20kveten--20230427)

## Command for starting postgresql@14
- postgres -D /opt/homebrew/var/postgresql@14

## Poznámky
### Objektový návrh
- Rozdělení Wrapperu na Connection, Parser a Saver pro rozdělení funkčnosti jednotlivých častí wrapperu a zároveň specifikování a možnost optimalizace pro multithread běh
- Connection se vytváří pro každou exekuci a analýzu dotazu => Jeden Wrapper pro paralelní spouštění anaklýzy dotazů
  - wrapper může držet data, a objekty důležité pro spojení s databází, které jsou ThreadSafe (Driver Neo4j)
  - Connection naopak pracuje s objekty, které nejsou ThreadSafe
- ResultSet (Postrgres) a Result (Neo4j) není vhodné vracet z metod, jsou totiž mutable a zároveň není bezpečné z nich číst, pokud se uzavře connection
  - Connection bude mít v sobě parser a saver, a bude vracet DataseDataModel a ResultDataModel, které se mergují do modelu
### 