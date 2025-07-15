# Collector

## Struktura projektu
Postupně projedeme všechny složky projektu a prozradíme si více o jejich obsahu pro lepší orientaci.


V kořenovém adresáři projektu se nachází především soubor `docker-compose.yaml`,
kde jsou nadefinované konetjnery pro všechny databáze a server.
Nachází se zde i toto README se základním popisem a jednoduchým návodem.
Dále se zde nachází soubory důležité pro gradle,
aby fungovaly všechny jeho nástroje.
Zároveň je zde soubor `gradle.settings`, který definuje hlavní závislosti aplikace.
A nakonec také soubory důležité pro git, například `.gitignore`.

### databases
Složka obsahuje inicializační skripty pro databáze, které načtou demo data po nastartování konetjenerů.
Na datech se následně demonstruje základní funkčnost programu.
Kontejnery mají do této složky namapovaná svá úložiště, takže zde jsou pak ukládána i vlastní data pro databáze běžící z dockeru

### gradle
Zde se nachazí .jar soubor pro gradle wrapper, pomocí kterého je možné projekt zkompilovat.

### model
Ve složce se nachází zdrojáky pro modul datového modelu aplikace.

### persistor
Zdrojáky pro modul peristoru.

### python_client
Drobný klient psaný v pythonu. Možné použít jako alternativu pro curl při testování aplikace.

### server
Zdrojáky pro modul serveru.
Nachází se zde Front Controller aplikace a zároveň je zde ve složce resources soubor `application.properties`,
kde se konfigurují parametry pro připojení k jednotlivým wrapperům a persistoru.
Je zde `Dockerfile`, který definuje docker image pro aplikaci.

### wrappers
Zdrojáku pro modul wrappers.
Jsou zde naimplmentované všechny wrappery a jejich abstraktní rozhraní.

## Uživatelská dokumentace
Uživatelská dokumentace uvedená zde je značně zjednodušená. Daleko detailněji je vše popsáno v bakalářské práci.

### Instalace
- po naklonování projektu z gitu je nutné projekt postavit a stáhnout všechny potřebné závislosti
  - to se provede spuštěním `./gradlew build`
- následně je třeba projekt spustit pomocí `./gradlew bootRun`
  - server do konzole na posledním řádku vypíše na jakém běží portu (měl by to být port 8080)

### Nastavení persistoru a instancí
- v modulu serveru se nachází ve složce `src/main/java/resources` soubor `application.properties`
- z tohoto souboru se načtou všechny informace, podle nichž se server k jedntlivým instancím a persistoru připojí

#### Nastavení instancí
- pomocí vlastnosti `dbType` se zvolí nad jakou databázi se wrapper inicializuje
  - povolené hodnoty jsou `PostgreSQL`, `MongoDB` a `Neo4j`
- pomocí hodnot `hostName`, `port`, `datsetName`, `userName`, `password` se následně na tuto instanci připojí
  - `datasetName` je název kolekce nebo datové sady případně grafu, se kterým má na dané instanci wrapper pracovat
- `instanceName` je pak uživatelem zvolený identifikátor dané instance, který musí být unikátní pro správné fungování celé aplikace
- příklad:
```
wrappers[1].dbType=PostgreSQL
wrappers[1].instanceName=postgres
wrappers[1].hostName=localhost
wrappers[1].port=5432
wrappers[1].datasetName=josefholubec
wrappers[1].credentials.userName=<USER_NAME>
wrappers[1].credentials.password=<PASSWORD>
```

#### Nastavení peristoru
- zde se používají pouze hodnoty `hostName`, `port`, `datasetName`, `userName` a `password`, které plní stejnou funkci jako u instancí výše
```
persistor.hostName=localhost
persistor.port=27017
persistor.datasetName=queries
persistor.credentials.userName=<USER_NAME>
persistor.credentials.password=<PASSWORD>
```

### REST API
- POST na adresu `<SERVER>/query`
  - v těle POST requestu musí být obsažen json dokument, který má dvě položky `instance` a `query`
    - pole `instance` musí obsahovat identifikátor instance databáze, nad kterou se dotaz z pole `query` spustí
    - hodnoty obou polí se předpokladají, že budou řetězce (Stringy)
    - tento příkaz následně vytvoří novou úlohu (v kódu koncept Execution) s náhodně vygenerovaným identifikátorem, který se pošle klientovi při úspěšném uložení do fronty jako výsledek operace
      - Úloha je pak plánovačem časem spuštěna a dotaz se vyhodnotí nad příslušnou instancí a výsledek se nakonec uloží do persistoru
- GET na adresu `<SERVER>/query/<EXECUTION_ID>/state`
  - tento příkaz získá status příslušné úlohy
  - výsledek může nabývat tří validních hodnot `Waiting` (pokud úloha stále čeká ve frontě), `Running` (pokud úloha byla již spuštěna plánovačem, ale její výsledek stále nebyl uložen), `Processed` (pokud byla úloha vyhodnocena a výsledek byl uložen)
  - případně pokud záznam neexistuje, server vrátí chybu 404 a informaci o neexistenci dané úlohy v systému
- GET na adresu `<SERVER>/query/<EXECUTION_ID>/result`
  - tento příkaz získá výsledek úlohy, pokud její stav už je `Processed`
    - v opačném případě vrátí chybu 404, jelikož záznam nebyl nalezen
  - Výsledkem této operace je buď json obsahující naměřené statistiky dotazu, pokud úloha byla korektně vyhodnocena a uložena
  - Nebo může být výsledkem chybová hláška, pokud při vyhodnocení nastala nějaká chyba
    - vy vyjímečných případech, kdy nejde uložit ani chybová hláška (došlo k chybě při zápisu chyby) se úloha úplně smaže z fronty
- GET na adresu `<SERVER>/instances/list`
  - je příkaz, který vrátí list json dokumentů, kde každý obsahuje pole `instanceName` a `type`, vrátí se tak výčet všech databázových instancí, které je možné použít pro vyhodnocování dotazů
    - `instanceName` má jako hodnotu název instance (defacto její unikátní identifikátor)
    - `type` pak vrací informaci o jakou databázi jde, může nabývat 3 hodnot:
      - `MongoDB` pro instanci databáze MongoDB
      - `Neo4j` pro instanci databáze Neo4j
      - `PostgreSQL` pro instanci databáze PostgreSQL