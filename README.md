# extract-ghent-data

A small ETL script to extract some public data from the city of Ghent and load it into a local sqlite3 database.

## Introduction

This script was intended to create a compact offline database that could be embedded into a standalone application, so it could be used to perform some basic analytics and charting.

The data is intentionally not flattened to keep the size footprint small, but it can be used as a starting point to achieve that goal. The structure follows a more or less normalized star schema with proper primary keys and indexes.

Future extensions might include configurable options and different output models like csv, parquet, etc.

## Available data

- Stadswijken
- Bevolkingscijfers
- Criminaliteitscijfers  

Future updates might include more data.

## Usage

The data required to build the database is downloaded and cached into a staging folder to avoid excessive downloads. 
The database is not updated in place but completely recreated with each script invocation from the available cache. 

If you require the latest crime data for example, make sure to delete staging/criminaliteitscijfers-per-wijk-per-maand-gent-2023.csv
before executing the script. This behavior might change in future updates. 

```bash
git clone https://github.com/hve/extract-ghent-data.git
cd extract-ghent-data
python extract-ghent-data.py
```
## License

- The script is licensed under the MIT license.
- The source data is available under [Modellicentie Gratis Hergebruik](https://www.vlaanderen.be/digitaal-vlaanderen/onze-oplossingen/open-data/voorwaarden-voor-het-hergebruik-van-overheidsinformatie/modellicentie-gratis-hergebruik).

## External sources

- [Gent Open Data](https://data.stad.gent)
- [Gent in cijfers](https://gent.buurtmonitor.be/)
- [Provincies in cijfers](https://provincies.incijfers.be/)
