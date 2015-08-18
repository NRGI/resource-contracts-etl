#Scraper for Open Oil contract wiki repository

##installation
*Clone main repository and navigate to folder*

```git clone https://github.com/NRGI/resource-contracts-etl.git```

```cd resource-contracts-etl/OO_SCRAPER```

*Install Dependancies (may require sudo)*

```pip install -r requirements.txt```

*run scraper*

```python scraper.py [-o | --output] [-s | --source]```

If flags arent set, scraper will point towards default [OO wiki page](http://repository.openoil.net/wiki/Downloads). Data will write to `./resource-contracts-etl/OO_SCRAPER/`.