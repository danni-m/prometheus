
## Write Path
### Add vs AddFast
labels and ref are cached by metric (scrap.go), if cache doesn't contains the data or `add` returned error, the
scraper will call Add.
we can use the reference as a caching key if needed. 
Q: is appender can be run in parallel to other appenders, single or multiple scrapers?
### commit & rollback
each line in the scrape output is beeing (fast)added and than if no error occurd scraper will call commit otherwise
it will call rollback.

## Read Path
LabelValues and LabelNames - we don't really have to implement.
We only need to implement `Select` function in `Querier`