import scrapy


class JobItem(scrapy.Item):
    """Raw job listing scraped from vieclam.gov.vn (CRAWL-01-01).

    Cleaning/normalization happens in CleaningPipeline (PR2).
    Fields are optional (None when missing) except source_url/title.
    salary_min/max/currency are set by CleaningPipeline (CRAWL-01-02),
    pg_id is set by PostgresPipeline for ElasticsearchPipeline (CRAWL-01-05).
    """

    source_url = scrapy.Field()  # canonical URL, dedup key (CRAWL-01-03)
    title = scrapy.Field()
    company = scrapy.Field()
    location = scrapy.Field()
    salary_raw = scrapy.Field()  # original string from page, parsed in PR2
    salary_min = scrapy.Field()
    salary_max = scrapy.Field()
    salary_currency = scrapy.Field()
    description = scrapy.Field()
    requirements = scrapy.Field()
    category = scrapy.Field()
    pg_id = scrapy.Field()
