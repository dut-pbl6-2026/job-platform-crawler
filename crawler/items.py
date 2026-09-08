import scrapy


class JobItem(scrapy.Item):
    """Raw job listing scraped from vieclam.gov.vn (CRAWL-01-01).

    Cleaning/normalization happens in CleaningPipeline (PR2).
    Fields are optional (None when missing) except source_url/title.
    """

    source_url = scrapy.Field()  # canonical URL, dedup key (CRAWL-01-03)
    title = scrapy.Field()
    company = scrapy.Field()
    location = scrapy.Field()
    salary_raw = scrapy.Field()  # original string from page, parsed in PR2
    description = scrapy.Field()
    requirements = scrapy.Field()
    category = scrapy.Field()
