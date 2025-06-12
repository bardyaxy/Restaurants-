"""Application settings for restaurant fetchers."""

from restaurants.fetchers import (
    GooglePlacesFetcher,
    GovCsvFetcher,
    OsmFetcher,
    GpvFetcher,
)

# (fetcher class, enabled)
FETCHERS = [
    (GooglePlacesFetcher, True),
    (GovCsvFetcher, False),
    (OsmFetcher, False),
    (GpvFetcher, False),
]
