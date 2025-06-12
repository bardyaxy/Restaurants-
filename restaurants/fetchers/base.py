class BaseFetcher:
    """Base interface for restaurant data fetchers."""

    def fetch(self, zip_codes: list[str], **opts):
        """Fetch data for the given ZIP codes."""
        raise NotImplementedError
