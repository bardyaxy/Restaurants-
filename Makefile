.PHONY: all refresh export

all:
        $(MAKE) refresh export

refresh:
	python -m restaurants.refresh_restaurants

export:
        python -m restaurants.export_geojson
