.PHONY: all refresh export serve

all:
	$(MAKE) refresh export serve

refresh:
	python -m restaurants.refresh_restaurants

export:
	python -m restaurants.export_geojson

serve:
	cd frontend && npm run dev
