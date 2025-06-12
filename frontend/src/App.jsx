import { useEffect, useRef, useState } from 'react';
import mapboxgl from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';
import './App.css';
import ZipFilter from './ZipFilter.jsx';

mapboxgl.accessToken = import.meta.env.VITE_MAPBOX_TOKEN;

function App() {
  const mapContainer = useRef(null);
  const mapRef = useRef(null);
  const [loading, setLoading] = useState(true);
  const [noData, setNoData] = useState(false);
  const [features, setFeatures] = useState([]);
  const [zipFilter, setZipFilter] = useState('');

  useEffect(() => {
    async function init() {
      setLoading(true)
      try {
        const res = await fetch('/static/restaurants.geojson')
        const geojson = await res.json()
        const all = geojson.features || []
        if (all.length === 0) {
          setNoData(true)
          setLoading(false)
          return
        }
        setFeatures(all)

        const map = new mapboxgl.Map({
          container: mapContainer.current,
          style: 'mapbox://styles/mapbox/streets-v12',
          center: [-122.9, 47.0379],
          zoom: 10,
        })
        mapRef.current = map

        map.on('load', () => {
          map.addSource('restaurants', {
            type: 'geojson',
            data: geojson,
            cluster: true,
            clusterMaxZoom: 14,
            clusterRadius: 50,
          })

      map.addLayer({
        id: 'clusters',
        type: 'circle',
        source: 'restaurants',
        filter: ['has', 'point_count'],
        paint: {
          'circle-color': '#f28cb1',
          'circle-radius': [
            'step',
            ['get', 'point_count'],
            20,
            100,
            30,
            750,
            40,
          ],
        },
      })

      map.addLayer({
        id: 'cluster-count',
        type: 'symbol',
        source: 'restaurants',
        filter: ['has', 'point_count'],
        layout: {
          'text-field': '{point_count_abbreviated}',
          'text-size': 12,
        },
      })

      map.addLayer({
        id: 'unclustered',
        type: 'circle',
        source: 'restaurants',
        filter: ['!', ['has', 'point_count']],
        paint: {
          'circle-color': '#11b4da',
          'circle-radius': 5,
        },
      })

          function handleSource(e) {
            if (e.sourceId === 'restaurants' && map.isSourceLoaded('restaurants')) {
              setLoading(false)
              map.off('sourcedata', handleSource)
            }
          }
          map.on('sourcedata', handleSource)
        })
      } catch (err) {
        console.error(err)
        setNoData(true)
        setLoading(false)
      }
    }

    init()

    return () => mapRef.current && mapRef.current.remove()
  }, [])

  useEffect(() => {
    if (!mapRef.current || features.length === 0) return
    const filtered = zipFilter
      ? features.filter(f => (f.properties?.zip_code || '').startsWith(zipFilter))
      : features
    const data = { type: 'FeatureCollection', features: filtered }
    mapRef.current.getSource('restaurants').setData(data)
  }, [zipFilter, features])

  if (noData) {
    return <div className="no-data">No data found</div>
  }

  return (
    <>
      <ZipFilter onChange={setZipFilter} />
      {loading && <div className="spinner">Loading...</div>}
      <div ref={mapContainer} className="map-container" />
    </>
  )
}

export default App
