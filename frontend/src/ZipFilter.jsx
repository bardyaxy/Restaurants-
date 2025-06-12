import { useState } from 'react'

function ZipFilter({ onChange }) {
  const [zip, setZip] = useState('')

  function handleChange(e) {
    const value = e.target.value
    setZip(value)
    onChange(value.trim())
  }

  return (
    <input
      type="text"
      placeholder="Filter by ZIP"
      value={zip}
      onChange={handleChange}
      style={{ position: 'absolute', top: 10, left: 10, zIndex: 1 }}
    />
  )
}

export default ZipFilter
