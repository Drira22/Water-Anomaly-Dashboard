import React, { useState, useEffect } from 'react';
import { FormControl, InputLabel, Select, MenuItem, Box, Typography } from '@mui/material';
import axios from 'axios';

const API_BASE_URL = 'http://localhost:8000'; // adjust if needed

function DMASelector({ onSelectionChange }) {
  const [regions, setRegions] = useState([]);
  const [dmas, setDmas] = useState([]);
  const [selectedRegion, setSelectedRegion] = useState('');
  const [selectedDMA, setSelectedDMA] = useState('');

  // Fetch regions on component mount
  useEffect(() => {
    axios.get(`${API_BASE_URL}/regions`)
      .then(res => setRegions(res.data.regions))
      .catch(err => console.error("Failed to load regions", err));
  }, []);

  // Fetch DMAs when a region is selected
  useEffect(() => {
    if (selectedRegion) {
      axios.get(`${API_BASE_URL}/dmas/${selectedRegion}`)
        .then(res => setDmas(res.data.dmas))
        .catch(err => console.error("Failed to load DMAs", err));
    } else {
      setDmas([]);
      setSelectedDMA('');
    }
  }, [selectedRegion]);

  // Notify parent of change
  useEffect(() => {
    if (selectedRegion && selectedDMA) {
      onSelectionChange({ region: selectedRegion, dmaId: selectedDMA });
    }
  }, [selectedRegion, selectedDMA, onSelectionChange]);

  return (
    <Box sx={{ display: 'flex', gap: 3, alignItems: 'center', mb: 4 }}>
      <FormControl sx={{ minWidth: 200 }}>
        <InputLabel>Region</InputLabel>
        <Select
          value={selectedRegion}
          label="Region"
          onChange={(e) => setSelectedRegion(e.target.value)}
        >
          {regions.map(region => (
            <MenuItem key={region} value={region}>{region.toUpperCase()}</MenuItem>
          ))}
        </Select>
      </FormControl>

      <FormControl sx={{ minWidth: 200 }} disabled={!selectedRegion}>
        <InputLabel>DMA</InputLabel>
        <Select
          value={selectedDMA}
          label="DMA"
          onChange={(e) => setSelectedDMA(e.target.value)}
        >
          {dmas.map(dma => (
            <MenuItem key={dma} value={dma}>{dma}</MenuItem>
          ))}
        </Select>
      </FormControl>
    </Box>
  );
}

export default DMASelector;
// This component allows users to select a DMA from a dropdown based on the selected region.    