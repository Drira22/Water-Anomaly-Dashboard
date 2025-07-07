import React, { useEffect, useState, useCallback } from 'react';
import axios from 'axios';
import Plot from 'react-plotly.js';
import useWebSocket from '../hooks/useWebSocket';
import { Select, MenuItem, Box, Typography } from '@mui/material';

const Dashboard = () => {
  const [region, setRegion] = useState('e3');
  const [dmaId, setDmaId] = useState('222');
  const [flowData, setFlowData] = useState([]);
  const [forecastData, setForecastData] = useState([]);

  // ✅ Fetch initial flow + forecast data
  const fetchData = useCallback(async () => {
    try {
      const [flowRes, forecastRes] = await Promise.all([
        axios.get(`http://localhost:8000/flow-data/${region}/${dmaId}`),
        axios.get(`http://localhost:8000/forecast-data/${region}/${dmaId}`)
      ]);

      if (flowRes.data.success && forecastRes.data.success) {
        setFlowData(flowRes.data.data.map(d => ({
          x: d.timestamp,
          y: d.flow
        })));

        setForecastData(forecastRes.data.data.map(d => ({
          x: d.timestamp,
          y: d.flow
        })));
      }
    } catch (err) {
      console.error("❌ Failed to load data:", err);
    }
  }, [region, dmaId]);

  // 🔁 Load on mount and region/DMA change
  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // ✅ Append new point from WebSocket
  const handleNewFlowPoint = useCallback((data) => {
    setFlowData(prev => [
      ...prev.slice(-200),  // Keep last 200 points only
      { x: data.timestamp, y: data.flow }
    ]);
  }, []);

  // 📡 WebSocket hook
  useWebSocket(`/ws/flow/${region}/${dmaId}`, handleNewFlowPoint);

  return (
    <Box sx={{ padding: 4 }}>
      <Typography variant="h4" gutterBottom>💧 Real-Time Flow Dashboard</Typography>

      {/* 🌍 Selectors */}
      <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
        <Select value={region} onChange={(e) => setRegion(e.target.value)}>
          <MenuItem value="e1">E1</MenuItem>
          <MenuItem value="e2">E2</MenuItem>
          <MenuItem value="e3">E3</MenuItem>
          <MenuItem value="e4">E4</MenuItem>
        </Select>

        <Select value={dmaId} onChange={(e) => setDmaId(e.target.value)}>
          <MenuItem value="222">DMA 222</MenuItem>
          {/* Add more if needed */}
        </Select>
      </Box>

      {/* 📈 Plotly Chart */}
      <Plot
        data={[
          {
            x: flowData.map(p => p.x),
            y: flowData.map(p => p.y),
            type: 'scatter',
            mode: 'lines+markers',
            name: 'Flow',
            line: { color: 'blue' }
          },
          {
            x: forecastData.map(p => p.x),
            y: forecastData.map(p => p.y),
            type: 'scatter',
            mode: 'lines',
            name: 'Forecast',
            line: { color: 'orange', dash: 'dash' }
          }
        ]}
        layout={{
          title: `Live Flow vs Forecast (DMA ${dmaId})`,
          xaxis: { title: 'Time' },
          yaxis: { title: 'Flow (L/min)' },
          margin: { t: 50 },
          showlegend: true
        }}
        useResizeHandler
        style={{ width: '100%', height: '500px' }}
      />
    </Box>
  );
};

export default Dashboard;