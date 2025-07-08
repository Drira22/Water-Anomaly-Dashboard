"use client"

import { useEffect, useState, useCallback } from "react"
import axios from "axios"
import Plot from "react-plotly.js"
import useWebSocket from "../hooks/useWebSocket"
import { Select, MenuItem, Box, Typography, Card, CardContent, Chip, Grid, Alert, Button } from "@mui/material"

const EnhancedDashboard = () => {
  const [region, setRegion] = useState("e3")
  const [dmaId, setDmaId] = useState("222")

  // Data states
  const [historicalData, setHistoricalData] = useState([])
  const [forecastData, setForecastData] = useState([])
  const [realTimeData, setRealTimeData] = useState([])
  const [forecastStartTime, setForecastStartTime] = useState(null)

  // UI states
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [dataStats, setDataStats] = useState({
    historicalCount: 0,
    forecastCount: 0,
    realTimeCount: 0,
    daysSinceStart: 0,
  })

  // Helper functions for date manipulation
  const subtractDays = (date, days) => {
    const result = new Date(date)
    result.setDate(result.getDate() - days)
    return result
  }

  const parseISOString = (dateString) => {
    return new Date(dateString)
  }

  // Calculate days since the very first data point in the system
  const calculateDaysSinceStart = useCallback(async () => {
    try {
      // Get the very first data point to calculate actual days since start
      const response = await axios.get(`http://localhost:8000/flow-data/${region}/${dmaId}`, {
        params: { limit: 1 },
      })

      if (response.data.success && response.data.data.length > 0) {
        const firstTimestamp = response.data.data[response.data.data.length - 1].timestamp // Last item is oldest due to DESC order
        const firstDate = parseISOString(firstTimestamp)
        const daysSince = Math.floor((new Date() - firstDate) / (1000 * 60 * 60 * 24))

        setDataStats((prev) => ({
          ...prev,
          daysSinceStart: daysSince,
        }))

        return daysSince
      }
    } catch (err) {
      console.error("❌ Failed to calculate days since start:", err)
    }
    return 0
  }, [region, dmaId])

  // Fetch exactly 7 days (672 points) of historical data - sliding window
  const fetchHistoricalData = useCallback(async () => {
    try {
      console.log("🔍 Fetching last 7 days (672 points) of historical data...")

      // Get last 672 points (7 days * 96 points per day) excluding today
      const response = await axios.get(`http://localhost:8000/flow-data/${region}/${dmaId}`, {
        params: { limit: 800 }, // Get a bit more to ensure we have enough after filtering
      })

      if (response.data.success && response.data.data.length > 0) {
        const allData = response.data.data
          .map((d) => ({
            x: d.timestamp,
            y: d.flow,
            timestamp: parseISOString(d.timestamp),
          }))
          .sort((a, b) => a.timestamp - b.timestamp) // Sort chronologically

        // Filter out today's data from historical (that should be live)
        const today = new Date()
        today.setHours(0, 0, 0, 0) // Start of today
        const historicalFiltered = allData.filter((point) => point.timestamp < today)

        // Keep exactly last 672 points (7 days)
        const historical = historicalFiltered.slice(-672)

        console.log(`✅ Loaded ${historical.length} historical points (last 7 days)`)
        setHistoricalData(historical)

        setDataStats((prev) => ({
          ...prev,
          historicalCount: historical.length,
        }))
      }
    } catch (err) {
      console.error("❌ Failed to load historical data:", err)
      setError("Failed to load historical data")
    }
  }, [region, dmaId])

  // Fetch forecast data with better error handling
  const fetchForecastData = useCallback(async () => {
    try {
      console.log("🔮 Fetching forecast data...")

      // First get available forecast dates
      const datesResponse = await axios.get(`http://localhost:8000/forecast-dates/${region}/${dmaId}`)

      if (datesResponse.data.success && datesResponse.data.dates.length > 0) {
        console.log("📅 Available forecast dates:", datesResponse.data.dates)

        // Get the latest forecast
        const latestDate = datesResponse.data.dates[0]
        const forecastResponse = await axios.get(`http://localhost:8000/forecast-data/${region}/${dmaId}`, {
          params: { forecast_date: latestDate },
        })

        if (forecastResponse.data.success && forecastResponse.data.data.length > 0) {
          const forecast = forecastResponse.data.data
            .map((d) => ({
              x: d.timestamp,
              y: d.flow,
              timestamp: parseISOString(d.timestamp),
            }))
            .sort((a, b) => a.timestamp - b.timestamp)

          console.log(`✅ Loaded ${forecast.length} forecast points for ${latestDate}`)
          setForecastData(forecast)
          setForecastStartTime(forecast[0]?.timestamp)

          setDataStats((prev) => ({
            ...prev,
            forecastCount: forecast.length,
          }))
        }
      } else {
        console.log("⚠️ No forecast data available yet")
        setForecastData([])
        setDataStats((prev) => ({
          ...prev,
          forecastCount: 0,
        }))
      }
    } catch (err) {
      console.error("❌ Failed to load forecast data:", err)
      setForecastData([])
    }
  }, [region, dmaId])

  // Sliding window logic - move yesterday's live data to historical AND refresh from DB
  const handleDayTransition = useCallback(() => {
    console.log("🌅 Day transition detected - implementing sliding window...")

    // Move yesterday's real-time data to historical data (immediate UI update)
    setHistoricalData((prevHistorical) => {
      setRealTimeData((prevRealTime) => {
        if (prevRealTime.length > 0) {
          // Add yesterday's live data to historical
          const combinedData = [...prevHistorical, ...prevRealTime]

          // Keep only last 672 points (7 days * 96 points)
          const slidingWindow = combinedData.slice(-672)

          console.log(
            `📊 Sliding window: ${prevHistorical.length} + ${prevRealTime.length} = ${combinedData.length} -> ${slidingWindow.length} points`,
          )

          // Update historical data stats
          setDataStats((prev) => ({
            ...prev,
            historicalCount: slidingWindow.length,
            realTimeCount: 0, // Reset live data count
          }))

          // Clear real-time data for new day
          return []
        }
        return prevRealTime
      })

      // Return updated historical data
      return prevHistorical
    })

    // 🚀 CRITICAL FIX: Refresh historical data from database after a short delay
    // This ensures we get the complete data including what was just written to DB
    setTimeout(() => {
      console.log("🔄 Refreshing historical data from database after day transition...")
      fetchHistoricalData()
    }, 2000) // Wait 2 seconds for DB writes to complete

    // Automatically fetch new forecast
    console.log("🔮 Fetching new forecast for new day...")
    setTimeout(() => {
      fetchForecastData()
    }, 3000) // Wait 3 seconds for forecast to be generated
  }, [fetchHistoricalData, fetchForecastData])

  // Load initial data
  useEffect(() => {
    const loadData = async () => {
      setLoading(true)
      setError(null)

      await Promise.all([fetchHistoricalData(), fetchForecastData(), calculateDaysSinceStart()])

      setLoading(false)
    }

    loadData()
  }, [fetchHistoricalData, fetchForecastData, calculateDaysSinceStart])

  // Handle real-time WebSocket data (both flow and forecast)
  const handleWebSocketMessage = useCallback(
    (data) => {
      console.log("📡 WebSocket message received:", data)

      if (data.type === "flow_data") {
        // Handle regular flow data
        const newPoint = {
          x: data.timestamp,
          y: data.flow,
          timestamp: parseISOString(data.timestamp),
        }

        // Check if it's a new day (midnight) - trigger sliding window
        const timestamp = parseISOString(data.timestamp)
        if (timestamp.getHours() === 0 && timestamp.getMinutes() === 0) {
          console.log("🌅 Midnight detected - triggering sliding window...")
          handleDayTransition()
        }

        setRealTimeData((prev) => {
          const updated = [...prev, newPoint]
          setDataStats((prevStats) => ({
            ...prevStats,
            realTimeCount: updated.length,
          }))
          return updated
        })
      } else if (data.type === "forecast_data") {
        // Handle forecast data - automatic update
        console.log("🔮 New forecast data received via WebSocket!")

        const forecast = data.forecast_data
          .map((d) => ({
            x: d.timestamp,
            y: d.flow,
            timestamp: parseISOString(d.timestamp),
          }))
          .sort((a, b) => a.timestamp - b.timestamp)

        setForecastData(forecast)
        setForecastStartTime(forecast[0]?.timestamp)

        setDataStats((prev) => ({
          ...prev,
          forecastCount: forecast.length,
        }))

        console.log(`✅ Automatically updated forecast with ${forecast.length} points`)
      }
    },
    [handleDayTransition],
  )

  // WebSocket connection
  useWebSocket(`ws://localhost:8000/ws/flow/${region}/${dmaId}`, handleWebSocketMessage)

  // Manual refresh function (for debugging only)
  const handleRefresh = async () => {
    setLoading(true)
    await Promise.all([fetchHistoricalData(), fetchForecastData(), calculateDaysSinceStart()])
    setLoading(false)
  }

  // Manual day transition for testing
  const handleManualDayTransition = () => {
    console.log("🧪 Manual day transition triggered for testing...")
    handleDayTransition()
  }

  // Prepare plot data
  const plotData = []

  // Historical data (blue solid line) - exactly 7 days (672 points)
  if (historicalData.length > 0) {
    plotData.push({
      x: historicalData.map((p) => p.x),
      y: historicalData.map((p) => p.y),
      type: "scatter",
      mode: "lines",
      name: `Past Flow (${Math.ceil(historicalData.length / 96)} days)`,
      line: {
        color: "#1f77b4",
        width: 2,
      },
      hovertemplate: "<b>Past Flow</b><br>Time: %{x}<br>Flow: %{y:.2f} L/min<extra></extra>",
    })
  }

  // Forecast data (orange dashed line)
  if (forecastData.length > 0) {
    plotData.push({
      x: forecastData.map((p) => p.x),
      y: forecastData.map((p) => p.y),
      type: "scatter",
      mode: "lines",
      name: "Predicted Future (24h)",
      line: {
        color: "#ff7f0e",
        width: 2,
        dash: "dash",
      },
      hovertemplate: "<b>Forecast</b><br>Time: %{x}<br>Flow: %{y:.2f} L/min<extra></extra>",
    })
  }

  // Real-time data (green solid line) - only today's live incoming data
  if (realTimeData.length > 0) {
    plotData.push({
      x: realTimeData.map((p) => p.x),
      y: realTimeData.map((p) => p.y),
      type: "scatter",
      mode: "lines+markers",
      name: `True Future (Live - ${realTimeData.length} points)`,
      line: {
        color: "#2ca02c",
        width: 3,
      },
      marker: {
        size: 4,
        color: "#2ca02c",
      },
      hovertemplate: "<b>Live Data</b><br>Time: %{x}<br>Flow: %{y:.2f} L/min<extra></extra>",
    })
  }

  // Forecast start marker (red vertical line)
  const shapes = []
  if (forecastStartTime) {
    shapes.push({
      type: "line",
      x0: forecastStartTime.toISOString(),
      x1: forecastStartTime.toISOString(),
      y0: 0,
      y1: 1,
      yref: "paper",
      line: {
        color: "#d62728",
        width: 3,
        dash: "dot",
      },
    })
  }

  const getStatusColor = (daysSince) => {
    if (daysSince < 7) return "warning"
    if (forecastData.length > 0) return "success"
    return "info"
  }

  const getStatusText = (daysSince) => {
    if (daysSince < 7) return `Accumulating data (${daysSince}/7 days)`
    if (forecastData.length > 0) return "Forecasting active (Auto-updating)"
    return "Waiting for forecast"
  }

  if (loading) {
    return (
      <Box sx={{ padding: 4, textAlign: "center" }}>
        <Typography variant="h6">Loading dashboard...</Typography>
      </Box>
    )
  }

  return (
    <Box sx={{ padding: 4 }}>
      <Typography variant="h4" gutterBottom sx={{ mb: 3 }}>
        💧 Enhanced Water Flow Dashboard (Sliding Window)
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
      )}

      {/* Controls */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Grid container spacing={3} alignItems="center">
            <Grid item>
              <Select value={region} onChange={(e) => setRegion(e.target.value)} size="small">
                <MenuItem value="e1">Region E1</MenuItem>
                <MenuItem value="e2">Region E2</MenuItem>
                <MenuItem value="e3">Region E3</MenuItem>
              </Select>
            </Grid>
            <Grid item>
              <Select value={dmaId} onChange={(e) => setDmaId(e.target.value)} size="small">
                <MenuItem value="222">DMA 222</MenuItem>
                <MenuItem value="223">DMA 223</MenuItem>
                <MenuItem value="224">DMA 224</MenuItem>
              </Select>
            </Grid>
            <Grid item>
              <Chip
                label={getStatusText(dataStats.daysSinceStart)}
                color={getStatusColor(dataStats.daysSinceStart)}
                variant="outlined"
              />
            </Grid>
            <Grid item>
              <Button variant="outlined" onClick={handleRefresh} disabled={loading} size="small">
                🔄 Manual Refresh (Debug)
              </Button>
            </Grid>
            <Grid item>
              <Button variant="outlined" onClick={handleManualDayTransition} size="small" color="secondary">
                🧪 Test Day Transition
              </Button>
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      {/* Statistics */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={3}>
          <Card>
            <CardContent sx={{ textAlign: "center" }}>
              <Typography variant="h6" color="primary">
                {dataStats.historicalCount}
              </Typography>
              <Typography variant="body2" color="textSecondary">
                Historical Points (≤672)
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={3}>
          <Card>
            <CardContent sx={{ textAlign: "center" }}>
              <Typography variant="h6" color="warning.main">
                {dataStats.forecastCount}
              </Typography>
              <Typography variant="body2" color="textSecondary">
                Forecast Points (96)
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={3}>
          <Card>
            <CardContent sx={{ textAlign: "center" }}>
              <Typography variant="h6" color="success.main">
                {dataStats.realTimeCount}
              </Typography>
              <Typography variant="body2" color="textSecondary">
                Live Points (Today)
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={3}>
          <Card>
            <CardContent sx={{ textAlign: "center" }}>
              <Typography variant="h6" color="info.main">
                {dataStats.daysSinceStart}
              </Typography>
              <Typography variant="body2" color="textSecondary">
                Total Days Since Start
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Main Chart */}
      <Card>
        <CardContent>
          <Plot
            data={plotData}
            layout={{
              title: {
                text: `Water Flow Forecast - ${region.toUpperCase()} DMA ${dmaId} (Auto-Sliding Window)`,
                font: { size: 18 },
              },
              xaxis: {
                title: "Time",
                type: "date",
                tickformat: "%Y-%m-%d %H:%M",
              },
              yaxis: {
                title: "Flow (L/min)",
                gridcolor: "#f0f0f0",
              },
              shapes: shapes,
              annotations: forecastStartTime
                ? [
                    {
                      x: forecastStartTime.toISOString(),
                      y: 1,
                      yref: "paper",
                      text: "Forecast Start",
                      showarrow: true,
                      arrowhead: 2,
                      arrowcolor: "#d62728",
                      font: { color: "#d62728", size: 12 },
                    },
                  ]
                : [],
              legend: {
                x: 0,
                y: 1,
                bgcolor: "rgba(255,255,255,0.8)",
                bordercolor: "#ccc",
                borderwidth: 1,
              },
              margin: { t: 60, b: 60, l: 60, r: 60 },
              showlegend: true,
              hovermode: "x unified",
              plot_bgcolor: "#fafafa",
              paper_bgcolor: "white",
            }}
            useResizeHandler
            style={{ width: "100%", height: "600px" }}
            config={{
              displayModeBar: true,
              displaylogo: false,
              modeBarButtonsToRemove: ["pan2d", "lasso2d", "select2d"],
            }}
          />
        </CardContent>
      </Card>

      {/* Legend Explanation */}
      <Card sx={{ mt: 2 }}>
        <CardContent>
          <Typography variant="h6" gutterBottom>
            Chart Legend - Auto-Sliding Window (7 days = 672 points)
          </Typography>
          <Grid container spacing={2}>
            <Grid item xs={12} sm={4}>
              <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                <Box sx={{ width: 20, height: 3, bgcolor: "#1f77b4" }} />
                <Typography variant="body2">
                  <strong>Past Flow:</strong> Last 7 days (max 672 points) - Auto-slides daily
                </Typography>
              </Box>
            </Grid>
            <Grid item xs={12} sm={4}>
              <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                <Box
                  sx={{
                    width: 20,
                    height: 3,
                    bgcolor: "#ff7f0e",
                    borderStyle: "dashed",
                    borderWidth: "1px 0",
                    borderColor: "#ff7f0e",
                  }}
                />
                <Typography variant="body2">
                  <strong>Predicted Future:</strong> Next 24h (96 points) - Auto-updates at midnight
                </Typography>
              </Box>
            </Grid>
            <Grid item xs={12} sm={4}>
              <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                <Box sx={{ width: 20, height: 3, bgcolor: "#2ca02c" }} />
                <Typography variant="body2">
                  <strong>True Future:</strong> Today's live data - Moves to Past at midnight
                </Typography>
              </Box>
            </Grid>
          </Grid>
        </CardContent>
      </Card>
    </Box>
  )
}

export default EnhancedDashboard
