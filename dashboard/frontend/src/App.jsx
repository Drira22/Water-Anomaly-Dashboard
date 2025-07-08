import { BrowserRouter as Router, Routes, Route, Navigate } from "react-router-dom"
import DashboardPage from "./pages/Dashboard"
import EnhancedDashboardPage from "./pages/EnhancedDashboard"
import KafkaControlPage from "./pages/KafkaControl"
import LogsPage from "./pages/Logs"
import Navbar from "./components/Navbar"

function App() {
  return (
    <Router>
      <Navbar />
      <Routes>
        <Route path="/" element={<Navigate to="/enhanced-dashboard" />} />
        <Route path="/dashboard" element={<DashboardPage />} />
        <Route path="/enhanced-dashboard" element={<EnhancedDashboardPage />} />
        <Route path="/kafka-control" element={<KafkaControlPage />} />
        <Route path="/logs" element={<LogsPage />} />
      </Routes>
    </Router>
  )
}

export default App
