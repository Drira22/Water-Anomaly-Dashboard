import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import DashboardPage from './pages/Dashboard';
import KafkaControlPage from './pages/KafkaControl';
import LogsPage from './pages/Logs';
import Navbar from './components/Navbar';


function App() {
  return (
    <Router>
      {/* Navbar always visible */}
      <Navbar />

      {/* Page content changes here */}
      <Routes>
        <Route path="/" element={<Navigate to="/dashboard" />} />
        <Route path="/dashboard" element={<DashboardPage />} />
        <Route path="/kafka-control" element={<KafkaControlPage />} />
        <Route path="/logs" element={<LogsPage />} />
      </Routes>
    </Router>
  );
}

export default App;
