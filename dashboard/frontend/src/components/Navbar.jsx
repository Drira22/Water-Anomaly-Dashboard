import { AppBar, Toolbar, Typography, Button } from "@mui/material"
import { Link } from "react-router-dom"

function Navbar() {
  return (
    <AppBar position="static">
      <Toolbar>
        <Typography variant="h6" sx={{ flexGrow: 1 }}>
          💧 Water Flow Dashboard
        </Typography>
        <Button color="inherit" component={Link} to="/enhanced-dashboard">
          Enhanced
        </Button>
        <Button color="inherit" component={Link} to="/dashboard">
          Simple
        </Button>
        <Button color="inherit" component={Link} to="/kafka-control">
          Kafka
        </Button>
        <Button color="inherit" component={Link} to="/logs">
          Logs
        </Button>
      </Toolbar>
    </AppBar>
  )
}

export default Navbar
