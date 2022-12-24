import './Layout.css';
import Box from '@mui/material/Box';
import CssBaseline from '@mui/material/CssBaseline';
import Navbar from '../navbar/Navbar';


function Layout({ children }) {
    return (
        <Box sx={{ display: 'flex' }}>
            <CssBaseline />
            <Navbar />
            {children}
        </Box>
    )
}

export default Layout;
