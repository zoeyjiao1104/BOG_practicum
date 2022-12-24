import './PageSkeleton.css';
import Box from '@mui/material/Box';
import Container from '@mui/material/Container';
import Toolbar from '@mui/material/Toolbar';


function PageSkeleton({ children }) {
    return (
        <Box
            component="main"
            sx={{
                flexGrow: 1,
                height: '100vh',
                overflow: 'auto',
            }}
        >
            <Toolbar />
            <Container className='pageContainer' sx={{ mt: 4, mb: 4, ml: 0 }}>
                {children}
            </Container>
        </Box>
    )
}

export default PageSkeleton;
