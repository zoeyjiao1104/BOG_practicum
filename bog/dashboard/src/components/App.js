import './App.css';
import { BrowserRouter as Router, Routes, Route, Outlet } from 'react-router-dom';
import BuoyReport from './report/BuoyReport';
import LandingPage from './landing-page/page/LandingPage';
import Layout from './common/layout/Layout';
import PageSkeleton from './common/page-skeleton/PageSkeleton';
import DateFnsAdapter from '@mui/lab/AdapterDateFns';
import LocalizationProvider from '@mui/lab/LocalizationProvider';



function FullScreenMapView() {
  return (
    <>
      <Layout />
      <Outlet />
    </>
  );
}


function PageView() {
  return (
    <Layout>
      <PageSkeleton>
        <Outlet />
      </PageSkeleton>
    </Layout>
  );
}

function App() {
  return (
    <LocalizationProvider dateAdapter={DateFnsAdapter}>
      <Router>
        <Routes>
            <Route element={<FullScreenMapView />} >
              <Route exact path="/" element={<LandingPage/>} />
            </Route>
            <Route element={<PageView />}>
              <Route exact path="/report/buoy/:id" element={<BuoyReport />} />
            </Route>
        </Routes>
      </Router>
    </LocalizationProvider>  
  );
}

export default App;
