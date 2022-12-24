import './Navbar.css';
import * as React from 'react';
import { styled } from '@mui/material/styles';
import MuiDrawer from '@mui/material/Drawer';
import MuiAppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import List from '@mui/material/List';
import Typography from '@mui/material/Typography';
import Divider from '@mui/material/Divider';
import IconButton from '@mui/material/IconButton';
import ListItem from '@mui/material/ListItem';
import ListItemText from '@mui/material/ListItemText';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import MenuIcon from '@mui/icons-material/Menu';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import HomeIcon from '@mui/icons-material/Home';
import SettingsIcon from '@mui/icons-material/Settings';
import BarChartIcon from '@mui/icons-material/BarChart';
import ListItemIcon from '@mui/material/ListItemIcon';
import NotificationsIcon from '@mui/icons-material/Notifications';
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import Logo from '../../static/icons/BOG.png';
import { useNavigate } from 'react-router-dom';

import MapFilter from '../../landing-page/filter/MapFilter.js';

const drawerWidth = 240;

const hzTheme = createTheme({
  status: {
    danger: '#fff',
  },
  palette: {
    primary: {
      main: '#fff',
      darker: '#fff',
    },
    neutral: {
      main: '#fff',
      contrastText: '#fff',
    },
  },
});

const openedMixin = (theme) => ({
  width: drawerWidth,
  transition: theme.transitions.create('width', {
    easing: theme.transitions.easing.sharp,
    duration: theme.transitions.duration.enteringScreen,
  }),
  overflowX: 'hidden',
});

const closedMixin = (theme) => ({
  transition: theme.transitions.create('width', {
    easing: theme.transitions.easing.sharp,
    duration: theme.transitions.duration.leavingScreen,
  }),
  overflowX: 'hidden',
  width: `calc(${theme.spacing(7)} + 1px)`,
  [theme.breakpoints.up('sm')]: {
    width: `calc(${theme.spacing(9)} + 1px)`,
  },
});

const DrawerHeader = styled('div')(({ theme }) => ({
  backgroundColor: 'transparent',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'flex-end',
  padding: theme.spacing(0, 1),
  // necessary for content to be below app bar
  ...theme.mixins.toolbar,
}));

const HorizontalAppBar = styled(MuiAppBar, {
  shouldForwardProp: (prop) => prop !== 'open',
})(({ theme, open }) => ({
  zIndex: theme.zIndex.drawer + 1,
  transition: theme.transitions.create(['width', 'margin'], {
    easing: theme.transitions.easing.sharp,
    duration: theme.transitions.duration.leavingScreen,
  }),
  ...(open && {
    marginLeft: drawerWidth,
    width: `calc(100% - ${drawerWidth}px)`,
    transition: theme.transitions.create(['width', 'margin'], {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.enteringScreen,
    }),
  }),
}));

const Drawer = styled(MuiDrawer, { shouldForwardProp: (prop) => prop !== 'open' })(
  ({ theme, open }) => ({
    hideBackdrop: true,
    width: drawerWidth,
    flexShrink: 0,
    whiteSpace: 'nowrap',
    boxSizing: 'border-box',
    ...(open && {
      ...openedMixin(theme),
      '& .MuiDrawer-paper': openedMixin(theme),
    }),
    ...(!open && {
      ...closedMixin(theme),
      '& .MuiDrawer-paper': closedMixin(theme),
    }),
  }),
);

export default function Navbar() {

  const menuOptions = ['Home', 'Analysis', 'Settings', 'Filters'];
  const navigate = useNavigate();
  const [open, setOpen] = React.useState(false);
  const handleDrawerOpen = () => { setOpen(true); };
  const handleDrawerClose = () => { setOpen(false); };

  // Filter
  const [isOpen, setIsOpen] = React.useState(false);
  const togglefilt = () => {
    setIsOpen(!isOpen);
  }

  const getIcon = (index) => {
    switch (index) {
      case 0:
        return <HomeIcon onClick={() => navigate("/") }/>;
      case 1:
        return <BarChartIcon />;
      case 2:
          return <SettingsIcon />;
      default:
        return;
    }
  };

  return (
    <>
      {/** HORIZONTAL NAVIGATION BAR */}
      <ThemeProvider theme={hzTheme}>
          <HorizontalAppBar position="fixed" open={open} >
            <Toolbar>
              <IconButton
                color="inherit"
                aria-label="open drawer"
                onClick={handleDrawerOpen}
                edge="start"
                sx={{
                  marginRight: '36px',
                  ...(open && { display: 'none' }),
                }}
              >
                
                <MenuIcon/>
              </IconButton>
              <Typography variant="h6" noWrap component="div" align="right">
              <img src={Logo} width="20" alt="logo" align="right" sx={{marginLeft: '36px'}}/>     
              </Typography>
              <NotificationsIcon sx={{marginLeft: '36px'}}/>
              <Typography variant="subtitle1" component="h1" fontSize='20' sx={{marginLeft: '24px'}}>
                Welcome back, Admin!
              </Typography>
            </Toolbar>
          </HorizontalAppBar>
      </ThemeProvider>

      {/** VERTICAL NAVIGATION BAR */}
      <Drawer variant="permanent" open={open}>
        <DrawerHeader>
          <IconButton onClick={handleDrawerClose}>
            {open === true ? <ChevronLeftIcon /> : ' '}
          </IconButton>
        </DrawerHeader>
        <Divider/>
        <List>
          {menuOptions.map((text, index) => (
            <ListItem button key={text}>
              <ListItemIcon>
                {getIcon(index)}
              </ListItemIcon>
              <ListItemText primary={text} />
            </ListItem>
          ))}
        </List>
      </Drawer>
    </>
  );
}

