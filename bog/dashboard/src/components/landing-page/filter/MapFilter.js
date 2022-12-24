import './MapFilter.css';
import "react-datepicker/dist/react-datepicker.css";
import Checkbox from '@mui/material/Checkbox';
import Accordion from '@mui/material/Accordion';
import AccordionDetails from '@mui/material/AccordionDetails';
import AccordionSummary from '@mui/material/AccordionSummary';
import Typography from '@mui/material/Typography';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import Button from '@mui/material/Button';
import FormGroup from '@mui/material/FormGroup';
import FormControlLabel from '@mui/material/FormControlLabel';
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import Dialog from '@mui/material/Dialog';
import { useState } from 'react';
import FormControl from '@mui/material/FormControl';


export default function MapFilter({
  filterOpen,
  toggleFunc,
  filterBuoysFunc,
  buoyIds,
  fisheries }) {

  /** MANAGE STATE FOR BUOY CHECKBOXES */

  let defaultBuoyFilters = buoyIds.reduce(
    (obj, id) => {
    obj[id] = false;
    return obj;
  }, {})

  const [buoyFilters, setBuoyFilters] = useState(
    defaultBuoyFilters
  );

  const handleBuoyChange = (event) => {
    setBuoyFilters({
      ...buoyFilters,
      [event.target.name]: event.target.checked,
    });
    console.log(event);
    console.log(buoyFilters);
  };

  /** MANAGE STATE FOR FISHERY CHECKBOXES */

  let defaultFisheryFilters = fisheries.reduce(
    (obj, fishery) => {
    obj[fishery] = false;
    return obj;
  }, {})

  const [fisheryFilters, setFisheryFilters] = useState(
    defaultFisheryFilters
  );

  const handleFisheryChange = (event) => {
    setFisheryFilters({
      ...fisheryFilters,
      [event.target.name]: event.target.checked,
    });
    console.log(event);
    console.log(fisheryFilters);
  };

  /** INITIALIZE SUB-FILTERS */

  function BuoyOptions() {
    return (
      <div className='buoyOptions'>
          {buoyIds.map((id, idx) =>(
            <FormControlLabel
              key={idx}
              control={
                <Checkbox
                  name={id}
                  onChange={handleBuoyChange}
                  checked={buoyFilters[id]}
                />
              }
              label={id}/>
          ))}
        </div> 
    );
  }

  function FisheryOptions() {
      return (
        <div className='fisheryOptions'>
          {fisheries.map((fishery, idx) =>(
            <FormControlLabel
              key={idx}
              control={
                <Checkbox
                  name={fishery}
                  onChange={handleFisheryChange}
                  checked={fisheryFilters[fishery]}
                />
              }
              label={fishery}
            />
        ))} 
        </div>    
      )
  }

  function DropDown(props) {
    return (
      <div className='AccordionClass'>
        <Accordion>
          <AccordionSummary
            expandIcon={<ExpandMoreIcon />}
            aria-controls="panel1a-content"
            id="panel1a-header"
          >
            <Typography>{props.title}</Typography>
          </AccordionSummary>
          <AccordionDetails>
            {props.body}
          </AccordionDetails>
        </Accordion>  
      </div>
    );
  }

  /** RENDER HTML */

  return (
    <>
    <Button variant="contained" sx={{backgroundColor:"#49667F"}}>
      <FilterAltIcon onClick={toggleFunc}/>
    </Button>
    {filterOpen && (
      <FormControl component='fieldset'>
        <Dialog onClose={toggleFunc} open={filterOpen}>
          <div className='modal'>    
            <div className='modal-content'>
              <span className='unmodal' onClick={toggleFunc}>&times;</span>
              <div className='formContainer'>
                <h2>Refine Your Search</h2>
                  <div className='Filters'>
                    <FormGroup>
                      <div className='AccordionClass'>
                        <Accordion>
                          <AccordionSummary
                            expandIcon={<ExpandMoreIcon />}
                            aria-controls="panel1a-content"
                            id="panel1a-header"
                          >
                            <Typography>Buoys</Typography>
                          </AccordionSummary>
                          <AccordionDetails>
                            <BuoyOptions/>
                          </AccordionDetails>
                        </Accordion>
                      </div>
                      <div className='AccordionClass'>
                        <Accordion>
                          <AccordionSummary
                            expandIcon={<ExpandMoreIcon />}
                            aria-controls="panel1a-content"
                            id="panel1a-header"
                          >
                            <Typography>Fishery Type</Typography>
                          </AccordionSummary>
                          <AccordionDetails>
                            <FisheryOptions/>
                          </AccordionDetails>
                        </Accordion>
                      </div>
                    </FormGroup>
                  </div>
                  <div className='filterButton'>
                    <Button
                      variant='contained'
                      onClick={() => filterBuoysFunc(buoyFilters, fisheryFilters)}>
                        Filter
                    </Button>
                  </div>          
              </div>
            </div>    
          </div>
        </Dialog>
      </FormControl>
    )}
    </>
  )
}