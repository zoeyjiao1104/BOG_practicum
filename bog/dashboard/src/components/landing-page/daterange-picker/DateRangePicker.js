import './DateRangePicker.css';

import { forwardRef, useState } from 'react';
import Button from '@mui/material/Button';
import DatePicker from "react-datepicker";
import Box from '@mui/material/Box';
import Slider from '@mui/material/Slider';
import Grid from '@mui/material/Grid';
import Stack from '@mui/material/Stack';
import CalendarTodayIcon from '@mui/icons-material/CalendarToday';

export default function DateRangePicker({ 
    defaultStartDate,
    defaultEndDate,
    defaultInterval,
    filterDatesFunc }){

    /** DEFINE HELPER FUNCTIONS */
    const dateToUtc = (date) => {
        let d = Date.UTC(
            date.getUTCFullYear(),
            date.getUTCMonth(),
            date.getUTCDate()
        )
        return d
    }

    function valuetext(millisecondsSinceEpoch) {
        let date = new Date(millisecondsSinceEpoch);
        let month = date.getUTCMonth() + 1; // For display only, given that months start at 0
        let day = date.getUTCDate();
        let year = date.getUTCFullYear();
        return`${month}/${day}/${year}`;
    }

    /** MANAGE STATE */
    const defaultStartDateUtc = dateToUtc(defaultStartDate);
    const defaultEndDateUtc = dateToUtc(defaultEndDate);
    const [startDate, setStartDate] = useState(defaultStartDate);
    const [endDate, setEndDate] = useState(defaultEndDate);
    const defaultSliderValue = [defaultStartDateUtc, defaultEndDateUtc]
    const [sliderValue, setSliderValue] = useState(defaultSliderValue);
    const stepValue = (defaultEndDateUtc - defaultStartDateUtc) / defaultInterval

    const handleSliderChange = (_, newValue) => {
        let [currentStartDate, currentEndDate] = newValue;
        setStartDate(new Date(currentStartDate));
        setEndDate(new Date(currentEndDate));
        setSliderValue(newValue);
        filterDatesFunc(newValue);
    }

    /** DEFINE SUB-COMPONENTS */
    const DateButton = forwardRef(({ value, onClick }, ref) => (
        <>
        <Button
            onClick={onClick}
            ref={ref}
            sx={{color:"#49667F"}}
        >
            <CalendarTodayIcon />
        </Button>
        </>
        
    ));

    return (
        <Box sx={{ width:1000 }}>
            <Grid container spacing={2} alignItems="center">
                <Grid item>
                <DatePicker
                    selected={startDate}
                    onChange={(date) => {
                        handleSliderChange(null, [dateToUtc(date), dateToUtc(endDate)])
                    }}
                    customInput={<DateButton />}
                    minDate={defaultStartDate}
                    maxDate={defaultEndDate}
                    />
                </Grid>
                <Grid item xs>
                <Slider
                    getAriaLabel={() => "date-range-slider"}
                    valueLabelFormat={valuetext}
                    value={sliderValue}
                    onChange={handleSliderChange}
                    step={stepValue}
                    valueLabelDisplay="on"
                    min={dateToUtc(defaultStartDate)}
                    max={dateToUtc(defaultEndDate)}
                />
                </Grid>
                <Grid item>
                    <DatePicker
                        selected={endDate}
                        onChange={(date) => {
                            handleSliderChange(null, [dateToUtc(startDate), dateToUtc(date)])
                        }}
                        customInput={<DateButton />}
                        minDate={defaultStartDate}
                        maxDate={defaultEndDate}
                    />
                </Grid>
            </Grid>
        </Box>
    );
}