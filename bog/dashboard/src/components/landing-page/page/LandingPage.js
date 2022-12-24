import './LandingPage.css';
import BigLoader from '../../common/loader/BigLoader';
import Map from '../map/Map';
import MapFilter from '../filter/MapFilter';
import DateRangePicker from '../daterange-picker/DateRangePicker';
import { 
    SearchForBuoys,
    FetchStations,
    FetchLandingPageFilters 
} from '../../../services/ApiClient';
import { useState, useEffect } from 'react';
import Dialog from '@mui/material/Dialog';
import DialogContent from '@mui/material/DialogContent';



export default function LandingPage() {

    /** HELPER FUNCTIONS */
    const formatDate = (date) => {
        let yyyy = date.getUTCFullYear();
        let m = ("0" + (date.getUTCMonth() + 1)).slice(-2);
        let d = ("0" + date.getUTCDate()).slice(-2);
        let final = `${yyyy}-${m}-${d}`
        return final
    }

    /** STATE MANAGEMENT */
    const daysBetween = 180
    const defaultEndDate = new Date();
    const defaultStartDate = new Date(Date.UTC(2021, 0, 1));
    const [filters, setFilters] = useState({
        startDate: formatDate(defaultStartDate),
        endDate: formatDate(defaultEndDate),
        buoyIds: [],
        fisheries: []
    })
    const [filterOptions, setFilterOptions] = useState(null)
    const [filterOpen, setFilterOpen] = useState(false);
    const [isLoading, setIsLoading] = useState(true);
    const [filteredBuoys, setFilteredBuoys] = useState(null);
    const [stations, setStations] = useState(null);

    useEffect(() => {
        setIsLoading(true);
        SearchForBuoys(filters).then(buoys => {
            setFilteredBuoys(buoys);
            setIsLoading(false);
        });
        FetchStations().then(stations => {
            setStations(stations);
        })
        FetchLandingPageFilters().then(opts => {
            setFilterOptions(opts);
        })
    }, [])

    useEffect(() => {
        let isDefault = (
            filters.startDate === formatDate(defaultStartDate) &&
            filters.endDate === formatDate(defaultEndDate) &&
            filters.buoyIds === [] &&
            filters.fisheries === []
        )
        
        if (isDefault !== true){
            setIsLoading(true);
            SearchForBuoys(filters).then(buoys => {
                setFilteredBuoys(buoys);
                setIsLoading(false);
            });
        }
    }, [filters])

    const filterBuoysFunc = (buoyFilters, fisheryFilters) => {
        let selectedBuoys = (Object
            .entries(buoyFilters)
            .filter(([_, value]) => {
                return value === true
            })
            .map(e => e[0])
        );
        let selectedFisheries = (Object
            .entries(fisheryFilters)
            .filter(([_, value]) => {
                return value === true
            })
            .map(e => e[0])
        );
        let newFilters = {
            ...filters,
            buoyIds: selectedBuoys,
            fisheries: selectedFisheries
        };
        setFilterOpen(false);
        setFilters(newFilters);
    }

    const filterDatesFunc = (dateFilters) => {
        let [startDateMs, endDateMs] = dateFilters;
        let startDate = new Date(startDateMs);
        let endDate = new Date(endDateMs);
        let newFilters = {
            ...filters,
            startDate: formatDate(startDate),
            endDate: formatDate(endDate)
        }
        setFilters(newFilters)
    }

    /** RENDER COMPONENT HTML */
    return (
        <div>
            {isLoading && (
                <div>
                <Dialog open >
                    <DialogContent >
                        Loading buoys and stations...
                    </DialogContent>
                </Dialog>
              </div>
            )}
            <Map 
                buoys={filteredBuoys}
                stations={stations}
                filterModal={
                    <MapFilter
                        filterOpen={filterOpen}
                        toggleFunc={() => setFilterOpen(!filterOpen)}
                        filterBuoysFunc={filterBuoysFunc}
                        buoyIds={filterOptions?.buoyIds ??  []}
                        fisheries={filterOptions?.fisheries ?? []}
                    />
                }
                dateRangePicker={
                    <DateRangePicker
                        defaultStartDate={defaultStartDate}
                        defaultEndDate={defaultEndDate}
                        defaultInterval={daysBetween}
                        filterDatesFunc={filterDatesFunc}
                    />
                }
            />
        </div>
    );
}