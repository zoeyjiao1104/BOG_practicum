import './Map.css';

import buoyBlue from "../../static/icons/buoy_blue.png";
import buoyRed from "../../static/icons/buoy_red.png";
import buoyYellow from "../../static/icons/buoy_yellow.png";
import stationGrey from "../../static/icons/station_grey.png";
import stationGreen from "../../static/icons/station_green.png";

import { MapContainer, TileLayer, Marker, Popup, CircleMarker } from 'react-leaflet';
import { Icon } from 'leaflet';
import MapPopup from '../popup/MapPopup';
import Stack from '@mui/material/Stack';
import { containerClasses } from '@mui/material';


export default function Map({ buoys, stations, filterModal, dateRangePicker }) {

    function BuoyIcon(risk, datetime) {
        let iconVar = (risk === "medium") ? buoyYellow : ((risk === "high") ? buoyRed : (risk === "low") ? buoyBlue : buoyBlue)
        return <CircleMarker/>
    }
    
    function StationIcon(source) {
        let iconVar = ((source === "noaa") ? stationGrey : stationGreen)
        return new Icon({
            iconUrl: iconVar,
            iconSize: [6, 10]
        })
    }

    const getColor = (anomalyScore) => {

        if (anomalyScore === null || anomalyScore === undefined){
            return 'gray';
        }
        else if (anomalyScore < 0.1) {
            return '#49667F';
        }
        else if (anomalyScore < 0.2) {
            return '#0070FF';
        }
        else if (anomalyScore < 0.3) {
            return '#00E4E6';
        }
        else if (anomalyScore < 0.4) {
            return '#00BCFF';
        }
        else if (anomalyScore < 0.5) {
            return '#00FF15';
        }
        else if (anomalyScore < 0.6) {
            return '#39FF00';
        }
        else if (anomalyScore < 0.7) {
            return '#D7F400';
        }
        else if (anomalyScore < 0.8) {
            return '#49667F';
        }
        else if (anomalyScore < 0.9) {
            return '#FFBA00';
        }
        else {
            return '#FF3C00';
        }
    }

    return (
        <MapContainer
            center={[42, -73]}
            zoom={4.5}
            scrollWheelZoom={true}>
            <TileLayer
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
                url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
            />
            {buoys && buoys.map((buoy, idx) => (
                <CircleMarker
                    key={idx}
                    center={[buoy.latitude, buoy.longitude]}
                    radius={3}
                    pathOptions={{
                        color: buoy.is_prediction ? 'purple' : getColor(buoy.anomaly_score),
                        opacity: 1
                    }}
                    eventHandlers={{
                        mouseover: (event) => {
                            console.log(event);
                            event.target.setStyle({
                                "fillColor": "yellow",
                                "color": "yellow"
                            })
                        },
                        mouseout: (event) => {
                            event.target.setStyle({
                                "fillColor": buoy.is_prediction ? 'purple' : getColor(buoy.anomaly_score),
                                "color": buoy.is_prediction ? 'purple' : getColor(buoy.anomaly_score)
                            })
                        }
                    }}
                >
                    <MapPopup buoy={buoy} />
                </CircleMarker>
            ))}

            {stations && stations.map((station, idx) => (
                <Marker
                    key={idx}
                    position={[station.latitude, station.longitude]}
                    icon={StationIcon(station.source)}>

                    <Popup position={[station.latitude, station.longitude]}>
                        <div className='Header'>
                            <h2>{station.name}, {station.state}</h2>
                           
                        </div>
                        <div className='Center'>
                            <p><b>Type:</b> {station.type === 'National Oceanic and Atmospheric Administration(NOAA) Tides and Currents' ? 'NOAA' : 'DFO'}</p>
                            <p><b>Location:</b> {station.latitude}, {station.longitude}</p>
                            <p><b>Station ID</b> {station.id}</p>
                        </div>
                    </Popup>
                </Marker>
            ))}

            <div className='spacer leaflet-bottom center'>
                <div className='leaflet-control'>
                    <Stack direction='row' spacing={2}>
                        {dateRangePicker}
                        {filterModal}
                    </Stack>
                </div>
            </div>
            <div className='spacer leaflet-top leaflet-right'>
                <div className='leaflet-control'>
                    <h3 className='legend-title'>Anomaly Score</h3>
                    <table id="legendTable" cellspacing="0">
                        <tr>
                            <td class="colour" id="legend10"></td>
                            <td class="table-spacer"></td>
                            <td class="legendLabel" id="legend10text">1.0</td> 
                        </tr>
                        <tr>
                            <td class="colour" id="legend9"></td>
                            <td class="table-spacer"></td>
                            <td class="legendLabel" id="legend9text">0.9</td> 
                        </tr>
                        <tr>
                            <td class="colour" id="legend8"></td>
                            <td class="table-spacer"></td>
                            <td class="legendLabel" id="legend8text">0.8</td> 
                        </tr>
                        <tr>
                            <td class="colour" id="legend7"></td>
                            <td class="table-spacer"></td>
                            <td class="legendLabel" id="legend7text">0.7</td> 
                        </tr>
                        <tr>
                            <td class="colour" id="legend6"></td>
                            <td class="table-spacer"></td>
                            <td class="legendLabel" id="legend6text">0.6</td> 
                        </tr>
                        <tr>
                            <td class="colour" id="legend5"></td>
                            <td class="table-spacer"></td>
                            <td class="legendLabel" id="legend5text">0.5</td>  
                        </tr>
                        <tr>
                            <td class="colour" id="legend4"></td>
                            <td class="table-spacer"></td>
                            <td class="legendLabel" id="legend4text">0.4</td> 
                        </tr>
                        <tr>
                            <td class="colour" id="legend3"></td>
                            <td class="table-spacer"></td>
                            <td class="legendLabel" id="legend3text">0.3</td> 
                        </tr>
                        <tr>
                            <td class="colour" id="legend2"></td>
                            <td class="table-spacer"></td>
                            <td class="legendLabel" id="legend2text">0.2</td> 
                        </tr>
                        <tr>
                            <td class="colour" id="legend1"></td>
                            <td class="table-spacer"></td>
                            <td class="legendLabel" id="legend1text">0.1</td> 
                        </tr>
                        <tr>
                            <td class="colour" id="legend1"></td>
                            <td class="table-spacer"></td>
                            <td class="legendLabel" id="legend1text">0.0</td> 
                        </tr>
                    </table>
                </div>
            </div>
        </MapContainer>
    );
}