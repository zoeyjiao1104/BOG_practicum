import styles from './BuoyReport.module.css';
import { FetchBuoyReport } from '../../services/ApiClient';
import {useState, useEffect} from 'react';
import BigLoader from '../common/loader/BigLoader';
import { MapContainer, TileLayer, CircleMarker } from 'react-leaflet';
import { Icon, popup } from 'leaflet';
import MapPopup from '../landing-page/popup/MapPopup';
import predictionIcon from "../static/icons/good.png";
import forecastIcon from "../static/icons/pred.png";
import Plot from 'react-plotly.js';
import Tabs from "../../components/Tabs";
import { useParams } from 'react-router-dom';
import Grid from '@mui/material/Grid';
import Stack from '@mui/material/Stack';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import TimelineIcon from '@mui/icons-material/Timeline';
import LocationTable from '../report/location-table/LocationTable';


export default function BuoyReport() {

  const headCells = [
    {
      id: 'datetime',
      numeric: false,
      disablePadding: true,
      label: 'Datetime',
    },
    {
        id: 'anomalyScore',
        numeric: false,
        disablePadding: true,
        label: 'Anomaly Score',
      },
    {
      id: 'latitude',
      numeric: true,
      disablePadding: false,
      label: 'Latitude',
    },
    {
      id: 'longitude',
      numeric: true,
      disablePadding: false,
      label: 'Longitude',
    },
    {
      id: 'predictedLatitude',
      numeric: true,
      disablePadding: false,
      label: 'Predicted Latitude',
    },
    {
      id: 'predictedLongitude',
      numeric: true,
      disablePadding: false,
      label: 'Predicted Longitude',
    },
    {
        id: 'predictionError',
        numeric: true,
        disablePadding: false,
        label: 'Prediction Error (km)',
    }
  ];


  /** DEFINE HELPER FUNCTIONS */
  function degreesToRadians(degrees) {
    return degrees * Math.PI / 180;
  }
  
  function distanceInKmBetweenEarthCoordinates(lat1, lon1, lat2, lon2) {
    let earthRadiusKm = 6371;
  
    let dLat = degreesToRadians(lat2-lat1);
    let dLon = degreesToRadians(lon2-lon1);
  
    lat1 = degreesToRadians(lat1);
    lat2 = degreesToRadians(lat2);
  
    let a = Math.sin(dLat/2) * Math.sin(dLat/2) +
            Math.sin(dLon/2) * Math.sin(dLon/2) * Math.cos(lat1) * Math.cos(lat2); 
    let c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a)); 
    let km = earthRadiusKm * c;

    return Math.round((km + Number.EPSILON) * 1000) / 1000
  }
  
  function parseReport(rawReport){

    let mobileSensor = rawReport['id'];
    let historicalSeries = rawReport['historicalSeries']; 
    let startDatetime = Date.parse(historicalSeries[0]['datetime']);
    let totalNumReadings = rawReport['historicalSeries'].length;

    let datetime = [];
    let mappedDatetime = [];
    let latitude = [];
    let longitude = [];
    let meanAcceleration = [];
    let meanBatteryTemperature = [];
    let meanDepth = [];
    let meanWaterTemperature = [];
    let positionDelta = [];
    let momsn = [];
    let anomalyScore = [];
    let markers = []
    let rows = []

    for (let i=0; i < totalNumReadings; i++) {
      let obv = rawReport['historicalSeries'][i];
      let predictedLat = null;
      let predictedLon = null;
      let predictionError = null;

      markers.push({
        anomaly_score: obv['anomaly_score'],
        datetime: obv['datetime'],
        mobile_sensor: mobileSensor,
        latitude: obv['latitude'],
        longitude: obv['longitude'],
        is_prediction: obv['is_prediction'],
        event_measurements: [
          {product: 'depth', value: obv['depth-m']},
          {product: 'pd', value: obv['pd-r']},
          {product: 'a', value: obv['a-m']},
          {product: 'wt', value: obv['wt-m']}
        ]
      })

      if (obv['is_prediction']){
        continue;
      }

      if (i - 1 >= 0 && rawReport['historicalSeries'][i - 1]['is_prediction']){
          let pred = rawReport['historicalSeries'][i - 1];
          predictedLat = pred['latitude'];
          predictedLon = pred['longitude'];
          predictionError = distanceInKmBetweenEarthCoordinates(
            obv['latitude'],
            obv['longitude'],
            predictedLat,
            predictedLon
          );
      }

      datetime.push(obv['datetime']);
      mappedDatetime.push((Date.parse(obv['datetime']) - startDatetime));
      latitude.push(obv['latitude']);
      longitude.push(obv['longitude']);
      meanAcceleration.push(obv?.['a-m']);
      meanBatteryTemperature.push(obv?.['bt-m']);
      meanDepth.push(obv?.['depth-m']);
      meanWaterTemperature.push(obv?.['wt-m']);
      positionDelta.push(obv?.['pd-r']);
      momsn.push(obv?.['momsn-r']);
      anomalyScore.push(obv['anomaly_score']);
      rows.push({
        datetime: obv['datetime'],
        anomalyScore: obv['anomaly_score'],
        latitude: obv['latitude'],
        longitude: obv['longitude'],
        predictedLatitude: predictedLat ?? '--',
        predictedLongitude: predictedLon ?? '--',
        predictionError: predictionError ?? '--'
      })
    }

    return {
      datetime: datetime,
      mappedDatetime: mappedDatetime,
      latitude: latitude,
      longitude: longitude,
      meanAcceleration: meanAcceleration,
      meanBatteryTemperature: meanBatteryTemperature,
      meanDepth: meanDepth,
      meanWaterTemperature: meanWaterTemperature,
      positionDelta: positionDelta,
      momsn: momsn,
      anomalyScore: anomalyScore,
      markers: markers,
      rows: rows
    };
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

  /** MANAGE STATE */
  const { id } = useParams();
  const [report, setReport] = useState(null);

  useEffect(() => {
    FetchBuoyReport(id).then(report => {
      let parsedReport = parseReport(report);
      setReport(parsedReport);
    })
  }, [])
  
  /** RENDER HTML */
  function DataGraph({
    title,
    timeSeries,
    mappedTimeSeries,
    dataSeries,
    colorscale}) {

    return (  
      <Plot
        data={[
          {
            x: timeSeries,
            y: dataSeries,
            type: 'scatter',
            mode: 'lines+markers',
            marker: {
              cmin: Math.min(mappedTimeSeries),
              cmax: Math.max(mappedTimeSeries),
              color: mappedTimeSeries,
              colorscale: colorscale,
            },
            line: {
              color: 'lightgrey'
            }
          },
        ]}
        layout={{width: 1000, height: 500, title: title}}
      />
    );
  }

  if (report === null){
    return <BigLoader loadingText={'Loading Buoy Report...'}/>
  }

  return (
    <>
      <h2 className='report-title'>Buoy {id}</h2>
      <Grid container className="custom-container" rowSpacing={1} columnSpacing={4}>
        {/** TAGS */}
        <Grid item xs={12}>
          <Stack direction="row" spacing={2}>
            <Stack direction="row" spacing={1}>
              <CheckCircleIcon color='primary' />
              <span className='chip'>Unknown Status</span>
            </Stack>
            <Stack direction="row" spacing={1}>
              <TimelineIcon color='primary' />
              <span className='chip'>Since {report.datetime[0].slice(0,10)}</span>
            </Stack>
          </Stack>
        </Grid>

        {/** LOCATION ANALYSIS */}
        <Grid item xs={12}>
          <h3>Location Analysis</h3>
        </Grid>
        {/** MAP */}
        <Grid item xs={12} >
          <MapContainer
              className={styles.leafletContainer}
              center={[Math.max(...report.latitude), Math.max(...report.longitude)]}
              zoom={6}
              scrollWheelZoom={true}>
              <TileLayer
                  attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
                  url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
              />
                {report?.markers && report.markers.map((m, idx) => (
                    <CircleMarker
                        key={idx}
                        center={[m.latitude, m.longitude]}
                        radius={3}
                        pathOptions={{
                            color: m.is_prediction ? 'purple' : getColor(m.anomaly_score),
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
                                  "fillColor": m.is_prediction ? 'purple' : getColor(m.anomaly_score),
                                  "color": m.is_prediction ? 'purple' : getColor(m.anomaly_score)
                              })
                          }
                      }}
                    >
                      <MapPopup buoy={m} />
                    </CircleMarker>
                  )
              )}
          </MapContainer>
        </Grid>
        {/** SUMMARY */}
        <Grid className='summary-section' item xs={12}>
          <h4>Summary</h4>
          {/** TABLE */}
          <LocationTable
            rows={report?.rows}
            headCells={headCells}
            isDense={true}
          />
        </Grid>
        {/** CHARTS */}
        <Grid item xs={12}>
          <DataGraph
            title="Mean Acceleration"
            timeSeries={report.datetime}
            mappedTimeSeries={report.mappedDatetime}
            dataSeries={report.meanAcceleration}
            colorscale='Reds'
          />
          <DataGraph
            title="Mean Depth"
            timeSeries={report.datetime}
            mappedTimeSeries={report.mappedDatetime}
            dataSeries={report.meanDepth}
            colorscale='Blues'
          />
        </Grid>
        <Grid item xs={12}>
          <DataGraph
            title="Position Delta"
            timeSeries={report.datetime}
            mappedTimeSeries={report.mappedDatetime}
            dataSeries={report.positionDelta}
            colorscale='Greens'
          />
        </Grid>
      </Grid>
    </>
  );
}
