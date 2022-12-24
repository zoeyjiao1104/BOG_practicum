import './MapPopup.css';
import { Popup } from 'react-leaflet';


function MapPopup({buoy}) {
  
  const getMeasurementProduct = (productName) => {
    let product = buoy?.event_measurements?.find(m => m.product === productName)
    return product?.value
  }

  let fisheryNames = buoy?.fishery_assignments?.map(m => m.fishery[0].toUpperCase() + m.fishery.slice(1)).join(', ')

  return (
    <Popup position={[buoy.latitude, buoy.longitude]}>

            <div className='Header'>
              <h2>
                <a href={`/report/buoy/${buoy['mobile_sensor']}`}>
                  Buoy {buoy.mobile_sensor} {buoy.is_prediction ? " (Predicted Event)" : ""}
                </a>
              </h2>
              <h3>Timestamp: {buoy.datetime} UTC</h3> 
            </div>
            <br/><br/>

            <div className='Left'>
              <p><b>Fisheries</b>: {fisheryNames}</p>
              <p><b>Latitude</b>: {buoy.latitude}</p>
              <p><b>Longitude</b>: {buoy.longitude}</p>
              <p><b>Anomaly Score</b>: {buoy.anomaly_score ?? 'Not Calculated'}</p>
            </div>
            
            <div className='Right'>
              <p><b>Avg Depth</b>: {getMeasurementProduct('depth')}</p>
              <p><b>Position Delta</b>: {getMeasurementProduct('pd')}</p>
              <p><b>Acceleration</b>: {getMeasurementProduct('a')}</p>
              <p><b>Avg Water Temperature</b>: {getMeasurementProduct('wt')}</p>
            </div>
    </Popup>
  );
}

export default MapPopup;
