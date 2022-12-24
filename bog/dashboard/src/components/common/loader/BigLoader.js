import './BigLoader.css';
import CircularProgress from '@mui/material/CircularProgress';

export default function BigLoader({loadingText}) {
    return (
        <div className="loaderOuterContainer">
            <div className="loaderInnerContainer">
                <CircularProgress size={100} color="secondary"/>
                <br/>
                <h3 className="loaderText">{loadingText}</h3>
            </div>     
        </div>
    )
}