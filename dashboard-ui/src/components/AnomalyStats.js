import React, { useEffect, useState } from 'react'
import '../App.css';

export default function AnomalyStats() {
    const [isLoaded, setIsLoaded] = useState(false);
    const [anomalies, setAnomalies] = useState({});
    const [error, setError] = useState(null)

    const getAnomalies = () => {
        // Get all anomalies and filter for the most recent of each type
        fetch(`http://acit3855-kafka.westus.cloudapp.azure.com/anomaly_detector/anomalies`)
            .then(res => res.json())
            .then((result)=>{
                console.log("Received Anomalies")
                // Group by event_type and get the most recent for each
                const latestAnomalies = result.reduce((acc, anomaly) => {
                    if (!acc[anomaly.event_type] || 
                        new Date(anomaly.timestamp) > new Date(acc[anomaly.event_type].timestamp)) {
                        acc[anomaly.event_type] = anomaly;
                    }
                    return acc;
                }, {});
                
                setAnomalies(latestAnomalies);
                setIsLoaded(true);
            },(error) =>{
                setError(error)
                setIsLoaded(true);
            })
    }
    
    useEffect(() => {
        const interval = setInterval(() => getAnomalies(), 2000); // Update every 2 seconds
        return() => clearInterval(interval);
    }, [getAnomalies]);

    if (error){
        return (<div className={"error"}>Error found when fetching from API</div>)
    } else if (isLoaded === false){
        return(<div>Loading...</div>)
    } else if (isLoaded === true){
        return(
            <div>
                <h1>Latest Anomalies</h1>
                {anomalies.air_quality && (
                    <div className="anomaly-box">
                        <h3>Air Quality Latest Anomaly</h3>
                        <p>UUID: {anomalies.air_quality.event_id}</p>
                        <p>{anomalies.air_quality.description}</p>
                        <p>Detected on {anomalies.air_quality.timestamp}</p>
                    </div>
                )}
                {anomalies.weather && (
                    <div className="anomaly-box">
                        <h3>Weather Latest Anomaly</h3>
                        <p>UUID: {anomalies.weather.event_id}</p>
                        <p>{anomalies.weather.description}</p>
                        <p>Detected on {anomalies.weather.timestamp}</p>
                    </div>
                )}
            </div>
        )
    }
}