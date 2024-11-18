import React, { useEffect, useState } from 'react'
import '../App.css';

export default function EndpointAnalyzer(props) {
    const [isLoaded, setIsLoaded] = useState(false);
    const [log, setLog] = useState(null);
    const [error, setError] = useState(null)
    const [index, setIndex] = useState(null);
	const rand_val = Math.floor(Math.random() * 100); // Get a random event from the event store

    const getAnalyzer = () => {
        fetch(`http://acit3855-kafka.westus.cloudapp.azure.com:8110/${props.endpoint}?index=${rand_val}`)
            .then(res => res.json())
            .then((result)=>{
				console.log("Received Analyzer Results for " + props.endpoint)
                setLog(result);
                setIsLoaded(true);
                setIndex(rand_val);
            },(error) =>{
                setError(error)
                setIsLoaded(true);
            })
    }
	useEffect(() => {
		const interval = setInterval(() => getAnalyzer(), 4000); // Update every 4 seconds
		return() => clearInterval(interval);
    }, [getAnalyzer]);

    if (error){
        return (<div className={"error"}>Error found when fetching from API</div>)
    } else if (isLoaded === false){
        return(<div>Loading...</div>)
    } else if (isLoaded === true){
        return (
            <div className="endpoint-box">
                <h3>{props.endpoint.charAt(0).toUpperCase() + props.endpoint.slice(1)} Reading (Index: {index})</h3>
                {log && log.payload && (
                    <div className="endpoint-details">
                        <p><strong>Reading ID:</strong> {log.payload.reading_id}</p>
                        <p><strong>Sensor ID:</strong> {log.payload.sensor_id}</p>
                        <p><strong>Timestamp:</strong> {log.payload.timestamp}</p>
                        {props.endpoint === 'air-quality' ? (
                            <>
                                <p><strong>PM2.5:</strong> {log.payload.pm2_5_concentration} µg/m³</p>
                                <p><strong>PM10:</strong> {log.payload.pm10_concentration} µg/m³</p>
                                <p><strong>CO2:</strong> {log.payload.co2_level} ppm</p>
                                <p><strong>O3:</strong> {log.payload.o3_level} ppm</p>
                            </>
                        ) : (
                            <>
                                <p><strong>Temperature:</strong> {log.payload.temperature}°C</p>
                                <p><strong>Humidity:</strong> {log.payload.humidity}%</p>
                                <p><strong>Wind Speed:</strong> {log.payload.wind_speed} m/s</p>
                                <p><strong>Noise Level:</strong> {log.payload.noise_level} dB</p>
                            </>
                        )}
                    </div>
                )}
            </div>
        )
    }
}
