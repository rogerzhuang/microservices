import React, { useEffect, useState } from 'react'
import '../App.css';

export default function AppStats() {
    const [isLoaded, setIsLoaded] = useState(false);
    const [stats, setStats] = useState({});
    const [error, setError] = useState(null)

    const getStats = () => {
        fetch(`http://acit3855-kafka.westus.cloudapp.azure.com/processing/stats`)
            .then(res => res.json())
            .then((result)=>{
                console.log("Received Stats")
                setStats(result);
                setIsLoaded(true);
            },(error) =>{
                setError(error)
                setIsLoaded(true);
            })
    }
    useEffect(() => {
        const interval = setInterval(() => getStats(), 2000); // Update every 2 seconds
        return() => clearInterval(interval);
    }, [getStats]);

    if (error){
        return (<div className={"error"}>Error found when fetching from API</div>)
    } else if (isLoaded === false){
        return(<div>Loading...</div>)
    } else if (isLoaded === true){
        return(
            <div>
                <h1>Latest Stats</h1>
                <table className={"StatsTable"}>
                    <tbody>
                        <tr>
                            <th>Air Quality</th>
                            <th>Weather</th>
                        </tr>
                        <tr>
                            <td># Air Quality Readings: {stats['num_air_quality_readings']}</td>
                            <td># Weather Readings: {stats['num_weather_readings']}</td>
                        </tr>
                        <tr>
                            <td colspan="2">Max PM2.5 Concentration: {stats['max_pm25_concentration']}</td>
                        </tr>
                        <tr>
                            <td colspan="2">Average Temperature: {stats['avg_temperature']}</td>
                        </tr>
                    </tbody>
                </table>
                <h3>Last Updated: {stats['last_updated']}</h3>
            </div>
        )
    }
}
