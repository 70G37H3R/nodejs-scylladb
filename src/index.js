const casual = require('casual');
const { randomSensorData, delay } = require('./helpers');
const { Sensor, Measure } = require('./model');

const INTERVAL = 1000;
const BUFFER_INTERVAL = 5000;
const moment = require('moment');

const cassandra = require('cassandra-driver');
require('dotenv').config();



function getClient() {
  const client = new cassandra.Client({
    contactPoints: ["18.167.230.229", "16.163.26.43", "18.167.189.166"],
    authProvider: new cassandra.auth.PlainTextAuthProvider('scylla', '1tojF6V8CieSspu'),
    localDataCenter: 'AWS_AP_EAST_1',
    keyspace: 'iot',
  });

  return client;
}

function insertQuery(table) {
  const tableName = table.tableName;
  const values = table.columns.map(() => '?').join(', ');
  const fields = table.columns.join(', ');
  return `INSERT INTO ${tableName} (${fields}) VALUES (${values})`;
}

module.exports = { getClient, insertQuery };

async function runSensorData(client, sensors) {
    while (true) {
      const measures = [];
      const last = moment();
      while (moment().diff(last) < BUFFER_INTERVAL) {
        await delay(INTERVAL);
        measures.push(
          ...sensors.map((sensor) => {
            const measure = new Measure(
              sensor.sensor_id,
              Date.now(),
              randomSensorData(sensor)
            );
            console.log(
              `New measure: sensor_id: ${measure.sensor_id} type: ${sensor.type} value: ${measure.value}`
            );
            return measure;
          })
        );
      }
      
      console.log('Pushing data');
      const batch = measures.map((measure) => ({
      query: insertQuery(Measure),
      params: measure,
    }));

    await client.batch(batch, { prepare: true });
    }
}

async function main() {

    // Create a session and connect to the database
    const client = getClient();
    
    // Generate random sensors
    const sensors = new Array(casual.integer(1, 4))
    .fill()
    .map(() => new Sensor());
    

    // Save sensors
    for (let sensor of sensors) {
        await client.execute(insertQuery(Sensor), sensor, { prepare: true });
        console.log(`New sensor # ${sensor.sensor_id}`);
    }
    // Generate random measurements
    await runSensorData(client, sensors);

    return client;
}
    
main();