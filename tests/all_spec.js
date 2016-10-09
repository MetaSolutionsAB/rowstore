var frisby = require('frisby');
var fs = require('fs');
var path = require('path');

var URL = 'http://localhost:8282/';

// Config for all tests
frisby.globalSetup({
    request: {
        headers: {'Accept': 'application/json'}
    }
});

frisby.create('GET status and check structure')
    .get(URL + 'status')
    .expectStatus(200)
    .expectHeaderContains('Content-Type', 'application/json')
    .expectJSONTypes({
        service: String,
        datasets: Number,
        activeEtlProcesses: Number,
        version: String
    })
    .toss();

frisby.create('GET datasets array')
    .get(URL + 'datasets')
    .expectStatus(200)
    .expectHeaderContains('Content-Type', 'application/json')
    .expectJSON([])
    .toss();

var csv1Path = path.resolve(__dirname, 'data/dataset1_utf8.csv');
var csv1Content = fs.readFileSync(csv1Path);

frisby.create('POST CSV file (UTF-8, comma-separated) to create new dataset1')
    .post(URL + 'datasets',
        csv1Content,
        {
            json: false,
            headers: {
                'Content-Type': 'text/csv'
            }
        })
    .expectStatus(202)
    .expectHeaderContains('Content-Type', 'application/json')
    .expectJSONTypes({
        id: String,
        url: String,
        info: String,
        status: Number
    })
    .afterJSON(function(json) {
        frisby.create('GET dataset1 info')
            .get(json.info)
            .expectStatus(200)
            .expectHeaderContains('Content-Type', 'application/json')
            .expectJSONTypes({
                rowcount: Number,
                created: String,
                columnnames: Array,
                status: Number
            })
            .toss();
        frisby.create('GET dataset1 aliases')
            .get(json.url + '/aliases')
            .expectStatus(200)
            .expectHeaderContains('Content-Type', 'application/json')
            .expectJSON([])
            .expectJSONLength(0)
            .after(function() {
                frisby.create('PUT dataset1 aliases')
                    .put(json.url + '/aliases', ['dataset1'], { json: true })
                    .expectStatus(204)
                    .after(function() {
                        frisby.create('GET dataset1 aliases and check for correct number 1')
                            .get(json.url + '/aliases')
                            .expectStatus(200)
                            .expectHeaderContains('Content-Type', 'application/json')
                            .expectJSON([])
                            .expectJSONLength(1)
                            .after(function() {
                                frisby.create('POST dataset1 aliases')
                                    .post(json.url + '/aliases', ['dataset1b', 'dataset1b'], { json: true }) // intentionally providing the same alias twice as we want to test the behavior
                                    .expectStatus(204)
                                    .after(function() {
                                        frisby.create('GET dataset1 aliases and check for correct number 2')
                                            .get(json.url + '/aliases')
                                            .expectStatus(200)
                                            .expectJSON([])
                                            .expectJSONLength(2)
                                            .after(function() {
                                                frisby.create('DELETE all aliases of dataset1 and check for correct number')
                                                    .delete(json.url + '/aliases')
                                                    .expectStatus(204)
                                                    .after(function() {
                                                        frisby.create('GET dataset1 aliases and check for correct number after removal of aliases')
                                                            .get(json.url + '/aliases')
                                                            .expectStatus(200)
                                                            .expectJSON([])
                                                            .expectJSONLength(0)
                                                            .after(function() {
                                                                frisby.create('PUT dataset1 aliases, to be used for duplicate avoidance check later')
                                                                    .put(json.url + '/aliases', ['theone'], { json: true })
                                                                    .expectStatus(204)
                                                                    .after(function() {
                                                                        frisby.create('POST CSV file and create dataset1-b')
                                                                            .post(URL + 'datasets',
                                                                                csv1Content,
                                                                                {
                                                                                    json: false,
                                                                                    headers: {
                                                                                        'Content-Type': 'text/csv'
                                                                                    }
                                                                                })
                                                                            .expectStatus(202)
                                                                            .expectHeaderContains('Content-Type', 'application/json')
                                                                            .afterJSON(function(json2) {
                                                                                frisby.create('PUT dataset1-b aliases with already existing alias')
                                                                                    .put(json2.url + '/aliases', ['theone'], { json: true })
                                                                                    .expectStatus(400)
                                                                                    .toss();
                                                                            })
                                                                            .toss();
                                                                    })
                                                                    .toss();
                                                            })
                                                            .toss();
                                                    })
                                                    .toss();
                                            })
                                            .toss();
                                    })
                                    .toss();
                            })
                            .toss();
                        frisby.create('GET dataset1 info via alias')
                            .get(URL + 'dataset/dataset1/info')
                            .expectStatus(200)
                            .expectHeaderContains('Content-Type', 'application/json')
                            .expectJSONTypes({
                                rowcount: Number,
                                created: String,
                                columnnames: Array,
                                status: Number
                            })
                            .toss();
                    })
                    .toss();
            })
            .toss();
        frisby.create('GET dataset1 query1 with exact match')
            .get(json.url + "?Name=%C3%85kesson")
            .expectStatus(200)
            .expectHeaderContains('Content-Type', 'application/json')
            .expectJSONTypes([{
                Name: String,
                Comment: String
            }])
            .expectJSON([{
                Name: 'Åkesson',
                Comment: 'Another comment with äöå'
            }])
            .waits(2500)
            .retry(2, 5000)
            .toss();
        frisby.create('GET dataset1 query2 with exact match')
            .get(json.url + "?Some+other+column=x")
            .expectStatus(200)
            .expectHeaderContains('Content-Type', 'application/json')
            .expectJSONTypes([{
                Name: String,
                Telephone: String,
                "Some other column": String,
                Comment: String
            }])
            .expectJSON([{
                Name: 'McLoud',
                Telephone: '0987654321',
                "Some other column": 'x',
                Comment: 'A comment with five words, and a comma'
            }])
            .waits(2500)
            .retry(2, 5000)
            .toss();
        frisby.create('GET dataset1 query3 with regexp')
            .get(json.url + "?Name=(%C3%85%7C%C3%A9)") // decoded: Name=(Å|é)
            .expectStatus(200)
            .expectHeaderContains('Content-Type', 'application/json')
            .expectJSONTypes([{
                Name: String,
                Telephone: String,
                "Some other column": String,
                Comment: String
            }])
            .expectJSONLength(2)
            .waits(2500)
            .retry(2, 5000)
            .toss();
    })
    .toss();

// TODO POST datasets, semi-colon (mixed with colon in values), location header

// TODO GET dataset2, 200, json, check correct amount of rows

// TODO GET dataset2-query, exact match (include åüé), 200, json, check result

// TODO POST datasets, western encoding with åüé, location header

// TODO GET dataset3-query, exact match (include åüé), 200, json, check result

// TODO POST dataset3, append data, check correct amount of rows

// TODO PUT dataset3, replace data, check correct amount of rows

var csv4Path = path.resolve(__dirname, 'data/dataset4_corrupt.csv');
var csv4Content = fs.readFileSync(csv4Path);

frisby.create('POST corrupt CSV file (less columns in first row that in following rows)')
    .post(URL + 'datasets',
        csv4Content,
        {
            json: false,
            headers: {
                'Content-Type': 'text/csv'
            }
        })
    .expectStatus(202)
    .afterJSON(function(json) {
        // TODO wait and load info, check status
    })
    .toss();

// TODO DELETE datasetX (the one that is not needed by the tests above), 200

// TODO rate limitation test