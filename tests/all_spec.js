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
                                    .post(json.url + '/aliases', ['dataset1b'], { json: true })
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
            .waits(5000)
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
            .waits(5000)
            .retry(2, 5000)
            .toss();
    })
    .toss();

// TODO GET dataset1-query, regexp Name = é | Å, 200, json, check result

// TODO POST datasets, semi-colon (mixed with colon in values), location header

// TODO PUT dataset1-alias, recreate alias, 204

// TODO PUT dataset2-alias with same alias as dataset1, 400, json

// TODO GET dataset2, 200, json, check correct amount of rows

// TODO GET dataset2-query, exact match (include åüé), 200, json, check result

// TODO POST datasets, western encoding with åüé, location header

// TODO GET dataset3-query, exact match (include åüé), 200, json, check result

// TODO POST dataset3, append data, check correct amount of rows

// TODO PUT dataset3, replace data, check correct amount of rows

// TODO POST datasets, faulty csv (row 2 has more columns than row 1), 400

// TODO DELETE dataset1, 200

// TODO DELETE dataset2, 200

// TODO DELETE dataset3, 200

// TODO rate limitation test