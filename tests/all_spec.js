/*
 RowStore REST API tests

 Requires a configured and running RowStore instance.

 Author: hannes@metasolutions.se
 */

var frisby = require('frisby');
var fs = require('fs');
var path = require('path');

// The base URL of the (empty or uninitialized) RowStore instance to be tested
var URL = 'http://localhost:8282/';

// Retrying tests is important since dataset population
// may be delayed due to its asynchronous character
var retryCount = 2;         // How often failed tests are retried
var retryDelay = 2500;      // Waiting time after a test has failed until next attempt
var initialDelay = 5000;    // Time to wait after a CSV has been submitted

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
  .afterJSON(function (json) {
    frisby.create('GET dataset1 info')
      .get(json.info)
      .expectStatus(200)
      .expectHeaderContains('Content-Type', 'application/json')
      .expectJSONTypes({
        rowcount: Number,
        created: String,
        columnnames: Array,
        status: Number,
                @id: String,
                @context: String
  })
    .
    expectJSON({
      rowcount: 5,
      status: 3
    })
      .waits(initialDelay)
      .retry(retryCount, retryDelay)
      .toss();
    frisby.create('GET dataset1 aliases')
      .get(json.url + '/aliases')
      .expectStatus(200)
      .expectHeaderContains('Content-Type', 'application/json')
      .expectJSON([])
      .expectJSONLength(0)
      .after(function () {
        frisby.create('PUT dataset1 aliases')
          .put(json.url + '/aliases', ['dataset1'], {json: true})
          .expectStatus(204)
          .after(function () {
            frisby.create('GET dataset1 aliases and check for correct number 1')
              .get(json.url + '/aliases')
              .expectStatus(200)
              .expectHeaderContains('Content-Type', 'application/json')
              .expectJSON([])
              .expectJSONLength(1)
              .after(function () {
                frisby.create('POST dataset1 aliases')
                  .post(json.url + '/aliases', ['dataset1b', 'dataset1b'], {json: true}) // intentionally providing the same alias twice as we want to test the behavior
                  .expectStatus(204)
                  .after(function () {
                    frisby.create('GET dataset1 aliases and check for correct number 2')
                      .get(json.url + '/aliases')
                      .expectStatus(200)
                      .expectJSON([])
                      .expectJSONLength(2)
                      .after(function () {
                        frisby.create('DELETE all aliases of dataset1 and check for correct number')
                          .delete(json.url + '/aliases')
                          .expectStatus(204)
                          .after(function () {
                            frisby.create('GET dataset1 aliases and check for correct number after removal of aliases')
                              .get(json.url + '/aliases')
                              .expectStatus(200)
                              .expectJSON([])
                              .expectJSONLength(0)
                              .after(function () {
                                frisby.create('PUT dataset1 aliases, to be used for duplicate avoidance check later')
                                  .put(json.url + '/aliases', ['theone'], {json: true})
                                  .expectStatus(204)
                                  .after(function () {
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
                                      .afterJSON(function (json2) {
                                        frisby.create('PUT dataset1-b aliases with already existing alias')
                                          .put(json2.url + '/aliases', ['theone'], {json: true})
                                          .expectStatus(400)
                                          .after(function () {
                                            frisby.create('DELETE dataset1-b')
                                              .delete(json2.url)
                                              .expectStatus(204)
                                              .waits(initialDelay * 2) // we give it a bit more time as it fails with Bitbucket Pipelines
                                              .retry(retryCount, retryDelay * 2)
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
      .expectJSONTypes("results", [{
        name: String,
        comment: String,
        "some other column": String,
        comment: String
      }])
      .expectJSON("results", [{name: 'Åkesson', comment: 'Another comment with äöå'}])
      .expectJSONLength("results", 1)
      .waits(initialDelay)
      .retry(retryCount, retryDelay)
      .toss();
    frisby.create('GET dataset1 query1-b (lower case key) with exact match')
      .get(json.url + "?name=%C3%85kesson")
      .expectStatus(200)
      .expectHeaderContains('Content-Type', 'application/json')
      .expectJSONTypes("results", [{
        name: String,
        comment: String,
        "some other column": String,
        comment: String
      }])
      .expectJSONLength("results", 1)
      .waits(initialDelay)
      .retry(retryCount, retryDelay)
      .toss();
    frisby.create('GET dataset1 query2 with exact match')
      .get(json.url + "?Some+other+column=x")
      .expectStatus(200)
      .expectHeaderContains('Content-Type', 'application/json')
      .expectJSONTypes("results", [{
        name: String,
        telephone: String,
        "some other column": String,
        comment: String
      }])
      .expectJSON("results", [{
        name: 'McLoud',
        telephone: '0987654321',
        "some other column": 'x',
        comment: 'A comment with five words, and a comma'
      }])
      .waits(initialDelay)
      .retry(retryCount, retryDelay)
      .toss();
    frisby.create('GET dataset1 query3 with regexp')
      .get(json.url + "?Name=(%C3%85%7C%C3%A9)") // decoded: Name=(Å|é)
      .expectStatus(200)
      .expectHeaderContains('Content-Type', 'application/json')
      .expectJSONTypes("results", [{
        name: String,
        telephone: String,
        "some other column": String,
        comment: String
      }])
      .expectJSONLength("results", 2)
      .waits(initialDelay)
      .retry(retryCount, retryDelay)
      .toss();
    frisby.create('GET dataset1 query4 (key does not match anything)')
      .get(json.url + "?nonexistingkey=test")
      .expectStatus(400)
      .waits(initialDelay)
      .retry(retryCount, retryDelay)
      .toss();
    frisby.create('GET dataset1 pagination test 1')
      .get(json.url + "?Name=(%C3%85%7C%C3%A9)&_limit=1") // decoded: Name=(Å|é)
      .expectStatus(200)
      .expectHeaderContains('Content-Type', 'application/json')
      .expectJSONTypes({
        results: Array,
        offset: Number,
        limit: Number,
        resultCount: Number
      })
      .expectJSON({
        offset: 0,
        resultCount: 2,
        limit: 1,
        results: [{
          name: 'Béringer'
        }]
      })
      .expectJSONLength("results", 1)
      .waits(initialDelay)
      .retry(retryCount, retryDelay)
      .toss();
    frisby.create('GET dataset1 pagination test 2')
      .get(json.url + "?Name=(%C3%85%7C%C3%A9)&_limit=1&_offset=1") // decoded: Name=(Å|é)
      .expectStatus(200)
      .expectHeaderContains('Content-Type', 'application/json')
      .expectJSONTypes({
        results: Array,
        offset: Number,
        limit: Number,
        resultCount: Number
      })
      .expectJSON({
        offset: 1,
        resultCount: 2,
        limit: 1,
        results: [{
          name: 'Åkesson'
        }]
      })
      .expectJSONLength("results", 1)
      .waits(initialDelay)
      .retry(retryCount, retryDelay)
      .toss();
  })
  .toss();

var csv2Path = path.resolve(__dirname, 'data/dataset2_utf8_semicolon.csv');
var csv2Content = fs.readFileSync(csv2Path);

frisby.create('POST CSV file (UTF-8, semicolon-separated) to create new dataset2')
  .post(URL + 'datasets',
    csv2Content,
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
  .afterJSON(function (json) {
    frisby.create('GET dataset2 info')
      .get(json.info)
      .expectStatus(200)
      .expectHeaderContains('Content-Type', 'application/json')
      .expectJSONTypes({
        rowcount: Number,
        created: String,
        columnnames: Array,
        status: Number
      })
      .expectJSON({
        rowcount: 5,
        status: 3
      })
      .after(function () {
        frisby.create('GET dataset2 query1 with exact match')
          .get(json.url + "?Name=B%C3%A9ringer")
          .expectStatus(200)
          .expectHeaderContains('Content-Type', 'application/json')
          .expectJSONTypes("results", [{
            name: String,
            comment: String
          }])
          .expectJSON("results", [{
            name: 'Béringer',
            comment: 'No, no comment'
          }])
          .toss();
      })
      .waits(initialDelay)
      .retry(retryCount, retryDelay)
      .toss();
  })
  .toss();

var csv3Path = path.resolve(__dirname, 'data/dataset3_windows1252.csv');
var csv3Content = fs.readFileSync(csv3Path);

frisby.create('POST CSV file (Windows-1252, comma-separated) to create new dataset3')
  .post(URL + 'datasets',
    csv3Content,
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
  .afterJSON(function (json) {
    frisby.create('GET dataset3 info')
      .get(json.info)
      .expectStatus(200)
      .expectHeaderContains('Content-Type', 'application/json')
      .expectJSONTypes({
        rowcount: Number,
        created: String,
        columnnames: Array,
        status: Number
      })
      .expectJSON({
        rowcount: 5,
        status: 3
      })
      .after(function () {
        frisby.create('GET dataset3 query1 with exact match')
          .get(json.url + "?Name=%C3%85kesson")
          .expectStatus(200)
          .expectHeaderContains('Content-Type', 'application/json')
          .expectJSONTypes("results", [{
            name: String,
            comment: String
          }])
          .expectJSON("results", [{
            name: 'Åkesson',
            comment: 'Another comment with äöå'
          }])
          .after(function () {
            frisby.create('POST CSV file (UTF-8, semicolon-separated) to append to dataset3')
              .post(json.url,
                csv2Content, // CSV2 is UTF-8 with semicolons
                {
                  json: false,
                  headers: {
                    'Content-Type': 'text/csv'
                  }
                })
              .expectStatus(202)
              .expectHeaderContains('Content-Type', 'application/json')
              .after(function () {
                frisby.create('GET dataset3 info to check amount of rows post-append')
                  .get(json.info)
                  .expectStatus(200)
                  .expectHeaderContains('Content-Type', 'application/json')
                  .expectJSONTypes({
                    rowcount: Number,
                    created: String,
                    columnnames: Array,
                    status: Number
                  })
                  .expectJSON({
                    rowcount: 10,
                    status: 3
                  })
                  .after(function () {
                    frisby.create('PUT CSV file (Windows-1252, comma-separated) to replace dataset3 post-append')
                      .put(json.url,
                        csv3Content,
                        {
                          json: false,
                          headers: {
                            'Content-Type': 'text/csv'
                          }
                        })
                      .expectStatus(202)
                      .expectHeaderContains('Content-Type', 'application/json')
                      .after(function () {
                        frisby.create('GET dataset3 info to check amount of rows post-append replacement')
                          .get(json.info)
                          .expectStatus(200)
                          .expectHeaderContains('Content-Type', 'application/json')
                          .expectJSONTypes({
                            rowcount: Number,
                            created: String,
                            columnnames: Array,
                            status: Number
                          })
                          .expectJSON({
                            rowcount: 5,
                            status: 3
                          })
                          .waits(initialDelay)
                          .retry(retryCount, retryDelay)
                          .toss();
                      })
                      .toss();
                  })
                  .waits(initialDelay)
                  .retry(retryCount, retryDelay)
                  .toss();
              })
              .toss();
          })
          .toss();
      })
      .waits(initialDelay)
      .retry(retryCount, retryDelay)
      .toss();
  })
  .toss();

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
  .afterJSON(function (json) {
    frisby.create('GET corrupt dataset status')
      .get(json.info)
      .expectStatus(200)
      .expectHeaderContains('Content-Type', 'application/json')
      .expectJSON({
        status: 4
      })
      .waits(initialDelay)
      .retry(retryCount, retryDelay)
      .toss();
  })
  .toss();

var csv5Path = path.resolve(__dirname, 'data/dataset1_utf8_emptycolumn.csv');
var csv5Content = fs.readFileSync(csv5Path);

frisby.create('POST CSV file (UTF-8, comma-separated, empty column label) to create new dataset5')
  .post(URL + 'datasets',
    csv5Content,
    {
      json: false,
      headers: {
        'Content-Type': 'text/csv'
      }
    })
  .expectStatus(202)
  .expectHeaderContains('Content-Type', 'application/json')
  .afterJSON(function (json) {
    frisby.create('GET dataset5 info')
      .get(json.info)
      .expectStatus(200)
      .expectHeaderContains('Content-Type', 'application/json')
      .expectJSONLength("columnnames", 4)
      .waits(initialDelay)
      .retry(retryCount, retryDelay)
      .toss();
  })
  .toss();

// TODO test rate limitation