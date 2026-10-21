/*
 * Copyright (c) 2011-2026 MetaSolutions AB <info@metasolutions.se>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.entrystore.rowstore;

import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test cases for byte order marks and soft hyphens in CSV headers, shared by the tests of the default and the legacy
 * parser, which must behave the same. Specification: 04-csv-formats.md (ETL-4.06, ETL-4.07)
 */
final class CsvBomTestCases {

    static final String BOM = "\uFEFF";

    static final String SHY = "\u00AD";

    private final RequestSpecification csvSpec;

    private final RequestSpecification jsonSpec;

    CsvBomTestCases(RequestSpecification csvSpec, RequestSpecification jsonSpec) {
        this.csvSpec = csvSpec;
        this.jsonSpec = jsonSpec;
    }

    /**
     * The case of catalog.goteborg.se: a UTF-8 byte order mark directly followed by a quoted first label.
     */
    void utf8BomWithQuotedFirstLabel_yieldsCleanColumnNames() {
        assertCleanDataset(bytes(BOM + "\"kopare_id\",namn\n1,Anna\n2,Bo\n", StandardCharsets.UTF_8));
    }

    void utf8BomWithUnquotedFirstLabel_yieldsCleanColumnNames() {
        assertCleanDataset(bytes(BOM + "kopare_id,namn\n1,Anna\n2,Bo\n", StandardCharsets.UTF_8));
    }

    void utf8BomWithSemicolons_detectsSeparator() {
        assertCleanDataset(bytes(BOM + "\"kopare_id\";\"namn\"\n1;Anna\n2;Bo\n", StandardCharsets.UTF_8));
    }

    /**
     * Charset detection returns UTF-16LE for this file, whose decoder keeps the byte order mark.
     */
    void utf16LeBomWithQuotedFirstLabel_yieldsCleanColumnNames() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(new byte[]{(byte) 0xFF, (byte) 0xFE});
        out.writeBytes(bytes("\"kopare_id\",namn\n1,Anna\n2,Bo\n", StandardCharsets.UTF_16LE));
        assertCleanDataset(out.toByteArray());
    }

    /**
     * Soft hyphens are removed from labels, but kept in values.
     */
    void softHyphensInLabels_areRemoved() {
        TestUtils.DatasetUrls urls = TestUtils.createDatasetAndWait(
                bytes("ko" + SHY + "pare_id,na" + SHY + "mn" + SHY + "\n1,An" + SHY + "na\n", StandardCharsets.UTF_8), csvSpec, jsonSpec);
        try {
            assertThat(columnNames(urls.infoUrl)).containsExactlyInAnyOrder("kopare_id", "namn");
            assertThat(rows(urls.datasetUrl, "kopare_id", "1")).containsExactly(Map.of("kopare_id", "1", "namn", "An" + SHY + "na"));
        } finally {
            TestUtils.deleteDataset(urls.datasetUrl);
        }
    }

    /**
     * Appending a file with byte order mark and quoted first label, as a client refreshing such a dataset does,
     * matches the column names of the dataset.
     */
    void appendWithBom_matchesColumnNames() {
        byte[] csv = bytes(BOM + "\"kopare_id\",namn\n1,Anna\n2,Bo\n", StandardCharsets.UTF_8);
        TestUtils.DatasetUrls urls = TestUtils.createDatasetAndWait(csv, csvSpec, jsonSpec);
        try {
            given().spec(csvSpec).body(bytes(BOM + "\"kopare_id\",namn\n3,Cecilia\n", StandardCharsets.UTF_8))
                    .post(urls.datasetUrl).then().statusCode(202);
            TestUtils.waitForDatasetAvailable(urls.infoUrl, jsonSpec);
            given().spec(jsonSpec).get(urls.infoUrl).then().body("rowcount", equalTo(3));
            assertThat(columnNames(urls.infoUrl)).containsExactlyInAnyOrder("kopare_id", "namn");
            assertThat(rows(urls.datasetUrl, "kopare_id", "3")).containsExactly(Map.of("kopare_id", "3", "namn", "Cecilia"));
        } finally {
            TestUtils.deleteDataset(urls.datasetUrl);
        }
    }

    /**
     * A first line that consists of a byte order mark only is an empty line (ETL-4.05), not a header.
     */
    void bomOnlyFirstLine_isEmptyLine() {
        Response response = TestUtils.createDataset(bytes(BOM + "\nkopare_id,namn\n1,Anna\n", StandardCharsets.UTF_8), csvSpec);
        response.then().statusCode(202);
        String datasetUrl = TestUtils.getDatasetUrl(response);
        try {
            TestUtils.waitForDatasetError(TestUtils.getInfoUrl(response), jsonSpec);
        } finally {
            TestUtils.deleteDataset(datasetUrl);
        }
    }

    private void assertCleanDataset(byte[] csv) {
        Response response = TestUtils.createDataset(csv, csvSpec);
        response.then().statusCode(202);
        String datasetUrl = TestUtils.getDatasetUrl(response);
        String datasetId = response.jsonPath().getString("id");
        try {
            TestUtils.waitForDatasetAvailable(TestUtils.getInfoUrl(response), jsonSpec);
            assertThat(columnNames(TestUtils.getInfoUrl(response))).containsExactlyInAnyOrder("kopare_id", "namn");
            assertThat(rows(datasetUrl, "kopare_id", "1")).containsExactly(Map.of("kopare_id", "1", "namn", "Anna"));
            assertThat(swaggerParameterNames(datasetUrl, datasetId)).contains("kopare_id", "namn")
                    .allSatisfy(name -> assertThat(name).doesNotContain(BOM, "\""));
        } finally {
            TestUtils.deleteDataset(datasetUrl);
        }
    }

    private List<String> columnNames(String infoUrl) {
        return given().spec(jsonSpec).get(infoUrl)
                .then().statusCode(200)
                .extract().jsonPath().getList("columnnames", String.class);
    }

    private List<Map<String, Object>> rows(String datasetUrl, String column, String value) {
        return given().spec(jsonSpec)
                .queryParam(column, value)
                .get(datasetUrl)
                .then().statusCode(200)
                .extract().jsonPath().getList("results");
    }

    private List<String> swaggerParameterNames(String datasetUrl, String datasetId) {
        String body = given().spec(jsonSpec).get(datasetUrl + "/swagger")
                .then().statusCode(200)
                .extract().asString();
        JSONArray parameters = new JSONObject(body).getJSONObject("paths").getJSONObject("/dataset/" + datasetId)
                .getJSONObject("get").getJSONArray("parameters");
        List<String> names = new ArrayList<>();
        for (int i = 0; i < parameters.length(); i++) {
            names.add(parameters.getJSONObject(i).getString("name"));
        }
        return names;
    }

    private static byte[] bytes(String csv, Charset charset) {
        return csv.getBytes(charset);
    }

}
