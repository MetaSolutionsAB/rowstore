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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for "maxuploadsize", which limits the decoded size of an upload (ETL-9.02).
 * Specification: API-3.17, ETL-9.01, ETL-9.02, test specification specs/tests/14-http-compatibility.md
 */
@ExtendWith(UploadSizeLimitExtension.class)
@Tag("upload-size-limit")
@DisplayName("Upload Size Limit Tests")
class UploadSizeLimitIT extends ConfigurableTestBase {

    @Override
    protected String getExtensionBaseUrl() {
        return UploadSizeLimitExtension.getBaseUrl();
    }

    @Test
    @DisplayName("TC-ENCODING-007: maxuploadsize limits the decoded size of an upload")
    void maxUploadSize_limitsDecodedSize() throws IOException {
        int limit = (int) UploadSizeLimitExtension.MAX_UPLOAD_SIZE;
        byte[] small = csv(limit / 2);
        byte[] large = csv(limit * 4);
        byte[] largeGzip = ContentCodings.encode(large, "gzip");
        // the compressed body is below the limit, the decoded body above it
        assertThat(largeGzip.length).isLessThan(limit);

        DatasetUrls urls = createDatasetAndWait(small);
        try {
            List<String> before = TestUtils.getDatasetsList(jsonSpec);

            given().spec(csvSpec).body(large).post("/datasets").then().statusCode(413);
            upload("POST", "/datasets", largeGzip).then().statusCode(413);
            upload("PUT", urls.datasetUrl, largeGzip).then().statusCode(413);

            assertThat(TestUtils.getDatasetsList(jsonSpec)).containsExactlyInAnyOrderElementsOf(before);
            given().spec(jsonSpec).get(urls.infoUrl).then()
                    .body("status", equalTo(ETL_STATUS_AVAILABLE))
                    .body("rowcount", equalTo(rowCount(small)));

            // a compressed upload whose decoded size is within the limit is loaded
            upload("PUT", urls.datasetUrl, ContentCodings.encode(small, "gzip")).then().statusCode(202);
            waitForDatasetAvailable(urls.infoUrl);
            given().spec(jsonSpec).get(urls.infoUrl).then().body("rowcount", equalTo(rowCount(small)));
        } finally {
            deleteDataset(urls.datasetUrl);
        }
    }

    private Response upload(String method, String path, byte[] gzipBody) {
        return given().spec(csvSpec).header("Content-Encoding", "gzip").body(gzipBody).request(method, path);
    }

    /**
     * @return Returns a CSV of at least the given size, with equal rows, which compress well.
     */
    private static byte[] csv(int size) {
        StringBuilder csv = new StringBuilder("name,value\n");
        while (csv.length() < size) {
            csv.append("same name,same value\n");
        }
        return csv.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static int rowCount(byte[] csv) {
        return (int) new String(csv, StandardCharsets.UTF_8).lines().count() - 1;
    }

}
