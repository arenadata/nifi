/*
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
package org.apache.nifi.gpfdist.service;

import org.apache.nifi.gpfdist.metadata.RecordProcessorLoadingResult;

import java.util.List;
import java.util.Objects;

public class LoadResult {
    private final String loadId;
    private final boolean aborted;
    private final List<RecordProcessorLoadingResult> results;
    private final List<Throwable> errors;

    public LoadResult(String loadId, boolean aborted, List<RecordProcessorLoadingResult> results, List<Throwable> errors) {
        this.loadId = loadId;
        this.aborted = aborted;
        this.results = results == null ? List.of() : List.copyOf(results);
        this.errors = errors == null ? List.of() : List.copyOf(errors);
    }

    public String getLoadId() {
        return loadId;
    }

    public boolean isAborted() {
        return aborted;
    }

    public List<RecordProcessorLoadingResult> getResults() {
        return results;
    }

    public List<Throwable> getErrors() {
        return errors;
    }

    public boolean isSuccess() {
        return !aborted && (errors == null || errors.isEmpty());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LoadResult result = (LoadResult) o;
        return Objects.equals(loadId, result.loadId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(loadId);
    }

    @Override
    public String toString() {
        return "LoadResult{" +
                "loadId='" + loadId + '\'' +
                ", aborted=" + aborted +
                ", results=" + results +
                ", errors=" + errors +
                '}';
    }
}
