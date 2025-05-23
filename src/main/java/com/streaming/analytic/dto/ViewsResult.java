// com.streaming.analytics.dto.ViewsResult.java
package com.streaming.analytic.dto;

import java.util.List;
import java.util.Map;

public class ViewsResult {
    public String type;
    public List<String> labels;
    public List<Map<String, Object>> datasets;

    public ViewsResult(String type, String labelName, List<String> labels, List<Long> data) {
        this.type = type;
        this.labels = labels;
        this.datasets = List.of(
                Map.of("label", labelName, "data", data)
        );
    }
}
