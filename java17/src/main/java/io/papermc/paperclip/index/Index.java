package io.papermc.paperclip.index;

import java.util.List;
import java.util.Map;

public record Index(
        Map<String, List<String>> classLookup,
        Map<String, List<String>> resourceLookup
) {

}
