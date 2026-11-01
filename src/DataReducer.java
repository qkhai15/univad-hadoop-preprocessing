import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataReducer extends Reducer<Text, Text, NullWritable, Text> {

    private FileSystem fs;
    private final Map<String, Map<String, List<Map<String, Object>>>> meta = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        fs = FileSystem.get(context.getConfiguration());
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text v : values) {
            String[] parts = v.toString().split("\\|", 3);
            if (parts.length < 3) continue;

            String srcPathStr = parts[0].trim();
            String dstPathStr = parts[1].trim();
            String jsonInfo = parts[2].trim();

            Path src = new Path(srcPathStr);
            Path dst = new Path(dstPathStr);
            Path dstParent = dst.getParent();

            if (!fs.exists(dstParent)) {
                fs.mkdirs(dstParent);
            }

            if (!fs.exists(dst)) {
                try {
                    if (fs.exists(src)) {
                        FileUtil.copy(fs, src, fs, dst, false, context.getConfiguration());
                    } else {
                        String[] toks = srcPathStr.split("\\s+");
                        Path altSrc = new Path(toks[toks.length - 1]);
                        if (fs.exists(altSrc)) {
                            FileUtil.copy(fs, altSrc, fs, dst, false, context.getConfiguration());
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Copy failed: " + srcPathStr + " -> " + dstPathStr);
                }
            }

            String[] dstParts = dstPathStr.split("/");
            if (dstParts.length < 4) continue;

            String cls = dstParts[dstParts.length - 4];
            String split = dstParts[dstParts.length - 3];

            meta.computeIfAbsent(split, k -> new HashMap<>())
                .computeIfAbsent(cls, k -> new ArrayList<>())
                .add(parseJsonItem(jsonInfo));
        }
    }

    // HÀM MỚI – SIÊU ỔN ĐỊNH – KHÔNG DÙNG REGEX PHỨC TẠP
    private Map<String, Object> parseJsonItem(String json) {
        Map<String, Object> map = new HashMap<>();
        if (json == null || json.isEmpty()) return map;

        // Bỏ { và }
        json = json.trim();
        if (json.startsWith("{") && json.endsWith("}")) {
            json = json.substring(1, json.length() - 1);
        }

        String[] pairs = json.split(",(?=\\s*\"\\w+\"\\s*:)");
        for (String pair : pairs) {
            pair = pair.trim();
            if (pair.isEmpty()) continue;

            int colonIndex = pair.indexOf(':');
            if (colonIndex == -1) continue;

            String rawKey = pair.substring(0, colonIndex).trim();
            String rawValue = pair.substring(colonIndex + 1).trim();

            // Lấy key
            String key = rawKey;
            if (key.startsWith("\"") && key.endsWith("\"")) {
                key = key.substring(1, key.length() - 1);
            }

            // Lấy value
            String value = rawValue;
            if (value.startsWith("\"") && value.endsWith("\"")) {
                value = value.substring(1, value.length() - 1);
            }

            // Xử lý escape
            value = value.replace("\\\"", "\"").replace("\\\\", "\\");

            // Gán giá trị
            if ("anomaly".equals(key)) {
                map.put(key, Integer.parseInt(value));
            } else {
                map.put(key, value);
            }
        }
        return map;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder("{\n");

        String[] splits = {"train", "test"};
        for (int i = 0; i < splits.length; i++) {
            String split = splits[i];
            if (i > 0) sb.append(",\n");
            sb.append("  \"").append(split).append("\": {\n");

            Map<String, List<Map<String, Object>>> classes = meta.getOrDefault(split, new HashMap<>());
            List<String> sortedKeys = new ArrayList<>(classes.keySet());
            Collections.sort(sortedKeys);

            for (int c = 0; c < sortedKeys.size(); c++) {
                String cls = sortedKeys.get(c);
                if (c > 0) sb.append(",\n");
                sb.append("    \"").append(cls).append("\": [\n");

                List<Map<String, Object>> items = classes.get(cls);
                for (int j = 0; j < items.size(); j++) {
                    Map<String, Object> item = items.get(j);
                    sb.append("      {\n");
                    sb.append("        \"img_path\": \"").append(item.get("img_path")).append("\",\n");
                    sb.append("        \"mask_path\": \"\",\n");
                    sb.append("        \"cls_name\": \"").append(item.get("cls_name")).append("\",\n");
                    sb.append("        \"specie_name\": \"").append(item.get("specie_name")).append("\",\n");
                    sb.append("        \"anomaly\": ").append(item.get("anomaly")).append("\n");
                    sb.append("      }");
                    if (j < items.size() - 1) sb.append(",");
                    sb.append("\n");
                }
                sb.append("    ]\n");
            }
            sb.append("  }\n");
        }
        sb.append("}\n");

        context.write(NullWritable.get(), new Text(sb.toString()));
    }
}