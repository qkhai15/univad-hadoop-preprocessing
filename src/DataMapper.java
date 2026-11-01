import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class DataMapper extends Mapper<Object, Text, Text, Text> {
    private int trainPercent = 20;

    @Override
    protected void setup(Context context) {
        trainPercent = context.getConfiguration().getInt("train.percent", 20);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String path = value.toString().trim();
        if (path.isEmpty() || !path.startsWith("/")) return;

        // Hỗ trợ output từ `hdfs dfs -ls`
        if (!path.toLowerCase().endsWith(".png")) {
            String[] tokens = path.split("\\s+");
            if (tokens.length > 0) {
                String last = tokens[tokens.length - 1];
                if (last.toLowerCase().endsWith(".png")) {
                    path = last;
                } else {
                    return;
                }
            } else {
                return;
            }
        }

        String[] parts = path.split("/");
        if (parts.length < 3) return;

        String imgName = parts[parts.length - 1];
        String specie = parts[parts.length - 2];
        String cls = parts[parts.length - 3];

        boolean isGood = specie.equalsIgnoreCase("good");
        String split;
        int anomaly;

        if (isGood) {
            int h = Math.abs(path.hashCode());
            split = (h % 100 < trainPercent) ? "train" : "test";
            anomaly = 0;
        } else {
            split = "test";
            anomaly = 1;
        }

        // Đường dẫn đích: KHÔNG có ground_truth
        String dstPath = "/output_private_dataset/Private_Dataset_N2_64CNTT2/" + cls + "/" + split + "/" + specie + "/" + imgName;

        // meta.json: mask_path = "" (rỗng) cho tất cả
        String meta = String.format(
            "{\"img_path\":\"%s/%s/%s\",\"mask_path\":\"\",\"cls_name\":\"%s\",\"specie_name\":\"%s\",\"anomaly\":%d}",
            split, specie, imgName,
            escape(cls),
            escape(specie),
            anomaly
        );

        context.write(new Text("SPLIT"), new Text(path + "|" + dstPath + "|" + meta));
    }

    private String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}