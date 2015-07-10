import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.LinkedList;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3 {
	public static class MyMapper extends TableMapper<Text, Text> {

		Text p_outKey = new Text("");
		Text p_outVal = new Text("");
		String p_fileName = new String("");
		boolean first = true;
		int i = 0;

		@Override
		protected void cleanup(Context context) {

			try {
				context.write(p_outKey, p_outVal);
				context.write(new Text("zzzzzz#zz#zz"), new Text("0#0.0"));
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		public void map(ImmutableBytesWritable row, Result value,
				Context context) {

			Text outKey = new Text();
			Text outVal = new Text();

			String fileName = new String(value.getValue(Bytes.toBytes("stock"),
					Bytes.toBytes("name")));
			String yr = new String(value.getValue(Bytes.toBytes("time"),
					Bytes.toBytes("yr")));
			String mm = new String(value.getValue(Bytes.toBytes("time"),
					Bytes.toBytes("mm")));
			String dd = new String(value.getValue(Bytes.toBytes("time"),
					Bytes.toBytes("dd")));
			String price = new String(value.getValue(Bytes.toBytes("price"),
					Bytes.toBytes("price")));

			outKey.set(fileName + "#" + yr + "#" + mm);
			outVal.set(dd + "#" + price);

			if (first == true) {
				try {
					context.write(outKey, outVal);
					first = false;
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			if (!p_outKey.equals(outKey) && i > 0) {
				try {
					context.write(p_outKey, p_outVal);
					context.write(outKey, outVal);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			i++;
			p_outKey.set(outKey);
			p_outVal.set(outVal);
			p_fileName = fileName;

		}

	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		LinkedList<Double> xi = new LinkedList<Double>();
		LinkedList<String> vol = new LinkedList<String>();
		String p_stock = new String("");

		@Override
		protected void cleanup(Context context) {
			try {
				Collections.sort(vol);
				context.write(new Text(
						"The top 10 stocks with highest volatlity:"), new Text(
						""));
				for (int i = vol.size() - 1; i > vol.size() - 11; i--) {
					context.write(new Text(vol.get(i).split("#")[1]), new Text(
							""));
				}
				context.write(new Text(
						"The top 10 stocks with lowest volatality:"), new Text(
						""));
				for (int i = 0; i < 10; i++) {
					context.write(new Text(vol.get(i).split("#")[1]), new Text(
							""));
				}
			} catch (IOException e) {

			} catch (InterruptedException e) {

			}
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String stock = key.toString().split("#")[0];
			int day1 = 0, day2 = 0;
			DecimalFormat df = new DecimalFormat("#.########");
			double p1, p2, sum = 0.0, sum1 = 0.0, xbar = 0.0;

			LinkedList<Text> inVal = new LinkedList<Text>();

			if (!p_stock.equalsIgnoreCase(stock) && xi.size() > 0) {

				for (int i = 0; i < xi.size(); i++)
					sum += xi.get(i);
				xbar = sum / xi.size();

				for (int i = 0; i < xi.size(); i++) {
					double temp = xi.get(i) - xbar;
					sum1 += temp * temp;
				}
				double tmp = 0.0;
				if (xi.size() > 1)
					tmp = Math.sqrt(sum1 / (xi.size() - 1));
				if (tmp > 0.0)
					vol.add(df.format(tmp) + "#" + p_stock);
				inVal.clear();
				xi.clear();
			}
			p_stock = stock;
			for (Text val : values) {
				String temp = val.toString();
				inVal.add(new Text(temp));
			}
			if (inVal.size() > 1) {
				day1 = Integer.parseInt(inVal.get(0).toString().split("#")[0]);
				day2 = Integer.parseInt(inVal.get(1).toString().split("#")[0]);
				if (day1 > day2) {
					p1 = Double
							.parseDouble(inVal.get(0).toString().split("#")[1]);
					p2 = Double
							.parseDouble(inVal.get(1).toString().split("#")[1]);
				} else {
					p1 = Double
							.parseDouble(inVal.get(1).toString().split("#")[1]);
					p2 = Double
							.parseDouble(inVal.get(0).toString().split("#")[1]);
				}
				xi.add((p1 - p2) / p2);
				inVal.clear();
			}

		}

	}
}

