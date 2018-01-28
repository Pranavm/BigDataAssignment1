
// Downloads zip file from web to hadoop
// And decompresses it
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

public class DownLoadFromWebToHadoop {

	public static String getFullPath(String dir, String fileName) {
		if (dir.charAt(dir.length() - 1) == '/') {
			dir += fileName;
		} else {
			dir += "/" + fileName;
		}
		return dir;
	}

	public static void download(String dir, String src) throws IOException {
		URL url = null;
		url = new URL(src);

		String[] path = src.split("/");

		String dst = getFullPath(dir, path[path.length - 1]);

		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));

		InputStream in = null;
		try {
			in = url.openStream();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		FileSystem fs = null;

		fs = FileSystem.get(URI.create(dst), conf);

		OutputStream out = null;
		out = fs.create(new Path(dst), new Progressable() {
			public void progress() {
				System.out.print(".");
			}
		});

		IOUtils.copyBytes(in, out, 4096, true);

	}

	public static void decompress(String dir, String fileName) {

		String uri = getFullPath(dir, fileName);

		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(uri), conf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		Path inputPath = new Path(uri);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(inputPath);
		if (codec == null) {
			System.err.println("No codec found for " + uri);
			System.exit(1);
		}

		String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());

		InputStream in = null;
		OutputStream out = null;

		try {
			in = codec.createInputStream(fs.open(inputPath));
			out = fs.create(new Path(outputUri));
			IOUtils.copyBytes(in, out, conf);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}

	private static void delete(String dir, String fileName) {
		// TODO Auto-generated method stub
		String uri = getFullPath(dir, fileName);

		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));

		FileSystem fs = null;
		Path path = new Path(uri);
		try {
			fs = FileSystem.get(conf);
			fs.delete(path, false);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		String dir = args[0];
		for (int i = 1; i < args.length; ++i) {
			String[] filePath = args[i].split("/");
			String zipFileName = filePath[filePath.length - 1];

			download(dir, args[i]);
			decompress(dir, zipFileName);
			delete(dir, zipFileName);
		}
	}
}