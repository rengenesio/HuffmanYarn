package br.ufrj.ppgi.huffmanyarn.encoder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;


public class Encoder {
	// Configuration
	private Configuration conf;
	
	private int id;
	private String fileName;
	
	private byte[][] memory;
	
	public Encoder(int id, String fileName) {
		this.conf = new Configuration();
		
		this.id = id;
		this.fileName = fileName;
	}
	
	public void encode() throws IOException {
		chunksToMemory();
	}

	private void chunksToMemory() throws IOException {
//		Path path = new Path(fileName);
//		FileSystem fs = FileSystem.get(new Configuration());
//		FileStatus[] status = fs.listStatus(path);
//		
//		for(short i = 1 ; i < status.length ; i++) {
//			FSDataInputStream f = fs.open(status[i].getPath());
//			while(f.available() > 0) {
//				int symbol = f.readInt();
//				frequency[symbol] = f.readLong();
//				symbols++;
//			}
//		}
//		
//		frequency[Defines.EOF] = 1;
//		symbols++;
		
		
//		// Search blocks from file
//		Path path = new Path(fileName);
//		FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
//		FileStatus fileStatus = fileSystem.getFileStatus(path);
//		BlockLocation[] blockLocationArray = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
//		
//		for(BlockLocation blockLocation : blockLocationArray) {
//			String[] blockLocationHostNameArray = blockLocation.getHosts();
//			for(String hostName : blockLocationHostNameArray) {
//				if(hostName == InetAddress.getLocalHost().getHostName()) {
//					System.out.println("Sou o " + InetAddress.getLocalHost().getHostName() + ". O bloco está na minha parte:" + blockLocation.getOffset() + "," + blockLocation.getLength());
//				}
//				else {
//					System.out.println("Sou o " + InetAddress.getLocalHost().getHostName() + ". O bloco NÃO está na minha parte:" + blockLocation.getOffset() + "," + blockLocation.getLength());
//				}
//			}
//		}
	}



	public static void main(String[] args) throws IOException {
		//Encoder encoder = new Encoder(0, args[0]);
		//encoder.encode();
		
		System.out.println("TESTANDOOOOOOOOOOOOOO!!!");
		int i = 0;
		for(String s : args) {
			System.out.println("Args[" + i++ + "]: " + s);
		}
		Runtime.getRuntime().exec("/usr/bin/sleep 20");

		
		// Resolver for hostname/rack
		
		
		// Search blocks from file
//		Path path = new Path(fileName);
//		FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
//		FileStatus fileStatus = fileSystem.getFileStatus(path);
//		BlockLocation[] blkLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		
		
//		for(short i = 1 ; i < status.length ; i++) {
//			FSDataInputStream f = fs.open(status[i].getPath());
//			while(f.available() > 0) {
//				int symbol = f.readInt();
//				frequency[symbol] = f.readLong();
//				symbols++;
//			}
//		}
	}
}
