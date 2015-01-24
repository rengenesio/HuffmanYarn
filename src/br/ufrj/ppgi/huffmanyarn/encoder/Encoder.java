package br.ufrj.ppgi.huffmanyarn.encoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Stack;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public final class Encoder {
	// Configuration
	private Configuration conf;
	
	private String fileName;
	private long inputOffset;
	private int inputLength;
	private String masterHostName;
	private int numTotalContainers;
	private int slavePort;
	private HostPortPair[] hostPortPairArray;
	
	private byte[] memory;
	private int[] frequency;
	private long[] totalFrequency;
	private short symbols = 0;
	private NodeArray nodeArray;
	private Codification[] codificationArray;
	//private CodificationArray codificationCollection;
	
	
	public Encoder(String[] args) {
		this.conf = new Configuration();
		
		this.fileName = args[0];
		this.inputOffset = Long.parseLong(args[1]);
		this.inputLength = Integer.parseInt(args[2]);
		this.masterHostName = args[3];
		this.numTotalContainers = Integer.parseInt(args[4]);
	}
	
	public void encode() throws IOException, InterruptedException {
		chunksToMemory();
		memoryToFrequency();
		
		if(this.inputOffset == 0) {
			this.hostPortPairArray = new HostPortPair[numTotalContainers - 1];
			
			for(int i = 0 ; i < numTotalContainers -1 ; i++) {
				System.out.println("Master aguardando client: " + i);
				ServerSocket serverSocket = new ServerSocket(9996);
			    Socket clientSocket = serverSocket.accept();
			    DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
			    for(int j = 0 ; j < 256 ; j++) {
			    	totalFrequency[j] += dataInputStream.readInt();
			    }
			    DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());
			    dataOutputStream.writeInt(3025 + i);
			    hostPortPairArray[i] = new HostPortPair(clientSocket.getInetAddress().getHostName(), 3025 + i);			    
			    
			    System.out.println("Master recebeu do client: " + i);
 			    serverSocket.close();
			}
			
			frequencyToNodeArray();
			huffmanEncode();
			treeToCode();
		}
		else {
			Socket socket;
			System.out.println("Client tentando conectar com master");
			while(true) {
				try {
					socket = new Socket(this.masterHostName, 9996);
					break;
				} catch(Exception e) {
					Thread.sleep(1000);
				}
			}
			DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
			for(int i = 0 ; i < 256 ; i++) {
				dataOutputStream.writeInt(frequency[i]);;
			}
			DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
			this.slavePort = dataInputStream.readInt();
			System.out.println("Client enviou para o master");
			socket.close();
		}
		
		if(this.inputOffset == 0) {
			byte[] codificationSerialized = SerializationUtils.serialize(codificationArray);
			//byte[] codificationByteArray = this.codificationCollection.toByteArray();
			for(int i = 0 ; i < numTotalContainers - 1 ; i++) {
				System.out.println("Master abrindo porta para aguardar client: " + i);
				Socket socket;
				while(true) {
					try {
						socket = new Socket(hostPortPairArray[i].hostName, hostPortPairArray[i].port);
						break;
					} catch(Exception e) {
						Thread.sleep(1000);
					}
				}
				DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
				dataOutputStream.writeShort(codificationSerialized.length);
				//dataOutputStream.writeShort(this.codificationCollection.lengthInBytes);
				
				dataOutputStream.write(codificationSerialized);
				//dataOutputStream.write(codificationByteArray);
				System.out.println("Master recebeu do client: " + i);
				socket.close();
			}
		}
		else {
			System.out.println("Client abrindo porta para aguardar master");
			ServerSocket serverSocket = new ServerSocket(slavePort);
		    Socket clientSocket = serverSocket.accept();
		    DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
		    short codificationSerializedLength = dataInputStream.readShort();
		    //short codificationCollectionLengthInBytes = dataInputStream.readShort();
		    byte[] codificationSerialized = new byte[codificationSerializedLength];
		    //byte[] codificationCollectionByteArray = new byte[codificationCollectionLengthInBytes];
		    dataInputStream.read(codificationSerialized, 0, codificationSerializedLength);
		    //dataInputStream.read(codificationCollectionByteArray, 0, codificationCollectionLengthInBytes);
		    this.codificationArray = (Codification[]) SerializationUtils.deserialize(codificationSerialized);
		    //codificationCollection = new CodificationArray(codificationCollectionByteArray);
		    System.out.println("Client recebeu do master");
		    serverSocket.close();
		}
		
		for(int i = 0 ; i < codificationArray.length ; i++) {
			System.out.print(codificationArray[i].toString() + " ");
		}
		
		memoryCompressor();
		
	}

	private void chunksToMemory() throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(fileName);
		
		FSDataInputStream f = fs.open(path);
		System.err.println("FSDATAINPUTSTREAM: "+ f.toString());
		memory = new byte[inputLength]; 
		f.read(inputOffset, memory, 0, inputLength);
	}
	
	public void memoryToFrequency() throws IOException {
		if(this.inputOffset == 0) {
			this.totalFrequency = new long[256];
			for (int i = 0; i < inputLength; i++)
				if (totalFrequency[(memory[i] & 0xFF)]++ == 0)
					symbols++;
		}
		else {
			this.frequency = new int[256];
			for (int i = 0; i < inputLength; i++)
				if (frequency[(memory[i] & 0xFF)]++ == 0)
					symbols++;
		}

        /*
        System.out.println("FREQUENCY: symbol (frequency)");
        for (int i = 0; i < frequency.length; i++)
                if (frequency[i] != 0)
                        System.out.println((int) i + "(" + frequency[i] + ")");
        System.out.println("------------------------------");
        */
	}
	
	public void frequencyToNodeArray() {
		this.nodeArray = new NodeArray((short) (this.symbols + 1));

		for (short i = 0 ; i < 256 ; i++)
			if (this.totalFrequency[i] > 0)
				this.nodeArray.insert(new Node((byte) i, this.totalFrequency[i]));

		/*
		System.out.println(nodeArray.toString());
		*/
	}

	public void huffmanEncode() {
		while (this.nodeArray.size() > 1) {
			Node a, b, c;
			a = this.nodeArray.get(this.nodeArray.size() - 2);
			b = this.nodeArray.get(this.nodeArray.size() - 1);
			c = new Node((byte) 0, a.frequency + b.frequency, a, b);

			this.nodeArray.removeLastTwoNodes();
			this.nodeArray.insert(c);
			/*
			System.out.println(nodeArray.toString() + "\n");
			*/
		}
	}
	
	public void treeToCode() {
		Stack<Node> s = new Stack<Node>();
		//codification = new Codification[symbols];
		codificationArray = new Codification[symbols];
		//codificationCollection = new CodificationArray();
		Node n = nodeArray.get(0);
		short codes = 0;
		byte[] path = new byte[33];
		
		byte i = 0;
		s.push(n);
		while (codes < symbols) {
			if (n.left != null) {
				if (!n.left.visited) {
					s.push(n);
					n.visited = true;
					n = n.left;
					path[i++] = 0;
				} else if (!n.right.visited) {
					s.push(n);
					n.visited = true;
					n = n.right;
					path[i++] = 1;
				} else {
					i--;
					n = s.pop();
				}
			} else {
				n.visited = true;
				codificationArray[codes] = new Codification(n.symbol, i, path);
				//codificationCollection.add(new Codification(n.symbol, i, path));
				n = s.pop();
				i--;
				codes++;
			}
		}

		/*
		System.out.println("CODIFICATION: symbol (size) code"); 
		for (short i = 0; i < symbols; i++)
			System.out.println(codification[i].toString());
		*/
	}
	
    public void memoryCompressor() throws IOException {
    	FileSystem fs = FileSystem.get(conf);
		Path path = new Path(fileName + ".dir/part-" + this.inputOffset);
		
		//FSDataInputStream f = fs.open(path);
		FSDataOutputStream f = fs.create(path);
		
		//f.read(inputOffset, memory, 0, inputLength);
    	
		//BitSetArray bufferCollection = new BitSetArray();
		//BitSet[] buffer = new BitSet[memory.length * 2/3];
		BitSet buffer = new BitSet();

        byte bits = 0;
        //Codification[] codificationArray = (Codification[]) codificationCollection.toArray();

        for (int i = 0; i < memory.length ; i++) {
        	if(i % 5000000 == 0) {
        		System.out.print(i + " ");
        	}
            for (short j = 0; j < codificationArray.length ; j++) {
                if (memory[i] == codificationArray[j].symbol) {
                    for (int k = 0; k < codificationArray[j].size; k++) {
                        if (codificationArray[j].code[k] == '1')
                                buffer.setBit(bits, true);
                        else
                                buffer.setBit(bits, false);

                        if (++bits == 8) {
                                f.write(buffer.b);
                        		//bufferCollection.add(buffer);
                        		buffer = new BitSet();
                                bits = 0;
                        }
                    }
                    break;
                }
            }
        }

        if (bits != 0) {
        	f.write(buffer.b);
        	//bufferCollection.add(buffer);
        }
        
        //System.out.println("Minha parte comprimida deu: " + bufferCollection.toByteArray().length);

        f.close();
}

	
	
	public static void main(String[] args) throws IOException, InterruptedException {
		Encoder encoder = new Encoder(args);
		encoder.encode();
	}
}
