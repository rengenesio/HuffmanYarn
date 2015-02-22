package br.ufrj.ppgi.huffmanyarn;


public class Defines {
	public static final String jobName = "HuffmanYarn"; 
	
	public static final int amMemory = 10;
	public static final int amVCores = 1;
	public static final int amPriority = 0;
	public static final String amQueue = "default";
	
	
	public static final int containerMemory = 512;
	public static final int containerVCores = 1;
	
	
	
	public static final String pathSuffix = ".yarndir/";
	public static final String compressedPath = "compressed/";
	public static final String compressedFileName = "part-";
	public static final String codificationFileName = "codification";
	public static final String decompressedFileName = "decompressed";
}
