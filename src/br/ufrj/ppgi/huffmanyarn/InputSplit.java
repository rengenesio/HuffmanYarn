package br.ufrj.ppgi.huffmanyarn;

public class InputSplit {

	public long offset;
	public long length;
	
	public InputSplit() {
		
	}
	
	public InputSplit(long offset, long length) {
		this.offset = offset;
		this.length = length;
	}
}
