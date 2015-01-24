package br.ufrj.ppgi.huffmanyarn.encoder;

import java.util.ArrayList;

public class BitSetArray extends ArrayList<BitSet> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3L;

	
	public byte[] toByteArray() {
		byte[] byteArray = new byte[this.size()];
		
		for(int i = 0 ; i < this.size() ; i++) {
			byteArray[i] = this.get(i).b;
		}
		
		return byteArray;
	}

}
