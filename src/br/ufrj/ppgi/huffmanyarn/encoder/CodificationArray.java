package br.ufrj.ppgi.huffmanyarn.encoder;

import java.util.ArrayList;

public class CodificationArray extends ArrayList<Codification> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2L;
	
	public short lengthInBytes;
	
	
	public CodificationArray() {
		super();
	}
	
	public CodificationArray(byte[] byteArray) {
		super();
		
		Codification codification;
		short i = 0;
		while(i < byteArray.length) {
			codification = new Codification();
			codification.symbol = byteArray[i];
			codification.size = byteArray[i+1];
			codification.code = new byte[codification.size];
			System.arraycopy(byteArray, i+2, codification.code, 0, codification.size);
			super.add(codification);
			i += (codification.size + 2);
		}
	}

	@Override
	public boolean add(Codification e) {
		this.lengthInBytes += e.lengthInBytes;
		return super.add(e);
	}

	public byte[] toByteArray() {
		byte[] byteArray = new byte[this.lengthInBytes];
		
		short i = 0;
		for(Codification codification : this) {
			byteArray[i] = codification.symbol;
			byteArray[i+1] = codification.size;
			System.arraycopy(codification.code, 0, byteArray, i+2, codification.size);
			i += codification.size + 2; 
		}
		
		return byteArray;
	}
	
	public void fromByteArray(byte[] byteArray) {
		Codification codification;
		for(short i = 0 ; i < byteArray.length ; i++) {
			codification = new Codification();
			codification.symbol = byteArray[i];
			codification.size = byteArray[i+1];
			System.arraycopy(byteArray, i+2, codification.code, 0, codification.symbol);
			super.add(codification);
		}
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		
		for(Codification codification : this) {
			stringBuilder.append(codification.toString());
		}
		
		return stringBuilder.toString();
	}
}
