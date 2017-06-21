//.
//.------------------------------------------------- S . T . A . R . T ------------------------------------------------
//.
package com.srini.LogAnalyzer;
//.
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;
import java.util.ArrayList;
import java.util.StringTokenizer;
//.
import com.srini.LogAnalyzer.chunkStat;
//.
public class ARLTLogAnalyzer
{
	//.
	int nArrSz;
	int nLineCount;
	long lChunk;
	Date dtBasis;
	String strInputFile;
	Calendar calChunkDt;
	StringBuilder strbldGlobal;
	SimpleDateFormat sdfDtFormat;
	ArrayList<chunkStat> arrlTheList;
	//.
	static final int COL_BASIS = 1; //.Basis Column : 0-based
	static final int COL_TARGET_0 = 3; //.Target Column 0 - Src Records
	static final int COL_TARGET_1 = 11; //.Target Column 1 - Total Time
	//.
	//.----------------------------------------------------------------------------------------------------------------
	//.
	public Date getChunkDate(int nIdx)
	{
		long lX;
		Date dtTemp;
		
		this.calChunkDt.setTime(this.dtBasis);
		lX = this.lChunk * nIdx;
		if(lX > Integer.MAX_VALUE) {
			while(lX > Integer.MAX_VALUE) {
				this.calChunkDt.add(Calendar.MILLISECOND, Integer.MAX_VALUE);
				lX = lX - Integer.MAX_VALUE;
			}
			this.calChunkDt.add(Calendar.MILLISECOND, (int)lX);
		}
		else {
			this.calChunkDt.add(Calendar.MILLISECOND, (int)(lX));
		}
		dtTemp = this.calChunkDt.getTime();
		return(dtTemp);
	}
	//.
	//.----------------------------------------------------------------------------------------------------------------
	//.
	int printStatistics()
	{
		int nX, nY;
		SimpleDateFormat sdfOutput;
		//.
		sdfOutput = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		nX = this.arrlTheList.size();
		for(nY = 0; nY < nX; nY++) {
			arrlTheList.get(nY).printStats(sdfOutput);
		}
		return(nY);
	}
	//.
	//.----------------------------------------------------------------------------------------------------------------
	//.
	int writeStatistics(String strOutput)
	{
		int nW, nX, nY, nZ;
		//.
		String strTemp;
		ByteBuffer bbBuffX;
		FileChannel fcOutput;
		SimpleDateFormat sdfOutput;
		RandomAccessFile rafOutput;
		//.
		nX = nY = nZ = 0;
		nW = 5 * 1024 * 1024; //. Five MB
		sdfOutput = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		try {
			rafOutput = new RandomAccessFile(strOutput, "rw");
			fcOutput = rafOutput.getChannel();
			bbBuffX = ByteBuffer.allocate(nW);
			bbBuffX.clear();
			//.
			strTemp = new String("Chunk, Chunk-Time, Total-Record, Total-Time\n");
			nZ += strTemp.length();
			bbBuffX.put(strTemp.getBytes());
			//.
			nX = this.arrlTheList.size();
			for(nY = 0; nY < nX; nY++) {
				strTemp = arrlTheList.get(nY).getCSVStats(sdfOutput);
				nZ += strTemp.length();
				if(nZ < nW) {
					bbBuffX.put(strTemp.getBytes());
				}
				else {
					bbBuffX.flip();
					while(bbBuffX.hasRemaining()) {
						fcOutput.write(bbBuffX);
					}
					bbBuffX.clear();
					bbBuffX.put(strTemp.getBytes());
					nZ = strTemp.length();
				}
			}
			bbBuffX.flip();
			while(bbBuffX.hasRemaining()) {
				fcOutput.write(bbBuffX);
			}
			bbBuffX.clear();
			fcOutput.close();
			rafOutput.close();
		}
		catch(Exception excpX) {
			System.out.println("ARLTLogAnalyzer::writeStatistics -> " + excpX.toString());
		}
		return(nY);
	}
	//.
	//.----------------------------------------------------------------------------------------------------------------
	//.
	int processLine(int nCount)
	{
		int nX, nY, nZ;
		long lDiff, lIdx;
		long lRecs, lTime;
		String strDebug;
		Date dtCurr, dtTemp;
		StringTokenizer stSplit;
		//.
		lRecs = lTime = 0;
		dtCurr = new Date();
		stSplit = new StringTokenizer(strbldGlobal.toString(), ",");
		for(nX = 0; nX < COL_BASIS; nX++) {
			strDebug = stSplit.nextToken();
		}
		try {
			nX++;
			dtCurr = sdfDtFormat.parse(stSplit.nextToken());
		}
		catch(Exception excpX) {
			System.out.println("ARLTLogAnalyzer::processLine::0 -> " + excpX.toString() + ", Line : " + nCount + ", Str : " + strbldGlobal.toString());
		}
		lDiff = dtCurr.getTime() - this.dtBasis.getTime();
		if(lDiff > 0) {
			for(; nX < COL_TARGET_0; nX++) {
				strDebug = stSplit.nextToken();
			}
			try {
				nX++;
				lRecs = Long.parseLong(stSplit.nextToken()); //. lRecs : COL_TARGET_0
			}
			catch(Exception excpX) {
				System.out.println("ARLTLogAnalyzer::processLine::1 -> " + excpX.toString() + ", Line : " + nCount + ", Str : " + strbldGlobal.toString());
			}
			for(; nX < COL_TARGET_1; nX++) {
				strDebug = stSplit.nextToken();
			}
			try {
				nX++;
				lTime = Long.parseLong(stSplit.nextToken()); //. lTime : COL_TARGET_1
			}
			catch(Exception excpX) {
				System.out.println("ARLTLogAnalyzer::processLine::2 -> " + excpX.toString() + ", Line : " + nCount + ", Str : " + strbldGlobal.toString());
			}
			lIdx = lDiff / this.lChunk;
			nZ = this.nArrSz - 1;
			if(lIdx > nZ) {
				dtTemp = getChunkDate(nZ);
				this.calChunkDt.setTime(dtTemp);
				for(nY = (nZ + 1); nY <= ((int)lIdx); nY++) {
					this.calChunkDt.add(Calendar.MILLISECOND, (int)this.lChunk);
					arrlTheList.add(new chunkStat((long)nY, calChunkDt.getTime()));
					this.nArrSz++;
				}
				arrlTheList.get((int)lIdx).updateStats(lRecs, lTime);
			}
			else {
				arrlTheList.get((int)lIdx).updateStats(lRecs, lTime);
			}
		}
		nX = strbldGlobal.length();
		strbldGlobal.delete(0, nX);
		return(0);
	}
	//.
	//.----------------------------------------------------------------------------------------------------------------
	//.
	int processBuffer(ByteBuffer bbInput)
	{
		int nX;
		byte bX, bY;
		char chrX, chrY;
		//.
		nX = 0;
		for(nX = 0; nX < bbInput.limit(); nX++) {
			try {
				bX = bbInput.get();
				if(bX == 0x0D) { //. Carriage-Return
					bY = bbInput.get();
					nX++;
					if(bY == 0x0A) { //. Line-Feed
						this.nLineCount++;
						processLine(this.nLineCount);
					}
					else {
						chrX = (char)bX;
						chrY = (char)bY;
						strbldGlobal.append(chrX);
						strbldGlobal.append(chrY);
					}
				}
				else if(bX == 0x0A) { //.Line-Feed
					bY = bbInput.get();
					nX++;
					if(bY == 0x0D) { //.Carriage-Return
						this.nLineCount++;
						processLine(this.nLineCount);
					}
					else {
						chrX = (char)bX;
						chrY = (char)bY;
						strbldGlobal.append(chrX);
						strbldGlobal.append(chrY);
					}
				}
				else {
					chrX = (char)bX;
					strbldGlobal.append(chrX);
				}
			}
			catch(Exception excpX) {
				System.out.println("ARLTLogAnalyzer::processBuffer -> " + excpX.toString() + " : bbInput( " + nX + " )");
			}
		}
		return(nX);
	}
	//.
	//.------------------------------------------------------------------------
	//.
	int processFile(String strInput)
	{
		int nW, nX, nY;
		ByteBuffer bbBuffX;
		FileChannel fcInput;
		RandomAccessFile rafInput;
		//.
		nX = nY = 0;
		nW = 5 * 1024 * 1024; //. Five MB
		try {
			rafInput = new RandomAccessFile(strInput, "r");
			fcInput = rafInput.getChannel();
			bbBuffX = ByteBuffer.allocate(nW);
			while(fcInput.read(bbBuffX) > 0) {
				nY++;
				bbBuffX.flip();
				nX = processBuffer(bbBuffX);
				bbBuffX.clear();
			}
			fcInput.close();
			rafInput.close();
		}
		catch(Exception excpX) {
			System.out.println("ARLTLogAnalyzer::initSequence -> " + excpX.toString());
		}
		return(nY);
	}
	//.
	//.----------------------------------------------------------------------------------------------------------------
	//.
	public ARLTLogAnalyzer(String strInput, String dtStart, int nChunk)
	{
		nLineCount = 0;
		lChunk = nChunk * 60 * 1000;
		sdfDtFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss"); //. MM/dd/yyyy HH:mm:ss
		calChunkDt = Calendar.getInstance();
		strInputFile = strInput;
		try {
			dtBasis = sdfDtFormat.parse(dtStart);
		}
		catch(Exception excpX) {
			System.out.println("ARLTLogAnalyzer::ARLTLogAnalyzer -> " + excpX.toString());
		}
		strbldGlobal = new StringBuilder(96);
		arrlTheList = new ArrayList<chunkStat>();
		arrlTheList.add(0, new chunkStat(0, dtBasis));
		nArrSz = 1;
	}
	//.
	//.----------------------------------------------------------------------------------------------------------------
	//.
	public ARLTLogAnalyzer(String strInput, String strDtStrt, String strDtFmt, int nChunk)
	{
		nLineCount = 0;
		lChunk = nChunk * 60 * 1000;
		sdfDtFormat = new SimpleDateFormat(strDtFmt);
		calChunkDt = Calendar.getInstance();
		strInputFile = strInput;
		try {
			dtBasis = sdfDtFormat.parse(strDtStrt);
		}
		catch(Exception excpX) {
			System.out.println("ARLTLogAnalyzer::ARLTLogAnalyzer -> " + excpX.toString());
		}
		strbldGlobal = new StringBuilder(96);
		arrlTheList = new ArrayList<chunkStat>();
		arrlTheList.add(0, new chunkStat(0, dtBasis));
		nArrSz = 1;
	}
	//.
	//.----------------------------------------------------------------------------------------------------------------
	//.
	public static void main(String[] args) throws IOException
	{
		int nLines, nFills;
		long lStart, lEnd;
		//.
		nFills = 0;
		ARLTLogAnalyzer alaMain = new ARLTLogAnalyzer("ARLT_7M_Data_Latest.CSV", "04/02/2014 00:00:00", 15); //. MM/dd/yyyy HH:mm:ss
		System.out.println("Sequence Start");
		System.out.println("Input File : " + alaMain.strInputFile);
		System.out.println("Start Date : " + alaMain.dtBasis);
		System.out.println("Chunk Time : " + alaMain.lChunk + " milliseconds");
		//.
		lStart = System.currentTimeMillis();
		nFills = alaMain.processFile(alaMain.strInputFile);
		nLines = alaMain.writeStatistics("TestDataOutputLatest.CSV");
		lEnd = System.currentTimeMillis();
		//.
		System.out.println("Sequence End");
		System.out.println("Sequence Took : " + (lEnd - lStart) + " millisecs");
		System.out.println("Buffer Fills : " + nFills);
		System.out.println("Lines Printed : " + nLines);
		//.
	}
	//.
}
//.
//.----------------------------------------------------- E . N . D ----------------------------------------------------
//.
