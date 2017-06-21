//.
//.------------------------------------------------- S . T . A . R . T ------------------------------------------------
//.
package com.srini.LogAnalyzer;
//.
import java.util.Date;
import java.text.SimpleDateFormat;
//.
public class chunkStat
{
	//.
	long lChunkID;
	Date dtChunkDt;
	long lRecords;
	long lProcTime;
	//.
	//.
	//.
	public chunkStat()
	{
		SimpleDateFormat sdfDtFormat;
		//.
		this.lChunkID = 0;
		sdfDtFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
		try {
			this.dtChunkDt = sdfDtFormat.parse("12/28/1972 17:30:00");
		}
		catch(Exception excpX) {
			System.out.println("chunkStat::chunkStat -> " + excpX.toString());
		}
		this.lRecords = 0;
		this.lProcTime = 0;
	}
	//.
	//.
	//.
	public chunkStat(long lChunkID, Date dtChunkDt)
	{
		this.lChunkID = lChunkID;
		this.dtChunkDt = dtChunkDt;
		this.lRecords = 0;
		this.lProcTime = 0;
	}
	//.
	//.
	//.
	public chunkStat(long lChunkID, Date dtChunkDt, long lRecords, long lProcTime)
	{
		this.lChunkID = lChunkID;
		this.dtChunkDt = dtChunkDt;
		this.lRecords = lRecords;
		this.lProcTime = lProcTime;
	}
	//.
	//.
	//.
	public int updateStats(long lRecords, long lProcTime)
	{
		this.lRecords += lRecords;
		this.lProcTime += lProcTime;
		return(0);
	}
	//.
	//.
	//.
	public void printStats()
	{
		System.out.println("Chunk : " + this.lChunkID + ", Time : " + this.dtChunkDt + ", Records : " + this.lRecords + ", Time : " + this.lProcTime);
	}
	//.
	//.
	//.
	public void printStats(SimpleDateFormat sdfOutput)
	{
		System.out.println("Chunk : " + this.lChunkID + ", Time : " + sdfOutput.format(this.dtChunkDt) + ", Records : " + this.lRecords + ", Time : " + this.lProcTime);
	}
	//.
	//.
	//.
	public String getStrStats()
	{
		String strTemp;
		//.
		strTemp = "Chunk : " + this.lChunkID + ", Time : " + this.dtChunkDt + ", Records : " + this.lRecords + ", Time : " + this.lProcTime + "\n";
		return(strTemp);
	}
	//.
	//.
	//.
	public String getStrStats(SimpleDateFormat sdfOutput)
	{
		String strTemp;
		//.
		strTemp = "Chunk : " + this.lChunkID + ", Time : " + sdfOutput.format(this.dtChunkDt) + ", Records : " + this.lRecords + ", Time : " + this.lProcTime + "\n";
		return(strTemp);
	}
	//.
	//.
	//.
	public String getCSVStats(SimpleDateFormat sdfOutput)
	{
		String strTemp;
		//.
		strTemp = this.lChunkID + ", " + sdfOutput.format(this.dtChunkDt) + ", " + this.lRecords + ", " + this.lProcTime + "\n";
		return(strTemp);
	}
	//.
	//.
	//.
}
//.
//.----------------------------------------------------- E . N . D ----------------------------------------------------
//.
