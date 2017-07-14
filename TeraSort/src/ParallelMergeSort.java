import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class ParallelMergeSort {

	public static void main(String[] args) {
		try{
//			if(args.length < 1){
//				System.err.println("No arguments");
//				System.exit(1);
//			}
			
			File file = new File("input.dat");
			String outputPath = "output";
			String intermediatePath = "intermediate";
			int numThreads = 8;
			long chunkSize, mb = 1024*1024;// = file.length()/100;
			
			long freeMemory = Runtime.getRuntime().freeMemory();
			long fileLength = 1000000000000L;
			System.out.println("Number of Threads: "+ numThreads);
			System.out.println("File Size: "+ (float)fileLength/mb + " MB");
			System.out.println("Free Memory: "+ (float)freeMemory/mb + " MB");
			if(fileLength >= freeMemory){
				chunkSize = freeMemory/150;
			}
			else{
				//chunkSize = 10000;
				chunkSize = fileLength/100;
			}
			System.out.println("Chunk Size: "+ chunkSize);
			System.out.println("");
			//chunkSize = Math.min(file.length()/(100*numThreads),1000*1000*1000);
			
			if(!file.exists()){
				System.err.println("File does not exist");
				System.exit(1);
			}
			BufferedReader br = new BufferedReader(new FileReader(file));
			ArrayList<String> chunkArr = new ArrayList<String>();
			FileWriter out = null;
			
			long startTime, endTime, totalTime = 0L;
			long startTimeCode, totalTimeCode = 0L;
			long count = 0, lineCount = 0;

			System.out.println("Creating sorted intermediate files...");
			//read till end of file
			startTimeCode = System.currentTimeMillis();
			while(true)
			{
				String line = null;
				//Divide the file into chunks of sorted files
				count = 0 + lineCount;
				//System.out.println("Reading for intermediate file-" + (lineCount/chunkSize+ 1) + "...");
				for(lineCount = count + 0; lineCount<count + chunkSize; lineCount++)
				{
					if((line = br.readLine()) == null)
					{
						break;
					}
					chunkArr.add(line);
					if(lineCount > 0 )
						if(lineCount%(fileLength/1000) == 0){
							System.out.println(lineCount/chunkSize + " intermediate files created");
						}
				}
				if(line == null && chunkArr.isEmpty()){
					break;
				}
				if(line == null && !chunkArr.isEmpty()){
					lineCount += chunkSize;
				}
				out = new FileWriter(new File(intermediatePath + lineCount/chunkSize + ".dat"));
				//System.out.println("Sorting intermediate file-" + (lineCount/chunkSize) + "...");
				startTime = System.currentTimeMillis();
				chunkArr = mergeSort(chunkArr, 0, chunkArr.size()-1, numThreads);
				endTime = System.currentTimeMillis();
				totalTime += (endTime - startTime);
				//System.out.println("Creating sorted intermediate file-" + (lineCount/chunkSize) + "...");
				for(int k=0;k<chunkArr.size();k++)
					out.write(chunkArr.get(k)+" \n");
				//System.out.println("Sorted Intermediate file-" + (lineCount/chunkSize) + " created");
				chunkArr.clear();
				out.close();
			}
			br.close();
			// delete input file from disk
			//file.delete();
			
			//merge final output file
			int numSortedFiles = (int)(lineCount/chunkSize);
			System.out.println(numSortedFiles + " sorted intermediate files created\n");
			BufferedReader brArr[] = new BufferedReader[numSortedFiles];
			long numLines[] = new long[numSortedFiles];
			long mergerLength = 0L;
			String mergerArray[] = new String[numSortedFiles];
			
			for(int fileNumber = 0; fileNumber < numSortedFiles; fileNumber++){
				brArr[fileNumber] = new BufferedReader(new FileReader(new File(intermediatePath + (fileNumber+1) + ".dat")));
				numLines[fileNumber] = file.length()/100;
				if(numLines[fileNumber] > mergerLength)
					mergerLength = numLines[fileNumber];
			}
			
			String val = "", small = "";
			//boolean containsElement = false;
			int position = -1, countPercent = 0;
			out = new FileWriter(new File(outputPath + ".dat"));
			
			System.out.println("Merging sorted intermediate files...");
			startTime = System.currentTimeMillis();
			for(int lines = 0; lines < fileLength/100; lines++){
				for(int element = 0; element < numSortedFiles; element++){
					if(mergerArray[element] == null){
						if((val = brArr[element].readLine()) != null){
							if(small.equals(""))
								small = val;
							if(val.compareTo(small)<=0){
								small = val;
								position = element;
							}
							mergerArray[element] = val;
							//containsElement = true;
						}
						else{
							mergerArray[element] = "";
						}
					}
					else{
						val = mergerArray[element];
						if(val.equals(""))
							continue;
						if(small.equals(""))
							small = val;
						if(val.compareTo(small)<=0){
							small = val;
							position = element;
						}
						//containsElement = false;
					}
				}
				
				out.write(small + "\n");
				small = "";
				mergerArray[position] = null;
				if(lines != 0)
					if(lines%(fileLength/1000) == 0){
						countPercent++;
						System.out.println("Merging: "+(countPercent*10)+" percent");
					}
			}
			endTime = System.currentTimeMillis();
			totalTime += (endTime - startTime);
			totalTimeCode += (endTime - startTimeCode);
			System.out.println("Sorting completed\n");
			System.out.println("Sorting time in seconds: "+ totalTime/1000.0 + " seconds");
			System.out.println("Program time in seconds: "+ totalTimeCode/1000.0 + " seconds");
			
			for(int intFiles = 1;intFiles <= numSortedFiles; intFiles++){
				brArr[intFiles-1].close();
				new File(intermediatePath+intFiles+".dat").delete();
			}
			out.close();
		}
		catch(NumberFormatException e){
			System.err.println("Argument must be integer type");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static ArrayList<String> mergeSort(ArrayList<String> chuckArr, long firstIndex, long lastIndex, int numThreads){
		ArrayList<String> leftHalf = null, rightHalf = null;
		MergeThread leftHalfSort = null;
		Thread leftHalfThread = null;
		
		long midIndex = (firstIndex + lastIndex)/2;
		if (firstIndex > lastIndex){
			return new ArrayList<String>();
		}
        if (firstIndex == lastIndex) {
        	ArrayList<String> temp = new ArrayList<String>();
        	temp.add(chuckArr.get((int)firstIndex));
            return temp;
        }
        
		//left half array sorting
		if(numThreads > 1){
			leftHalfSort = new MergeThread(chuckArr, firstIndex, midIndex, numThreads/2);
			leftHalfThread = new Thread(leftHalfSort);
			leftHalfThread.start();
		}
		else{
			leftHalf = mergeSort(chuckArr, firstIndex, midIndex, numThreads/2);
		}
		
		//right half array sorting
		rightHalf = mergeSort(chuckArr, midIndex+1, lastIndex, numThreads - (numThreads/2));
		if(numThreads > 1){
			try {
				leftHalfThread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.err.println("Could not join the threads");
			}
			leftHalf = leftHalfSort.getChunkData();
		}
		
		return merge(leftHalf, rightHalf);
	}
	
	private static ArrayList<String> merge(ArrayList<String> left, ArrayList<String> right){
		ArrayList<String> chunkResult = new ArrayList<String>();

        int leftIndex = 0, rightIndex = 0;

        for (long i = 0; i < left.size()+right.size(); i++) {
        	if (rightIndex >= right.size()) {
                chunkResult.add(left.get(leftIndex));
                leftIndex++;
            }
            else if (leftIndex >= left.size()) {
                chunkResult.add(right.get(rightIndex));
                rightIndex++;
            }
            else if (left.get(leftIndex).compareTo(right.get(rightIndex)) <= 0) {
            	chunkResult.add(left.get(leftIndex));
                leftIndex++;
            }
            else {
            	chunkResult.add(right.get(rightIndex));
                rightIndex++;
            }
        }
        return chunkResult;
	}
	
	private static class MergeThread implements Runnable{
		private ArrayList<String> chunkData;
		private ArrayList<String> chunkResult;
		private long firstIndex;
		private long lastIndex;
		private int numThreads;
		
		public MergeThread(ArrayList<String> chunkData, long firstIndex, long lastIndex, int numThreads) {
			// TODO Auto-generated constructor stub
			this.chunkData = chunkData;
			this.firstIndex = firstIndex;
			this.lastIndex = lastIndex;
			this.numThreads = numThreads;
		}
		
		public ArrayList<String> getChunkData(){
			return chunkResult;
		}
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			chunkResult = mergeSort(chunkData, firstIndex, lastIndex, numThreads);
		}
	}
}