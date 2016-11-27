package Enron;


import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class EnronPartitioner<K, V> extends Partitioner<K, V> {
	private int m=22;
	private int firstLetterValue=0;

	// override getPartition(K, V, int) method
	public int getPartition(K key, V value, int numReduceTasks) {
		
		if (numReduceTasks>2)
		{
			return -1;
		}
		else if (numReduceTasks == 0)
		{
			return 0;
		}
		else 
		{
			firstLetterValue = (int) key.toString().charAt(0);	//Change this later
			
			if(firstLetterValue >= 97 && firstLetterValue <= 109)
			{
				return 1 % numReduceTasks;
			} 
			else
			{
				return 2 % numReduceTasks; 
			}			
		}
	}
}
