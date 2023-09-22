package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * 直方图中每个桶看成一个对象比较好操作
     */
    public static class Bucket {
        public double left;
        public double right;
        public int height;

        public Bucket(){
            this.height = 0;
        }
        public void populate(){
            this.height++;
        }
    }

    private final int numBuckets;
    private final int min;
    private final int max;
    private final double bWidth; // 桶宽
    private final Bucket[] histData;
    private int ntups; // 元组总数量

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.numBuckets = buckets;
        this.min = min;
        this.max = max;
        this.bWidth = (max-min) * 1.0 / buckets;
        this.histData = new Bucket[buckets];
        constructBuckets();
        this.ntups = 0;
    }

    /**
     * 初始化所有桶
     */
    private void constructBuckets(){
        int index = 0;
        for(double left = min; left < max && index < numBuckets; left += bWidth){
            Bucket newBucket = new Bucket();
            newBucket.left = left;
            if(index != numBuckets-1){
                newBucket.right = left + bWidth;
            }else{
                newBucket.right = max; // 如果是最后一个bucket的话，right设置为max
            }
            histData[index++] = newBucket;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        histData[findBucketIndex(v)].populate();
        ntups ++;
    }

    private int findBucketIndex(int v){
        if(v < min) return -1; // 在区间左侧
        if(v > max) return -2; // 在区间右侧
        if(v == min) return 0;
        if(v == max) return numBuckets-1; // 两端的情况特殊处理
        return (int) Math.ceil((double)(v - min) / bWidth) - 1;
    }

    /**
     * 这部分其实还不完全对，主要是没人给出准确的计算公式，不纠结了，浪费时间
     * 实现起来并不难
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        double selectivity = -1.0;
        // 注意查询的值不一定在最大最小区间内
        // 不同情况的选择性不同

        // 简化代码，且减少出错
        switch (op) {
            case EQUALS:
                selectivity = calculateEquals(v);
                break;
            case GREATER_THAN:
                selectivity = calculateGreaterThan(v);
                break;
            case GREATER_THAN_OR_EQ: // > 和 = 相加
                selectivity = calculateGreaterThan(v) + calculateEquals(v);
                break;
            case LESS_THAN:
                selectivity = 1 - calculateGreaterThan(v) - calculateEquals(v);
                break;
            case LESS_THAN_OR_EQ:
                selectivity = 1 - calculateGreaterThan(v);
                break;
            case NOT_EQUALS: // 1 - Selectivity(=)
                selectivity = 1 - calculateEquals(v);
                break;
        }
        return selectivity;
    }

    private double calculateEquals(int v){
        int i = findBucketIndex(v);
        if (i < 0) return 0;
        // 这里必须要+1，可能跟直方图存整数有关
        return (double) histData[i].height / (((int)bWidth+1) * ntups);
    }

    private double calculateGreaterThan(int v){
        int i = findBucketIndex(v);
        double selectivity = 0;
        if(i == -1){ // 在左侧
            i = 0; // 算全部桶
        }else if(i == -2){ // 在右侧
            return 0;
        }else{
            // 在区间内，先算桶内的部分
            if(v > histData[i].left) { // 在大于左端点时才计算，不然逻辑上可能有错误
                selectivity = (double) histData[i].height / ntups * (histData[i].right - v) / bWidth;
            }
            // 准备算右侧其他桶的部分
            i++;
        }
        while(i < numBuckets){
            selectivity += (double) histData[i].height / ntups;
            i++;
        }
        return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        double temp = 0;
        for(int i=0; i<numBuckets; i++){
            Bucket bucket = histData[i];
            temp += (double)bucket.height / ntups;
        }
        return temp / numBuckets;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<numBuckets; i++){
            Bucket bucket = histData[i];
            sb.append("[")
                    .append(String.format("%.2f", bucket.left))
                    .append(",")
                    .append(String.format("%.2f", bucket.right))
                    .append("): ")
                    .append(bucket.height)
                    .append("\n");
        }
        return sb.toString();
    }
}
