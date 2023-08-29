package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * 直方图中每个桶抽看成一个对象比较好操作
     */
    public class Bucket {
        public double left;
        public double right;
        public Bucket pre;
        public Bucket next;
        public int height;

        public Bucket(){
            this.height = 0;
        }
        public void populate(){
            this.height++;
        }
    }

    private int numBuckets;
    private int min;
    private int max;
    private double bWidth; // 桶宽
    private Bucket histHead; // 头桶（哨兵）
    private Bucket histTail; // 尾桶（哨兵）
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
        this.histHead = new Bucket();
        this.histTail = new Bucket();
        histHead.next = histTail;
        histTail.pre = histHead;
        constructBuckets();
        this.ntups = 0;
    }

    /**
     * 初始化所有桶
     */
    private void constructBuckets(){
        Bucket current = histHead;
        for(double left = min; left < max; left += bWidth){
            Bucket newBucket = new Bucket();
            newBucket.left = left;
            newBucket.right = left + bWidth;
            newBucket.pre = current;
            newBucket.next = current.next;
            current.next.pre = newBucket;
            current.next = newBucket;
            current = newBucket;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        findBucket(v).height ++;
        ntups ++;
    }

    private Bucket findBucket(int v){
        Bucket current = histHead.next;
        if(current.left > v) return histHead;
        while(current != histTail){
            if(v >= current.left && v < current.right){
                return current;
            }
            current = current.next;
        }
        return histTail;
    }

    /**
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
        double temp;
        Bucket bucket = findBucket(v);
        switch (op){
            case EQUALS:
                selectivity = (double)bucket.height / (bWidth * ntups);
                break;
            case GREATER_THAN:
                temp = (double)bucket.height/ntups * (bucket.right-v)/bWidth;
                if(bucket.next == null){ // 例如(1, 10)查询 >12 的情况
                    selectivity = 0;
                }else {
                    bucket = bucket.next;
                    while (bucket != histTail) {
                        temp += (double) bucket.height / ntups;
                        bucket = bucket.next;
                    }
                    selectivity = temp;
                }
                break;
            case GREATER_THAN_OR_EQ: // > 和 = 相加
                temp = (double)bucket.height / (bWidth * ntups);
                temp += (double)bucket.height/ntups * (bucket.right-v)/bWidth;
                if(bucket.next == null){
                    selectivity = 0;
                }else {
                    bucket = bucket.next;
                    while (bucket != histTail) {
                        temp += (double) bucket.height / ntups;
                        bucket = bucket.next;
                    }
                    selectivity = temp;
                }
                break;
            case LESS_THAN:
                temp = (double)bucket.height/ntups * (v-bucket.left)/bWidth;
                if(bucket.pre == null){
                    selectivity = 0;
                }else {
                    bucket = bucket.pre;
                    while (bucket != histHead) {
                        temp += (double) bucket.height / ntups;
                        bucket = bucket.pre;
                    }
                    selectivity = temp;
                }
                break;
            case LESS_THAN_OR_EQ:
                temp = (double)bucket.height / (bWidth * ntups);
                temp += (double)bucket.height/ntups * (v-bucket.left)/bWidth;
                if(bucket.pre == null){
                    selectivity = 0;
                }else {
                    bucket = bucket.pre;
                    while (bucket != histHead) {
                        temp += (double) bucket.height / ntups;
                        bucket = bucket.pre;
                    }
                    selectivity = temp;
                }
                break;
            case NOT_EQUALS: // 1 - Selectivity(=)
                selectivity = 1 - (double)bucket.height / (bWidth * ntups);
                break;
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
        Bucket current = histHead.next;
        while(current != histTail){
            temp += (double)current.height / ntups;
            current = current.next;
        }
        return temp / numBuckets;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        Bucket current = histHead.next;
        while(current != histTail){
            sb.append("["+current.left+","+current.right+"): "+current.height+"\n");
            current = current.next;
        }
        return sb.toString();
    }
}
