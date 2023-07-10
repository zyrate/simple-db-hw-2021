package simpledb.execution;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op what;

    private Map<Field, List<Integer>> groups; // 分组，只保留聚合列

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groups = new HashMap<>();
        if(gbfield == NO_GROUPING){
            groups.put(null, new LinkedList<>());
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        List<Integer> group = groups.get(gbfield==NO_GROUPING?null:tup.getField(gbfield));
        if(group == null){
            group = new LinkedList<>();
            groups.put(tup.getField(gbfield), group);
        }
        IntField field = (IntField) tup.getField(afield);
        group.add(field.getValue());
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        OpIterator opIterator = new OpIterator() {

            private Iterator<Entry<Field,List<Integer>>> iter;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                if(iter == null)
                   iter = groups.entrySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return iter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!hasNext()) throw new NoSuchElementException();
                Entry<Field,List<Integer>> entry = iter.next(); // 拿到下一个分组
                Field groupField = entry.getKey(); // group by的key
                List<Integer> group = entry.getValue(); // 分组的所有value
                Integer result = null; // 聚合结果
                if(what == Op.COUNT){
                    result = group.size();
                }else{
                    int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE, sum = 0;
                    for(Integer val:group){ // 遍历该组
                        if(min > val) min = val;
                        if(max < val) max = val;
                        if(what == Op.SUM || what == Op.AVG){
                            sum += val;
                        }
                    }
                    switch(what){ // 根据不同的函数赋不同的结果
                        case MIN: result = min; break;
                        case MAX: result = max; break;
                        case SUM: result = sum; break;
                        case AVG: result = sum / group.size(); break;
                    }
                }
                Tuple aggResult = new Tuple(getTupleDesc()); // 结果元组
                if(groupField == null){
                    aggResult.setField(0, new IntField(result));
                }else{
                    aggResult.setField(0, groupField);
                    aggResult.setField(1, new IntField(result));
                }
                return aggResult;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                iter = groups.entrySet().iterator();
            }

            /**
             * 示例
             * 无分组：|count(2)|
             * 有分组：|col(1)|sum(2)|
             */
            @Override
            public TupleDesc getTupleDesc() {
                TupleDesc td;
                if(gbfield == NO_GROUPING){
                    td = new TupleDesc(new Type[]{Type.INT_TYPE},
                                         new String[]{what.toString()+"("+afield+")"});
                }else{
                    td = new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE},
                                         new String[]{"col("+gbfield+")", what.toString()+"("+afield+")"});
                }
                return td;
            }

            @Override
            public void close() {
                iter = null;
            }
            
        };
        return opIterator;
    }

}
