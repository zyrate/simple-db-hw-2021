package simpledb.execution;

import java.util.HashMap;
import java.util.Iterator;
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
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op what;

    private Map<Field, Integer> groups; // 分组，实时计算count

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what != Op.COUNT) throw new IllegalArgumentException("only COUNT aggregation in StringAggregator");
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groups = new HashMap<>();
        if(gbfield == NO_GROUPING){
            groups.put(null, 0);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Integer countVal = groups.get(gbfield==NO_GROUPING?null:tup.getField(gbfield));
        if(countVal == null){
            countVal = 0;
            groups.put(tup.getField(gbfield), countVal);
        }
        groups.put(tup.getField(gbfield), countVal+1); // 注意这里不能countVal++
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        OpIterator opIterator = new OpIterator() {

            private Iterator<Entry<Field, Integer>> iter;

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
                Entry<Field, Integer> entry = iter.next();
                Field groupField = entry.getKey();
                int countVal = entry.getValue();
                Tuple aggResult = new Tuple(getTupleDesc()); // 结果元组
                if(groupField == null){
                    aggResult.setField(0, new IntField(countVal));
                }else{
                    aggResult.setField(0, groupField);
                    aggResult.setField(1, new IntField(countVal));
                }
                return aggResult;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                iter = groups.entrySet().iterator();
            }

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
