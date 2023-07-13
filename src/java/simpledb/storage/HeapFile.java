package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td; // 每个HeapFile（DbFile）存储一个table

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsolutePath().hashCode(); // 唯一的ID - tableId
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        // 找到对应Page所在的偏移量，读取后生成HeapPage
        int pageSize = BufferPool.getPageSize();
        int offset = pid.getPageNumber() * pageSize;
        byte[] data = new byte[pageSize];
        Page heapPage = null;
        try (RandomAccessFile f = new RandomAccessFile(file, "r")) {
            f.seek(offset);
            f.read(data);
            heapPage = new HeapPage((HeapPageId)pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageSize = BufferPool.getPageSize();
        int offset = page.getId().getPageNumber() * pageSize;
        byte[] data = page.getPageData();
        try (RandomAccessFile f = new RandomAccessFile(file, "rw")) {
            f.seek(offset);
            f.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // 文件大小除以PageSize
        long numPages = file.length() / (long) BufferPool.getPageSize();
        return (int) numPages;
    }

    // see DbFile.java for javadocs
    // 这里的多个Page受影响可能是有副本的意思？
    // 注意不要依赖tuple里面的RecordId，因为是无意义的
    // 但是这个RecordId什么时候会用到？这里需不需要赋值？- 不需要，在删除的时候用到
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        List<Page> pages = new ArrayList<>();

        // 遍历所有的页面看是否有空闲
        for(int i=0; i<numPages(); i++){
            PageId pid = new HeapPageId(getId(), i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if(heapPage.getNumEmptySlots() > 0){
                heapPage.insertTuple(t);
                heapPage.markDirty(true, tid);
                pages.add(heapPage);
                return pages;
            }
        }
        
        // 页面不够了，新建页面，写入文件
        HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        pages.add(newPage);
        writePage(newPage); // 因为测试里要计算numPages，所以在这里要写入文件
        return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pages = new ArrayList<>();
        PageId pid = t.getRecordId().getPageId();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        DbFileIterator dbFileIterator = new DbFileIterator() {
            private BufferPool bufferPool;
            private int nextPageNo = 0;
            private Iterator<Tuple> currPageIter;

            private Iterator<Tuple> getPageIter(int pageNo) throws TransactionAbortedException, DbException{
                HeapPageId pid = new HeapPageId(getId(), pageNo); 
                Iterator<Tuple> iterator = ((HeapPage) bufferPool.getPage(tid, pid, Permissions.READ_ONLY)).iterator();
                return iterator;
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                if(bufferPool == null){
                    bufferPool = Database.getBufferPool();
                    currPageIter = getPageIter(nextPageNo);
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(bufferPool == null) return false;
                
                if(currPageIter.hasNext()){
                    return true;
                }else{
                    nextPageNo ++;
                    while(nextPageNo < numPages()){ // 这里注意要能够跳过中间的空位，直到最后一页
                        currPageIter = getPageIter(nextPageNo);
                        if(currPageIter.hasNext())
                            return true;
                        nextPageNo ++; 
                    }
                    return false;              
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!hasNext()) throw new NoSuchElementException();
                return currPageIter.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                nextPageNo = 0;
                currPageIter = getPageIter(nextPageNo);
            }

            @Override
            public void close() {
                bufferPool = null;
            }
            
        };
        return dbFileIterator;
    }

}

