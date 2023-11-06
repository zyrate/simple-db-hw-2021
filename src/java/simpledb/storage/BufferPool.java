package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.storage.cache.LRUBasedCache;
import simpledb.storage.cache.PageCache;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages; // 页面缓存上限
    private final PageCache pageCache; // 自定义的页面缓存结构

    private final LockManager lockManager; // 锁管理器

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pageCache = new LRUBasedCache(numPages);
        this.lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here

        if(perm == Permissions.READ_ONLY){
            // 获取共享锁
            if(!holdsLock(tid, pid)) {
                lockManager.acquireSharedLock(tid, pid);
            }
        }else{
            // 获取排他锁 - 存在锁升级情况，所以不判断holdsLock
            lockManager.acquireExclusiveLock(tid, pid);
        }

        Page page = pageCache.accessPage(pid);
        if(page == null) {
            Catalog catalog = Database.getCatalog();
            DbFile f = catalog.getDatabaseFile(pid.getTableId());
            page = f.readPage(pid);
            
            if(pageCache.isFull()){ // 需要页面置换
                evictPage();
            }
            pageCache.putPage(page);
        }

        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        List<PageId> pages = lockManager.getLookupList(tid);
        if(commit){ // 提交，所有相关页面写入磁盘
            try {
                flushPages(tid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }else{ // Abort，丢弃所有页面
            for(PageId pid : pages){
                discardPage(pid);
            }
        }
        for(PageId pid : pages){
            lockManager.releaseLock(tid, pid); // 逐个释放锁
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        Catalog catalog = Database.getCatalog();
        DbFile f = catalog.getDatabaseFile(tableId);
        List<Page> pages = f.insertTuple(tid, t);
        for(Page page:pages){
            page.markDirty(true, tid);
            // 更新缓存
            pageCache.putPage(page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        Catalog catalog = Database.getCatalog();
        DbFile f = catalog.getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = f.deleteTuple(tid, t);
        for(Page page:pages){
            page.markDirty(true, tid);
            // 更新缓存
            pageCache.putPage(page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Iterator<Page> iterator = pageCache.iterator();
        while(iterator.hasNext()){
            Page page = iterator.next();
            if(page.isDirty() != null) {
                flushPage(page.getId());
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageCache.removePage(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = pageCache.getPage(pid);
        if(page == null) return;

        TransactionId dirtier = page.isDirty();
        if(dirtier != null){
            Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
            Database.getLogFile().force();
            f.writePage(page);
        }

        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for(PageId pid : lockManager.getLookupList(tid)){
            flushPage(pid);
            Page page = pageCache.getPage(pid);
            if(page != null)
                page.setBeforeImage(); // 用当前的内容当做下一次的before
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        PageId pageToEvict = pageCache.pidToBeEvicted();
        if(pageToEvict == null){
            throw new DbException("All pages in the buffer pool are dirty.");
        }
        try {
            flushPage(pageToEvict);
        } catch (IOException e) {
            e.printStackTrace();
        }

        pageCache.evictPage();
    }

    public PageCache getPageCache(){
        return pageCache;
    }
}
