package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.LinkedList;
import java.util.concurrent.CopyOnWriteArrayList;

class PageLock{
    private PageId pageId;
    private int lockState; // 0 空闲，-1 排他锁，>0 获取到共享锁事务数量
    CopyOnWriteArrayList<TransactionId> holds; // 获取锁到的事务
    public PageLock(PageId pageId){
        this.pageId = pageId;
        holds = new CopyOnWriteArrayList<>();
    }
    // 下面两个必须要同步
    public synchronized void stateIncrement(int n){
        lockState += n;
    }
    public synchronized int getLockState(){
        return lockState;
    }
}