package simpledb.transaction;

import simpledb.common.Database;
import simpledb.storage.PageId;

import java.util.*;

/**
 * 管理事务和锁的状态
 */
public class LockManager {
    class PageLock{
        PageId pageId;
        int lockState; // 0 空闲，-1 排他锁，>0 获取到共享锁事务数量
        LinkedList<TransactionId> holds; // 获取锁到的事务
        public PageLock(PageId pageId){
            this.pageId = pageId;
            holds = new LinkedList<>();
        }
        public synchronized void stateIncrement(int n){
            lockState += n;
        }
    }

    // 页面锁集合
    private Map<PageId, PageLock> pageLocks;

    // 事务持有锁查询表
    private Map<TransactionId, List<PageId>> lookups;

    // 死锁检测器
    private DeadlockDetector deadlockDetector;

    public LockManager(){
        this.pageLocks = new HashMap<>();
        this.lookups = new HashMap<>();
        this.deadlockDetector = new DeadlockDetector();
    }

    private PageLock requirePageLock(PageId pid){
        PageLock pageLock = pageLocks.get(pid);
        if(pageLock == null){
            pageLock = new PageLock(pid);
            pageLocks.put(pid, pageLock);
        }
        return pageLock;
    }

    private void addToLookups(TransactionId tid, PageId pid){
        List<PageId> list = lookups.computeIfAbsent(tid, k -> new LinkedList<>());
        list.add(pid);
    }

    private void removeFromLookups(TransactionId tid, PageId pid){
        List<PageId> list = lookups.get(tid);
        list.remove(pid);
        if(list.isEmpty()){
            lookups.remove(tid);
        }
    }

    // 获取某个事务拥有哪些页面的锁
    public List<PageId> getLookupList(TransactionId tid){
        return new ArrayList<>(lookups.get(tid)); // 新建一个List避免并发修改问题
    }

    // 获取共享锁
    public synchronized void acquireSharedLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        PageLock pageLock = requirePageLock(pid);

        // 已经有排他锁且不是同一个事务（一个事务可以同时拥有两种锁）
        while(pageLock.lockState == -1 && !pageLock.holds.get(0).equals(tid)){
            deadlockDetector.blockOccurs(tid, pageLock.holds);
            if(deadlockDetector.detectCycle()){ // 检测到死锁
                deadlockDetector.notified(tid);
                throw new TransactionAbortedException();
            }
            try {
                wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // 被唤醒后，记录已获取状态
        pageLock.stateIncrement(1);
        pageLock.holds.add(tid);
        addToLookups(tid, pid);
    }

    // 获取排他锁
    public synchronized void acquireExclusiveLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        PageLock pageLock = requirePageLock(pid);

        while(pageLock.lockState != 0){
            // 该事务已经获取了共享锁，升级为排他锁
            if(pageLock.lockState == 1 && pageLock.holds.get(0).equals(tid)){
                pageLock.stateIncrement(-2);
                return;
            }else if(pageLock.lockState == -1 && pageLock.holds.get(0).equals(tid)){
                // 重入排他锁 - 不记录
                return;
            }
            // 否则阻塞
            deadlockDetector.blockOccurs(tid, pageLock.holds);
            if(deadlockDetector.detectCycle()){ // 检测到死锁
                deadlockDetector.notified(tid);
                throw new TransactionAbortedException();
            }
            try {
                wait();
                deadlockDetector.notified(tid);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // 被唤醒后，记录已获取状态
        pageLock.stateIncrement(-1);
        pageLock.holds.add(tid);
        addToLookups(tid, pid);
    }

    // 事务是否对某一页面持有锁
    public boolean holdsLock(TransactionId tid, PageId pid){
        PageLock pageLock = requirePageLock(pid);
        return pageLock.holds.contains(tid);
    }

    // 释放锁
    public synchronized void releaseLock(TransactionId tid, PageId pid){
        PageLock pageLock = requirePageLock(pid);
        pageLock.holds.remove(tid);
        removeFromLookups(tid, pid);
        if(pageLock.lockState == -1) {
            pageLock.stateIncrement(1);
        }else if(pageLock.lockState > 0){
            pageLock.stateIncrement(-1);
        }
        // 释放后，没有事务拿着锁了，唤醒阻塞的事务去竞争（或只有一个事务拿着锁 - 锁升级的情况）
        if(pageLock.lockState == 0 || pageLock.lockState == 1){
            notifyAll();
        }
    }


    // 不用定时检测的方法了 - 因为主程序接不到异常
    @Deprecated
    private volatile boolean detecting = false;

    @Deprecated
    public void startDetecting(int interval){
        detecting = true;
        new Thread(() -> {
            while(detecting){
                System.out.println("Detecting...");
                if(deadlockDetector.detectCycle()){
                    TransactionId victim = deadlockDetector.getVictim();
                    System.out.println("victim: "+victim);
                    Database.getBufferPool().transactionComplete(victim, false);
                }
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
    }
    @Deprecated
    public void stopDetecting(){
        detecting = false;
    }


}
