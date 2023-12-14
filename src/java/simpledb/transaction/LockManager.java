package simpledb.transaction;

import simpledb.common.Database;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 管理事务和锁的状态
 */
public class LockManager {
    // 页面锁集合
    private Map<PageId, PageLock> pageLocks;

    // 事务持有锁查询表
    private Map<TransactionId, List<PageId>> lookups;

    // 死锁检测器
    private DeadlockDetector deadlockDetector;

    public LockManager(){
        this.pageLocks = new ConcurrentHashMap<>();
        this.lookups = new ConcurrentHashMap<>();
        this.deadlockDetector = new DeadlockDetector();
    }

    private PageLock getPageLock(PageId pid){
        PageLock pageLock = pageLocks.get(pid);
        if(pageLock == null){
            pageLock = new PageLock(pid);
            pageLocks.put(pid, pageLock);
        }
        return pageLock;
    }

    private void addToLookups(TransactionId tid, PageId pid){
        List<PageId> list = lookups.computeIfAbsent(tid, k -> new CopyOnWriteArrayList<>());
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
        return lookups.getOrDefault(tid, new ArrayList<>(0));
    }

    // 获取共享锁
    public void acquireSharedLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        PageLock pageLock = getPageLock(pid);
        synchronized (pageLock){
            // 已经有排他锁且不是同一个事务（一个事务可以同时拥有两种锁）
            while(pageLock.getLockState() == -1 && !pageLock.holds.get(0).equals(tid)){
                deadlockDetector.blockOccurs(tid, pageLock.holds);
                if(deadlockDetector.detectCycle()){ // 检测到死锁
                    deadlockDetector.notified(tid);
                    throw new TransactionAbortedException();
                }
                try {
                    pageLock.wait();
                    deadlockDetector.notified(tid);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if(pageLock.getLockState() > 0 && pageLock.holds.contains(tid)){
                // 重入共享锁 - 不记录
                return;
            }
            // 被唤醒后，记录已获取状态
            pageLock.stateIncrement(1);
            pageLock.holds.add(tid);
            addToLookups(tid, pid);
        }
    }

    // 获取排他锁
    public void acquireExclusiveLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        PageLock pageLock = getPageLock(pid);
        synchronized (pageLock){
            while(pageLock.getLockState() != 0){
                // 该事务已经获取了共享锁，升级为排他锁
                if(pageLock.getLockState() == 1 && pageLock.holds.get(0).equals(tid)){
                    pageLock.stateIncrement(-2);
                    return;
                }else if(pageLock.getLockState() == -1 && pageLock.holds.get(0).equals(tid)){
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
                    pageLock.wait();
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
    }

    // 事务是否对某一页面持有锁
    public boolean holdsLock(TransactionId tid, PageId pid){
        PageLock pageLock = getPageLock(pid);
        return pageLock.holds.contains(tid);
    }

    // 释放锁
    public void releaseLock(TransactionId tid, PageId pid){
        PageLock pageLock = getPageLock(pid);
        synchronized (pageLock){
            pageLock.holds.remove(tid);
            removeFromLookups(tid, pid);
            if(pageLock.getLockState() == -1) {
                pageLock.stateIncrement(1);
            }else if(pageLock.getLockState() > 0){
                pageLock.stateIncrement(-1);
            }
            // 释放后，没有事务拿着锁了，唤醒阻塞的事务去竞争（或只有一个事务拿着锁 - 锁升级的情况）
            if(pageLock.getLockState() == 0 || pageLock.getLockState() == 1){
                pageLock.notify();
            }
        }
    }


    // 不用下面的定时检测的方法了 - 因为主程序接不到异常
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
