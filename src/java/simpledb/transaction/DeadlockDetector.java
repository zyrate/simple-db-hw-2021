package simpledb.transaction;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 死锁检测
 * - 用DFS算法去定位环的位置
 */
public class DeadlockDetector {

    private Map<TransactionId, List<TransactionId>> adjList; // 图的邻接表
    private Map<TransactionId, Integer> nodeState; // 节点状态 - null:未访问，1:已访问，2:在递归栈内
    private TransactionId victim; // 要被abort的事务 - 暂时用不上

    public DeadlockDetector(){
        adjList = new ConcurrentHashMap<>();
        nodeState = new ConcurrentHashMap<>();
    }

    /**
     * 发生了阻塞
     * @param tid - 被阻塞的事务
     * @param listToWait - 有等待多个持共享锁事务的情况（可以直接将holds传进来）
     */
    public void blockOccurs(TransactionId tid, List<TransactionId> listToWait){
        adjList.put(tid, listToWait);
    }

    /**
     * 被唤醒了（不管什么原因）
     * - 删除此节点
     * @param tid
     */
    public void notified(TransactionId tid){
        adjList.remove(tid);
    }

    /**
     * 检测是否有死锁（回路）
     * @return
     */
    public boolean detectCycle(){
        nodeState.clear();
        for(TransactionId tid:adjList.keySet()){
            if(dfs(tid)){
                return true;
            }
        }
        return false;
    }

    /**
     * DFS检测环，跳过自反边
     * @param tid
     * @return
     */
    private boolean dfs(TransactionId tid){
        nodeState.put(tid, 2); // 入栈
        List<TransactionId> adj = adjList.get(tid);
        if(adj != null){
            for(TransactionId t:adj){
                if(tid.equals(t)) continue; // 跳过自反边的情况 - 单个锁升级等待不算死锁
                int state = nodeState.getOrDefault(t, 0);
                if(state == 2){
                    victim = t;
                    return true; // 找到环
                }else if(state == 0 && dfs(t)){
                    return true; // 找到环
                }
            }
        }
        nodeState.put(tid, 1); // 出栈，标记已访问
        return false;
    }

    public TransactionId getVictim(){
        return victim;
    }

}
