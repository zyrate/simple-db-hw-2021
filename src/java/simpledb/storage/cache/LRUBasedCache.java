package simpledb.storage.cache;

import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 自定义的PageCache结构
 * 实现LRU置换算法
 * GET/PUT操作时间复杂度O(1)
 */
public class LRUBasedCache implements PageCache{
    /**
     * 双向链表结点
     */
    private static class Node{
        Page page;
        Node pre;
        Node next;
        public Node(Page page){
            this.page = page;
        }
    }


    private final int capacity;
    private Map<PageId, Node> map;
    private Node head;
    private Node tail;

    public LRUBasedCache(int capacity){
        this.capacity = capacity;
        map = new ConcurrentHashMap<>(capacity);
        // 创建两个哨兵节点
        this.head = new Node(null);
        this.tail = new Node(null);
        head.next = tail;
        tail.pre = head;
    }

    private void addToHead(Node node){
        node.next = head.next;
        node.pre = head;
        head.next.pre = node;
        head.next = node;
    }

    private void moveToHead(Node node){
        node.pre.next = node.next;
        node.next.pre = node.pre;
        addToHead(node);
    }

    @Override
    public synchronized void putPage(Page page) {
        Node node = map.get(page.getId());
        if(node == null){
            node = new Node(page);
            map.put(page.getId(), node);
            addToHead(node);
        }else {
            node.page = page;
            moveToHead(node);
        }
    }

    // BufferPool内部访问Page
    @Override
    public Page getPage(PageId pid) {
        Node node = map.get(pid);
        if(node == null){
            return null;
        }
        return node.page;
    }

    // BufferPool外部访问Page
    @Override
    public synchronized Page accessPage(PageId pid) {
        Node node = map.get(pid);
        if(node == null){
            return null;
        }
        moveToHead(node); // LRU算法 - 向链表头部移动
        return node.page;
    }

    @Override
    public synchronized void removePage(PageId pid) {
        Node node = map.get(pid);
        if(node == null){
            return;
        }
        node.pre.next = node.next;
        node.next.pre = node.pre;
        map.remove(pid);
    }

    @Override
    public boolean isFull() {
        return map.size() >= capacity;
    }

    @Override
    public synchronized PageId pidToBeEvicted() {
        Node n = tail.pre;
        while(n != head){
            if(n.page.isDirty() != null){ // 确保不是脏页
                n = n.pre;
                continue;
            }
            break;
        }
        return n==head?null:n.page.getId(); // 返回null代表全都是脏页
    }

    @Override
    public synchronized void evictPage() {
        removePage(pidToBeEvicted()); // 移除掉链表尾部结点
    }

    @Override
    public Iterator<Page> iterator() {
        return new Iterator<Page>() {
            Node curr = head;
            @Override
            public boolean hasNext() {
                return curr.next != tail;
            }

            @Override
            public Page next() {
                curr = curr.next;
                return curr.page;
            }
        };
    }
}
