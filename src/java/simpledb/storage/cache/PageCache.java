package simpledb.storage.cache;

import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.Iterator;

public interface PageCache {
    void putPage(Page page);
    Page getPage(PageId pid);
    Page accessPage(PageId pid);
    void removePage(PageId pid);
    boolean isFull();
    PageId pidToBeEvicted();
    void evictPage();
    Iterator<Page> iterator();
}
