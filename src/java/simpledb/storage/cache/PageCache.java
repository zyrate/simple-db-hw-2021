package simpledb.storage.cache;

import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.Iterator;

public interface PageCache {
    void addPage(Page page);
    Page getPage(PageId pid);
    Page accessPage(PageId pid);
    boolean updatePage(Page page);
    boolean removePage(PageId pid);
    boolean isFull();
    PageId pidToBeEvicted();
    void evictPage();
    Iterator<Page> iterator();
}
