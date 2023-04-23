package io.mysnippet.fuzzy.goggles.evit;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Counter is used to record the actual visits number of keys. <br>
 * To minimize the memory footprint, only the keys that are frequently accessed are recorded. <br>
 * It's not goroutine-safe, every redis client should have its own. <br>
 *
 * @author wangyongtao
 * @date 2020/4/28
 */
@Data
public class LruEviction {

  /** 集群名称 */
  private String clusterName;

  /** 热key容量 */
  private int capacity;

  /** Main lock guarding all access */
  private final ReentrantLock lock;

  private Map<String, ItemNode> items;

  private FreqNode freqHead;

  private Supplier<Boolean> freeCb;

  public LruEviction(int capacity, Supplier<Boolean> freeCb, String clusterName) {
    this.capacity = capacity;
    this.freeCb = freeCb;
    this.lock = new ReentrantLock(true);
    this.items = new ConcurrentHashMap<>(1000);
    this.clusterName = clusterName;
  }

  /**
   * increases the specified key visits.
   *
   * @param key
   */
  public void incr(String key) {

    lock.lock();
    boolean ok = items.containsKey(key);

    if (ok) {
      try {
        // update the item's freq
        increment(items.get(key));
        return;
      } finally {
        lock.unlock();
      }
    }

    try {
      if (this.items.size() >= this.capacity) {
        // evict the least frequently used item
        this.evict();
      }
      ItemNode item = new ItemNode();
      item.setKey(key);
      this.add(item); // 当前key首次计算频次，需要添加
    } finally {
      lock.unlock();
    }
  }

  /** returns the cached key visits and reset it. */
  public Map<String, Long> latch() {
    this.lock.lock();
    Map<String, Long> res = new HashMap<>();
    try {
      items.forEach(
          (key, item) -> {
            res.put(key, item.getFreqNode().getFreq());
          });
      this.reset();
    } finally {
      this.lock.unlock();
    }
    return res;
  }

  public void reset() {
    // TODO: reuse the old datastructures.
    this.setItems(new ConcurrentHashMap<>(1000));
    this.setFreqHead(null);
  }

  public void increment(ItemNode item) {

    FreqNode curFreqNode = item.getFreqNode();
    long curFreq = curFreqNode.getFreq();

    FreqNode targetFreqNode;

    if (curFreqNode.getNext() == null || curFreqNode.getNext().getFreq() != curFreq + 1) {
      FreqNode freqNode = new FreqNode();
      freqNode.setFreq(curFreq + 1);
      targetFreqNode = freqNode;
      curFreqNode.insertAfterMe(targetFreqNode);
    } else {
      targetFreqNode = curFreqNode.getNext();
    }

    item.free();
    targetFreqNode.appendItem(item);

    if (curFreqNode.getItemHead() != null) {
      return;
    }

    // remove current freq node if empty
    if (this.getFreqHead() == curFreqNode) {
      this.setFreqHead(targetFreqNode);
    }
    curFreqNode.free();
  }

  /**
   * 添加一个ItemNode
   *
   * @param item
   */
  public void add(ItemNode item) {
    // 放入一个Map中
    this.items.put(item.getKey(), item);

    if (this.freqHead != null && this.freqHead.freq == 1) {
      this.freqHead.appendItem(item);
      return;
    }

    FreqNode fNode = new FreqNode();
    fNode.setFreq(1);
    fNode.appendItem(item);

    if (this.freqHead != null) {
      this.freqHead.insertBeforeMe(fNode);
    }

    this.freqHead = fNode;
  }

  /** 驱逐最少频繁访问的item */
  public void evict() {
    FreqNode fNode = this.freqHead;
    ItemNode item = fNode.getItemHead();
    this.items.remove(item.getKey());
    fNode.popItem(); // 弹出并删除第一个元素，重新调整itemHead指向

    if (fNode.getItemHead() != null) {
      return;
    }

    this.freqHead = fNode.getNext();
    fNode.free();
  }

  /** frees the counter */
  public void free() {
    if (this.getFreeCb() != null) {
      this.getFreeCb().get();
    }

    this.lock.lock();
    try {
      this.reset();
    } finally {
      this.lock.unlock();
    }
  }

  @Data
  private static final class ItemNode {

    String key;

    FreqNode freqNode;

    ItemNode prev;

    ItemNode next;

    public void free() {
      FreqNode fNode = this.getFreqNode();

      if (fNode.getItemHead() == fNode.getItemTail()) {
        fNode.setItemHead(null);
        fNode.setItemTail(null);
      } else if (fNode.getItemHead() == this) {
        this.getNext().setPrev(null);
        fNode.setItemHead(this.getNext());
      } else if (fNode.getItemTail() == this) {
        this.getPrev().setNext(null);
        fNode.setItemTail(this.getPrev());
      } else {
        this.getPrev().setNext(this.getNext());
        this.getNext().setPrev(this.getPrev());
      }

      // remove all links
      this.setPrev(null);
      this.setNext(null);
      this.setFreqNode(null);
    }
  }

  @Data
  private static final class FreqNode {

    long freq;

    FreqNode prev;

    FreqNode next;

    ItemNode itemHead;

    ItemNode itemTail;

    /**
     * Removes and returns the first(head) ItemNode from this ItemNode linked list.
     *
     * @return
     */
    public ItemNode popItem() {

      if (this.getItemHead() == null) {
        return null;
      }

      // only have one item
      if (this.itemHead == this.itemTail) {
        ItemNode item = this.getItemHead();
        this.itemHead = null;
        this.itemTail = null;
        return item;
      }

      // 多个的情况
      ItemNode item = this.getItemHead();
      item.getNext().setPrev(null); // 存在多个的情况下，next不会为空
      this.setItemHead(item.getNext());
      return item;
    }

    public void appendItem(ItemNode item) {
      item.setFreqNode(this); // ItemNode绑定FreqNode

      if (this.getItemHead() == null) {
        this.setItemHead(item);
        this.setItemTail(item);
        return;
      }

      item.setPrev(this.getItemTail());
      item.setNext(null);
      this.getItemTail().setNext(item);
      this.setItemTail(item);
    }

    public void insertBeforeMe(FreqNode o) {
      if (this.getPrev() != null) {
        this.getPrev().setNext(o);
      }
      o.setPrev(this.getPrev());
      o.setNext(this);
      this.setPrev(o);
    }

    public void insertAfterMe(FreqNode target) {
      target.setNext(this.getNext());
      if (this.getNext() != null) {
        this.getNext().setPrev(target);
      }
      this.setNext(target);
      target.setPrev(this);
    }

    public void free() {
      if (this.getPrev() != null) {
        this.getPrev().setNext(this.getNext());
      }

      if (this.getNext() != null) {
        this.getNext().setPrev(this.getPrev());
      }

      this.setPrev(null);
      this.setNext(null);
      this.setItemHead(null);
      this.setItemTail(null);
    }
  }

  public String getClusterName() {
    return clusterName;
  }
}
