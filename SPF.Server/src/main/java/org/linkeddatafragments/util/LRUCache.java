package org.linkeddatafragments.util;

import java.util.HashMap;
import java.util.Map;

public class LRUCache<K, V> {
    private Node<K, V> lru;
    private Node<K, V> mru;
    private Map<K, Node<K, V>> container;
    private int capacity;
    private int currentSize;
    private boolean inserted = false;
    private Lock lock = new Lock();
    private boolean hasLock = false;

    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.currentSize = 0;
        lru = new Node<K, V>(null, null, null, null);
        mru = lru;
        container = new HashMap<K, Node<K, V>>();
    }

    public V get(K key) {
        Node<K, V> tempNode = container.get(key);
        try {
            lock.lock();
            if (tempNode == null) {
                return null;
            } else if (tempNode.key == mru.key) {
                return mru.value;
            }

            Node<K, V> nextNode = tempNode.next;
            Node<K, V> prevNode = tempNode.prev;

            if (tempNode.key == lru.key) {
                nextNode.prev = null;
                lru = nextNode;
            } else if (tempNode.key != mru.key) {
                prevNode.next = nextNode;
                nextNode.prev = prevNode;
            }

            // Finally move our item to the MRU
            tempNode.prev = mru;
            mru.next = tempNode;
            mru = tempNode;
            mru.next = null;
        } catch (InterruptedException e) {
            return null;
        } finally {
            lock.unlock();
        }

        return tempNode.value;
    }

    public boolean containsKey(K key) {
        return container.containsKey(key) || (inserted &&
                ((lru.key != null && lru.key.equals(key)) ||
                        (mru.key != null && mru.key.equals(key))));
    }

    public void remove(K key) {
        if (!container.containsKey(key)) return;

        try {
            lock.lock();
            currentSize--;
            Node<K, V> node = container.get(key);
            if (node.next == null && node.prev == null) {
                inserted = false;
                lru = new Node<K, V>(null, null, null, null);
                mru = lru;
                container.remove(key);
                return;
            }

            if (node.next == null) {
                Node<K, V> mru = node.prev;
                mru.next = null;
                this.mru = mru;
                container.remove(key);
                return;
            }

            if (node.prev == null) {
                Node<K, V> lru = node.next;
                lru.prev = null;
                this.lru = lru;
                container.remove(key);
                return;
            }

            node.prev.next = node.next;
            node.next.prev = node.prev;
            container.remove(key);
        } catch (InterruptedException e) {
            return;
        } finally {
            lock.unlock();
        }
    }

    public void put(K key, V value) {
        if (container.containsKey(key)) {
            return;
        }

        try {
            lock.lock();
            inserted = true;
            Node<K, V> myNode;
            if (mru.key == null)
                myNode = new Node<K, V>(null, null, key, value);
            else
                myNode = new Node<K, V>(mru, null, key, value);
            mru.next = myNode;
            container.put(key, myNode);
            mru = myNode;

            if (currentSize == capacity) {
                container.remove(lru.key);
                lru = lru.next;
                lru.prev = null;
            } else if (currentSize < capacity) {
                if (currentSize == 0) {
                    lru = myNode;
                }
                currentSize++;
            }
        } catch (InterruptedException e) {
            return;
        } finally {
            lock.unlock();
        }
    }

    static class Lock {
        boolean isLocked = false;
        Thread lockedBy = null;
        int lockedCount = 0;

        public synchronized void lock()
                throws InterruptedException {
            Thread callingThread = Thread.currentThread();
            while (isLocked && lockedBy != callingThread) {
                wait();
            }
            isLocked = true;
            lockedCount++;
            lockedBy = callingThread;
        }


        public synchronized void unlock() {
            if (Thread.currentThread() == this.lockedBy) {
                lockedCount--;

                if (lockedCount == 0) {
                    isLocked = false;
                    notify();
                }
            }
        }
    }

    static class Node<T, U> {
        T key;
        U value;
        Node<T, U> prev;
        Node<T, U> next;

        public Node(Node<T, U> prev, Node<T, U> next, T key, U value) {
            this.prev = prev;
            this.next = next;
            this.key = key;
            this.value = value;
        }
    }

}
