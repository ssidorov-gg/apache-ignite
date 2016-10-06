package org.apache.ignite.internal.managers.communication;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.locks.LockSupport.park;
import static java.util.concurrent.locks.LockSupport.unpark;

public final class ConcurrentLinkedBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
    static final int INITIAL_ARRAY_SIZE = 512;
    static final Node BLOCKED = new Node();

    final AtomicReference<Node> putStack = new AtomicReference<Node>();
    private final AtomicInteger takeStackSize = new AtomicInteger();

    private Thread consumerThread;
    private Object[] takeStack = new Object[INITIAL_ARRAY_SIZE];
    private int takeStackIndex = -1;

    public ConcurrentLinkedBlockingQueue(Thread consumerThread) {
        this.consumerThread = consumerThread;
    }

    public ConcurrentLinkedBlockingQueue() {
    }
    public static int nextPowerOfTwo(final int value) {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    public void setConsumerThread(Thread consumerThread) {
        this.consumerThread = consumerThread;
    }

    @Override
    public void clear() {
        putStack.set(BLOCKED);
    }

    @Override
    public boolean offer(E item) {
        AtomicReference<Node> putStack = this.putStack;
        Node newHead = new Node();
        newHead.item = item;

        for (; ; ) {
            Node oldHead = putStack.get();
            if (oldHead == null || oldHead == BLOCKED) {
                newHead.next = null;
                newHead.size = 1;
            } else {
                newHead.next = oldHead;
                newHead.size = oldHead.size + 1;
            }

            if (!putStack.compareAndSet(oldHead, newHead)) {
                continue;
            }

            if (oldHead == BLOCKED) {
                unpark(consumerThread);
            }

            return true;
        }
    }

    @Override
    public E take() throws InterruptedException {
        E item = next();
        if (item != null) {
            return item;
        }

        takeAll();
        assert takeStackIndex == 0;
        assert takeStack[takeStackIndex] != null;

        return next();
    }

    @Override
    public E poll() {
        E item = next();

        if (item != null) {
            return item;
        }

        if (!drainPutStack()) {
            return null;
        }

        return next();
    }

    private E next() {
        if (takeStackIndex == -1) {
            return null;
        }

        if (takeStackIndex == takeStack.length) {
            takeStackIndex = -1;
            return null;
        }

        E item = (E) takeStack[takeStackIndex];
        if (item == null) {
            takeStackIndex = -1;
            return null;
        }

        takeStack[takeStackIndex] = null;
        takeStackIndex++;
        takeStackSize.lazySet(takeStackSize.get() - 1);
        return item;
    }

    private void takeAll() throws InterruptedException {
        long iteration = 0;
        AtomicReference<Node> putStack = this.putStack;
        for (; ; ) {
            if (consumerThread.isInterrupted()) {
                putStack.compareAndSet(BLOCKED, null);
                throw new InterruptedException();
            }

            Node currentPutStackHead = putStack.get();

            if (currentPutStackHead == null) {
                // there is nothing to be take, so lets block.
                if (!putStack.compareAndSet(null, BLOCKED)) {
                    // we are lucky, something is available
                    continue;
                }

                // lets block for real.
                park();
            } else if (currentPutStackHead == BLOCKED) {
                park();
            } else {
                if (!putStack.compareAndSet(currentPutStackHead, null)) {
                    continue;
                }

                copyIntoTakeStack(currentPutStackHead);
                break;
            }
            iteration++;
        }
    }

    private boolean drainPutStack() {
        for (; ; ) {
            Node head = putStack.get();
            if (head == null) {
                return false;
            }

            if (putStack.compareAndSet(head, null)) {
                copyIntoTakeStack(head);
                return true;
            }
        }
    }

    private void copyIntoTakeStack(Node putStackHead) {
        int putStackSize = putStackHead.size;

        takeStackSize.lazySet(putStackSize);

        if (putStackSize > takeStack.length) {
            takeStack = new Object[nextPowerOfTwo(putStackHead.size)];
        }

        for (int i = putStackSize - 1; i >= 0; i--) {
            takeStack[i] = putStackHead.item;
            putStackHead = putStackHead.next;
        }

        takeStackIndex = 0;
        assert takeStack[0] != null;
    }

    /**
     * {@inheritDoc}.
     *
     * Best effort implementation.
     */
    @Override
    public int size() {
        Node h = putStack.get();
        int putStackSize = h == null ? 0 : h.size;
        return putStackSize + takeStackSize.get();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public void put(E e) throws InterruptedException {
        offer(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        add(e);
        return true;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public E peek() {
        throw new UnsupportedOperationException();
    }

    private static final class Node<E> {
        Node next;
        E item;
        int size;
    }
}
