/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.nio;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class BufferChunk {
    public static final byte FIRST_MASK = 0x1;
    public static final byte LAST_MASK = 0x2;
    public static final byte ORDERED_MASK = 0x4;

    private final int idx;

    private int next = -1;

    private final ByteBuffer buf;

    private final AtomicInteger state;

    private volatile int subchunksCreated;

    private final AtomicInteger subchunksReleased;

    public BufferChunk(
        int idx,
        ByteBuffer buf
    ) {
        this.idx = idx;
        this.buf = buf;

        state = new AtomicInteger();
        subchunksReleased = new AtomicInteger();
    }

    boolean reserved() {
        return state.get() != 0;
    }

    public boolean reserve() {
        return state.get() == 0 && state.compareAndSet(0, 1);
    }

    public void release() {
        next = -1;
        buf.clear();
        subchunksCreated = 0;
        subchunksReleased.set(0);
        state.set(0);
    }

    public ByteBuffer buffer() {
        return buf;
    }

    public int index() {
        return idx;
    }

    public void next(int next) {
        assert next != idx;

        this.next = next;
    }

    public int next() {
        return next;
    }

    public boolean hasNext() {
        return next != -1;
    }

    public void releaseSubChunk() {
        int c = subchunksReleased.incrementAndGet();

        if (c == subchunksCreated)
            release();
    }

    void onBeforeRead() {
        buf.flip();
    }

    public BufferReadSubChunk nextSubChunk() {
        if (buf.remaining() < 12)
            return null;

        long threadId = buf.getLong();
        int expLen = buf.getInt();

        byte plc = (byte)(expLen >> 24);
        byte flags = (byte)((expLen >> 16) & 0xFF);

        expLen = expLen & 0xFFFF;

        if (buf.remaining() < expLen) {
            buf.position(buf.position() - 12);

            return null;
        }

        ByteBuffer subBuf = buf.slice();

        subBuf.limit(expLen);

        BufferReadSubChunk ret = new BufferReadSubChunk(threadId, subBuf, this, plc, flags);

        subchunksCreated--;

        buf.position(buf.position() + expLen);

        return ret;
    }

    void copyTo(BufferChunk dst) {
        assert dst.reserved();

        dst.buffer().put(buf);
    }

    void tryRelease() {
        int c = subchunksCreated;

        if (c == 0) {
            release();

            return;
        }

        assert c < 0;

        subchunksCreated = -c;

        if (subchunksReleased.get() == -c)
            release();
    }

    public void onBeforeWrite() {
        buf.clear(); // TODO once threw java.nio.BufferOverflowException
        buf.putLong(Thread.currentThread().getId());
        buf.putInt(0); // Reserve space for size.
    }

    public void onAfterWrite(byte plc, boolean ordered, boolean first, boolean last) {
        int flags = 0;

        if (ordered)
            flags |= ORDERED_MASK;

        if (last)
            flags |= LAST_MASK;

        if (first)
            flags |= FIRST_MASK;

        int data = ((plc & 0xFF) << 24) | (flags << 16) | (buf.position() - 12);

        buf.putInt(8, data);
        buf.flip();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BufferChunk.class, this);
    }
}
