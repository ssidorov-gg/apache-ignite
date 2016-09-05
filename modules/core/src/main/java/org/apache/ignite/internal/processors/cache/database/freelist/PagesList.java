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

package org.apache.ignite.internal.processors.cache.database.freelist;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.InitNewPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListAddPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListInitNewPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListRemoveLastPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListRemovePageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListSetNextRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListSetPreviousRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.RecycleRecord;
import org.apache.ignite.internal.processors.cache.database.DataStructure;
import org.apache.ignite.internal.processors.cache.database.freelist.io.PagesListMetaIO;
import org.apache.ignite.internal.processors.cache.database.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.util.GridArrays;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.getPageId;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.initPage;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.isWalDeltaRecordNeeded;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.writePage;

/**
 * Striped doubly-linked list of page IDs optionally organized in buckets.
 */
public abstract class PagesList extends DataStructure {
    /** */
    private AtomicIntegerArray cnts = new AtomicIntegerArray(256);

    /** */
    private final CheckingPageHandler<Void> cutTail = new CheckingPageHandler<Void>() {
        @Override protected boolean run0(long pageId, Page page, ByteBuffer buf, PagesListNodeIO io,
            Void ignore, int bucket) throws IgniteCheckedException {
            long tailId = io.getNextId(buf);

            assert tailId != 0;

            io.setNextId(buf, 0L);

            if (isWalDeltaRecordNeeded(wal, page))
                wal.log(new PagesListSetNextRecord(cacheId, pageId, 0L));

            updateTail(bucket, tailId, pageId);

            return true;
        }
    };

    /** */
    private final CheckingPageHandler<ByteBuffer> putDataPage = new CheckingPageHandler<ByteBuffer>() {
        @Override protected boolean run0(long pageId, Page page, ByteBuffer buf, PagesListNodeIO io,
            ByteBuffer dataPageBuf, int bucket) throws IgniteCheckedException {
            if (io.getNextId(buf) != 0L)
                return false; // Splitted.

            long dataPageId = getPageId(dataPageBuf);
            DataPageIO dataIO = DataPageIO.VERSIONS.forPage(dataPageBuf);

            int idx = io.addPage(buf, dataPageId);

            if (idx == -1)
                handlePageFull(pageId, page, buf, io, dataPageId, dataIO, dataPageBuf, bucket);
            else {
                if (isWalDeltaRecordNeeded(wal, page))
                    wal.log(new PagesListAddPageRecord(cacheId, pageId, dataPageId));

                dataIO.setFreeListPageId(dataPageBuf, pageId);

                cnts.incrementAndGet(bucket);
            }

            return true;
        }

        /**
         * @param pageId Page ID.
         * @param page Page.
         * @param buf Buffer.
         * @param io IO.
         * @param dataPageId Data page ID.
         * @param dataIO Data page IO.
         * @param dataPageBuf Data page buffer.
         * @param bucket Bucket index.
         * @throws IgniteCheckedException If failed.
         */
        private void handlePageFull(
            long pageId,
            Page page,
            ByteBuffer buf,
            PagesListNodeIO io,
            long dataPageId,
            DataPageIO dataIO,
            ByteBuffer dataPageBuf,
            int bucket
        ) throws IgniteCheckedException {
            // Attempt to add page failed: the node page is full.
            if (isReuseBucket(bucket)) {
                // If we are on the reuse bucket, we can not allocate new page, because it may cause deadlock.
                assert dataIO.isEmpty(dataPageBuf); // We can put only empty data pages to reuse bucket.

                // Change page type to index and add it as next node page to this list.
                dataPageId = PageIdUtils.changeType(dataPageId, FLAG_IDX);

                setupNextPage(io, pageId, buf, dataPageId, dataPageBuf);
                updateTail(bucket, pageId, dataPageId);
            }
            else {
                // Just allocate a new node page and add our data page there.
                long nextId = allocatePage(null);

                try (Page next = page(nextId)) {
                    ByteBuffer nextBuf = next.getForWrite();

                    try {
                        setupNextPage(io, pageId, buf, nextId, nextBuf);

                        if (isWalDeltaRecordNeeded(wal, page))
                            wal.log(new PagesListSetNextRecord(cacheId, pageId, nextId));

                        int idx = io.addPage(nextBuf, dataPageId);

                        if (isWalDeltaRecordNeeded(wal, next))
                            wal.log(new PagesListInitNewPageRecord(cacheId, nextId, pageId, dataPageId));

                        assert idx != -1;

                        dataIO.setFreeListPageId(dataPageBuf, nextId);

                        updateTail(bucket, pageId, nextId);

                        cnts.incrementAndGet(bucket);
                    }
                    finally {
                        next.releaseWrite(true);
                    }
                }
            }
        }
    };

    /** */
    private final CheckingPageHandler<ReuseBag> putReuseBag = new CheckingPageHandler<ReuseBag>() {
        @SuppressWarnings("ForLoopReplaceableByForEach")
        @Override protected boolean run0(final long pageId, Page page, final ByteBuffer buf, PagesListNodeIO io,
            ReuseBag bag, int bucket) throws IgniteCheckedException {
            if (io.getNextId(buf) != 0L)
                return false; // Splitted.

            long nextId;
            ByteBuffer prevBuf = buf;
            long prevId = pageId;

            List<Page> locked = null;

            try {
                while ((nextId = bag.pollFreePage()) != 0L) {
                    int idx = io.addPage(prevBuf, nextId);

                    if (idx == -1) { // Attempt to add page failed: the node page is full.
                        Page next = page(nextId);

                        ByteBuffer nextBuf = next.getForWrite();

                        if (locked == null)
                            locked = new ArrayList<>(2);

                        locked.add(next);

                        setupNextPage(io, prevId, prevBuf, nextId, nextBuf);

                        if (isWalDeltaRecordNeeded(wal, page))
                            wal.log(new PagesListSetNextRecord(cacheId, pageId, nextId));

                        if (isWalDeltaRecordNeeded(wal, next))
                            wal.log(new PagesListInitNewPageRecord(cacheId, nextId, pageId, 0L));

                        // Switch to this new page, which is now a part of our list
                        // to add the rest of the bag to the new page.
                        prevBuf = nextBuf;
                        prevId = nextId;
                        page = next;
                    }
                    else {
                        // TODO: use single WAL record for bag?
                        if (isWalDeltaRecordNeeded(wal, page))
                            wal.log(new PagesListAddPageRecord(cacheId, pageId, nextId));

                        cnts.incrementAndGet(bucket);
                    }
                }
            }
            finally {
                if (locked != null) {
                    // We have to update our bucket with the new tail.
                    updateTail(bucket, pageId, prevId);

                    // Release write.
                    for (int i = 0; i < locked.size(); i++)
                        locked.get(i).releaseWrite(true);
                }
            }

            return true;
        }
    };

    /** */
    private long metaPageId;

    /**
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     * @throws IgniteCheckedException If failed.
     */
    public PagesList(int cacheId,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        long metaPageId)
        throws IgniteCheckedException {
        super(cacheId, pageMem, wal);
        Map<Integer, GridLongList> buckets = new HashMap<>();

        long nextPageId = metaPageId;

        while (nextPageId != 0) {
            try (Page page = page(metaPageId)) {
                ByteBuffer buf = page.getForRead();

                try {
                    PagesListMetaIO io = PagesListMetaIO.VERSIONS.forPage(buf);

                    io.getBucketsData(buf, buckets);

                    nextPageId = io.getNextMetaPageId(buf);
                }
                finally {
                    page.releaseRead();
                }
            }
        }

        for (Map.Entry<Integer, GridLongList> e : buckets.entrySet()) {
            long[] old = getBucket(e.getKey());
            assert old == null;

            long[] upd = e.getValue().array();

            boolean ok = casBucket(e.getKey(), null, upd);
            assert ok;
        }

        this.metaPageId = nextPageId;
    }

    /**
     * @param bucket Bucket index.
     * @return Bucket.
     */
    protected abstract long[] getBucket(int bucket);

    /**
     * @param bucket Bucket index.
     * @param exp Expected bucket.
     * @param upd Updated bucket.
     * @return {@code true} If succeeded.
     */
    protected abstract boolean casBucket(int bucket, long[] exp, long[] upd);

    /**
     * @param bucket Bucket index.
     * @return {@code true} If it is a reuse bucket.
     */
    protected abstract boolean isReuseBucket(int bucket);

    /**
     * @param io IO.
     * @param prevId Previous page ID.
     * @param prev Previous page buffer.
     * @param nextId Next page ID.
     * @param next Next page buffer.
     */
    private void setupNextPage(PagesListNodeIO io, long prevId, ByteBuffer prev, long nextId, ByteBuffer next) {
        assert io.getNextId(prev) == 0L;

        io.initNewPage(next, nextId);
        io.setPreviousId(next, prevId);

        io.setNextId(prev, nextId);
    }

    /**
     * Adds stripe to the given bucket.
     *
     * @param bucket Bucket.
     * @throws IgniteCheckedException If failed.
     */
    protected final long addStripe(int bucket) throws IgniteCheckedException {
        long pageId = allocatePage(null);

        initPage(pageId, page(pageId), PagesListNodeIO.VERSIONS.latest(), wal);

        for (;;) {
            long[] old = getBucket(bucket);
            long[] upd;

            if (old != null) {
                int len = old.length;

                upd = Arrays.copyOf(old, len + 2);

                // Tail will be from the left, head from the right, but now they are the same.
                upd[len + 1] = upd[len] = pageId;
            }
            else
                upd = new long[]{pageId, pageId};

            if (casBucket(bucket, old, upd))
                return pageId;
        }
    }

    /**
     * @param bucket Bucket index.
     * @param oldTailId Old tail page ID to replace.
     * @param newTailId New tail page ID.
     */
    private void updateTail(int bucket, long oldTailId, long newTailId) {
        int idx = -1;

        for (;;) {
            long[] tails = getBucket(bucket);

            // Tail must exist to be updated.
            assert !F.isEmpty(tails) : "Missing tails [bucket=" + bucket + ", tails=" + Arrays.toString(tails) + ']';

            idx = findTailIndex(tails, oldTailId, idx);

            assert tails[idx] == oldTailId;

            long[] newTails;

            if (newTailId == 0L) {
                // Have to drop stripe.
                assert tails[idx + 1] == oldTailId; // The last page must be the same for both: tail and head.

                if (tails.length != 2) {
                    // Remove tail and head.
                    newTails = GridArrays.remove2(tails, idx);
                }
                else
                    newTails = null; // Drop the bucket completely.
            }
            else {
                newTails = tails.clone();

                newTails[idx] = newTailId;
            }

            if (casBucket(bucket, tails, newTails))
                return;
        }
    }

    /**
     * @param tails Tails.
     * @param tailId Tail ID to find.
     * @param expIdx Expected index.
     * @return First found index of the given tail ID.
     */
    private static int findTailIndex(long[] tails, long tailId, int expIdx) {
        if (expIdx != -1 && tails.length > expIdx && tails[expIdx] == tailId)
            return expIdx;

        for (int i = 0; i < tails.length; i++) {
            if (tails[i] == tailId)
                return i;
        }

        throw new IllegalStateException("Tail not found: " + tailId);
    }

    /**
     * @param bucket Bucket.
     * @return Page ID where the given page
     * @throws IgniteCheckedException If failed.
     */
    private long getPageForPut(int bucket) throws IgniteCheckedException {
        long[] tails = getBucket(bucket);

        if (tails == null)
            return addStripe(bucket);

        return randomTail(tails);
    }

    /**
     * @param tails Tails.
     * @return Random tail.
     */
    private static long randomTail(long[] tails) {
        int len = tails.length;

        assert len != 0;

        return tails[randomInt(len >>> 1) << 1]; // Choose only even tails, because odds are heads.
    }

    /**
     * !!! For tests only, does not provide any correctness guarantees for concurrent access.
     *
     * @param bucket Bucket index.
     * @return Number of pages stored in this list.
     * @throws IgniteCheckedException If failed.
     */
    protected final long storedPagesCount(int bucket) throws IgniteCheckedException {
        long res = 0;

        long[] tails = getBucket(bucket);

        if (tails != null) {
            // Step == 2 because we store both tails of the same list.
            for (int i = 0; i < tails.length; i += 2) {
                long pageId = tails[i];

                try (Page page = page(pageId)) {
                    ByteBuffer buf = page.getForRead();

                    try {
                        PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(buf);

                        int cnt = io.getCount(buf);

                        assert cnt >= 0;

                        res += cnt;
                    }
                    finally {
                        page.releaseRead();
                    }
                }
            }
        }

        return res;
    }

    /**
     * @param bag Reuse bag.
     * @param dataPageBuf Data page buffer.
     * @param bucket Bucket.
     * @throws IgniteCheckedException If failed.
     */
    protected final void put(ReuseBag bag, ByteBuffer dataPageBuf, int bucket) throws IgniteCheckedException {
        assert bag == null ^ dataPageBuf == null;

        for (;;) {
            long tailId = getPageForPut(bucket);

            try (Page tail = page(tailId)) {
                if (bag != null ?
                    // Here we can always take pages from the bag to build our list.
                    writePage(tailId, tail, putReuseBag, bag, bucket) :
                    // Here we can use the data page to build list only if it is empty and
                    // it is being put into reuse bucket. Usually this will be true, but there is
                    // a case when there is no reuse bucket in the free list, but then deadlock
                    // on node page allocation from separate reuse list is impossible.
                    // If the data page is not empty it can not be put into reuse bucket and thus
                    // the deadlock is impossible as well.
                    writePage(tailId, tail, putDataPage, dataPageBuf, bucket))
                    return;
            }
        }
    }

    /**
     * @param bucket Bucket index.
     * @return Page for take.
     */
    private long getPageForTake(int bucket) {
        long[] tails = getBucket(bucket);

        if (tails == null)
            return 0L;

        return randomTail(tails);
    }

    /**
     * @param bucket Bucket index.
     * @param initIoVers Optional IO to initialize page.
     * @return Removed page ID.
     * @throws IgniteCheckedException If failed.
     */
    protected final long takeEmptyPage(int bucket, @Nullable IOVersions initIoVers) throws IgniteCheckedException {
        for (;;) {
            if (cnts.get(bucket) == 0)
                return 0L;

            long tailId = getPageForTake(bucket);

            if (tailId == 0L)
                return 0L;

            try (Page tail = page(tailId)) {
                ByteBuffer tailBuf = tail.getForWrite();

                try {
                    if (getPageId(tailBuf) != tailId)
                        continue;

                    PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(tailBuf);

                    if (io.getNextId(tailBuf) != 0)
                        continue;

                    long pageId = io.takeAnyPage(tailBuf);

                    if (pageId != 0L)
                        return pageId;

                    if (isWalDeltaRecordNeeded(wal, tail))
                        wal.log(new PagesListRemoveLastPageRecord(cacheId, tailId));

                    // The tail page is empty, we can unlink and return it if we have a previous page.
                    long prevId = io.getPreviousId(tailBuf);

                    if (prevId != 0L) {
                        try (Page prev = page(prevId)) {
                            // Lock pages from next to previous.
                            Boolean ok = writePage(prevId, prev, cutTail, null, bucket);

                            assert ok;
                        }

                        // Rotate page so that successors will see this update.
                        tailId = PageIdUtils.rotatePageId(tailId);

                        if (initIoVers != null) {
                            PageIO initIo = initIoVers.latest();

                            initIo.initNewPage(tailBuf, tailId);

                            if (isWalDeltaRecordNeeded(wal, tail))
                                wal.log(new InitNewPageRecord(cacheId, tail.id(), initIo.getType(), initIo.getVersion(), tailId));
                        }
                        else {
                            PageIO.setPageId(tailBuf, tailId);

                            if (isWalDeltaRecordNeeded(wal, tail))
                                wal.log(new RecycleRecord(cacheId, tail.id(), tailId));
                        }

                        return tailId;
                    }

                    // If we do not have a previous page (we are at head), then we still can return
                    // current page but we have to drop the whole stripe. Since it is a reuse bucket,
                    // we will not do that, but just return 0L, because this may produce contention on
                    // meta page.

                    return 0L;
                }
                finally {
                    tail.releaseWrite(true);
                }
            }
        }
    }

    /**
     * @param dataPageBuf Data page buffer.
     * @param bucket Bucket index.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if page was removed.
     */
    protected final boolean removeDataPage(ByteBuffer dataPageBuf, int bucket) throws IgniteCheckedException {
        long dataPageId = getPageId(dataPageBuf);

        DataPageIO dataIO = DataPageIO.VERSIONS.forPage(dataPageBuf);

        long pageId = dataIO.getFreeListPageId(dataPageBuf);

        assert pageId != 0;

        try (Page page = page(pageId)) {
            long prevId;
            long nextId;

            long recycleId = 0L;

            ByteBuffer buf = page.getForWrite();

            boolean rmvd = false;

            try {
                if (getPageId(buf) != pageId)
                    return false;

                PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(buf);

                rmvd = io.removePage(buf, dataPageId);

                if (!rmvd)
                    return false;

                if (isWalDeltaRecordNeeded(wal, page))
                    wal.log(new PagesListRemovePageRecord(cacheId, pageId, dataPageId));

                cnts.decrementAndGet(bucket);

                // Reset free list page ID.
                dataIO.setFreeListPageId(dataPageBuf, 0L);

                if (!io.isEmpty(buf))
                    return true; // In optimistic case we still have something in the page and can leave it as is.

                // If the page is empty, we have to try to drop it and link next and previous with each other.
                nextId = io.getNextId(buf);
                prevId = io.getPreviousId(buf);

                // If there are no next page, then we can try to merge without releasing current write lock,
                // because if we will need to lock previous page, the locking order will be already correct.
                if (nextId == 0L)
                    recycleId = mergeNoNext(pageId, page, buf, prevId, bucket);
            }
            finally {
                page.releaseWrite(rmvd);
            }

            // Perform a fair merge after lock release (to have a correct locking order).
            if (nextId != 0L)
                recycleId = merge(page, pageId, nextId, bucket);

            if (recycleId != 0L)
                reuseList.addForRecycle(new SingletonReuseBag(recycleId));

            return true;
        }
    }

    /**
     * @param page Page.
     * @param pageId Page ID.
     * @param buf Page byte buffer.
     * @param prevId Previous page ID.
     * @param bucket Bucket index.
     * @return Page ID to recycle.
     * @throws IgniteCheckedException If failed.
     */
    private long mergeNoNext(long pageId, Page page, ByteBuffer buf, long prevId, int bucket)
        throws IgniteCheckedException {
        // If we do not have a next page (we are tail) and we are on reuse bucket,
        // then we can leave as is as well, because it is normal to have an empty tail page here.
        if (isReuseBucket(bucket))
            return 0L;

        if (prevId != 0L) { // Cut tail if we have a previous page.
            try (Page prev = page(prevId)) {
                Boolean ok = writePage(prevId, prev, cutTail, null, bucket);

                assert ok; // Because we keep lock on current tail and do a world consistency check.
            }
        }
        else // If we don't have a previous, then we are tail page of free list, just drop the stripe.
            updateTail(bucket, pageId, 0L);

        return recyclePage(pageId, page, buf);
    }

    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param nextId Next page ID.
     * @param bucket Bucket index.
     * @return Page ID to recycle.
     * @throws IgniteCheckedException If failed.
     */
    private long merge(Page page, long pageId, long nextId, int bucket)
        throws IgniteCheckedException {
        assert nextId != 0; // We should do mergeNoNext then.

        // Lock all the pages in correct order (from next to previous) and do the merge in retry loop.
        for (;;) {
            try (Page next = nextId == 0L ? null : page(nextId)) {
                boolean write = false;

                ByteBuffer nextBuf = next == null ? null : next.getForWrite();
                ByteBuffer buf = page.getForWrite();

                try {
                    if (getPageId(buf) != pageId)
                        return 0L; // Someone has merged or taken our empty page concurrently. Nothing to do here.

                    PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(buf);

                    if (!io.isEmpty(buf))
                        return 0L; // No need to merge anymore.

                    // Check if we see a consistent state of the world.
                    if (io.getNextId(buf) == nextId) {
                        long recycleId = doMerge(pageId, page, buf, io, next, nextId, nextBuf, bucket);

                        write = true;

                        return recycleId; // Done.
                    }

                    // Reread next page ID and go for retry.
                    nextId = io.getNextId(buf);
                }
                finally {
                    if (next != null)
                        next.releaseWrite(write);

                    page.releaseWrite(write);
                }
            }
        }
    }

    /**
     * @param page Page.
     * @param pageId Page ID.
     * @param io IO.
     * @param buf Byte buffer.
     * @param next Next page.
     * @param nextId Next page ID.
     * @param nextBuf Next buffer.
     * @param bucket Bucket index.
     * @return Page to recycle.
     * @throws IgniteCheckedException If failed.
     */
    private long doMerge(
        long pageId,
        Page page,
        ByteBuffer buf,
        PagesListNodeIO io,
        Page next,
        long nextId,
        ByteBuffer nextBuf,
        int bucket)
        throws IgniteCheckedException {
        long prevId = io.getPreviousId(buf);

        if (nextId == 0L)
            return mergeNoNext(pageId, page, buf, prevId, bucket);
        else {
            // No one must be able to merge it while we keep a reference.
            assert getPageId(nextBuf) == nextId;

            if (prevId == 0L) { // No previous page: we are at head.
                // These references must be updated at the same time in write locks.
                assert PagesListNodeIO.VERSIONS.forPage(nextBuf).getPreviousId(nextBuf) == pageId;

                PagesListNodeIO nextIO = PagesListNodeIO.VERSIONS.forPage(nextBuf);
                nextIO.setPreviousId(nextBuf, 0);

                if (isWalDeltaRecordNeeded(wal, next))
                    wal.log(new PagesListSetPreviousRecord(cacheId, nextId, 0L));

                // Drop the page from meta: replace current head with next page.
                // It is a bit hacky, but method updateTail should work here.
                updateTail(bucket, pageId, nextId);
            }
            else // Do a fair merge: link previous and next to each other.
                fairMerge(prevId, pageId, nextId, next, nextBuf);

            return recyclePage(pageId, page, buf);
        }
    }

    /**
     * Link previous and next to each other.
     *
     * @param prevId Previous Previous page ID.
     * @param pageId Page ID.
     * @param next Next page.
     * @param nextId Next page ID.
     * @param nextBuf Next buffer.
     * @throws IgniteCheckedException If failed.
     */
    private void fairMerge(long prevId,
        long pageId,
        long nextId,
        Page next,
        ByteBuffer nextBuf)
        throws IgniteCheckedException {
        try (Page prev = page(prevId)) {
            ByteBuffer prevBuf = prev.getForWrite();

            try {
                assert getPageId(prevBuf) == prevId; // Because we keep a reference.

                PagesListNodeIO prevIO = PagesListNodeIO.VERSIONS.forPage(prevBuf);
                PagesListNodeIO nextIO = PagesListNodeIO.VERSIONS.forPage(nextBuf);

                // These references must be updated at the same time in write locks.
                assert prevIO.getNextId(prevBuf) == pageId;
                assert nextIO.getPreviousId(nextBuf) == pageId;

                prevIO.setNextId(prevBuf, nextId);

                if (isWalDeltaRecordNeeded(wal, prev))
                    wal.log(new PagesListSetNextRecord(cacheId, prevId, nextId));

                nextIO.setPreviousId(nextBuf, prevId);

                if (isWalDeltaRecordNeeded(wal, next))
                    wal.log(new PagesListSetPreviousRecord(cacheId, nextId, prevId));
            }
            finally {
                prev.releaseWrite(true);
            }
        }
    }

    /**
     * @param page Page.
     * @param pageId Page ID.
     * @param buf Byte buffer.
     * @return Rotated page ID.
     * @throws IgniteCheckedException If failed.
     */
    private long recyclePage(long pageId, Page page, ByteBuffer buf) throws IgniteCheckedException {
        pageId = PageIdUtils.rotatePageId(pageId);

        PageIO.setPageId(buf, pageId);

        if (isWalDeltaRecordNeeded(wal, page))
            wal.log(new RecycleRecord(cacheId, page.id(), pageId));

        return pageId;
    }

    /**
     * Page handler.
     */
    private static abstract class CheckingPageHandler<X> extends PageHandler<X, PageIO, Boolean> {
        /** {@inheritDoc} */
        @Override public final Boolean run(long pageId, Page page, PageIO io, ByteBuffer buf, X arg, int intArg)
            throws IgniteCheckedException {
            if (io.getType() != PageIO.T_PAGE_LIST_NODE)
                return Boolean.FALSE;

            if (getPageId(buf) != pageId)
                return Boolean.FALSE;

            return run0(pageId, page, buf, (PagesListNodeIO)io, arg, intArg);
        }

        /**
         * @param pageId Page ID.
         * @param page Page.
         * @param buf Buffer.
         * @param io IO.
         * @param arg Argument.
         * @param intArg Integer argument.
         * @throws IgniteCheckedException If failed.
         * @return Result.
         */
        protected abstract boolean run0(long pageId, Page page, ByteBuffer buf, PagesListNodeIO io, X arg, int intArg)
            throws IgniteCheckedException;
    }

    /**
     * Singleton reuse bag.
     */
    private static final class SingletonReuseBag implements ReuseBag {
        /** */
        long pageId;

        /**
         * @param pageId Page ID.
         */
        SingletonReuseBag(long pageId) {
            this.pageId = pageId;
        }

        /** {@inheritDoc} */
        @Override public void addFreePage(long pageId) {
            throw new IllegalStateException("Should never be called.");
        }

        /** {@inheritDoc} */
        @Override public long pollFreePage() {
            long res = pageId;

            pageId = 0L;

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SingletonReuseBag.class, this);
        }
    }
}