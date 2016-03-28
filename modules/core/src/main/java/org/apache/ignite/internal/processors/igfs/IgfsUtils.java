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

package org.apache.ignite.internal.processors.igfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.IgfsEvent;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_RETRIES_COUNT;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Common IGFS utility methods.
 */
public class IgfsUtils {
    /** ID for the root directory. */
    public static final IgniteUuid ROOT_ID = new IgniteUuid(new UUID(0, 0), 0);

    /** Lock Id used to lock files being deleted from TRASH. This is a global constant. */
    public static final IgniteUuid DELETE_LOCK_ID = new IgniteUuid(new UUID(0, 0), 0);

    /** Constant trash concurrency level. */
    public static final int TRASH_CONCURRENCY = 64;

    /** File property: user name. */
    public static final String PROP_USER_NAME = "usrName";

    /** File property: group name. */
    public static final String PROP_GROUP_NAME = "grpName";

    /** File property: permission. */
    public static final String PROP_PERMISSION = "permission";

    /** File property: prefer writes to local node. */
    public static final String PROP_PREFER_LOCAL_WRITES = "locWrite";

    /** Generic property index. */
    private static final byte PROP_IDX = 0;

    /** User name property index. */
    private static final byte PROP_USER_NAME_IDX = 1;

    /** Group name property index. */
    private static final byte PROP_GROUP_NAME_IDX = 2;

    /** Permission property index. */
    private static final byte PROP_PERMISSION_IDX = 3;

    /** Prefer local writes property index. */
    private static final byte PROP_PREFER_LOCAL_WRITES_IDX = 4;

    /** Trash directory IDs. */
    private static final IgniteUuid[] TRASH_IDS;

    /** Maximum number of file unlock transaction retries when topology changes. */
    private static final int MAX_CACHE_TX_RETRIES = IgniteSystemProperties.getInteger(IGNITE_CACHE_RETRIES_COUNT, 100);

    /**
     * Static initializer.
     */
    static {
        TRASH_IDS = new IgniteUuid[TRASH_CONCURRENCY];

        for (int i = 0; i < TRASH_CONCURRENCY; i++)
            TRASH_IDS[i] = new IgniteUuid(new UUID(0, i + 1), 0);
    }

    /**
     * Get random trash ID.
     *
     * @return Trash ID.
     */
    public static IgniteUuid randomTrashId() {
        return TRASH_IDS[ThreadLocalRandom.current().nextInt(TRASH_CONCURRENCY)];
    }

    /**
     * Get trash ID for the given index.
     *
     * @param idx Index.
     * @return Trahs ID.
     */
    public static IgniteUuid trashId(int idx) {
        assert idx >= 0 && idx < TRASH_CONCURRENCY;

        return TRASH_IDS[idx];
    }

    /**
     * Check whether provided ID is either root ID or trash ID.
     *
     * @param id ID.
     * @return {@code True} if this is root ID or trash ID.
     */
    public static boolean isRootOrTrashId(@Nullable IgniteUuid id) {
        return isRootId(id) || isTrashId(id);
    }

    /**
     * Check whether provided ID is root ID.
     *
     * @param id ID.
     * @return {@code True} if this is root ID.
     */
    public static boolean isRootId(@Nullable IgniteUuid id) {
        return id != null && ROOT_ID.equals(id);
    }

    /**
     * Check whether provided ID is trash ID.
     *
     * @param id ID.
     * @return {@code True} if this is trash ID.
     */
    private static boolean isTrashId(IgniteUuid id) {
        if (id == null)
            return false;

        UUID gid = id.globalId();

        return id.localId() == 0 && gid.getMostSignificantBits() == 0 &&
            gid.getLeastSignificantBits() > 0 && gid.getLeastSignificantBits() <= TRASH_CONCURRENCY;
    }

    /**
     * Converts any passed exception to IGFS exception.
     *
     * @param err Initial exception.
     * @return Converted IGFS exception.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public static IgfsException toIgfsException(Exception err) {
        IgfsException err0 = err instanceof IgfsException ? (IgfsException)err : null;

        IgfsException igfsErr = X.cause(err, IgfsException.class);

        while (igfsErr != null && igfsErr != err0) {
            err0 = igfsErr;

            igfsErr = X.cause(err, IgfsException.class);
        }

        // If initial exception is already IGFS exception and no inner stuff exists, just return it unchanged.
        if (err0 != err) {
            if (err0 != null)
                // Dealing with a kind of IGFS error, wrap it once again, preserving message and root cause.
                err0 = newIgfsException(err0.getClass(), err0.getMessage(), err0);
            else {
                if (err instanceof ClusterTopologyServerNotFoundException)
                    err0 = new IgfsException("Cache server nodes not found.", err);
                else
                    // Unknown error nature.
                    err0 = new IgfsException("Generic IGFS error occurred.", err);
            }
        }

        return err0;
    }

    /**
     * Construct new IGFS exception passing specified message and cause.
     *
     * @param cls Class.
     * @param msg Message.
     * @param cause Cause.
     * @return New IGFS exception.
     */
    public static IgfsException newIgfsException(Class<? extends IgfsException> cls, String msg, Throwable cause) {
        try {
            Constructor<? extends IgfsException> ctor = cls.getConstructor(String.class, Throwable.class);

            return ctor.newInstance(msg, cause);
        }
        catch (ReflectiveOperationException e) {
            throw new IgniteException("Failed to create IGFS exception: " + cls.getName(), e);
        }
    }

    /**
     * Constructor.
     */
    private IgfsUtils() {
        // No-op.
    }

    /**
     * Provides non-null user name.
     * If the user name is null or empty string, defaults to {@link FileSystemConfiguration#DFLT_USER_NAME},
     * which is the current process owner user.
     * @param user a user name to be fixed.
     * @return non-null interned user name.
     */
    public static String fixUserName(@Nullable String user) {
        if (F.isEmpty(user))
           user = FileSystemConfiguration.DFLT_USER_NAME;

        return user;
    }

    /**
     * Performs an operation with transaction with retries.
     *
     * @param cache Cache to do the transaction on.
     * @param clo Closure.
     * @return Result of closure execution.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public static <T> T doInTransactionWithRetries(IgniteInternalCache cache, IgniteOutClosureX<T> clo)
        throws IgniteCheckedException {
        assert cache != null;

        int attempts = 0;

        while (attempts < MAX_CACHE_TX_RETRIES) {
            try (Transaction tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                T res = clo.applyx();

                tx.commit();

                return res;
            }
            catch (IgniteException | IgniteCheckedException e) {
                ClusterTopologyException cte = X.cause(e, ClusterTopologyException.class);

                if (cte != null)
                    ((IgniteFutureImpl)cte.retryReadyFuture()).internalFuture().getUninterruptibly();
                else
                    throw U.cast(e);
            }

            attempts++;
        }

        throw new IgniteCheckedException("Failed to perform operation since max number of attempts " +
            "exceeded. [maxAttempts=" + MAX_CACHE_TX_RETRIES + ']');
    }

    /**
     * Sends a series of event.
     *
     * @param path The path of the created file.
     * @param type The type of event to send.
     */
    public static void sendEvents(GridKernalContext kernalCtx, IgfsPath path, int type) {
        sendEvents(kernalCtx, path, null, type);
    }

    /**
     * Sends a series of event.
     *
     * @param path The path of the created file.
     * @param newPath New path.
     * @param type The type of event to send.
     */
    public static void sendEvents(GridKernalContext kernalCtx, IgfsPath path, IgfsPath newPath, int type) {
        assert kernalCtx != null;
        assert path != null;

        GridEventStorageManager evts = kernalCtx.event();
        ClusterNode locNode = kernalCtx.discovery().localNode();

        if (evts.isRecordable(type)) {
            if (newPath == null)
                evts.record(new IgfsEvent(path, locNode, type));
            else
                evts.record(new IgfsEvent(path, newPath, locNode, type));
        }
    }

    /**
     * @param cfg Grid configuration.
     * @param cacheName Cache name.
     * @return {@code True} in this is IGFS data or meta cache.
     */
    public static boolean isIgfsCache(IgniteConfiguration cfg, @Nullable String cacheName) {
        FileSystemConfiguration[] igfsCfgs = cfg.getFileSystemConfiguration();

        if (igfsCfgs != null) {
            for (FileSystemConfiguration igfsCfg : igfsCfgs) {
                // IGFS config probably has not been validated yet => possible NPE, so we check for null.
                if (igfsCfg != null) {
                    if (F.eq(cacheName, igfsCfg.getDataCacheName()) || F.eq(cacheName, igfsCfg.getMetaCacheName()))
                        return true;
                }
            }
        }

        return false;
    }

    /**
     * Prepare cache configuration if this is IGFS meta or data cache.
     *
     * @param cfg Configuration.
     * @param ccfg Cache configuration.
     */
    public static void prepareCacheConfiguration(IgniteConfiguration cfg, CacheConfiguration ccfg) {
        FileSystemConfiguration[] igfsCfgs = cfg.getFileSystemConfiguration();

        if (igfsCfgs != null) {
            for (FileSystemConfiguration igfsCfg : igfsCfgs) {
                if (igfsCfg != null) {
                    if (F.eq(ccfg.getName(), igfsCfg.getMetaCacheName())) {
                        ccfg.setCopyOnRead(false);

                        // Set co-located affinity mapper if needed.
                        if (igfsCfg.isColocateMetadata() && ccfg.getCacheMode() == CacheMode.REPLICATED &&
                            ccfg.getAffinityMapper() == null)
                            ccfg.setAffinityMapper(new IgfsColocatedMetadataAffinityKeyMapper());

                        return;
                    }

                    if (F.eq(ccfg.getName(), igfsCfg.getDataCacheName())) {
                        ccfg.setCopyOnRead(false);

                        return;
                    }
                }
            }
        }
    }

    /**
     * Create empty directory with the given ID.
     *
     * @param id ID.
     * @return File info.
     */
    public static IgfsDirectoryInfo createDirectory(IgniteUuid id) {
        return createDirectory(id, null, null);
    }

    /**
     * Create directory.
     *
     * @param id ID.
     * @param listing Listing.
     * @param props Properties.
     * @return File info.
     */
    public static IgfsDirectoryInfo createDirectory(
        IgniteUuid id,
        @Nullable Map<String, IgfsListingEntry> listing,
        @Nullable Map<String, String> props) {
        long time = System.currentTimeMillis();

        return createDirectory(id, listing, props, time, time);
    }

    /**
     * Create directory.
     *
     * @param id ID.
     * @param listing Listing.
     * @param props Properties.
     * @param createTime Create time.
     * @param modificationTime Modification time.
     * @return File info.
     */
    public static IgfsDirectoryInfo createDirectory(
        IgniteUuid id,
        @Nullable Map<String, IgfsListingEntry> listing,
        @Nullable Map<String,String> props,
        long createTime,
        long modificationTime) {
        return new IgfsDirectoryInfo(id, listing, props, createTime, modificationTime);
    }

    /**
     * Create file.
     *
     * @param id File ID.
     * @param blockSize Block size.
     * @param len Length.
     * @param affKey Affinity key.
     * @param lockId Lock ID.
     * @param evictExclude Evict exclude flag.
     * @param props Properties.
     * @param accessTime Access time.
     * @param modificationTime Modification time.
     * @return File info.
     */
    public static IgfsFileInfo createFile(IgniteUuid id, int blockSize, long len, @Nullable IgniteUuid affKey,
        @Nullable IgniteUuid lockId, boolean evictExclude, @Nullable Map<String, String> props, long accessTime,
        long modificationTime) {
        return new IgfsFileInfo(id, blockSize, len, affKey, props, null, lockId, accessTime, modificationTime,
            evictExclude);
    }

    /**
     * Write listing entry.
     *
     * @param out Writer.
     * @param entry Entry.
     */
    public static void writeListingEntry(BinaryRawWriter out, @Nullable IgfsListingEntry entry) {
        if (entry != null) {
            out.writeBoolean(true);

            BinaryUtils.writeIgniteUuid(out, entry.fileId());

            out.writeBoolean(entry.isDirectory());
        }
        else
            out.writeBoolean(false);
    }

    /**
     * Read listing entry.
     *
     * @param in Reader.
     * @return Entry.
     */
    @Nullable public static IgfsListingEntry readListingEntry(BinaryRawReader in) {
        if (in.readBoolean()) {
            IgniteUuid id = BinaryUtils.readIgniteUuid(in);
            boolean dir = in.readBoolean();

            return new IgfsListingEntry(id, dir);
        }
        else
            return null;
    }

    /**
     * Write listing entry.
     *
     * @param out Writer.
     * @param entry Entry.
     * @throws IOException If failed.
     */
    public static void writeListingEntry(DataOutput out, @Nullable IgfsListingEntry entry) throws IOException {
        if (entry != null) {
            out.writeBoolean(true);

            IgniteUtils.writeGridUuid(out, entry.fileId());

            out.writeBoolean(entry.isDirectory());
        }
        else
            out.writeBoolean(false);
    }

    /**
     * Read listing entry.
     *
     * @param in Reader.
     * @return Entry.
     */
    @Nullable public static IgfsListingEntry readListingEntry(DataInput in) throws IOException {
        if (in.readBoolean()) {
            IgniteUuid id = IgniteUtils.readGridUuid(in);
            boolean dir = in.readBoolean();

            return new IgfsListingEntry(id, dir);
        }
        else
            return null;
    }

    /**
     * Write entry properties. Rely on reference equality for well-known properties.
     *
     * @param out Writer.
     * @param props Properties.
     */
    @SuppressWarnings("StringEquality")
    public static void writeProperties(BinaryRawWriter out, @Nullable Map<String, String> props) {
        if (props != null) {
            out.writeInt(props.size());

            for (Map.Entry<String, String> entry : props.entrySet()) {
                String key = entry.getKey();

                if (key == PROP_PERMISSION)
                    out.writeByte(PROP_PERMISSION_IDX);
                else if (key == PROP_PREFER_LOCAL_WRITES)
                    out.writeByte(PROP_PREFER_LOCAL_WRITES_IDX);
                else if (key == PROP_USER_NAME)
                    out.writeByte(PROP_USER_NAME_IDX);
                else if (key == PROP_GROUP_NAME)
                    out.writeByte(PROP_GROUP_NAME_IDX);
                else {
                    out.writeByte(PROP_IDX);
                    out.writeString(key);
                }

                out.writeString(entry.getValue());
            }
        }
        else
            out.writeInt(-1);
    }

    /**
     * Read entry properties.
     *
     * @param in Reader.
     * @return Properties.
     */
    @Nullable public static Map<String, String> readProperties(BinaryRawReader in) {
        int size = in.readInt();

        if (size >= 0) {
            Map<String, String> props = new HashMap<>(size);

            for (int i = 0; i < size; i++) {
                byte idx = in.readByte();

                String key;

                switch (idx) {
                    case PROP_PERMISSION_IDX:
                        key = PROP_PERMISSION;

                        break;

                    case PROP_PREFER_LOCAL_WRITES_IDX:
                        key = PROP_PREFER_LOCAL_WRITES;

                        break;

                    case PROP_USER_NAME_IDX:
                        key = PROP_USER_NAME;

                        break;

                    case PROP_GROUP_NAME_IDX:
                        key = PROP_GROUP_NAME;

                        break;

                    default:
                        key = in.readString();
                }

                props.put(key, in.readString());
            }

            return props;
        }
        else
            return null;
    }

    /**
     * Write entry properties. Rely on reference equality for well-known properties.
     *
     * @param out Writer.
     * @param props Properties.
     * @throws IOException If failed.
     */
    @SuppressWarnings("StringEquality")
    public static void writeProperties(DataOutput out, @Nullable Map<String, String> props) throws IOException {
        if (props != null) {
            out.writeInt(props.size());

            for (Map.Entry<String, String> entry : props.entrySet()) {
                String key = entry.getKey();

                if (key == PROP_PERMISSION)
                    out.writeByte(PROP_PERMISSION_IDX);
                else if (key == PROP_PREFER_LOCAL_WRITES)
                    out.writeByte(PROP_PREFER_LOCAL_WRITES_IDX);
                else if (key == PROP_USER_NAME)
                    out.writeByte(PROP_USER_NAME_IDX);
                else if (key == PROP_GROUP_NAME)
                    out.writeByte(PROP_GROUP_NAME_IDX);
                else {
                    out.writeByte(PROP_IDX);
                    U.writeString(out, key);
                }

                U.writeString(out, entry.getValue());
            }
        }
        else
            out.writeInt(-1);
    }

    /**
     * Read entry properties.
     *
     * @param in Reader.
     * @return Properties.
     * @throws IOException If failed.
     */
    @Nullable public static Map<String, String> readProperties(DataInput in) throws IOException {
        int size = in.readInt();

        if (size >= 0) {
            Map<String, String> props = new HashMap<>(size);

            for (int i = 0; i < size; i++) {
                byte idx = in.readByte();

                String key;

                switch (idx) {
                    case PROP_PERMISSION_IDX:
                        key = PROP_PERMISSION;

                        break;

                    case PROP_PREFER_LOCAL_WRITES_IDX:
                        key = PROP_PREFER_LOCAL_WRITES;

                        break;

                    case PROP_USER_NAME_IDX:
                        key = PROP_USER_NAME;

                        break;

                    case PROP_GROUP_NAME_IDX:
                        key = PROP_GROUP_NAME;

                        break;

                    default:
                        key = U.readString(in);
                }

                props.put(key, U.readString(in));
            }

            return props;
        }
        else
            return null;
    }
}