/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.server.session;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.seata.common.Constants;
import io.seata.common.XID;
import io.seata.common.util.StringUtils;
import io.seata.core.exception.GlobalTransactionException;
import io.seata.core.exception.TransactionException;
import io.seata.core.exception.TransactionExceptionCode;
import io.seata.core.model.BranchStatus;
import io.seata.core.model.BranchType;
import io.seata.core.model.GlobalStatus;
import io.seata.server.UUIDGenerator;
import io.seata.server.lock.LockerManagerFactory;
import io.seata.server.store.SessionStorable;
import io.seata.server.store.StoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Global session.
 *
 * @author sharajava
 */
public class GlobalSession implements SessionLifecycle, SessionStorable {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalSession.class);

    private static final int MAX_GLOBAL_SESSION_SIZE = StoreConfig.getMaxGlobalSessionSize();

    private static ThreadLocal<ByteBuffer> byteBufferThreadLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(
        MAX_GLOBAL_SESSION_SIZE));

    /**
     * xid
     */
    private String xid;

    private long transactionId;

    private volatile GlobalStatus status;

    private String applicationId;

    private String transactionServiceGroup;

    private String transactionName;

    private int timeout;

    private long beginTime;

    private String applicationData;

    private volatile boolean active = true;

    private final ArrayList<BranchSession> branchSessions = new ArrayList<>();

    private GlobalSessionLock globalSessionLock = new GlobalSessionLock();


    /**
     * Add boolean.
     *
     * @param branchSession the branch session
     * @return the boolean
     */
    public boolean add(BranchSession branchSession) {
        return branchSessions.add(branchSession);
    }

    /**
     * Remove boolean.
     *
     * @param branchSession the branch session
     * @return the boolean
     */
    public boolean remove(BranchSession branchSession) {
        return branchSessions.remove(branchSession);
    }

    private Set<SessionLifecycleListener> lifecycleListeners = new HashSet<>();

    /**
     * Can be committed async boolean.
     *
     * @return the boolean
     */
    public boolean canBeCommittedAsync() {
        for (BranchSession branchSession : branchSessions) {
            if (branchSession.getBranchType() == BranchType.TCC || branchSession.getBranchType() == BranchType.XA) {
                return false;
            }
        }
        return true;
    }

    /**
     * Is saga type transaction
     *
     * @return is saga
     */
    public boolean isSaga() {
        if (branchSessions.size() > 0) {
            return BranchType.SAGA == branchSessions.get(0).getBranchType();
        }
        else if (StringUtils.isNotBlank(transactionName)
                && transactionName.startsWith(Constants.SAGA_TRANS_NAME_PREFIX)) {
            return true;
        }
        return false;
    }

    /**
     * Is timeout boolean.
     *
     * @return the boolean
     */
    public boolean isTimeout() {
        return (System.currentTimeMillis() - beginTime) > timeout;
    }

    /**
     * prevent could not handle rollbacking transaction
     * @return if true force roll back
     */
    public boolean isRollbackingDead() {
        return (System.currentTimeMillis() - beginTime) > (2 * 6000);
    }

    @Override
    public void begin() throws TransactionException {
        this.status = GlobalStatus.Begin;
        this.beginTime = System.currentTimeMillis();
        this.active = true;
        for (SessionLifecycleListener lifecycleListener : lifecycleListeners) {
            lifecycleListener.onBegin(this);
        }
    }

    @Override
    public void changeStatus(GlobalStatus status) throws TransactionException {
        this.status = status;
        for (SessionLifecycleListener lifecycleListener : lifecycleListeners) {
            lifecycleListener.onStatusChange(this, status);
        }

    }

    @Override
    public void changeBranchStatus(BranchSession branchSession, BranchStatus status)
        throws TransactionException {
        branchSession.setStatus(status);
        for (SessionLifecycleListener lifecycleListener : lifecycleListeners) {
            lifecycleListener.onBranchStatusChange(this, branchSession, status);
        }
    }

    @Override
    public boolean isActive() {
        return active;
    }

    @Override
    public void close() throws TransactionException {
        if (active) {
            for (SessionLifecycleListener lifecycleListener : lifecycleListeners) {
                lifecycleListener.onClose(this);
            }
        }
    }

    @Override
    public void end() throws TransactionException {
        // Clean locks first
        clean();

        for (SessionLifecycleListener lifecycleListener : lifecycleListeners) {
            lifecycleListener.onEnd(this);
        }

    }

    public void clean() throws TransactionException {
        LockerManagerFactory.getLockManager().releaseGlobalSessionLock(this);

    }

    /**
     * Close and clean.
     *
     * @throws TransactionException the transaction exception
     */
    public void closeAndClean() throws TransactionException {
        close();
        clean();

    }

    /**
     * Add session lifecycle listener.
     *
     * @param sessionLifecycleListener the session lifecycle listener
     */
    public void addSessionLifecycleListener(SessionLifecycleListener sessionLifecycleListener) {
        lifecycleListeners.add(sessionLifecycleListener);
    }

    /**
     * Remove session lifecycle listener.
     *
     * @param sessionLifecycleListener the session lifecycle listener
     */
    public void removeSessionLifecycleListener(SessionLifecycleListener sessionLifecycleListener) {
        lifecycleListeners.remove(sessionLifecycleListener);
    }

    @Override
    public void addBranch(BranchSession branchSession) throws TransactionException {
        for (SessionLifecycleListener lifecycleListener : lifecycleListeners) {
            lifecycleListener.onAddBranch(this, branchSession);
        }
        branchSession.setStatus(BranchStatus.Registered);
        add(branchSession);
    }

    @Override
    public void removeBranch(BranchSession branchSession) throws TransactionException {
        for (SessionLifecycleListener lifecycleListener : lifecycleListeners) {
            lifecycleListener.onRemoveBranch(this, branchSession);
        }
        branchSession.unlock();
        remove(branchSession);
    }

    /**
     * Gets branch.
     *
     * @param branchId the branch id
     * @return the branch
     */
    public BranchSession getBranch(long branchId) {
        synchronized (branchSessions) {
            for (BranchSession branchSession : branchSessions) {
                if (branchSession.getBranchId() == branchId) {
                    return branchSession;
                }
            }

            return null;
        }

    }

    /**
     * Gets sorted branches.
     *
     * @return the sorted branches
     */
    public ArrayList<BranchSession> getSortedBranches() {
        return new ArrayList<>(branchSessions);
    }

    /**
     * Gets reverse sorted branches.
     *
     * @return the reverse sorted branches
     */
    public ArrayList<BranchSession> getReverseSortedBranches() {
        ArrayList<BranchSession> reversed = new ArrayList<>(branchSessions);
        Collections.reverse(reversed);
        return reversed;
    }

    /**
     * Instantiates a new Global session.
     */
    public GlobalSession() {}

    /**
     * Instantiates a new Global session.
     *
     * @param applicationId           the application id
     * @param transactionServiceGroup the transaction service group
     * @param transactionName         the transaction name
     * @param timeout                 the timeout
     */
    public GlobalSession(String applicationId, String transactionServiceGroup, String transactionName, int timeout) {
        this.transactionId = UUIDGenerator.generateUUID();
        this.status = GlobalStatus.Begin;

        this.applicationId = applicationId;
        this.transactionServiceGroup = transactionServiceGroup;
        this.transactionName = transactionName;
        this.timeout = timeout;
        this.xid = XID.generateXID(transactionId);
    }

    /**
     * Gets transaction id.
     *
     * @return the transaction id
     */
    public long getTransactionId() {
        return transactionId;
    }

    /**
     * Sets transaction id.
     *
     * @param transactionId the transaction id
     */
    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    /**
     * Gets status.
     *
     * @return the status
     */
    public GlobalStatus getStatus() {
        return status;
    }

    /**
     * Sets status.
     *
     * @param status the status
     */
    public void setStatus(GlobalStatus status) {
        this.status = status;
    }

    /**
     * Gets xid.
     *
     * @return the xid
     */
    public String getXid() {
        return xid;
    }

    /**
     * Sets xid.
     *
     * @param xid the xid
     */
    public void setXid(String xid) {
        this.xid = xid;
    }

    /**
     * Gets application id.
     *
     * @return the application id
     */
    public String getApplicationId() {
        return applicationId;
    }

    /**
     * Gets transaction service group.
     *
     * @return the transaction service group
     */
    public String getTransactionServiceGroup() {
        return transactionServiceGroup;
    }

    /**
     * Gets transaction name.
     *
     * @return the transaction name
     */
    public String getTransactionName() {
        return transactionName;
    }

    /**
     * Gets timeout.
     *
     * @return the timeout
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * Gets begin time.
     *
     * @return the begin time
     */
    public long getBeginTime() {
        return beginTime;
    }

    /**
     * Sets begin time.
     *
     * @param beginTime the begin time
     */
    public void setBeginTime(long beginTime) {
        this.beginTime = beginTime;
    }

    /**
     * Gets application data.
     *
     * @return the application data
     */
    public String getApplicationData() {
        return applicationData;
    }

    /**
     * Sets application data.
     *
     * @param applicationData the application data
     */
    public void setApplicationData(String applicationData) {
        this.applicationData = applicationData;
    }

    /**
     * Create global session global session.
     *
     * @param applicationId  the application id
     * @param txServiceGroup the tx service group
     * @param txName         the tx name
     * @param timeout        the timeout
     * @return the global session
     */
    public static GlobalSession createGlobalSession(String applicationId, String txServiceGroup, String txName,
                                                    int timeout) {
        return new GlobalSession(applicationId, txServiceGroup, txName, timeout);
    }

    /**
     * Sets active.
     *
     * @param active the active
     */
    public void setActive(boolean active) {
        this.active = active;
    }

    /**
     * TODO: 这个方法很有意思
     * @return
     */
    @Override
    public byte[] encode() {
        // TODO: 进行编码 globalSession信息

        // TODO: 当前应用ID
        byte[] byApplicationIdBytes = applicationId != null ? applicationId.getBytes() : null;

        byte[] byServiceGroupBytes = transactionServiceGroup != null ? transactionServiceGroup.getBytes() : null;

        byte[] byTxNameBytes = transactionName != null ? transactionName.getBytes() : null;

        byte[] xidBytes = xid != null ? xid.getBytes() : null;

        byte[] applicationDataBytes = applicationData != null ? applicationData.getBytes() : null;

        /**
         * 计算应当需要的size
         */
        int size = calGlobalSessionSize(byApplicationIdBytes, byServiceGroupBytes, byTxNameBytes, xidBytes,
            applicationDataBytes);

        // TODO: 如果大于全局session的最大size则抛出异常
        if (size > MAX_GLOBAL_SESSION_SIZE) {
            // TODO: 这地方异常信息，有个错误
            throw new RuntimeException("global session size exceeded, size : " + size + " maxBranchSessionSize : " +
                MAX_GLOBAL_SESSION_SIZE);
        }
        // TODO: 获得一段内存，size为最大的全局session的size
        ByteBuffer byteBuffer = byteBufferThreadLocal.get();
        //recycle 先clear下
        byteBuffer.clear();


        /**
         * 填充内存
         *  final int size = 8 // transactionId
         *             + 4 // timeout
         *             + 2 // byApplicationIdBytes.length
         *             + 2 // byServiceGroupBytes.length
         *             + 2 // byTxNameBytes.length
         *             + 4 // xidBytes.length
         *             + 4 // applicationDataBytes.length
         *             + 8 // beginTime
         *             + 1 // statusCode
         *             + (byApplicationIdBytes == null ? 0 : byApplicationIdBytes.length)
         *             + (byServiceGroupBytes == null ? 0 : byServiceGroupBytes.length)
         *             + (byTxNameBytes == null ? 0 : byTxNameBytes.length)
         *             + (xidBytes == null ? 0 : xidBytes.length)
         *             + (applicationDataBytes == null ? 0 : applicationDataBytes.length);
         */
        /* ---------------- 接下来开始填充byteBuffer -------------- */
        // TODO: 首先填充transactionId 事务ID
        byteBuffer.putLong(transactionId);
        // TODO: 放置timeout 超时时间
        byteBuffer.putInt(timeout);
        // TODO: 如果byApplicationIdBytes 不为空
        if (null != byApplicationIdBytes) {
            // TODO: 先放 byApplicationIdBytes的size
            byteBuffer.putShort((short)byApplicationIdBytes.length);
            // TODO: 然后把实际的数据放进去
            byteBuffer.put(byApplicationIdBytes);
        } else {
            // TODO: 否则直接在size位置 放个0就好了，表示没有 ApplicationId
            byteBuffer.putShort((short)0);
        }
        // TODO: 下面逻辑都是一样的
        if (null != byServiceGroupBytes) {
            byteBuffer.putShort((short)byServiceGroupBytes.length);
            byteBuffer.put(byServiceGroupBytes);
        } else {
            byteBuffer.putShort((short)0);
        }
        if (null != byTxNameBytes) {
            byteBuffer.putShort((short)byTxNameBytes.length);
            byteBuffer.put(byTxNameBytes);
        } else {
            byteBuffer.putShort((short)0);
        }
        if (xidBytes != null) {
            byteBuffer.putInt(xidBytes.length);
            byteBuffer.put(xidBytes);
        } else {
            byteBuffer.putInt(0);
        }
        if (applicationDataBytes != null) {
            byteBuffer.putInt(applicationDataBytes.length);
            byteBuffer.put(applicationDataBytes);
        } else {
            byteBuffer.putInt(0);
        }
        // TODO: 放置beginTime
        byteBuffer.putLong(beginTime);
        // TODO: 放状态
        byteBuffer.put((byte)status.getCode());
        // TODO: 这一步不能忘 flip()一下
        byteBuffer.flip();
        byte[] result = new byte[byteBuffer.limit()];
        // TODO: 将byteBuffer中的值 放到 result中，然后返回
        byteBuffer.get(result);
        return result;
    }

    /**
     * TODO: 这地方处理很精细了
     * @param byApplicationIdBytes
     * @param byServiceGroupBytes
     * @param byTxNameBytes
     * @param xidBytes
     * @param applicationDataBytes
     * @return
     */
    private int calGlobalSessionSize(byte[] byApplicationIdBytes, byte[] byServiceGroupBytes, byte[] byTxNameBytes,
                                     byte[] xidBytes, byte[] applicationDataBytes) {
        final int size = 8 // transactionId
            + 4 // timeout
            + 2 // byApplicationIdBytes.length
            + 2 // byServiceGroupBytes.length
            + 2 // byTxNameBytes.length
            + 4 // xidBytes.length
            + 4 // applicationDataBytes.length
            + 8 // beginTime
            + 1 // statusCode
            + (byApplicationIdBytes == null ? 0 : byApplicationIdBytes.length)
            + (byServiceGroupBytes == null ? 0 : byServiceGroupBytes.length)
            + (byTxNameBytes == null ? 0 : byTxNameBytes.length)
            + (xidBytes == null ? 0 : xidBytes.length)
            + (applicationDataBytes == null ? 0 : applicationDataBytes.length);
        return size;
    }

    /**
     * 解码操作， 和编码操作是相反的
     * @param a
     */
    @Override
    public void decode(byte[] a) {
        // TODO: 包成ByteBuffer
        ByteBuffer byteBuffer = ByteBuffer.wrap(a);
        // TODO: 拿到transactionId
        this.transactionId = byteBuffer.getLong();
        // TODO: 直接获取超时时间
        this.timeout = byteBuffer.getInt();
        // TODO: 依次和编码操作相反，先拿长度，长度大于0，才表示有值嘛，然后get指定长度的值，用string包一下，成为字符串
        short applicationIdLen = byteBuffer.getShort();
        /* ---------------- 依次取出来，很有意思 -------------- */
        if (applicationIdLen > 0) {
            byte[] byApplicationId = new byte[applicationIdLen];
            byteBuffer.get(byApplicationId);
            this.applicationId = new String(byApplicationId);
        }
        short serviceGroupLen = byteBuffer.getShort();
        if (serviceGroupLen > 0) {
            byte[] byServiceGroup = new byte[serviceGroupLen];
            byteBuffer.get(byServiceGroup);
            this.transactionServiceGroup = new String(byServiceGroup);
        }
        short txNameLen = byteBuffer.getShort();
        if (txNameLen > 0) {
            byte[] byTxName = new byte[txNameLen];
            byteBuffer.get(byTxName);
            this.transactionName = new String(byTxName);
        }
        int xidLen = byteBuffer.getInt();
        if (xidLen > 0) {
            byte[] xidBytes = new byte[xidLen];
            byteBuffer.get(xidBytes);
            this.xid = new String(xidBytes);
        }
        int applicationDataLen = byteBuffer.getInt();
        if (applicationDataLen > 0) {
            byte[] applicationDataLenBytes = new byte[applicationDataLen];
            byteBuffer.get(applicationDataLenBytes);
            this.applicationData = new String(applicationDataLenBytes);
        }

        this.beginTime = byteBuffer.getLong();
        this.status = GlobalStatus.get(byteBuffer.get());
    }

    /**
     * Has branch boolean.
     *
     * @return the boolean
     */
    public boolean hasBranch() {
        return branchSessions.size() > 0;
    }

    /**
     * TODO: 其实内部用的ReentrantLock, 利用了锁超时，2秒 加不上锁，直接抛异常
     * @throws TransactionException
     */
    public void lock() throws TransactionException {
        globalSessionLock.lock();
    }

    public void unlock() {
        globalSessionLock.unlock();
    }

    /**
     * TODO: 全局事务锁
     */
    private static class GlobalSessionLock {

        /**
         * 内部持有了一份 ReentrantLock(); 委托给了ReentrantLock()
         */
        private Lock globalSessionLock = new ReentrantLock();

        /**
         * 默认 2 秒 超时锁，2秒锁失败，则抛出异常
         */
        private static final int GLOBAL_SESSION_LOCK_TIME_OUT_MILLS = 2 * 1000;

        public void lock() throws TransactionException {
            try {
                if (globalSessionLock.tryLock(GLOBAL_SESSION_LOCK_TIME_OUT_MILLS, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted error", e);
            }
            throw new GlobalTransactionException(TransactionExceptionCode.FailedLockGlobalTranscation, "Lock global session failed");
        }

        public void unlock() {
            globalSessionLock.unlock();
        }

    }

    @FunctionalInterface
    public interface LockRunnable {

        void run() throws TransactionException;
    }

    /**
     * 定义了一个 接口，用于执行加锁中间的模板代码
     * @param <V>
     */
    @FunctionalInterface
    public interface LockCallable<V> {

        V call() throws TransactionException;
    }

    public ArrayList<BranchSession> getBranchSessions() {
        return branchSessions;
    }

    public void asyncCommit() throws TransactionException {
        this.addSessionLifecycleListener(SessionHolder.getAsyncCommittingSessionManager());
        SessionHolder.getAsyncCommittingSessionManager().addGlobalSession(this);
        this.changeStatus(GlobalStatus.AsyncCommitting);
    }

    public void queueToRetryCommit() throws TransactionException {
        this.addSessionLifecycleListener(SessionHolder.getRetryCommittingSessionManager());
        SessionHolder.getRetryCommittingSessionManager().addGlobalSession(this);
        this.changeStatus(GlobalStatus.CommitRetrying);
    }

    public void queueToRetryRollback() throws TransactionException {
        this.addSessionLifecycleListener(SessionHolder.getRetryRollbackingSessionManager());
        SessionHolder.getRetryRollbackingSessionManager().addGlobalSession(this);
        GlobalStatus currentStatus = this.getStatus();
        if (SessionHelper.isTimeoutGlobalStatus(currentStatus)) {
            this.changeStatus(GlobalStatus.TimeoutRollbackRetrying);
        } else {
            this.changeStatus(GlobalStatus.RollbackRetrying);
        }
    }
}
