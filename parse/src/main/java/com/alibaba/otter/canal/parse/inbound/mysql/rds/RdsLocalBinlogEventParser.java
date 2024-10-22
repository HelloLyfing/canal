package com.alibaba.otter.canal.parse.inbound.mysql.rds;

import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.support.ParseHelper;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.exception.PositionNotFoundException;
import com.alibaba.otter.canal.parse.exception.ServerIdNotMatchException;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.LocalBinLogConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.LocalBinlogEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.data.BinlogFile;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.alibaba.otter.canal.parse.support.ParseHelper.getBinLogFileInfo;
import static com.alibaba.otter.canal.parse.support.ParseHelper.getFirstItem;
import static com.alibaba.otter.canal.parse.support.ParseHelper.getLastItem;

/**
 * 基于rds binlog备份文件的复制
 *
 * @author agapple 2017年10月15日 下午1:27:36
 * @since 1.0.25
 * @author luyu 2024-10-18，适配RDS-OSS-Binlog
 */
public class RdsLocalBinlogEventParser extends LocalBinlogEventParser implements CanalEventParser, LocalBinLogConnection.FileParserListener {

    private String              url;                // openapi地址
    private String              accesskey;          // 云账号的ak
    private String              secretkey;          // 云账号sk
    private String              instanceId;         // rds实例id
    private Long                startTime;
    private Long                endTime;
    private BinlogDownloadQueue binlogDownloadQueue;
    private ParseFinishListener finishListener;
    private int                 batchFileSize;

    public RdsLocalBinlogEventParser(){
    }

    @Override
    public void start() throws CanalParseException {
        prepareLogMDC();
        try {
            Assert.notNull(accesskey);
            Assert.notNull(secretkey);
            Assert.notNull(instanceId);
            Assert.notNull(url);
            Assert.notNull(directory);

            if (endTime == null) {
                endTime = System.currentTimeMillis();
            }

            /**
             * 配置形式1）消费位点中，需要包含的信息项: journalName(必须), position(必须，如果不知道填4即可); timestamp(可选), serverId(可选)
             * 由于阿里云的oss同时存储了主备多份binlog信息，如果单纯依靠时间戳，无法具体判断要使用哪个serverId的binlog文件;
             * 而且, 如果没有journalName，仅有timestamp，则此处会直接报错: xx.mysql.MysqlConnection#dump(long, com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor)，所以需要包含以上二个必选信息
             *
             * 配置形式2）TODO: 消费位点中包含 serverId + timestamp其实也可以支持自动寻找合适的binlog文件，不过由于阿里云oss到底存储了多长时间内的
             * rds文件并不确定，通过这个配置进行消息回溯，有可能因为回溯时间和节点不确定导致意外的结果. 所以从精确性和健壮性的角度来讲，还是用配置形式1确定性更好
             */
            EntryPosition entryPosition = findStartPosition(null);
            if (entryPosition == null) {
                throw new PositionNotFoundException("position not found!");
            }
            Long startTimeInMill = entryPosition.getTimestamp();
            if (Objects.isNull(startTimeInMill)) {
                // 搜索范围大一些，后续会通过journalName进行二次筛选
                startTimeInMill = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(10);
            }

            // 稍微获取更早一点的，以便获取更全面的binlog
            startTime = startTimeInMill - TimeUnit.SECONDS.toMillis(30);
            List<BinlogFile> binlogFiles = RdsBinlogOpenApi.listBinlogFiles(url,
                accesskey,
                secretkey,
                instanceId,
                new Date(startTime),
                new Date(endTime));
            if (binlogFiles.isEmpty()) {
                throw new CanalParseException("start timestamp : " + startTimeInMill + " binlog files is empty");
            }
            ParseHelper.formatBinlogFiles(binlogFiles);

            logger.info("fetch binlogFilesInfo from oss, binlogFilesSize: {}, firstFileInfo: {}, lastFileInfo: {}",
                    binlogFiles.size(), getBinLogFileInfo(getFirstItem(binlogFiles)), getBinLogFileInfo(getLastItem(binlogFiles)));

            // 通过消费位点中的配置项，精确获取第一个需要消费的文件及点位，是后续解析的基础
            BinlogFile ossBinlogFile = findOssBinlogFileByEntryPosition(entryPosition, binlogFiles);
            if (Objects.isNull(ossBinlogFile)) {
                throw new CanalParseException("entry position not in binlogFiles, entryPosition:" + entryPosition);
            }

            /**
             * 阿里云的主备binlog都存储在一个空间内，此处需要过滤一下EntryPosition中的binlog对应实例下的所有binlogs
             */
            binlogFiles = filterSameServerIdFiles(ossBinlogFile, binlogFiles);
            logger.info("filter binlogFiles, binlogFilesSize: {}, firstFileInfo: {}, lastFileInfo: {}", binlogFiles.size(),
                    getBinLogFileInfo(getFirstItem(binlogFiles)), getBinLogFileInfo(getLastItem(binlogFiles)));

            if (CollectionUtils.isEmpty(binlogFiles)) {
                throw new CanalParseException("entry position not in binlogFiles, entryPosition:" + entryPosition);
            }

            needWait = true;
            binlogDownloadQueue = new BinlogDownloadQueue(binlogFiles, ossBinlogFile, batchFileSize, directory, destination);
            binlogDownloadQueue.silenceDownload();
            // wait first binlog file ready
            binlogDownloadQueue.waitFileReady(ossBinlogFile, getMaxWaitMills());
        } catch (Throwable e) {
            logger.error("download binlog failed", e);
            throw new CanalParseException(e);
        }

        setParserExceptionHandler(this::handleMysqlParserException);
        super.start();
    }

    protected List<BinlogFile> filterSameServerIdFiles(BinlogFile ossBinlogFile, List<BinlogFile> binlogFiles) {
        List<BinlogFile> newList = new ArrayList<>();
        for (BinlogFile binlogFile : binlogFiles) {
            if (binlogFile.getHostInstanceID().equals(ossBinlogFile.getHostInstanceID())) {
                newList.add(binlogFile);
            }
        }
        return newList;
    }

    private BinlogFile findOssBinlogFileByEntryPosition(EntryPosition entryPosition, List<BinlogFile> binlogFiles) {
        for (BinlogFile binlogFile : binlogFiles) {
            if (binlogFile.getFileName().equals(entryPosition.getJournalName())) {
                logger.info("found binlog file from oss: {}", getBinLogFileInfo(binlogFile));
                return binlogFile;
            }
        }
        return null;
    }

    private void handleMysqlParserException(Throwable throwable) {
        // 如果上游已经指定了initBinlogFile，并根据该文件过滤了同一serverId下的binlogFiles，则应该不会出现以下错误
        if (throwable instanceof ServerIdNotMatchException) {
            logger.error("server id not match, try download another rds binlog!");
            binlogDownloadQueue.notifyNotMatch();
            try {
                binlogDownloadQueue.cleanDir();
                binlogDownloadQueue.prepareMore();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }

            try {
                binlogDownloadQueue.execute(() -> {
                    RdsLocalBinlogEventParser.super.stop();
                    RdsLocalBinlogEventParser.super.start();
                });
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
    }

    @Override
    protected ErosaConnection buildErosaConnection() {
        ErosaConnection connection = super.buildErosaConnection();
        if (connection instanceof LocalBinLogConnection) {
            LocalBinLogConnection localBinLogConnection = (LocalBinLogConnection) connection;
            localBinLogConnection.setNeedWait(true);
            localBinLogConnection.setServerId(serverId);
            localBinLogConnection.setParserListener(this);
            localBinLogConnection.setRdsOssMode(true);
            localBinLogConnection.setMaxWaitMills(getMaxWaitMills());
        }
        return connection;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        if (StringUtils.isNotEmpty(url)) {
            this.url = url;
        }
    }

    public void setAccesskey(String accesskey) {
        this.accesskey = accesskey;
    }

    public void setSecretkey(String secretkey) {
        this.secretkey = secretkey;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    /**
     * on one binlog file parse finish
     * @return true: should continue next file parse; false: should not continue
     */
    @Override
    public boolean onFinish(String fileName) {
        try {
            File needDeleteFile = new File(directory + File.separator + fileName);
            if (needDeleteFile.exists()) {
                needDeleteFile.delete();
                logger.info("delete local binlog file done, {}", needDeleteFile);
            }

            // 同步logManager位点
            LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);
            Long timestamp = 0L;
            if (logPosition != null && logPosition.getPostion() != null) {
                // 当前文件解析消费完成后，需要将位点置为下一个文件的开始位置
                timestamp = logPosition.getPostion().getTimestamp();
                EntryPosition position = logPosition.getPostion();
                LogPosition newLogPosition = new LogPosition();
                String newJournalName = getNextJournalNameByIncr(position);

                newLogPosition.setPostion(new EntryPosition(newJournalName, 4L, position.getTimestamp(), position.getServerId()));
                newLogPosition.setIdentity(logPosition.getIdentity());
                logPositionManager.persistLogPosition(destination, newLogPosition);
                logger.info("update persistLogPosition, {}, {}", destination, newLogPosition);
            }

            if (binlogDownloadQueue.isLastFile(fileName)) {
                logger.warn("last file: {}, timestamp: {}, all local file parse complete, switch to mysql parser!", fileName, timestamp);
                finishListener.onFinish();
                return false;
            }

            logger.warn("parse local binlog file : " + fileName + " , timestamp : " + timestamp
                        + " , try the next binlog !");
            binlogDownloadQueue.prepareMore();
        } catch (Exception e) {
            logger.error("prepare download binlog file failed!", e);
            throw new RuntimeException(e);
        }

        return true;
    }

    protected String getNextJournalNameByIncr(EntryPosition position) {
        String prefix = "mysql-bin.";
        String journalName = position.getJournalName();
        String oldNum = journalName.replace(prefix, "");
        Integer nextNum = Integer.parseInt(oldNum) + 1;
        String nextNumStr = String.valueOf(nextNum);
        while (nextNumStr.length() < oldNum.length()) {
            nextNumStr = "0" + nextNumStr;
        }
        return prefix + nextNumStr;
    }

    @Override
    public void stop() {
        this.binlogDownloadQueue.release();
        super.stop();
    }

    public void setFinishListener(ParseFinishListener finishListener) {
        this.finishListener = finishListener;
    }

    public interface ParseFinishListener {
        void onFinish();
    }

    public void setBatchFileSize(int batchFileSize) {
        this.batchFileSize = batchFileSize;
    }

}
