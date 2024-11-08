package com.alibaba.otter.canal.parse.inbound.mysql.rds;

import com.alibaba.otter.canal.parse.inbound.mysql.rds.data.BinlogFile;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

import static com.alibaba.otter.canal.parse.support.ParseHelper.getBinLogFileInfo;
import static com.alibaba.otter.canal.parse.support.ParseHelper.getLastItem;
import static com.alibaba.otter.canal.parse.support.ParseHelper.prepareLogMDC;

/**
 * @author chengjin.lyf on 2018/8/7 下午3:10
 * @since 1.0.25
 */
public class BinlogDownloadQueue {

    private static final Logger             logger        = LoggerFactory.getLogger(BinlogDownloadQueue.class);
    private static final int                TIMEOUT       = 10000;

    private LinkedBlockingQueue<BinlogFile> downloadQueue;
    private LinkedBlockingQueue<Runnable>   taskQueue    ;

    private LinkedList<BinlogFile>          binlogList;

    /**
     * 从binlogList copy一份完整的有序的binlog文件信息列表
     */
    private List<BinlogFile>                immutableBinlogList;

    private final int                       batchFileSize;
    private Thread                          downloadThread;
    private String                          destination;
    public boolean                          running       = true;
    private final String                    destDir;
    private String                          hostId;

    /**
     * @param initFile 最先从哪个文件开始下载
     */
    public BinlogDownloadQueue(List<BinlogFile> binlogFiles, BinlogFile initFile, int batchFileSize, String destDir, String destination)
            throws IOException {
        this.binlogList = new LinkedList(binlogFiles);
        this.batchFileSize = batchFileSize;
        this.destDir = destDir;
        // 方便追踪给定destination的下载日志信息
        this.destination = destination;

        downloadQueue = new LinkedBlockingQueue<>((int) (batchFileSize * 1.5f));
        taskQueue = new LinkedBlockingQueue<>(4);

        prepareBinlogList(initFile);
        cleanDir();
    }

    public void waitFileReady(BinlogFile ossBinlogFile, long maxWaitMills) throws TimeoutException, InterruptedException {
        String fileName = ossBinlogFile.getFileName();
        String filePath = destDir + File.separator + fileName;
        // 最大等待时长, 正常网络情况不会等这么久
        long timeoutTs = System.currentTimeMillis() + maxWaitMills;
        while (timeoutTs > System.currentTimeMillis()) {
            File waitFile = new File(filePath);
            if (waitFile.exists()) {
                // file is ready
                logger.info("binlog file downloading is ready! file: {}", waitFile);
                return;
            } else {
                logger.info("waiting binlog file ready, file: {}", waitFile);
                Thread.sleep(TimeUnit.SECONDS.toMillis(10));
            }
        }
        // timeout
        throw new TimeoutException(String.format("waitFile(%s) timeout, timeoutTs:%s", fileName, timeoutTs));
    }

    private void prepareBinlogList(BinlogFile initFile) {
        hostId = initFile.getHostInstanceID();

        // binlog按照文件ID升序排列
        this.binlogList.sort(Comparator.comparing(BinlogFile::getFileName));

        // 早于initFile的都抛弃掉
        while (!binlogList.isEmpty() && !binlogList.peek().getFileName().equals(initFile.getFileName())) {
            binlogList.poll();
        }

        this.immutableBinlogList = Collections.unmodifiableList(new ArrayList<>(binlogList));

        // 打印出binlogs
        List<String> binlogFiles = new ArrayList<>();
        for (BinlogFile binlog : this.binlogList) {
            binlogFiles.add(binlog.getFileName());
        }
        logger.info("prepareBinlogList: {}", binlogFiles);
    }

    public void cleanDir() throws IOException {
        logger.info("before start, clean dir: {}", destDir);

        File destDirFile = new File(destDir);
        FileUtils.forceMkdir(destDirFile);
        FileUtils.cleanDirectory(destDirFile);
    }

    public void silenceDownload() throws InterruptedException {
        if (downloadThread != null) {
            return;
        }

        downloadThread = new Thread(new DownloadThread(), "rdsBinLogDownload-" + destination);
        downloadThread.setDaemon(true);
        downloadThread.start();

        // 往下载任务队列放入待下载文件
        prepareMore();
    }

    public void notifyNotMatch() {
        filter(hostId);
    }

    private void filter(String hostInstanceId) {
        Iterator<BinlogFile> it = binlogList.iterator();
        while (it.hasNext()) {
            BinlogFile bf = it.next();
            if (bf.getHostInstanceID().equalsIgnoreCase(hostInstanceId)) {
                it.remove();
            } else {
                hostId = bf.getHostInstanceID();
            }
        }
    }

    public boolean isLastFile(String fileName) {
        String needCompareName = fileName;
        if (StringUtils.isNotEmpty(needCompareName) && StringUtils.endsWith(needCompareName, "tar")) {
            needCompareName = needCompareName.substring(0, needCompareName.lastIndexOf("."));
        }
        return getLastItem(immutableBinlogList).getFileName().equals(needCompareName) && binlogList.isEmpty();
    }

    /**
     * 往下载队列中放入待下载文件任务, 下载数量: batchFileSize
     */
    public void prepareMore() throws InterruptedException {
        int putCount = 0;
        while (putCount < batchFileSize && !binlogList.isEmpty()) {
            BinlogFile binlogFile = null;
            while (!binlogList.isEmpty()) {
                binlogFile = binlogList.poll();
                if (binlogFile.getHostInstanceID().equalsIgnoreCase(hostId)) {
                    // 上游已经过滤过，所以此处都是同一个hostId的binlog
                    break;
                }
            }

            if (binlogFile == null) {
                break;
            }

            this.downloadQueue.put(binlogFile);
            putCount += 1;
            logger.info("put into downloadQueue, binlogFile: {}", getBinLogFileInfo(binlogFile));
        }
    }

    public void release() {
        running = false;
        binlogList.clear();
        downloadQueue.clear();
        immutableBinlogList = null;
        try {
            logger.info("downloadThread destroy begin");
            downloadThread.interrupt();
            downloadThread.join(); // 等待其结束
            logger.info("downloadThread destroy done");
        } catch (InterruptedException e) {
            // ignore
        } finally {
            downloadThread = null;
        }
    }

    private void download(BinlogFile binlogFile) throws Throwable {
        String downloadLink = binlogFile.getDownloadLink();
        String fileName = binlogFile.getFileName();

        logger.info("begin download binlog, file: {}, destDir: {}, link: {}", fileName, destDir, downloadLink);

        downloadLink = downloadLink.trim();
        CloseableHttpClient httpClient = null;
        if (downloadLink.startsWith("https")) {
            HttpClientBuilder builder = HttpClientBuilder.create();
            builder.setMaxConnPerRoute(50);
            builder.setMaxConnTotal(100);
            // 创建支持忽略证书的https
            final SSLContext sslContext = new SSLContextBuilder()
                    .loadTrustMaterial(null, (x509Certificates, s) -> true)
                    .build();

            httpClient = HttpClientBuilder.create()
                .setSSLContext(sslContext)
                .setConnectionManager(new PoolingHttpClientConnectionManager(RegistryBuilder.<ConnectionSocketFactory> create()
                    .register("http", PlainConnectionSocketFactory.INSTANCE)
                    .register("https", new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE))
                    .build()))
                .build();
        } else {
            httpClient = HttpClientBuilder.create().setMaxConnPerRoute(50).setMaxConnTotal(100).build();
        }

        HttpGet httpGet = new HttpGet(downloadLink);
        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(TIMEOUT)
            .setConnectionRequestTimeout(TIMEOUT)
            .setSocketTimeout(TIMEOUT)
            .build();
        httpGet.setConfig(requestConfig);
        HttpResponse response = httpClient.execute(httpGet);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpResponseStatus.OK.code()) {
            throw new RuntimeException("download failed , url:" + downloadLink + " , statusCode:" + statusCode);
        }
        saveFile(new File(destDir), fileName, response);
    }

    private static void saveFile(File parentFile, String fileName, HttpResponse response) throws IOException {
        InputStream is = response.getEntity().getContent();
        boolean isChunked = response.getEntity().isChunked();
        Header contentLengthHeader = response.getFirstHeader("Content-Length");
        long totalSize = (isChunked || contentLengthHeader == null) ? 0 : Long.parseLong(contentLengthHeader.getValue());
        if (response.getFirstHeader("Content-Disposition") != null) {
            fileName = response.getFirstHeader("Content-Disposition").getValue();
            fileName = StringUtils.substringAfter(fileName, "filename=");
        }
        boolean isTar = StringUtils.endsWith(fileName, ".tar");
        FileUtils.forceMkdir(parentFile);
        FileOutputStream fos = null;
        try {
            if (isTar) {
                TarArchiveInputStream tais = new TarArchiveInputStream(is);
                TarArchiveEntry tarArchiveEntry = null;
                while ((tarArchiveEntry = tais.getNextTarEntry()) != null) {
                    String name = tarArchiveEntry.getName();
                    File tarFile = new File(parentFile, name + ".tmp");
                    logger.info("start to download file " + tarFile.getName());
                    if (tarFile.exists()) {
                        tarFile.delete();
                    }
                    BufferedOutputStream bos = null;
                    try {
                        bos = new BufferedOutputStream(new FileOutputStream(tarFile));
                        int read = -1;
                        byte[] buffer = new byte[1024];
                        while ((read = tais.read(buffer)) != -1) {
                            bos.write(buffer, 0, read);
                        }
                        logger.info("download file " + tarFile.getName() + " end!");
                        tarFile.renameTo(new File(parentFile, name));
                    } finally {
                        IOUtils.closeQuietly(bos);
                    }
                }
                tais.close();
            } else {
                File file = new File(parentFile, fileName + ".tmp");
                if (file.exists()) {
                    file.delete();
                }

                if (!file.isFile()) {
                    file.createNewFile();
                }
                try {
                    fos = new FileOutputStream(file);
                    byte[] buffer = new byte[1024];
                    int len;
                    long copySize = 0;
                    long nextPrintProgress = 0;
                    logger.info("start to download file " + file.getName());
                    while ((len = is.read(buffer)) != -1) {
                        fos.write(buffer, 0, len);
                        copySize += len;
                        if (totalSize > 0){
                            long progress = copySize * 100 / totalSize;
                            if (progress >= nextPrintProgress) {
                                logger.info("download " + file.getName() + " progress : " + progress
                                        + "% , download size : " + copySize + ", total size : " + totalSize);
                                nextPrintProgress += 10;
                            }
                        }
                    }
                    logger.info("download file " + file.getName() + " end!");
                    fos.flush();
                } finally {
                    IOUtils.closeQuietly(fos);
                }
                file.renameTo(new File(parentFile, fileName));
            }
        } finally {
            IOUtils.closeQuietly(fos);
        }
    }

    public void execute(Runnable runnable) throws InterruptedException {
        taskQueue.put(runnable);
    }

    private class DownloadThread implements Runnable {

        @Override
        public void run() {
            prepareLogMDC(destination);
            while (running) {
                BinlogFile binlogFile = null;
                try {
                    // 在死循环式while + trycatch的组合中，如果要控制sleep频率，最好写在trycatch一开始，否则写在trycatch最后，则可能
                    // 因为中间出现问题，永远执行不到sleep而导致真的死循环式执行
                    Thread.sleep(2000);
                    binlogFile = downloadQueue.poll();
                    if (binlogFile != null) {
                        int retry = 0;
                        while (retry < 100) {
                            try {
                                download(binlogFile);
                                break;
                            } catch (Throwable e) {
                                retry = retry + 1;
                                if (retry % 10 == 0) {
                                    try {
                                        logger.warn("download failed + " + binlogFile.toString() + "], retry : "
                                                    + retry, e);
                                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100 * retry));
                                    } catch (Throwable e1) {
                                        logger.error("download wait exception", e1);
                                    }
                                }
                            }
                        }
                    }

                    Runnable runnable = taskQueue.poll(5000, TimeUnit.MILLISECONDS);
                    if (runnable != null) {
                        runnable.run();
                    }
                } catch (Throwable e) {
                    logger.error("task process failed, {}", e.getMessage(), e);
                }
            }
        }
    }

}
